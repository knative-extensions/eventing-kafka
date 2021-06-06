/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

var newConsumerGroup = sarama.NewConsumerGroup

// KafkaConsumerGroupFactory creates the ConsumerGroup and start consuming the specified topic
// The manager parameter is optional and may be nil if pause/resume featues are not required
type KafkaConsumerGroupFactory interface {
	StartConsumerGroup(manager KafkaConsumerGroupManager, groupID string, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error)
}

type kafkaConsumerGroupFactoryImpl struct {
	config *sarama.Config
	addrs  []string
}

type customConsumerGroup struct {
	cancel              func()
	handlerErrorChannel chan error
	sarama.ConsumerGroup
	releasedCh chan bool
}

// Merge handler errors chan and consumer group error chan
func (c *customConsumerGroup) Errors() <-chan error {
	return mergeErrorChannels(c.ConsumerGroup.Errors(), c.handlerErrorChannel)
}

func (c *customConsumerGroup) Close() error {
	c.cancel()

	// Wait for graceful session claims release
	<-c.releasedCh

	return c.ConsumerGroup.Close()
}

var _ sarama.ConsumerGroup = (*customConsumerGroup)(nil)

func (c kafkaConsumerGroupFactoryImpl) StartConsumerGroup(
	manager KafkaConsumerGroupManager,
	groupID string,
	topics []string,
	logger *zap.SugaredLogger,
	handler KafkaConsumerHandler,
	options ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {

	// Use the KafkaConsumerGroupManager to create the consumer group, if provided
	var consumerGroup sarama.ConsumerGroup
	var err error
	if manager != nil {
		consumerGroup, err = manager.CreateConsumerGroup(newConsumerGroup, c.addrs, groupID, c.config)
	} else {
		consumerGroup, err = newConsumerGroup(c.addrs, groupID, c.config)
	}
	if err != nil {
		return nil, err
	}

	errorCh := make(chan error, 10)
	releasedCh := make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer func() {
			close(errorCh)
			releasedCh <- true
		}()
		for {
			consumerHandler := NewConsumerHandler(logger, handler, errorCh, options...)

			var consumeErr error
			if manager == nil {
				consumeErr = consumerGroup.Consume(ctx, topics, &consumerHandler)
			} else {
				consumeErr = manager.Consume(groupID, ctx, topics, &consumerHandler)
			}
			if consumeErr == sarama.ErrClosedConsumerGroup {
				return
			} else if consumeErr == ErrStoppedConsumerGroup {
				// This error can only be returned if the manager is non-nil
				fmt.Printf("EDV:  Group '%s' is stopped; waiting for start\n", groupID)
				// Wait for the group corresponding to this Group ID to be recreated (This could take an
				// indefinite amount of time as the pause/resume commands are not necessarily related,
				// though in the case of a resetoffset command, there shouldn't be much time between
				// the two signals)
				waitErr := manager.WaitForStart(groupID, 60*time.Minute)
				if waitErr != nil {
					logger.Error("ConsumerGroup Failed To Restart", zap.Error(waitErr))
				}
			} else {
				return
			}
			if consumeErr != nil {
				errorCh <- consumeErr
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	return &customConsumerGroup{cancel, errorCh, consumerGroup, releasedCh}, err
}

func NewConsumerGroupFactory(addrs []string, config *sarama.Config) KafkaConsumerGroupFactory {
	return kafkaConsumerGroupFactoryImpl{addrs: addrs, config: config}
}

var _ KafkaConsumerGroupFactory = (*kafkaConsumerGroupFactoryImpl)(nil)

func mergeErrorChannels(channels ...<-chan error) <-chan error {
	out := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(channels))
	for _, channel := range channels {
		go func(c <-chan error) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(channel)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
