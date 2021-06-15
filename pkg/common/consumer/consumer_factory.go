/*
Copyright 2019 The Knative Authors

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
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

var newConsumerGroup = sarama.NewConsumerGroup

// KafkaConsumerGroupFactory creates the ConsumerGroup and start consuming the specified topic
type KafkaConsumerGroupFactory interface {
	StartConsumerGroup(groupID string, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error)
}

type kafkaConsumerGroupFactoryImpl struct {
	config *sarama.Config
	addrs  []string
}

type customConsumerGroup struct {
	cancel              func()
	consume             func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error
	handlerErrorChannel chan error
	sarama.ConsumerGroup
	releasedCh chan bool
}

// Errors merges handler errors chan and consumer group error chan
func (c *customConsumerGroup) Errors() <-chan error {
	return mergeErrorChannels(c.ConsumerGroup.Errors(), c.handlerErrorChannel)
}

func (c *customConsumerGroup) Close() error {
	c.cancel()

	// Wait for graceful session claims release
	<-c.releasedCh

	return c.ConsumerGroup.Close()
}

func (c *customConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	return c.consume(ctx, topics, handler)
}

var _ sarama.ConsumerGroup = (*customConsumerGroup)(nil)

// StartConsumerGroup creates a new customConsumerGroup and starts a Consume goroutine on it
func (c kafkaConsumerGroupFactoryImpl) StartConsumerGroup(groupID string, topics []string, logger *zap.SugaredLogger, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	ctx, consumerGroup, err := c.createConsumerGroup(context.Background(), groupID)
	if err != nil {
		return nil, err
	}
	c.startExistingConsumerGroup(ctx, consumerGroup, topics, logger, handler, options...)
	return consumerGroup, nil

}

// createConsumerGroup creates a Sarama ConsumerGroup and adds status/error channels to it, returning
// a customConsumerGroup wrapper around the Sarama group.
func (c kafkaConsumerGroupFactoryImpl) createConsumerGroup(ctx context.Context, groupID string) (context.Context, *customConsumerGroup, error) {
	consumerGroup, err := newConsumerGroup(c.addrs, groupID, c.config)
	if err != nil {
		return ctx, nil, err
	}
	errorCh := make(chan error, 10)
	releasedCh := make(chan bool)
	ctx, cancel := context.WithCancel(ctx)
	return ctx, &customConsumerGroup{cancel, consumerGroup.Consume, errorCh, consumerGroup, releasedCh}, nil
}

// startExistingConsumerGroup creates a goroutine that begin a Consume loop on the provided customConsumerGroup
// It is cancelable via the Done channel in the provided context
func (c kafkaConsumerGroupFactoryImpl) startExistingConsumerGroup(ctx context.Context,
	consumerGroup *customConsumerGroup,
	topics []string,
	logger *zap.SugaredLogger,
	handler KafkaConsumerHandler,
	options ...SaramaConsumerHandlerOption) {

	go func() {
		defer func() {
			close(consumerGroup.handlerErrorChannel)
			consumerGroup.releasedCh <- true
		}()
		for {
			consumerHandler := NewConsumerHandler(logger, handler, consumerGroup.handlerErrorChannel, options...)

			consumeErr := consumerGroup.Consume(ctx, topics, &consumerHandler)
			if consumeErr == sarama.ErrClosedConsumerGroup {
				return
			}
			if consumeErr != nil {
				consumerGroup.handlerErrorChannel <- consumeErr
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

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
