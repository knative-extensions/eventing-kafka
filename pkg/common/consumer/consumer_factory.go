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
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/logging"
)

// wrapper functions for the Sarama functions, to facilitate unit testing
var newConsumerGroup = sarama.NewConsumerGroup

// consumeFunc is a function type that matches the Sarama ConsumerGroup's Consume function
type consumeFunc func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error

// KafkaConsumerGroupFactory creates the ConsumerGroup and start consuming the specified topic
type KafkaConsumerGroupFactory interface {
	StartConsumerGroup(ctx context.Context, groupID string, topics []string, handler KafkaConsumerHandler, channelRef types.NamespacedName, options ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error)
}

type kafkaConsumerGroupFactoryImpl struct {
	config         *sarama.Config
	addrs          []string
	offsetsChecker ConsumerGroupOffsetsChecker
	enqueue        func(ref types.NamespacedName)
}

type customConsumerGroup struct {
	cancel              func()
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

var _ sarama.ConsumerGroup = (*customConsumerGroup)(nil)

// StartConsumerGroup creates a new customConsumerGroup and starts a Consume goroutine on it
func (c kafkaConsumerGroupFactoryImpl) StartConsumerGroup(ctx context.Context, groupID string, topics []string, handler KafkaConsumerHandler, channelRef types.NamespacedName, options ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	logger := logging.FromContext(ctx)

	consumerGroup, err := c.createConsumerGroup(groupID)
	if err != nil {
		logger.Errorw("unable to create consumer group", zap.String("groupId", groupID), zap.Error(err))
		return nil, err
	}

	// Start the consumerGroup.Consume function in a separate goroutine
	return c.startExistingConsumerGroup(groupID, consumerGroup, consumerGroup.Consume, topics, logger, handler, channelRef, options...), nil
}

// createConsumerGroup creates a Sarama ConsumerGroup using the newConsumerGroup wrapper, with the
// factory's internal brokers and sarama config.
func (c kafkaConsumerGroupFactoryImpl) createConsumerGroup(groupID string) (sarama.ConsumerGroup, error) {
	return newConsumerGroup(c.addrs, groupID, c.config)
}

// startExistingConsumerGroup creates a goroutine that begins a custom Consume loop on the provided ConsumerGroup
// This loop is cancelable via the function provided in the returned customConsumerGroup.
func (c kafkaConsumerGroupFactoryImpl) startExistingConsumerGroup(
	groupID string,
	saramaGroup sarama.ConsumerGroup,
	consume consumeFunc,
	topics []string,
	logger *zap.SugaredLogger,
	handler KafkaConsumerHandler,
	channelRef types.NamespacedName,
	options ...SaramaConsumerHandlerOption) *customConsumerGroup {

	errorCh := make(chan error, 10)
	releasedCh := make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())
	groupLogger := logger.With(zap.Any("topics", topics), zap.String("groupId", groupID), zap.String("channel", channelRef.String()))

	go func() {
		// this is a blocking func
		// do not proceed until the check is done
		err := c.offsetsChecker.WaitForOffsetsInitialization(ctx, groupID, topics, logger, c.addrs, c.config)
		if err != nil {
			groupLogger.Errorw("error while checking if offsets are initialized", zap.Error(err))
			errorCh <- err
			c.enqueue(channelRef)
			return
		}

		groupLogger.Debugw("all offsets are initialized")

		defer func() {
			close(errorCh)
			releasedCh <- true
		}()
		for {
			consumerHandler := NewConsumerHandler(logger, handler, errorCh, options...)

			err = consume(ctx, topics, &consumerHandler)
			if err == sarama.ErrClosedConsumerGroup {
				groupLogger.Infow("consumer group was closed", zap.Error(err))
				return
			}
			if err != nil {
				errorCh <- err
			}

			select {
			case <-ctx.Done():
				groupLogger.Info("consumer group terminated gracefully")
				return
			default:
			}
		}
	}()
	return &customConsumerGroup{cancel, errorCh, saramaGroup, releasedCh}
}

func NewConsumerGroupFactory(addrs []string, config *sarama.Config, offsetsChecker ConsumerGroupOffsetsChecker, enqueue func(ref types.NamespacedName)) KafkaConsumerGroupFactory {
	return kafkaConsumerGroupFactoryImpl{addrs: addrs, config: config, offsetsChecker: offsetsChecker, enqueue: enqueue}
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
