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
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"
)

// wrapper functions for the Sarama functions, to facilitate unit testing
var newConsumerGroup = sarama.NewConsumerGroup

// consumeFunc is a function type that matches the Sarama ConsumerGroup's Consume function
type consumeFunc func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error

// KafkaConsumerGroupFactory creates the ConsumerGroup and start consuming the specified topic
type KafkaConsumerGroupFactory interface {
	StartConsumerGroup(ctx context.Context, channelRef types.NamespacedName, subID types.UID, groupID string, topics []string, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error)
}

type kafkaConsumerGroupFactoryImpl struct {
	config         *sarama.Config
	addrs          []string
	offsetsChecker ConsumerGroupOffsetsChecker
	kafkaclientset kafkaclientset.Interface
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
func (c kafkaConsumerGroupFactoryImpl) StartConsumerGroup(ctx context.Context, channelRef types.NamespacedName, subID types.UID, groupID string, topics []string, handler KafkaConsumerHandler, options ...SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	logger := logging.FromContext(ctx)

	consumerGroup, err := c.createConsumerGroup(groupID)
	if err != nil {
		logger.Errorw("unable to create consumer group", zap.String("groupId", groupID), zap.Error(err))
		return nil, err
	}

	// Start the consumerGroup.Consume function in a separate goroutine
	return c.startExistingConsumerGroup(ctx, channelRef, subID, groupID, consumerGroup, consumerGroup.Consume, topics, logger, handler, options...), nil
}

// createConsumerGroup creates a Sarama ConsumerGroup using the newConsumerGroup wrapper, with the
// factory's internal brokers and sarama config.
func (c kafkaConsumerGroupFactoryImpl) createConsumerGroup(groupID string) (sarama.ConsumerGroup, error) {
	return newConsumerGroup(c.addrs, groupID, c.config)
}

// startExistingConsumerGroup creates a goroutine that begins a custom Consume loop on the provided ConsumerGroup
// This loop is cancelable via the function provided in the returned customConsumerGroup.
func (c kafkaConsumerGroupFactoryImpl) startExistingConsumerGroup(
	ctx context.Context,
	channelRef types.NamespacedName,
	subID types.UID,
	groupID string,
	saramaGroup sarama.ConsumerGroup,
	consume consumeFunc,
	topics []string,
	logger *zap.SugaredLogger,
	handler KafkaConsumerHandler,
	options ...SaramaConsumerHandlerOption) *customConsumerGroup {

	errorCh := make(chan error, 10)
	releasedCh := make(chan bool)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		logger.Infow("not Getting kafka client")

		check := func() (bool, error) {
			logger.Infow("Running the check")
			channel, err := c.kafkaclientset.MessagingV1beta1().KafkaChannels(channelRef.Namespace).Get(ctx, channelRef.Name, corev1.GetOptions{})
			if err != nil {
				return false, fmt.Errorf("error checking if offsets are initialized. stopping trying. %w", err)
			}

			return isSubscriberReady(channel.Status.Subscribers, subID), nil
		}

		pollCtx, pollCtxCancel := context.WithTimeout(ctx, OffsetCheckRetryTimeout)
		err := wait.PollUntil(OffsetCheckRetryInterval, check, pollCtx.Done())
		defer pollCtxCancel()

		logger.Infow("Check done. Any errors?", zap.Any("anyErrors?", err != nil), zap.Error(err))

		//// this is a blocking func
		//// do not proceed until the check is done
		//err := c.offsetsChecker.WaitForOffsetsInitialization(ctx, groupID, topics, logger, c.addrs, c.config)
		if err != nil {
			logger.Errorw("error while checking if subscription is ready", zap.Any("topics", topics), zap.String("groupId", groupID), zap.Error(err))
			errorCh <- err
		}

		logger.Infow("all offsets are initialized", zap.Any("topics", topics), zap.Any("groupID", groupID))

		defer func() {
			close(errorCh)
			releasedCh <- true
		}()
		for {
			consumerHandler := NewConsumerHandler(logger, handler, errorCh, options...)

			err := consume(ctx, topics, &consumerHandler)
			if err == sarama.ErrClosedConsumerGroup {
				return
			}
			if err != nil {
				errorCh <- err
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	return &customConsumerGroup{cancel, errorCh, saramaGroup, releasedCh}
}

func isSubscriberReady(subscribers []eventingduck.SubscriberStatus, subID types.UID) bool {
	for _, sub := range subscribers {
		if sub.UID == subID {
			// TODO: fucking corev1.ConditionTrue
			return sub.Ready == "True"
		}
	}
	return false
}

func NewConsumerGroupFactory(addrs []string, config *sarama.Config, kafkaclient kafkaclientset.Interface, offsetsChecker ConsumerGroupOffsetsChecker) KafkaConsumerGroupFactory {
	return kafkaConsumerGroupFactoryImpl{addrs: addrs, config: config, kafkaclientset: kafkaclient, offsetsChecker: offsetsChecker}
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
