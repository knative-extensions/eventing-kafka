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
	"errors"
	"sync"
	"testing"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

//------ Mocks

type mockConsumerGroup struct {
	mockInputMessageCh             chan *sarama.ConsumerMessage
	mustGenerateConsumerGroupError bool
	mustGenerateHandlerError       bool
	consumeMustReturnError         bool
	generateErrorOnce              sync.Once
}

func (m *mockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	if m.mustGenerateHandlerError {
		go func() {
			m.generateErrorOnce.Do(func() {
				h := handler.(*SaramaConsumerHandler)
				h.errors <- errors.New("consumer group handler error")
				// Don't close h.errors here; the deferred shutdown in startExistingConsumerGroup already does that
			})
		}()
	}
	if m.consumeMustReturnError {
		return errors.New("consume error")
	}
	return nil
}

func (m *mockConsumerGroup) Errors() <-chan error {
	ch := make(chan error)
	go func() {
		if m.mustGenerateConsumerGroupError {
			ch <- errors.New("consumer group error")
		}
		close(ch)
	}()
	return ch
}

func (m *mockConsumerGroup) Close() error {
	return nil
}

func mockedNewConsumerGroupFromClient(mockInputMessageCh chan *sarama.ConsumerMessage, mustGenerateConsumerGroupError bool, mustGenerateHandlerError bool, consumeMustReturnError bool, mustFail bool) func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	if !mustFail {
		return func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
			return &mockConsumerGroup{
				mockInputMessageCh:             mockInputMessageCh,
				mustGenerateConsumerGroupError: mustGenerateConsumerGroupError,
				mustGenerateHandlerError:       mustGenerateHandlerError,
				consumeMustReturnError:         consumeMustReturnError,
				generateErrorOnce:              sync.Once{},
			}, nil
		}
	} else {
		return func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
			return nil, errors.New("failed")
		}
	}
}

//------ Tests

func TestErrorPropagationCustomConsumerGroup(t *testing.T) {

	newConsumerGroup = mockedNewConsumerGroupFromClient(nil, true, true, false, false)

	factory := kafkaConsumerGroupFactoryImpl{
		config: sarama.NewConfig(),
		addrs:  []string{"b1", "b2"},
	}

	consumerGroup, err := factory.StartConsumerGroup("bla", []string{}, zap.NewNop().Sugar(), nil)
	if err != nil {
		t.Errorf("Should not throw error %v", err)
	}

	errorsSlice := make([]error, 0)

	errorsWait := sync.WaitGroup{}
	errorsWait.Add(2)
	go func() {
		for e := range consumerGroup.Errors() {
			errorsSlice = append(errorsSlice, e)
			errorsWait.Done() // Should be called twice, once for the ConsumerGroup error and once for the Handler error
		}
	}()

	// Wait for the mock to send the errors
	errorsWait.Wait()
	consumerGroup.(*customConsumerGroup).cancel() // Stop the consume loop and close the error channel

	if len(errorsSlice) != 2 {
		t.Errorf("len(errorsSlice) != 2")
	}

	// Wait for the goroutine inside of startExistingConsumerGroup to finish
	<-consumerGroup.(*customConsumerGroup).releasedCh

	assertContainsError(t, errorsSlice, "consumer group handler error")
	assertContainsError(t, errorsSlice, "consumer group error")
}

func assertContainsError(t *testing.T, collection []error, errorStr string) {
	for _, el := range collection {
		if el.Error() == errorStr {
			return
		}
	}
	t.Errorf("Cannot find error %v in error collection %v", errorStr, collection)
}

func TestErrorWhileCreatingNewConsumerGroup(t *testing.T) {

	newConsumerGroup = mockedNewConsumerGroupFromClient(nil, true, true, false, true)

	factory := kafkaConsumerGroupFactoryImpl{
		config: sarama.NewConfig(),
		addrs:  []string{"b1", "b2"},
	}
	_, err := factory.StartConsumerGroup("bla", []string{}, zap.L().Sugar(), nil)

	if err == nil || err.Error() != "failed" {
		t.Errorf("Should contain an error with message failed. Got %v", err)
	}
}

func TestErrorWhileNewConsumerGroup(t *testing.T) {

	newConsumerGroup = mockedNewConsumerGroupFromClient(nil, false, false, true, false)

	factory := kafkaConsumerGroupFactoryImpl{
		config: sarama.NewConfig(),
		addrs:  []string{"b1", "b2"},
	}
	consumerGroup, _ := factory.StartConsumerGroup("bla", []string{}, zap.L().Sugar(), nil)

	consumerGroup.(*customConsumerGroup).cancel() // Stop the consume loop from spinning after the error is generated
	err := <-consumerGroup.Errors()
	// Wait for the goroutine inside of startExistingConsumerGroup to finish
	<-consumerGroup.(*customConsumerGroup).releasedCh

	if err == nil || err.Error() != "consume error" {
		t.Errorf("Should contain an error with message consume error. Got %v", err)
	}
}
