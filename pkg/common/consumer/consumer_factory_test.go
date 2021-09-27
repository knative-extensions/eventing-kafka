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
	controllertesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/controller/testing"
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

func mockedNewSaramaClient(client *controllertesting.MockClient, mustFail bool) func(addrs []string, config *sarama.Config) (sarama.Client, error) {
	if !mustFail {
		return func(addrs []string, config *sarama.Config) (sarama.Client, error) {
			return client, nil
		}
	} else {
		return func(addrs []string, config *sarama.Config) (sarama.Client, error) {
			return nil, errors.New("failed")
		}
	}
}

func mockedNewSaramaClusterAdmin(clusterAdmin sarama.ClusterAdmin, mustFail bool) func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
	if !mustFail {
		return func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
			return clusterAdmin, nil
		}
	} else {
		return func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
			return nil, errors.New("failed")
		}
	}
}

//------ Tests

type mockConsumerOffsetInitializer struct {
}

func (m mockConsumerOffsetInitializer) checkOffsetsInitialized(ctx context.Context, groupID string, topics []string, logger *zap.SugaredLogger, client sarama.Client, clusterAdmin sarama.ClusterAdmin) error {
	return nil
}

func TestErrorPropagationCustomConsumerGroup(t *testing.T) {
	ctx := context.TODO()
	client := controllertesting.NewMockClient(
		controllertesting.WithClientMockClosed(false),
		controllertesting.WithClientMockClose(nil))
	clusterAdmin := &mockClusterAdmin{}

	// override some functions
	newConsumerGroup = mockedNewConsumerGroupFromClient(nil, true, true, false, false)
	newSaramaClient = mockedNewSaramaClient(client, false)
	newSaramaClusterAdmin = mockedNewSaramaClusterAdmin(clusterAdmin, false)

	factory := kafkaConsumerGroupFactoryImpl{
		config: sarama.NewConfig(),
		addrs:  []string{"b1", "b2"},
		kcoi:   &mockConsumerOffsetInitializer{},
	}

	consumerGroup, err := factory.StartConsumerGroup(ctx, "bla", []string{}, nil)
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
	ctx := context.TODO()
	newConsumerGroup = mockedNewConsumerGroupFromClient(nil, true, true, false, true)

	factory := kafkaConsumerGroupFactoryImpl{
		config: sarama.NewConfig(),
		addrs:  []string{"b1", "b2"},
		kcoi:   &mockConsumerOffsetInitializer{},
	}
	_, err := factory.StartConsumerGroup(ctx, "bla", []string{}, nil)

	if err == nil || err.Error() != "failed" {
		t.Errorf("Should contain an error with message failed. Got %v", err)
	}
}

func TestErrorWhileNewConsumerGroup(t *testing.T) {
	ctx := context.TODO()
	newConsumerGroup = mockedNewConsumerGroupFromClient(nil, false, false, true, false)

	factory := kafkaConsumerGroupFactoryImpl{
		config: sarama.NewConfig(),
		addrs:  []string{"b1", "b2"},
		kcoi:   &mockConsumerOffsetInitializer{},
	}
	consumerGroup, _ := factory.StartConsumerGroup(ctx, "bla", []string{}, nil)

	consumerGroup.(*customConsumerGroup).cancel() // Stop the consume loop from spinning after the error is generated
	err := <-consumerGroup.Errors()
	// Wait for the goroutine inside of startExistingConsumerGroup to finish
	<-consumerGroup.(*customConsumerGroup).releasedCh

	if err == nil || err.Error() != "consume error" {
		t.Errorf("Should contain an error with message consume error. Got %v", err)
	}
}

type mockClusterAdmin struct {
	mockCreateTopicFunc func(topic string, detail *sarama.TopicDetail, validateOnly bool) error
	mockDeleteTopicFunc func(topic string) error
}

func (ca *mockClusterAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	return nil
}

func (ca *mockClusterAdmin) ListPartitionReassignments(topics string, partitions []int32) (topicStatus map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, err error) {
	return nil, nil
}

func (ca *mockClusterAdmin) DescribeLogDirs(brokers []int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) DescribeUserScramCredentials(users []string) ([]*sarama.DescribeUserScramCredentialsResult, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) DeleteUserScramCredentials(delete []sarama.AlterUserScramCredentialsDelete) ([]*sarama.AlterUserScramCredentialsResult, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) UpsertUserScramCredentials(upsert []sarama.AlterUserScramCredentialsUpsert) ([]*sarama.AlterUserScramCredentialsResult, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	if ca.mockCreateTopicFunc != nil {
		return ca.mockCreateTopicFunc(topic, detail, validateOnly)
	}
	return nil
}

func (ca *mockClusterAdmin) Close() error {
	return nil
}

func (ca *mockClusterAdmin) DeleteTopic(topic string) error {
	if ca.mockDeleteTopicFunc != nil {
		return ca.mockDeleteTopicFunc(topic)
	}
	return nil
}

func (ca *mockClusterAdmin) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {
	return nil, nil
}

func (ca *mockClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	return nil
}

func (ca *mockClusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {
	return nil
}

func (ca *mockClusterAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) AlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {
	return nil
}

func (ca *mockClusterAdmin) CreateACL(resource sarama.Resource, acl sarama.Acl) error {
	return nil
}

func (ca *mockClusterAdmin) ListAcls(filter sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) DeleteACL(filter sarama.AclFilter, validateOnly bool) ([]sarama.MatchingAcl, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) ListConsumerGroups() (map[string]string, error) {
	return map[string]string{}, nil
}

func (ca *mockClusterAdmin) DescribeConsumerGroups(groups []string) ([]*sarama.GroupDescription, error) {
	return nil, nil
}

func (ca *mockClusterAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	return &sarama.OffsetFetchResponse{}, nil
}

func (ca *mockClusterAdmin) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	return nil, 0, nil
}

// Delete a consumer group.
func (ca *mockClusterAdmin) DeleteConsumerGroup(group string) error {
	return nil
}
