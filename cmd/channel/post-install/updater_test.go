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

package main

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	logtesting "knative.dev/pkg/logging/testing"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	fakekafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned/fake"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
)

const (
	kafkachannel1 = "kafkachannel1"
	kafkachannel2 = "kafkachannel2"
	kafkachannel3 = "kafkachannel3"

	namespace1 = "namespace1"
	namespace2 = "namespace2"
	namespace3 = "namespace3"

	topicName1 = namespace1 + "." + kafkachannel1
	topicName2 = namespace1 + "." + kafkachannel2
	topicName3 = "knative-messaging-kafka." + namespace2 + "." + kafkachannel3
	topicName4 = namespace3 + "." + "foo"
	topicName5 = "__foo"

	retentionMs1 = 3600000   // 1 Hour (PT1H)
	retentionMs2 = 43200000  // 12 Hours (PT12H)
	retentionMs3 = 604800000 // 1 Week (PT168H)
)

var (
	retentionMsString1 = strconv.Itoa(retentionMs1)
	retentionMsString2 = strconv.Itoa(retentionMs2)
	retentionMsString3 = strconv.Itoa(retentionMs3)
)

func TestNewKafkaChannelUpdater(t *testing.T) {

	testLogger := logtesting.TestLogger(t).Desugar()
	fakeKafkaClientSet := fakekafkaclientset.NewSimpleClientset()
	mockClusterAdmin := &MockClusterAdmin{}

	updater := NewKafkaChannelUpdater(testLogger, fakeKafkaClientSet, mockClusterAdmin)

	assert.NotNil(t, updater)
	assert.Equal(t, testLogger, updater.logger)
	assert.Equal(t, fakeKafkaClientSet, updater.clientset)
	assert.Equal(t, mockClusterAdmin, updater.clusterAdmin)
}

func TestUpdate(t *testing.T) {

	// Create The Test KafkaChannels For Fake K8S Client
	kafkaChannel1 := NewKafkaChannel(namespace1, kafkachannel1, "")
	kafkaChannel2 := NewKafkaChannel(namespace1, kafkachannel2, "")
	kafkaChannel3 := NewKafkaChannel(namespace2, kafkachannel3, "")
	kafkaChannels := []runtime.Object{kafkaChannel1, kafkaChannel2, kafkaChannel3}

	// Set Up The Fake K8S Client & Simple Reactor To Track Updates
	updatedKafkaChannels := make(map[string]*kafkav1beta1.KafkaChannel)
	fakeKafkaClientSet := fakekafkaclientset.NewSimpleClientset(kafkaChannels...)
	fakeKafkaClientSet.PrependReactor("update", "kafkachannels",
		func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
			got := action.(clientgotesting.UpdateAction).GetObject()
			if kafkachannel, ok := got.(*kafkav1beta1.KafkaChannel); ok {
				updatedKafkaChannels[kafkachannel.Name] = kafkachannel
			} else {
				assert.Fail(t, "Non KafkaChannel Update")
			}
			return true, got, nil
		},
	)

	// Set Up The Mock Sarama ClusterAdmin With Test Topics
	topicDetailMap := map[string]sarama.TopicDetail{
		topicName1: {ConfigEntries: map[string]*string{commonconstants.KafkaTopicConfigRetentionMs: &retentionMsString1}},
		topicName2: {ConfigEntries: map[string]*string{commonconstants.KafkaTopicConfigRetentionMs: &retentionMsString2}},
		topicName3: {ConfigEntries: map[string]*string{commonconstants.KafkaTopicConfigRetentionMs: &retentionMsString3}},
		topicName4: {ConfigEntries: map[string]*string{commonconstants.KafkaTopicConfigRetentionMs: &retentionMsString3}},
		topicName5: {ConfigEntries: map[string]*string{commonconstants.KafkaTopicConfigRetentionMs: &retentionMsString3}},
	}
	mockClusterAdmin := &MockClusterAdmin{}
	mockClusterAdmin.On("ListTopics").Return(topicDetailMap, nil)

	// Create An KafkaChannel Updater & Perform The Test
	testLogger := logtesting.TestLogger(t).Desugar()
	updater := NewKafkaChannelUpdater(testLogger, fakeKafkaClientSet, mockClusterAdmin)
	err := updater.Update(context.Background())

	// Verify The Results
	mockClusterAdmin.AssertExpectations(t)
	assert.Nil(t, err)
	assert.Len(t, updatedKafkaChannels, 3)
	assert.NotNil(t, updatedKafkaChannels[kafkachannel1])
	assert.NotNil(t, updatedKafkaChannels[kafkachannel2])
	assert.NotNil(t, updatedKafkaChannels[kafkachannel3])
	assert.Equal(t, "PT1H", updatedKafkaChannels[kafkachannel1].Spec.RetentionDuration)
	assert.Equal(t, "PT12H", updatedKafkaChannels[kafkachannel2].Spec.RetentionDuration)
	assert.Equal(t, "PT168H", updatedKafkaChannels[kafkachannel3].Spec.RetentionDuration)
}

func TestGetKafkaChannelNamespacedName(t *testing.T) {

	tests := []struct {
		name     string
		topic    string
		expected *types.NamespacedName
		err      bool
	}{
		{
			name:     "Empty",
			topic:    "",
			expected: nil,
		},
		{
			name:     "Internal",
			topic:    "__consumer_offsets",
			expected: nil,
		},
		{
			name:     "Too Short",
			topic:    "ab",
			expected: nil,
		},
		{
			name:     "No Separator",
			topic:    "Foo",
			expected: nil,
		},
		{
			name:     "Extra Component",
			topic:    "Foo.Bar.Baz",
			expected: nil,
			err:      true,
		},
		{
			name:     "Valid Consolidated",
			topic:    "knative-messaging-kafka.kcnamespace.kcname",
			expected: &types.NamespacedName{Namespace: "kcnamespace", Name: "kcname"},
		},
		{
			name:     "Valid Distributed",
			topic:    "kcnamespace.kcname",
			expected: &types.NamespacedName{Namespace: "kcnamespace", Name: "kcname"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := getKafkaChannelNamespacedName(test.topic)
			assert.Equal(t, test.expected, result)
			assert.Equal(t, test.err, err != nil)
		})
	}
}

func TestGetExpectedRetentionDurationString(t *testing.T) {

	tests := []struct {
		name        string
		retentionMs string
		expect      string
		err         bool
	}{
		{
			name:        "Empty",
			retentionMs: "",
			expect:      "",
			err:         true,
		},
		{
			name:        "Non-Numeric",
			retentionMs: "FOO",
			expect:      "",
			err:         true,
		},
		{
			name:        "1 Second",
			retentionMs: "1000",
			expect:      "PT1S",
		},
		{
			name:        "1.5 Second",
			retentionMs: "1500",
			expect:      "PT1.5S",
		},
		{
			name:        "12 Hours",
			retentionMs: strconv.FormatInt((12 * time.Hour).Milliseconds(), 10),
			expect:      "PT12H",
		},
		{
			name:        "12.5 Hours",
			retentionMs: strconv.FormatInt(((12 * time.Hour) + (30 * time.Minute)).Milliseconds(), 10),
			expect:      "PT12H30M",
		},
		{
			name:        "Full Detail",
			retentionMs: strconv.FormatInt(((12 * time.Hour) + (30 * time.Minute) + (15 * time.Second) + (5 * time.Millisecond)).Milliseconds(), 10),
			expect:      "PT12H30M15S", // Millis Dropped
		},
		{
			name:        "7 Days",
			retentionMs: strconv.FormatInt((7 * 24 * time.Hour).Milliseconds(), 10),
			expect:      "PT168H", // 7 Days
		},
		{
			name:        "30 Days",
			retentionMs: strconv.FormatInt((30 * 24 * time.Hour).Milliseconds(), 10),
			expect:      "PT720H", // 30 Days
		},
		{
			name:        "60 Days",
			retentionMs: strconv.FormatInt((60 * 24 * time.Hour).Milliseconds(), 10),
			expect:      "PT1440H", // 60 Days
		},
		{
			name:        "180 Days",
			retentionMs: strconv.FormatInt((180 * 24 * time.Hour).Milliseconds(), 10),
			expect:      "P180D", // Precision Switch-Over
		},
		{
			name:        "Long Full Detail",
			retentionMs: strconv.FormatInt(((180 * 24 * time.Hour) + (30 * time.Minute) + (15 * time.Second) + (5 * time.Millisecond)).Milliseconds(), 10),
			expect:      "P180DT30M15S", // Precision Switch-Over & Millis Dropped
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			topicDetail := sarama.TopicDetail{
				ConfigEntries: map[string]*string{commonconstants.KafkaTopicConfigRetentionMs: &test.retentionMs},
			}

			result, err := getExpectedRetentionDurationString(topicDetail)

			assert.Equal(t, test.expect, result)
			assert.Equal(t, test.err, err != nil)
		})
	}
}

//
// Test Utilities
//

func NewKafkaChannel(namespace, name, retentionDurationISO8601 string) *kafkav1beta1.KafkaChannel {
	kafkaChannel := &kafkav1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
	if len(retentionDurationISO8601) > 0 {
		kafkaChannel.Spec.RetentionDuration = retentionDurationISO8601
	}
	return kafkaChannel
}

//
// Mock Sarama Kafka ClusterAdmin
//

// Verify The Mock Sarama ClusterAdmin Implements The Interface
var _ sarama.ClusterAdmin = &MockClusterAdmin{}

// The Mock Sarama ClusterAdmin
type MockClusterAdmin struct {
	mock.Mock
}

func (m *MockClusterAdmin) DescribeLogDirs(brokers []int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	panic("implement me")
}

func (m *MockClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	args := m.Called()
	return args.Get(0).(map[string]sarama.TopicDetail), args.Error(1)
}

func (m *MockClusterAdmin) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DeleteTopic(topic string) error {
	panic("implement me")
}

func (m *MockClusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	panic("implement me")
}

func (m *MockClusterAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	panic("implement me")
}

func (m *MockClusterAdmin) ListPartitionReassignments(topics string, partitions []int32) (topicStatus map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, err error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {
	panic("implement me")
}

func (m *MockClusterAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) AlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {
	panic("implement me")
}

func (m *MockClusterAdmin) CreateACL(resource sarama.Resource, acl sarama.Acl) error {
	panic("implement me")
}

func (m *MockClusterAdmin) ListAcls(filter sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DeleteACL(filter sarama.AclFilter, validateOnly bool) ([]sarama.MatchingAcl, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) ListConsumerGroups() (map[string]string, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DescribeConsumerGroups(groups []string) ([]*sarama.GroupDescription, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DeleteConsumerGroup(group string) error {
	panic("implement me")
}

func (m *MockClusterAdmin) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DescribeUserScramCredentials(users []string) ([]*sarama.DescribeUserScramCredentialsResult, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) DeleteUserScramCredentials(delete []sarama.AlterUserScramCredentialsDelete) ([]*sarama.AlterUserScramCredentialsResult, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) UpsertUserScramCredentials(upsert []sarama.AlterUserScramCredentialsUpsert) ([]*sarama.AlterUserScramCredentialsResult, error) {
	panic("implement me")
}

func (m *MockClusterAdmin) Close() error {
	args := m.Called()
	return args.Error(0)
}
