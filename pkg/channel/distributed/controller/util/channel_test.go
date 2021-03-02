/*
Copyright 2020 The Knative Authors

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

package util

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	distributedcommonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test Data
const (
	kafkaSecret              = "testkafkasecret"
	channelName              = "testname"
	channelNamespace         = "testnamespace"
	numPartitions            = int32(123)
	defaultNumPartitions     = int32(987)
	replicationFactor        = int16(22)
	defaultReplicationFactor = int16(33)
	defaultRetentionMillis   = int64(55555)
)

// Test The ChannelLogger() Functionality
func TestChannelLogger(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Test Data
	channel := &kafkav1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{Name: "TestChannelName", Namespace: "TestChannelNamespace"},
	}

	// Perform The Test
	channelLogger := ChannelLogger(logger, channel)
	assert.NotNil(t, channelLogger)
	assert.NotEqual(t, logger, channelLogger)
	channelLogger.Info("Testing Channel Logger")
}

// Test The ChannelKey() Functionality
func TestChannelKey(t *testing.T) {

	// Test Data
	channel := &kafkav1beta1.KafkaChannel{ObjectMeta: metav1.ObjectMeta{Name: channelName, Namespace: channelNamespace}}

	// Perform The Test
	actualResult := ChannelKey(channel)

	// Verify The Results
	expectedResult := fmt.Sprintf("%s/%s", channelNamespace, channelName)
	assert.Equal(t, expectedResult, actualResult)
}

// Test The NewChannelOwnerReference() Functionality
func TestNewChannelOwnerReference(t *testing.T) {

	// Test Data
	channel := &kafkav1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{Name: channelName},
	}

	// Perform The Test
	controllerRef := NewChannelOwnerReference(channel)

	// Validate Results
	assert.NotNil(t, controllerRef)
	assert.Equal(t, kafkav1beta1.SchemeGroupVersion.String(), controllerRef.APIVersion)
	assert.Equal(t, constants.KafkaChannelKind, controllerRef.Kind)
	assert.Equal(t, channel.ObjectMeta.Name, controllerRef.Name)
	assert.True(t, *controllerRef.BlockOwnerDeletion)
	assert.True(t, *controllerRef.Controller)
}

// Test The ReceiverDnsSafeName() Functionality
func TestReceiverDnsSafeName(t *testing.T) {

	// Perform The Test
	actualResult := ReceiverDnsSafeName(kafkaSecret)

	// Verify The Results
	expectedResult := fmt.Sprintf("%s-%s-receiver", strings.ToLower(kafkaSecret), GenerateHash(kafkaSecret, 8))
	assert.Equal(t, expectedResult, actualResult)
}

// Test The Channel Host Name Formatter / Generator
func TestChannelHostName(t *testing.T) {
	testChannelName := "TestChannelName"
	testChannelNamespace := "TestChannelNamespace"
	expectedChannelHostName := testChannelName + "." + testChannelNamespace + ".channels.cluster.local"
	actualChannelHostName := ChannelHostName(testChannelName, testChannelNamespace)
	assert.Equal(t, expectedChannelHostName, actualChannelHostName)
}

// Test The NumPartitions Accessor
func TestNumPartitions(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Test Data
	configuration := &distributedcommonconfig.EventingKafkaConfig{Kafka: distributedcommonconfig.EKKafkaConfig{Topic: distributedcommonconfig.EKKafkaTopicConfig{DefaultNumPartitions: defaultNumPartitions}}}

	// Test The Default Failover Use Case
	channel := &kafkav1beta1.KafkaChannel{}
	actualNumPartitions := NumPartitions(channel, configuration, logger)
	assert.Equal(t, defaultNumPartitions, actualNumPartitions)

	// Test The Valid NumPartitions Use Case
	channel = &kafkav1beta1.KafkaChannel{Spec: kafkav1beta1.KafkaChannelSpec{NumPartitions: numPartitions}}
	actualNumPartitions = NumPartitions(channel, configuration, logger)
	assert.Equal(t, numPartitions, actualNumPartitions)
}

// Test The ReplicationFactor Accessor
func TestReplicationFactor(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Test Data
	configuration := &distributedcommonconfig.EventingKafkaConfig{Kafka: distributedcommonconfig.EKKafkaConfig{Topic: distributedcommonconfig.EKKafkaTopicConfig{DefaultReplicationFactor: defaultReplicationFactor}}}

	// Test The Default Failover Use Case
	channel := &kafkav1beta1.KafkaChannel{}
	actualReplicationFactor := ReplicationFactor(channel, configuration, logger)
	assert.Equal(t, defaultReplicationFactor, actualReplicationFactor)

	// Test The Valid ReplicationFactor Use Case
	channel = &kafkav1beta1.KafkaChannel{Spec: kafkav1beta1.KafkaChannelSpec{ReplicationFactor: replicationFactor}}
	actualReplicationFactor = ReplicationFactor(channel, configuration, logger)
	assert.Equal(t, replicationFactor, actualReplicationFactor)
}

// Test The RetentionMillis Accessor
func TestRetentionMillis(t *testing.T) {

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Test Data
	configuration := &distributedcommonconfig.EventingKafkaConfig{Kafka: distributedcommonconfig.EKKafkaConfig{Topic: distributedcommonconfig.EKKafkaTopicConfig{DefaultRetentionMillis: defaultRetentionMillis}}}

	// Test The Default Failover Use Case
	channel := &kafkav1beta1.KafkaChannel{}
	actualRetentionMillis := RetentionMillis(channel, configuration, logger)
	assert.Equal(t, defaultRetentionMillis, actualRetentionMillis)

	// TODO - No RetentionMillis In eventing-contrib KafkaChannel
	//// Test The Valid RetentionMillis Use Case
	//channel = &kafkav1beta1.KafkaChannel{Spec: kafkav1beta1.KafkaChannelSpec{RetentionMillis: retentionMillis}}
	//actualRetentionMillis = RetentionMillis(channel, environment, logger)
	//assert.Equal(t, retentionMillis, actualRetentionMillis)
}
