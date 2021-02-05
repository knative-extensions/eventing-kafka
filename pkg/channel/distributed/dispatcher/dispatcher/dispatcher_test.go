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

package dispatcher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	commonconfigtesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/config/testing"
	configtesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/config/testing"
	consumertesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/consumer/testing"
	consumerwrapper "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/consumer/wrapper"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/metrics"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	dispatchertesting "knative.dev/eventing-kafka/pkg/channel/distributed/dispatcher/testing"
	receivertesting "knative.dev/eventing-kafka/pkg/channel/distributed/receiver/testing"
	commonclient "knative.dev/eventing-kafka/pkg/common/client"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test Data
const (
	id123  = "123"
	id456  = "456"
	id789  = "789"
	uid123 = types.UID(id123)
	uid456 = types.UID(id456)
	uid789 = types.UID(id789)
)

// Test The NewSubscriberWrapper() Functionality
func TestNewSubscriberWrapper(t *testing.T) {

	// Test Data
	subscriber := eventingduck.SubscriberSpec{UID: uid123}
	groupId := "TestGroupId"
	consumerGroup := consumertesting.NewMockConsumerGroup()

	// Perform The Test
	subscriberWrapper := NewSubscriberWrapper(subscriber, groupId, consumerGroup)

	// Verify Results
	assert.NotNil(t, subscriberWrapper)
	assert.Equal(t, subscriber.UID, subscriberWrapper.UID)
	assert.Equal(t, consumerGroup, subscriberWrapper.ConsumerGroup)
	assert.Equal(t, groupId, subscriberWrapper.GroupId)
	assert.NotNil(t, subscriberWrapper.StopChan)
}

// Test The NewDispatcher() Functionality
func TestNewDispatcher(t *testing.T) {

	// Perform The Test & Verify Results (Not Much To See Due To Interface)
	createTestDispatcher(t, nil, nil)
}

// Test The Dispatcher's Shutdown() Functionality
func TestShutdown(t *testing.T) {

	// Create Mock ConsumerGroups To Register Close() Requests
	consumerGroup1 := consumertesting.NewMockConsumerGroup()
	consumerGroup2 := consumertesting.NewMockConsumerGroup()
	consumerGroup3 := consumertesting.NewMockConsumerGroup()

	// Create Test Subscribers To Close The ConsumerGroups Of
	subscriber1 := eventingduck.SubscriberSpec{UID: id123}
	subscriber2 := eventingduck.SubscriberSpec{UID: id456}
	subscriber3 := eventingduck.SubscriberSpec{UID: id789}
	groupId1 := fmt.Sprintf("kafka.%s", subscriber1.UID)
	groupId2 := fmt.Sprintf("kafka.%s", subscriber2.UID)
	groupId3 := fmt.Sprintf("kafka.%s", subscriber3.UID)

	// Create The Dispatcher To Test With Existing Subscribers
	dispatcher := &DispatcherImpl{
		DispatcherConfig: DispatcherConfig{
			Logger: logtesting.TestLogger(t).Desugar(),
		},
		subscribers: map[types.UID]*SubscriberWrapper{
			subscriber1.UID: NewSubscriberWrapper(subscriber1, groupId1, consumerGroup1),
			subscriber2.UID: NewSubscriberWrapper(subscriber2, groupId2, consumerGroup2),
			subscriber3.UID: NewSubscriberWrapper(subscriber3, groupId3, consumerGroup3),
		},
	}

	// Perform The Test
	dispatcher.Shutdown()

	// Verify The Results
	assert.True(t, consumerGroup1.Closed)
	assert.True(t, consumerGroup2.Closed)
	assert.True(t, consumerGroup3.Closed)
	assert.Len(t, dispatcher.subscribers, 0)
}

// Test The UpdateSubscriptions() Functionality
func TestUpdateSubscriptions(t *testing.T) {

	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Restore Any Stubbing Of NewConsumerGroupWrapper After Test Is Finished
	defer consumertesting.RestoreNewConsumerGroupFn()

	// Test Data
	brokers := []string{dispatchertesting.KafkaBroker}
	config, err := commonclient.NewConfigBuilder().WithDefaults().FromYaml(commonconfigtesting.DefaultSaramaConfigYaml).Build(ctx)
	assert.Nil(t, err)

	dispatcherConfig := DispatcherConfig{
		Logger:       logger.Desugar(),
		Brokers:      brokers,
		SaramaConfig: config,
	}

	// Define The TestCase Struct
	type fields struct {
		DispatcherConfig DispatcherConfig
		subscribers      map[types.UID]*SubscriberWrapper
	}
	type args struct {
		subscriberSpecs []eventingduck.SubscriberSpec
	}

	// Define The TestCase Struct
	type TestCase struct {
		only   bool
		name   string
		fields fields
		args   args
		want   map[eventingduck.SubscriberSpec]error
	}

	// Create The Test Cases
	testCases := []TestCase{
		{
			name: "Add First Subscription",
			fields: fields{
				DispatcherConfig: dispatcherConfig,
				subscribers:      map[types.UID]*SubscriberWrapper{},
			},
			args: args{
				subscriberSpecs: []eventingduck.SubscriberSpec{
					{UID: uid123},
				},
			},
			want: map[eventingduck.SubscriberSpec]error{},
		},
		{
			name: "Add Second Subscription",
			fields: fields{
				DispatcherConfig: dispatcherConfig,
				subscribers: map[types.UID]*SubscriberWrapper{
					uid123: createSubscriberWrapper(uid123),
				},
			},
			args: args{
				subscriberSpecs: []eventingduck.SubscriberSpec{
					{UID: uid123},
					{UID: uid456},
				},
			},
			want: map[eventingduck.SubscriberSpec]error{},
		},
		{
			name: "Add And Remove Subscriptions",
			fields: fields{
				DispatcherConfig: dispatcherConfig,
				subscribers: map[types.UID]*SubscriberWrapper{
					uid123: createSubscriberWrapper(uid123),
					uid456: createSubscriberWrapper(uid456),
				},
			},
			args: args{
				subscriberSpecs: []eventingduck.SubscriberSpec{
					{UID: uid456},
					{UID: uid789},
				},
			},
			want: map[eventingduck.SubscriberSpec]error{},
		},
		{
			name: "Remove Penultimate Subscription",
			fields: fields{
				DispatcherConfig: dispatcherConfig,
				subscribers: map[types.UID]*SubscriberWrapper{
					uid123: createSubscriberWrapper(uid123),
					uid456: createSubscriberWrapper(uid456),
				},
			},
			args: args{
				subscriberSpecs: []eventingduck.SubscriberSpec{
					{UID: uid123},
				},
			},
			want: map[eventingduck.SubscriberSpec]error{},
		},
		{
			name: "Remove Last Subscription",
			fields: fields{
				DispatcherConfig: dispatcherConfig,
				subscribers: map[types.UID]*SubscriberWrapper{
					uid123: createSubscriberWrapper(uid123),
				},
			},
			args: args{
				subscriberSpecs: []eventingduck.SubscriberSpec{},
			},
			want: map[eventingduck.SubscriberSpec]error{},
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]TestCase, 0)
	for _, testCase := range testCases {
		if testCase.only {
			filteredTestCases = append(filteredTestCases, testCase)
		}
	}
	if len(filteredTestCases) == 0 {
		filteredTestCases = testCases
	}

	// Execute The Test Cases (Create A DispatcherImpl & UpdateSubscriptions() :)
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Mock The ConsumerGroup & Stub The NewSyncConsumerGroupWrapper()
			mockConsumerGroup := consumertesting.NewMockConsumerGroup()
			consumertesting.StubNewConsumerGroupFn(customValidationNewConsumerGroupFn(t,
				testCase.fields.DispatcherConfig.Brokers,
				testCase.args.subscriberSpecs,
				testCase.fields.DispatcherConfig.SaramaConfig,
				mockConsumerGroup))

			// Create A New DispatcherImpl To Test
			dispatcher := &DispatcherImpl{
				DispatcherConfig: testCase.fields.DispatcherConfig,
				subscribers:      testCase.fields.subscribers,
			}

			// Perform The Test
			got := dispatcher.UpdateSubscriptions(testCase.args.subscriberSpecs)

			// Verify Results
			assert.Equal(t, testCase.want, got)

			// Verify The Dispatcher's Tracking Of Subscribers Matches Specified State
			assert.Len(t, dispatcher.subscribers, len(testCase.args.subscriberSpecs))
			for _, subscriber := range testCase.args.subscriberSpecs {
				assert.NotNil(t, dispatcher.subscribers[subscriber.UID])
			}

			// Shutdown The Dispatcher to Cleanup Resources
			dispatcher.Shutdown()
			assert.Len(t, dispatcher.subscribers, 0)

			// Pause Briefly To Let Any Async Shutdown Finish (Lame But Only For Visual Confirmation Of Logging ;)
			time.Sleep(500 * time.Millisecond)
		})
	}
}

// Test The Dispatcher's ConfigChanged Functionality
func TestConfigChanged(t *testing.T) {

	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Setup Test Environment Namespaces
	commontesting.SetTestEnvironment(t)

	// Test Data
	brokers := []string{receivertesting.KafkaBroker}
	baseSaramaConfig, err := commonclient.NewConfigBuilder().
		WithDefaults().
		FromYaml(commonconfigtesting.DefaultSaramaConfigYaml).
		WithVersion(&sarama.V2_0_0_0).
		Build(ctx)
	assert.Nil(t, err)

	// Define The TestCase Struct
	type TestCase struct {
		only                bool
		name                string
		newConfigMap        *corev1.ConfigMap
		expectNewDispatcher bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:                "No Changes (Same Dispatcher)",
			newConfigMap:        configtesting.NewKafkaConfigMap(),
			expectNewDispatcher: false,
		},
		{
			name:                "No EventingKafka Config (Same Dispatcher)",
			newConfigMap:        configtesting.NewKafkaConfigMap(configtesting.WithoutEventingKafkaConfiguration),
			expectNewDispatcher: false,
		},
		{
			name:                "Admin Change (New Dispatcher)",
			newConfigMap:        configtesting.NewKafkaConfigMap(configtesting.WithModifiedSaramaAdmin),
			expectNewDispatcher: true,
		},
		{
			name:                "Net Change (New Dispatcher)",
			newConfigMap:        configtesting.NewKafkaConfigMap(configtesting.WithModifiedSaramaNet),
			expectNewDispatcher: true,
		},
		{
			name:                "Metadata Change (New Dispatcher)",
			newConfigMap:        configtesting.NewKafkaConfigMap(configtesting.WithModifiedSaramaMetadata),
			expectNewDispatcher: true,
		},
		{
			name:                "Consumer Change (New Dispatcher)",
			newConfigMap:        configtesting.NewKafkaConfigMap(configtesting.WithModifiedSaramaConsumer),
			expectNewDispatcher: true,
		},
		{
			name:                "Producer Change (Same Dispatcher)",
			newConfigMap:        configtesting.NewKafkaConfigMap(configtesting.WithModifiedSaramaProducer),
			expectNewDispatcher: false,
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]TestCase, 0)
	for _, testCase := range testCases {
		if testCase.only {
			filteredTestCases = append(filteredTestCases, testCase)
		}
	}
	if len(filteredTestCases) == 0 {
		filteredTestCases = testCases
	}

	// Make Sure To Restore The NewConsumerGroup Wrapper After The Test
	defer consumertesting.RestoreNewConsumerGroupFn()

	// Run The Filtered TestCases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Mock The SyncProducer & Stub The NewConsumerGroupWrapper()
			mockConsumerGroup := consumertesting.NewMockConsumerGroup()
			consumertesting.StubNewConsumerGroupFn(consumertesting.NonValidatingNewConsumerGroupFn(mockConsumerGroup))

			// Create A Test Dispatcher To Perform Tests Against
			dispatcher := createTestDispatcher(t, brokers, baseSaramaConfig)

			// Perform The Test
			newDispatcher := dispatcher.ConfigChanged(ctx, testCase.newConfigMap)

			// Verify Expected State (Not Much To Verify Due To Interface)
			assert.Equal(t, testCase.expectNewDispatcher, newDispatcher != nil)
		})
	}
}

// Test The Dispatcher's SecretChanged Functionality
func TestSecretChanged(t *testing.T) {

	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Setup Test Environment Namespaces
	commontesting.SetTestEnvironment(t)

	// Test Data
	brokers := []string{receivertesting.KafkaBroker}
	auth := &commonclient.KafkaAuthConfig{
		SASL: &commonclient.KafkaSaslConfig{
			User:     commonconfigtesting.DefaultSecretUsername,
			Password: commonconfigtesting.DefaultSecretPassword,
			SaslType: commonconfigtesting.DefaultSecretSaslType,
		},
	}
	baseSaramaConfig, err := commonclient.NewConfigBuilder().
		WithDefaults().
		FromYaml(commonconfigtesting.DefaultSaramaConfigYaml).
		WithVersion(&sarama.V2_0_0_0).
		WithAuth(auth).
		Build(ctx)
	assert.Nil(t, err)

	// Define The TestCase Struct
	type TestCase struct {
		only                bool
		name                string
		newSecret           *corev1.Secret
		expectNewDispatcher bool
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:                "No Changes (Same Dispatcher)",
			newSecret:           configtesting.NewKafkaSecret(),
			expectNewDispatcher: false,
		},
		{
			name:                "Password Change (New Dispatcher)",
			newSecret:           configtesting.NewKafkaSecret(configtesting.WithModifiedPassword),
			expectNewDispatcher: true,
		},
		{
			name:                "Username Change (New Dispatcher)",
			newSecret:           configtesting.NewKafkaSecret(configtesting.WithModifiedUsername),
			expectNewDispatcher: true,
		},
		{
			name:                "SaslType Change (New Dispatcher)",
			newSecret:           configtesting.NewKafkaSecret(configtesting.WithModifiedSaslType),
			expectNewDispatcher: true,
		},
		{
			name:                "Namespace Change (Same Dispatcher)",
			newSecret:           configtesting.NewKafkaSecret(configtesting.WithModifiedNamespace),
			expectNewDispatcher: false,
		},
		{
			name:                "Brokers Change (Same Dispatcher)",
			newSecret:           configtesting.NewKafkaSecret(configtesting.WithModifiedBrokers),
			expectNewDispatcher: false,
		},
	}

	// Filter To Those With "only" Flag (If Any Specified)
	filteredTestCases := make([]TestCase, 0)
	for _, testCase := range testCases {
		if testCase.only {
			filteredTestCases = append(filteredTestCases, testCase)
		}
	}
	if len(filteredTestCases) == 0 {
		filteredTestCases = testCases
	}

	// Make Sure To Restore The NewConsumerGroup Wrapper After The Test
	defer consumertesting.RestoreNewConsumerGroupFn()

	// Run The Filtered TestCases
	for _, testCase := range filteredTestCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Mock The SyncProducer & Stub The NewConsumerGroupWrapper()
			mockConsumerGroup := consumertesting.NewMockConsumerGroup()
			consumertesting.StubNewConsumerGroupFn(consumertesting.NonValidatingNewConsumerGroupFn(mockConsumerGroup))

			// Create A Test Dispatcher To Perform Tests Against
			dispatcher := createTestDispatcher(t, brokers, baseSaramaConfig)

			// Perform The Test
			newDispatcher := dispatcher.SecretChanged(ctx, testCase.newSecret)

			// Verify Expected State (Not Much To Verify Due To Interface)
			assert.Equal(t, testCase.expectNewDispatcher, newDispatcher != nil)
		})
	}
}

// Utility Function For Creating A SubscriberWrapper With Specified UID & Mock ConsumerGroup
func createSubscriberWrapper(uid types.UID) *SubscriberWrapper {
	return NewSubscriberWrapper(eventingduck.SubscriberSpec{UID: uid}, fmt.Sprintf("kafka.%s", string(uid)), nil)
}

// Utility Function For Creating A Dispatcher With Specified Configuration
func createTestDispatcher(t *testing.T, brokers []string, config *sarama.Config) Dispatcher {

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create StatsReporter
	statsReporter := metrics.NewStatsReporter(logger)

	// Create An Empty Set Of SubscriberSpecs
	subscriberSpecs := make([]eventingduck.SubscriberSpec, 0)

	// Create The DispatcherConfig
	dispatcherConfig := DispatcherConfig{
		Logger:          logger,
		Brokers:         brokers,
		StatsReporter:   statsReporter,
		SaramaConfig:    config,
		SubscriberSpecs: subscriberSpecs,
	}

	// Create The Dispatcher
	dispatcher := NewDispatcher(dispatcherConfig)

	// Verify State
	assert.NotNil(t, dispatcher)

	// Return The Dispatcher
	return dispatcher
}

// Custom Validation Function For NewConsumerGroup - Capable Of Verifying Multiple GroupIds
func customValidationNewConsumerGroupFn(t *testing.T,
	expectedBrokers []string,
	subscriberSpecs []eventingduck.SubscriberSpec,
	expectedConfig *sarama.Config,
	mockConsumerGroup sarama.ConsumerGroup) consumerwrapper.NewConsumerGroupFnType {

	expectedGroupIds := make([]string, len(subscriberSpecs))
	for _, subscriberSpec := range subscriberSpecs {
		expectedGroupIds = append(expectedGroupIds, fmt.Sprintf("kafka.%s", subscriberSpec.UID))
	}

	return func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error) {
		assert.Equal(t, brokers, expectedBrokers)
		assert.Contains(t, expectedGroupIds, groupId)
		assert.Equal(t, config, expectedConfig)
		return mockConsumerGroup, nil
	}
}
