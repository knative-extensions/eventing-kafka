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
	"sync"
	"testing"
	"time"

	"knative.dev/eventing-kafka/pkg/common/metrics"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"

	commonclient "knative.dev/eventing-kafka/pkg/common/client"
	clienttesting "knative.dev/eventing-kafka/pkg/common/client/testing"
	configtesting "knative.dev/eventing-kafka/pkg/common/config/testing"
	consumertesting "knative.dev/eventing-kafka/pkg/common/consumer/testing"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
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
	subscriberWrapper := NewSubscriberWrapper(subscriber, groupId)

	// Verify Results
	assert.NotNil(t, subscriberWrapper)
	assert.Equal(t, subscriber.UID, subscriberWrapper.UID)
	assert.Equal(t, groupId, subscriberWrapper.GroupId)
}

// Test The NewDispatcher() Functionality
func TestNewDispatcher(t *testing.T) {

	baseSaramaConfig, err := commonclient.NewConfigBuilder().
		WithDefaults().
		WithVersion(&sarama.V2_0_0_0).
		Build(context.TODO())
	assert.Nil(t, err)

	// Perform The Test & Verify Results (Not Much To See Due To Interface)
	createTestDispatcher(t, nil, baseSaramaConfig)
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

	// Verify that calling Shutdown a second time does not cause a panic
	dispatcher.Shutdown()
}

// Test The UpdateSubscriptions() Functionality
func TestUpdateSubscriptions(t *testing.T) {

	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Test Data
	brokers := []string{configtesting.DefaultKafkaBroker}
	config, err := commonclient.NewConfigBuilder().WithDefaults().FromYaml(clienttesting.DefaultSaramaConfigYaml).Build(ctx)
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

			// Create A New DispatcherImpl To Test
			dispatcher := &DispatcherImpl{
				DispatcherConfig:     testCase.fields.DispatcherConfig,
				subscribers:          testCase.fields.subscribers,
				consumerGroupFactory: &consumertesting.MockKafkaConsumerGroupFactory{},
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

// Test The Dispatcher's SecretChanged Functionality
func TestSecretChanged(t *testing.T) {

	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Setup Test Environment Namespaces
	commontesting.SetTestEnvironment(t)

	// Test Data
	brokers := []string{configtesting.DefaultKafkaBroker}
	auth := &commonclient.KafkaAuthConfig{
		SASL: &commonclient.KafkaSaslConfig{
			User:     configtesting.DefaultSecretUsername,
			Password: configtesting.DefaultSecretPassword,
			SaslType: configtesting.DefaultSecretSaslType,
		},
	}
	baseSaramaConfig, err := commonclient.NewConfigBuilder().
		WithDefaults().
		FromYaml(clienttesting.DefaultSaramaConfigYaml).
		WithVersion(&sarama.V2_0_0_0).
		WithAuth(auth).
		Build(ctx)
	assert.Nil(t, err)

	// Define The TestCase Struct
	type TestCase struct {
		name                string
		newSecret           *corev1.Secret
		expectEmptyUsername bool
		expectNewUsername   string
		expectNewPassword   string
		expectNewSaslType   string
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:      "No Changes (No Modifications)",
			newSecret: configtesting.NewKafkaSecret(),
		},
		{
			name:              "Password Change (Modifications)",
			newSecret:         configtesting.NewKafkaSecret(configtesting.WithModifiedPassword),
			expectNewPassword: configtesting.ModifiedSecretPassword,
		},
		{
			name:              "Username Change (Modifications)",
			newSecret:         configtesting.NewKafkaSecret(configtesting.WithModifiedUsername),
			expectNewUsername: configtesting.ModifiedSecretUsername,
		},
		{
			name:                "Empty Username Change (Modifications)",
			newSecret:           configtesting.NewKafkaSecret(configtesting.WithEmptyUsername),
			expectEmptyUsername: true,
		},
		{
			name:              "SaslType Change (Modifications)",
			newSecret:         configtesting.NewKafkaSecret(configtesting.WithModifiedSaslType),
			expectNewSaslType: configtesting.ModifiedSecretSaslType,
		},
		{
			name:      "Namespace Change (No Modifications)",
			newSecret: configtesting.NewKafkaSecret(configtesting.WithModifiedNamespace),
		},
		{
			name:      "No Auth Config In Secret (No Modifications)",
			newSecret: configtesting.NewKafkaSecret(configtesting.WithMissingConfig),
		},
	}

	// Run The Filtered TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			// Create A Test Dispatcher To Perform Tests Against
			dispatcher := createTestDispatcher(t, brokers, baseSaramaConfig)
			impl := dispatcher.(*DispatcherImpl)
			impl.subscribers = map[types.UID]*SubscriberWrapper{uid123: createSubscriberWrapper(uid123)}

			// Perform The Test
			dispatcher.SecretChanged(ctx, testCase.newSecret)
			assert.NotNil(t, impl)

			if testCase.expectEmptyUsername {
				// An empty username in the secret will force no-authorization even if it was enabled before
				assert.Equal(t, false, impl.SaramaConfig.Net.SASL.Enable)
				assert.Equal(t, "", impl.SaramaConfig.Net.SASL.User)
				assert.Equal(t, "", impl.SaramaConfig.Net.SASL.Password)
			} else if testCase.expectNewUsername != "" {
				assert.Equal(t, testCase.expectNewUsername, impl.SaramaConfig.Net.SASL.User)
			}
			if testCase.expectNewPassword != "" {
				assert.Equal(t, testCase.expectNewPassword, impl.SaramaConfig.Net.SASL.Password)
			}
			if testCase.expectNewSaslType != "" {
				assert.Equal(t, testCase.expectNewSaslType, string(impl.SaramaConfig.Net.SASL.Mechanism))
			}
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
		MetricsRegistry: config.MetricRegistry,
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

func TestConfigImpl_ObserveMetrics(t *testing.T) {
	baseSaramaConfig, err := commonclient.NewConfigBuilder().
		WithDefaults().
		FromYaml(clienttesting.DefaultSaramaConfigYaml).
		WithVersion(&sarama.V2_0_0_0).
		Build(context.Background())
	assert.Nil(t, err)

	reporter := &statsReporterMock{}

	// Create our own DispatcherImpl instead of using NewDispatcher(), so that we can start the metrics
	// reporting function with a very small interval
	dispatcher := &DispatcherImpl{
		DispatcherConfig: DispatcherConfig{
			Logger:          logtesting.TestLogger(t).Desugar(),
			MetricsRegistry: baseSaramaConfig.MetricRegistry,
			StatsReporter:   reporter,
		},
		MetricsStopChan:    make(chan struct{}),
		MetricsStoppedChan: make(chan struct{}),
	}

	// Start the metrics observing loop and verify that the report function was called at least once
	dispatcher.ObserveMetrics(5 * time.Millisecond)
	assert.Eventually(t, reporter.GetReportCalled, time.Second, 5*time.Millisecond)
	close(dispatcher.MetricsStopChan)
	<-dispatcher.MetricsStoppedChan
}

// A mock for the StatsReporter that will provide feedback when the Report function is called
type statsReporterMock struct {
	reportCalled bool
	mutex        sync.Mutex // Prevent race conditions between writing the value and assert.Eventually reading it
}

func (s *statsReporterMock) GetReportCalled() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.reportCalled
}

func (s *statsReporterMock) Report(_ metrics.ReportingList) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.reportCalled = true
}

// Shutdown is required to implement the StatsReporter interface
func (s *statsReporterMock) Shutdown() {
}
