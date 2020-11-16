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
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
	kafkaconsumer "knative.dev/eventing-kafka/pkg/common/kafka/consumer"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/channel"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"
)

// Test Data
const (
	id123         = "123"
	id456         = "456"
	id789         = "789"
	uid123        = types.UID(id123)
	uid456        = types.UID(id456)
	uid789        = types.UID(id789)
	TestConfigNet = `
Net:
  TLS:
    Config:
      ClientAuth: 0
  SASL:
    Mechanism: PLAIN
    Version: 1
`
	TestConfigConsumer = `
Consumer:
  Offsets:
    AutoCommit:
        Interval: 5000000000
    Retention: 604800000000000
  Return:
    Errors: true
`

	TestConfigMeta = `
Metadata:
  RefreshFrequency: 300000000000`

	TestConfigBase = TestConfigNet + TestConfigMeta + TestConfigConsumer

	TestConfigMetadataChange = TestConfigNet + `
Metadata:
  RefreshFrequency: 200000` + TestConfigConsumer

	TestConfigProducerChange = TestConfigNet + TestConfigMeta + `
Producer:
  MaxMessageBytes: 300` + TestConfigConsumer

	TestConfigConsumerAdd = TestConfigNet + TestConfigMeta + TestConfigConsumer + `
  Fetch:
    Min: 200
`

	TestConfigConsumerChange = TestConfigNet + TestConfigMeta + `
Consumer:
  Offsets:
    AutoCommit:
        Interval: 5000000000
    Retention: 604800000000001
  Return:
    Errors: true
`

	TestConfigAdminChange = `
Admin:
  Retry:
    Max: 100` + TestConfigNet + TestConfigMeta + TestConfigConsumer

	TestEventingKafka = `
kafka:
  enableSaramaLogging: true`
)

// Test The NewSubscriberWrapper() Functionality
func TestNewSubscriberWrapper(t *testing.T) {

	// Test Data
	subscriber := eventingduck.SubscriberSpec{UID: uid123}
	groupId := "TestGroupId"
	consumerGroup := kafkatesting.NewMockConsumerGroup(t)

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

	// Test Data
	dispatcherConfig := DispatcherConfig{}

	// Perform The Test
	dispatcher := NewDispatcher(dispatcherConfig)

	// Verify The Results
	assert.NotNil(t, dispatcher)
}

// Test The Dispatcher's Shutdown() Functionality
func TestShutdown(t *testing.T) {

	// Create Mock ConsumerGroups To Register Close() Requests
	consumerGroup1 := kafkatesting.NewMockConsumerGroup(t)
	consumerGroup2 := kafkatesting.NewMockConsumerGroup(t)
	consumerGroup3 := kafkatesting.NewMockConsumerGroup(t)

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

func getSaramaConfigFromYaml(t *testing.T, saramaYaml string) *sarama.Config {
	var config *sarama.Config
	jsonSettings, err := yaml.YAMLToJSON([]byte(saramaYaml))
	assert.Nil(t, err)
	assert.Nil(t, json.Unmarshal(jsonSettings, &config))
	return config
}

// Test The UpdateSubscriptions() Functionality
func TestUpdateSubscriptions(t *testing.T) {

	// Define The TestCase Struct
	type fields struct {
		DispatcherConfig DispatcherConfig
		subscribers      map[types.UID]*SubscriberWrapper
	}
	type args struct {
		subscriberSpecs []eventingduck.SubscriberSpec
	}
	type testCase struct {
		name   string
		fields fields
		args   args
		want   map[eventingduck.SubscriberSpec]error
	}

	// Define The Test Cases
	tests := []testCase{
		{
			name: "Add First Subscription",
			fields: fields{
				DispatcherConfig: DispatcherConfig{
					SaramaConfig: getSaramaConfigFromYaml(t, TestConfigBase),
					Logger:       logtesting.TestLogger(t).Desugar(),
				},
				subscribers: map[types.UID]*SubscriberWrapper{},
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
				DispatcherConfig: DispatcherConfig{
					SaramaConfig: getSaramaConfigFromYaml(t, TestConfigBase),
					Logger:       logtesting.TestLogger(t).Desugar(),
				},
				subscribers: map[types.UID]*SubscriberWrapper{
					uid123: createSubscriberWrapper(t, uid123),
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
				DispatcherConfig: DispatcherConfig{
					SaramaConfig: getSaramaConfigFromYaml(t, TestConfigBase),
					Logger:       logtesting.TestLogger(t).Desugar(),
				},
				subscribers: map[types.UID]*SubscriberWrapper{
					uid123: createSubscriberWrapper(t, uid123),
					uid456: createSubscriberWrapper(t, uid456),
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
				DispatcherConfig: DispatcherConfig{
					SaramaConfig: getSaramaConfigFromYaml(t, TestConfigBase),
					Logger:       logtesting.TestLogger(t).Desugar(),
				},
				subscribers: map[types.UID]*SubscriberWrapper{
					uid123: createSubscriberWrapper(t, uid123),
					uid456: createSubscriberWrapper(t, uid456),
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
				DispatcherConfig: DispatcherConfig{
					SaramaConfig: getSaramaConfigFromYaml(t, TestConfigBase),
					Logger:       logtesting.TestLogger(t).Desugar(),
				},
				subscribers: map[types.UID]*SubscriberWrapper{
					uid123: createSubscriberWrapper(t, uid123),
				},
			},
			args: args{
				subscriberSpecs: []eventingduck.SubscriberSpec{},
			},
			want: map[eventingduck.SubscriberSpec]error{},
		},
	}

	// Execute The Test Cases (Create A DispatcherImpl & UpdateSubscriptions() :)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Mock ConsumerGroup To Test With
			consumerGroup := kafkatesting.NewMockConsumerGroup(t)

			// Replace The NewConsumerGroupWrapper With Mock For Testing & Restore After TestCase
			newConsumerGroupWrapperPlaceholder := kafkaconsumer.NewConsumerGroupWrapper
			kafkaconsumer.NewConsumerGroupWrapper = func(brokersArg []string, groupIdArg string, configArg *sarama.Config) (sarama.ConsumerGroup, error) {
				return consumerGroup, nil
			}
			defer func() {
				kafkaconsumer.NewConsumerGroupWrapper = newConsumerGroupWrapperPlaceholder
			}()

			// Create A New DispatcherImpl To Test
			dispatcher := &DispatcherImpl{
				DispatcherConfig: tt.fields.DispatcherConfig,
				subscribers:      tt.fields.subscribers,
			}

			// Perform The Test
			got := dispatcher.UpdateSubscriptions(tt.args.subscriberSpecs)

			// Verify Results
			assert.Equal(t, tt.want, got)

			// Verify The Dispatcher's Tracking Of Subscribers Matches Specified State
			assert.Len(t, dispatcher.subscribers, len(tt.args.subscriberSpecs))
			for _, subscriber := range tt.args.subscriberSpecs {
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

// Utility Function For Creating A SubscriberWrapper With Specified UID & Mock ConsumerGroup
func createSubscriberWrapper(t *testing.T, uid types.UID) *SubscriberWrapper {
	return NewSubscriberWrapper(eventingduck.SubscriberSpec{UID: uid}, fmt.Sprintf("kafka.%s", string(uid)), kafkatesting.NewMockConsumerGroup(t))
}

func getBaseConfigMap() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      commonconfig.SettingsConfigMapName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{
			commonconfig.SaramaSettingsConfigKey: TestConfigBase,
		},
	}
}

// Test The Dispatcher's ConfigChanged Functionality
func TestConfigChanged(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()

	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))

	// Create Mocks
	var dispatcher Dispatcher
	dispatcher = &DispatcherImpl{
		DispatcherConfig:  DispatcherConfig{Logger: logger},
		subscribers:       make(map[types.UID]*SubscriberWrapper),
		messageDispatcher: channel.NewMessageDispatcher(logger),
	}

	// Apply a change to the Consumer config
	dispatcher = runConfigChangedTest(t, dispatcher, getBaseConfigMap(), TestConfigConsumerChange, "", true)

	// Apply an additional setting to the Consumer config
	dispatcher = runConfigChangedTest(t, dispatcher, getBaseConfigMap(), TestConfigConsumerAdd, "", true)

	// Change one of the metadata settings
	dispatcher = runConfigChangedTest(t, dispatcher, getBaseConfigMap(), TestConfigMetadataChange, "", true)

	// Change one of the admin settings
	dispatcher = runConfigChangedTest(t, dispatcher, getBaseConfigMap(), TestConfigAdminChange, "", true)

	// Verify that Producer changes do not cause Reconfigure to be called
	dispatcher = runConfigChangedTest(t, dispatcher, getBaseConfigMap(), TestConfigProducerChange, "", false)

	// Verify that having eventing-kafka settings in the configmap doesn't cause trouble
	dispatcher = runConfigChangedTest(t, dispatcher, getBaseConfigMap(), TestConfigBase, TestEventingKafka, false)
	assert.NotNil(t, dispatcher)
}

func runConfigChangedTest(t *testing.T, originalDispatcher Dispatcher, base *corev1.ConfigMap, changed string, eventingKafka string, expectedNewDispatcher bool) Dispatcher {
	// Change the Consumer settings to the base config
	newDispatcher := originalDispatcher.ConfigChanged(base)
	if newDispatcher != nil {
		// Simulate what happens in main() when the dispatcher changes
		originalDispatcher = newDispatcher
	}

	// Alter the configmap to use the changed settings
	newConfig := base
	newConfig.Data[commonconfig.SaramaSettingsConfigKey] = changed
	newConfig.Data[commonconfig.EventingKafkaSettingsConfigKey] = eventingKafka

	// Inform the Dispatcher that the config has changed to the new settings
	newDispatcher = originalDispatcher.ConfigChanged(newConfig)

	// Verify that a new dispatcher was created or not, as expected
	assert.Equal(t, expectedNewDispatcher, newDispatcher != nil)

	// Return either the new or original dispatcher for use by the rest of the TestConfigChanged test
	if expectedNewDispatcher {
		return newDispatcher
	}
	return originalDispatcher
}
