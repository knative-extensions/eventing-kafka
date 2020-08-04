package dispatcher

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
	kafkaconsumer "knative.dev/eventing-kafka/pkg/common/kafka/consumer"
	kafkatesting "knative.dev/eventing-kafka/pkg/common/kafka/testing"
	"knative.dev/eventing/pkg/apis/duck/v1alpha1"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	logtesting "knative.dev/pkg/logging/testing"
	"testing"
	"time"
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
	subscriber := v1alpha1.SubscriberSpec{UID: uid123}
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
	subscriber1 := v1alpha1.SubscriberSpec{UID: id123}
	subscriber2 := v1alpha1.SubscriberSpec{UID: id456}
	subscriber3 := v1alpha1.SubscriberSpec{UID: id789}
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
					Logger: logtesting.TestLogger(t).Desugar(),
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
					Logger: logtesting.TestLogger(t).Desugar(),
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
					Logger: logtesting.TestLogger(t).Desugar(),
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
					Logger: logtesting.TestLogger(t).Desugar(),
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
					Logger: logtesting.TestLogger(t).Desugar(),
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
