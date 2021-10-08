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

package dispatcher

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"

	"knative.dev/eventing-kafka/pkg/common/config"

	"github.com/Shopify/sarama"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
	"knative.dev/eventing-kafka/pkg/common/consumer"
	eventingchannels "knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/channel/fanout"
	klogtesting "knative.dev/pkg/logging/testing"
	_ "knative.dev/pkg/system/testing"
)

// ----- Mocks

type mockKafkaConsumerFactory struct {
	// createErr will return an error when creating a consumer
	createErr bool
}

func (c mockKafkaConsumerFactory) StartConsumerGroup(ctx context.Context, groupID string, topics []string, handler consumer.KafkaConsumerHandler, ref types.NamespacedName, options ...consumer.SaramaConsumerHandlerOption) (sarama.ConsumerGroup, error) {
	if c.createErr {
		return nil, errors.New("error creating consumer")
	}

	return mockConsumerGroup{}, nil
}

var _ consumer.KafkaConsumerGroupFactory = (*mockKafkaConsumerFactory)(nil)

type mockConsumerGroup struct{}

func (m mockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	return nil
}

func (m mockConsumerGroup) Errors() <-chan error {
	return nil
}

func (m mockConsumerGroup) Close() error {
	return nil
}

var _ sarama.ConsumerGroup = (*mockConsumerGroup)(nil)

// ----- Tests

func (d *KafkaDispatcher) getHostToChannelMap() map[string]eventingchannels.ChannelReference {
	m := make(map[string]eventingchannels.ChannelReference)
	d.hostToChannelMap.Range(func(key, value interface{}) bool {
		m[key.(string)] = value.(eventingchannels.ChannelReference)
		return true
	})
	return m
}

func TestKafkaDispatcher_RegisterChannelHost(t *testing.T) {
	firstChannelConfig := &ChannelConfig{
		Namespace: "default",
		Name:      "test-channel-1",
		HostName:  "a.b.c.d",
	}
	secondChannelConfig := &ChannelConfig{
		Namespace: "default",
		Name:      "test-channel-2",
		HostName:  "a.b.c.d",
	}

	d := &KafkaDispatcher{
		kafkaConsumerFactory: &mockKafkaConsumerFactory{},
		channelSubscriptions: make(map[types.NamespacedName]*KafkaSubscription),
		subsConsumerGroups:   make(map[types.UID]sarama.ConsumerGroup),
		subscriptions:        make(map[types.UID]Subscription),
		topicFunc:            utils.TopicName,
		logger:               zaptest.NewLogger(t).Sugar(),
	}

	require.NoError(t, d.RegisterChannelHost(firstChannelConfig))
	require.Error(t, d.RegisterChannelHost(secondChannelConfig))
}

func TestKafkaDispatcher_RegisterSameChannelTwiceShouldNotFail(t *testing.T) {
	channelConfig := &ChannelConfig{
		Namespace: "default",
		Name:      "test-channel-1",
		HostName:  "a.b.c.d",
	}

	d := &KafkaDispatcher{
		kafkaConsumerFactory: &mockKafkaConsumerFactory{},
		channelSubscriptions: make(map[types.NamespacedName]*KafkaSubscription),
		subsConsumerGroups:   make(map[types.UID]sarama.ConsumerGroup),
		subscriptions:        make(map[types.UID]Subscription),
		topicFunc:            utils.TopicName,
		logger:               zaptest.NewLogger(t).Sugar(),
	}

	require.NoError(t, d.RegisterChannelHost(channelConfig))
	require.Contains(t, d.getHostToChannelMap(), "a.b.c.d")
	require.NoError(t, d.RegisterChannelHost(channelConfig))
	require.Contains(t, d.getHostToChannelMap(), "a.b.c.d")
}

func TestDispatcher_UpdateConsumers(t *testing.T) {
	subscriber, _ := url.Parse("http://test/subscriber")

	testCases := []struct {
		name             string
		oldConfig        *ChannelConfig
		newConfig        *ChannelConfig
		subscribes       []string
		unsubscribes     []string
		createErr        string
		oldHostToChanMap map[string]eventingchannels.ChannelReference
		newHostToChanMap map[string]eventingchannels.ChannelReference
	}{
		{
			name:             "nil config",
			oldConfig:        &ChannelConfig{},
			newConfig:        nil,
			createErr:        "nil config",
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{},
		},
		{
			name:             "same config",
			oldConfig:        &ChannelConfig{},
			newConfig:        &ChannelConfig{},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{},
		},
		{
			name:      "config with no subscription",
			oldConfig: &ChannelConfig{},
			newConfig: &ChannelConfig{
				Namespace: "default",
				Name:      "test-channel",
				HostName:  "a.b.c.d",
			},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel", Namespace: "default"},
			},
		},
		{
			name:      "single channel w/ new subscriptions",
			oldConfig: &ChannelConfig{},
			newConfig: &ChannelConfig{
				Namespace: "default",
				Name:      "test-channel",
				HostName:  "a.b.c.d",
				Subscriptions: []Subscription{
					{
						UID: "subscription-1",
						Subscription: fanout.Subscription{
							Subscriber: subscriber,
						},
					},
					{
						UID: "subscription-2",
						Subscription: fanout.Subscription{
							Subscriber: subscriber,
						},
					},
				},
			},
			subscribes:       []string{"subscription-1", "subscription-2"},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel", Namespace: "default"},
			},
		},
		{
			name: "single channel w/ existing subscriptions",
			oldConfig: &ChannelConfig{
				Namespace: "default",
				Name:      "test-channel",
				HostName:  "a.b.c.d",
				Subscriptions: []Subscription{
					{
						UID: "subscription-1",
						Subscription: fanout.Subscription{
							Subscriber: subscriber,
						},
					},
					{
						UID: "subscription-2",
						Subscription: fanout.Subscription{
							Subscriber: subscriber,
						},
					},
				},
			},
			newConfig: &ChannelConfig{
				Namespace: "default",
				Name:      "test-channel",
				HostName:  "a.b.c.d",
				Subscriptions: []Subscription{
					{
						UID: "subscription-2",
						Subscription: fanout.Subscription{
							Subscriber: subscriber,
						},
					},
					{
						UID: "subscription-3",
						Subscription: fanout.Subscription{
							Subscriber: subscriber,
						},
					},
				},
			},
			subscribes:   []string{"subscription-2", "subscription-3"},
			unsubscribes: []string{"subscription-1"},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel", Namespace: "default"},
			},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel", Namespace: "default"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			t.Logf("Running %s", t.Name())
			d := &KafkaDispatcher{
				kafkaConsumerFactory: &mockKafkaConsumerFactory{},
				channelSubscriptions: make(map[types.NamespacedName]*KafkaSubscription),
				subsConsumerGroups:   make(map[types.UID]sarama.ConsumerGroup),
				subscriptions:        make(map[types.UID]Subscription),
				topicFunc:            utils.TopicName,
				logger:               zaptest.NewLogger(t).Sugar(),
			}

			ctx := context.TODO()

			// Initialize using oldConfig
			require.NoError(t, d.RegisterChannelHost(tc.oldConfig))
			require.NoError(t, d.ReconcileConsumers(ctx, tc.oldConfig))

			oldSubscribers := sets.NewString()
			for _, sub := range d.subscriptions {
				oldSubscribers.Insert(string(sub.UID))
			}
			if diff := sets.NewString(tc.unsubscribes...).Difference(oldSubscribers); diff.Len() != 0 {
				t.Errorf("subscriptions %+v were never subscribed", diff)
			}
			if diff := cmp.Diff(tc.oldHostToChanMap, d.getHostToChannelMap()); diff != "" {
				t.Errorf("unexpected hostToChannelMap (-want, +got) = %v", diff)
			}

			// Update with new config
			err := d.ReconcileConsumers(ctx, tc.newConfig)
			if tc.createErr != "" {
				if err == nil {
					t.Errorf("Expected UpdateConfig error: '%v'. Actual nil", tc.createErr)
				} else if err.Error() != tc.createErr {
					t.Errorf("Unexpected UpdateConfig error. Expected '%v'. Actual '%v'", tc.createErr, err)
				}
				return
			} else if err != nil {
				t.Errorf("Unexpected UpdateConfig error. Expected nil. Actual '%v'", err)
			}

			var newSubscribers []string
			for id := range d.subscriptions {
				newSubscribers = append(newSubscribers, string(id))
			}

			if diff := cmp.Diff(tc.subscribes, newSubscribers, sortStrings); diff != "" {
				t.Errorf("unexpected subscribers (-want, +got) = %v", diff)
			}
			if diff := cmp.Diff(tc.newHostToChanMap, d.getHostToChannelMap()); diff != "" {
				t.Errorf("unexpected hostToChannelMap (-want, +got) = %v", diff)
			}

		})
	}
}

func TestDispatcher_MultipleChannelsInParallel(t *testing.T) {
	subscriber, _ := url.Parse("http://test/subscriber")

	configs := []*ChannelConfig{
		{
			Namespace: "default",
			Name:      "test-channel",
			HostName:  "a.b.c.d",
		},
		{
			Namespace: "default",
			Name:      "test-channel-1",
			HostName:  "x.y.w.z",
			Subscriptions: []Subscription{
				{
					UID: "subscription-1",
					Subscription: fanout.Subscription{
						Subscriber: subscriber,
					},
				},
			},
		},
		{
			Namespace: "default",
			Name:      "test-channel-2",
			HostName:  "e.f.g.h",
			Subscriptions: []Subscription{
				{
					UID: "subscription-3",
					Subscription: fanout.Subscription{
						Subscriber: subscriber,
					},
				},
				{
					UID: "subscription-4",
					Subscription: fanout.Subscription{
						Subscriber: subscriber,
					},
				},
			},
		},
	}

	d := &KafkaDispatcher{
		kafkaConsumerFactory: &mockKafkaConsumerFactory{},
		channelSubscriptions: make(map[types.NamespacedName]*KafkaSubscription),
		subsConsumerGroups:   make(map[types.UID]sarama.ConsumerGroup),
		subscriptions:        make(map[types.UID]Subscription),
		topicFunc:            utils.TopicName,
		logger:               zaptest.NewLogger(t).Sugar(),
	}

	ctx := context.TODO()

	// Let's register channel configs first
	for _, c := range configs {
		require.NoError(t, d.RegisterChannelHost(c))
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ { // Let's reiterate several times to check everything is fine
		for _, c := range configs {
			wg.Add(1)
			go func(c *ChannelConfig) {
				defer wg.Done()
				assert.NoError(t, d.ReconcileConsumers(ctx, c))
			}(c)
		}
	}
	wg.Wait()

	// Assert the state is the final one we want
	require.Contains(t, d.getHostToChannelMap(), "a.b.c.d")
	require.Contains(t, d.getHostToChannelMap(), "x.y.w.z")
	require.Contains(t, d.getHostToChannelMap(), "e.f.g.h")

	require.Contains(t, d.subscriptions, types.UID("subscription-1"))
	require.Contains(t, d.subscriptions, types.UID("subscription-3"))
	require.Contains(t, d.subscriptions, types.UID("subscription-4"))

	// Now let's remove all of them
	wg = sync.WaitGroup{}
	for _, c := range configs {
		wg.Add(1)
		go func(c *ChannelConfig) {
			defer wg.Done()
			assert.NoError(t, d.CleanupChannel(c.Name, c.Namespace, c.HostName))
		}(c)
	}
	wg.Wait()

	require.Empty(t, d.getHostToChannelMap())
	require.Empty(t, d.subscriptions)
	require.Empty(t, d.channelSubscriptions)
	require.Empty(t, d.subsConsumerGroups)
}

func TestKafkaDispatcher_CleanupChannel(t *testing.T) {
	subscriber, _ := url.Parse("http://test/subscriber")

	d := &KafkaDispatcher{
		kafkaConsumerFactory: &mockKafkaConsumerFactory{},
		channelSubscriptions: make(map[types.NamespacedName]*KafkaSubscription),
		subsConsumerGroups:   make(map[types.UID]sarama.ConsumerGroup),
		subscriptions:        make(map[types.UID]Subscription),
		topicFunc:            utils.TopicName,
		logger:               zaptest.NewLogger(t).Sugar(),
	}

	ctx := context.TODO()

	channelConfig := &ChannelConfig{
		Namespace: "default",
		Name:      "test-channel",
		HostName:  "a.b.c.d",
		Subscriptions: []Subscription{
			{
				UID: "subscription-1",
				Subscription: fanout.Subscription{
					Subscriber: subscriber,
				},
			},
			{
				UID: "subscription-2",
				Subscription: fanout.Subscription{
					Subscriber: subscriber,
				},
			},
		},
	}
	require.NoError(t, d.RegisterChannelHost(channelConfig))
	require.NoError(t, d.ReconcileConsumers(ctx, channelConfig))

	require.NoError(t, d.CleanupChannel(channelConfig.Name, channelConfig.Namespace, channelConfig.HostName))
	require.NotContains(t, d.subscriptions, "subscription-1")
	require.NotContains(t, d.subscriptions, "subscription-2")
	require.NotContains(t, d.channelSubscriptions, eventingchannels.ChannelReference{
		Namespace: "default",
		Name:      "test-channel",
	})
	require.NotContains(t, d.subsConsumerGroups, "subscription-1")
	require.NotContains(t, d.subsConsumerGroups, "subscription-2")
}

func TestSubscribeError(t *testing.T) {
	cf := &mockKafkaConsumerFactory{createErr: true}
	d := &KafkaDispatcher{
		kafkaConsumerFactory: cf,
		logger:               zap.NewNop().Sugar(),
		topicFunc:            utils.TopicName,
		subscriptions:        map[types.UID]Subscription{},
		channelSubscriptions: map[types.NamespacedName]*KafkaSubscription{},
	}

	ctx := context.TODO()

	channelRef := types.NamespacedName{
		Name:      "test-channel",
		Namespace: "test-ns",
	}

	subRef := Subscription{
		UID:          "test-sub",
		Subscription: fanout.Subscription{},
	}
	err := d.subscribe(ctx, channelRef, subRef)
	if err == nil {
		t.Errorf("Expected error want %s, got %s", "error creating consumer", err)
	}
}

func TestUnsubscribeUnknownSub(t *testing.T) {
	cf := &mockKafkaConsumerFactory{createErr: true}
	d := &KafkaDispatcher{
		kafkaConsumerFactory: cf,
		logger:               zap.NewNop().Sugar(),
	}

	channelRef := types.NamespacedName{
		Name:      "test-channel",
		Namespace: "test-ns",
	}

	subRef := Subscription{
		UID:          "test-sub",
		Subscription: fanout.Subscription{},
	}
	if err := d.unsubscribe(channelRef, subRef); err != nil {
		t.Errorf("Unsubscribe error: %v", err)
	}
}

func TestKafkaDispatcher_Start(t *testing.T) {
	d := &KafkaDispatcher{}
	err := d.Start(context.TODO())
	if err == nil {
		t.Errorf("Expected error want %s, got %s", "message receiver is not set", err)
	}
}

func TestNewDispatcher(t *testing.T) {
	args := &KafkaDispatcherArgs{
		Config:    &config.EventingKafkaConfig{},
		Brokers:   []string{"localhost:10000"},
		TopicFunc: utils.TopicName,
	}
	_, err := NewDispatcher(context.TODO(), args, func(ref types.NamespacedName) {})
	if err == nil {
		t.Errorf("Expected error want %s, got %s", "message receiver is not set", err)
	}
}

func TestSetReady(t *testing.T) {
	logger := klogtesting.TestLogger(t)
	testCases := []struct {
		name             string
		ready            bool
		subID            types.UID
		partition        int32
		originalKafkaSub *KafkaSubscription
		desiredKafkaSub  *KafkaSubscription
	}{
		{
			name:      "doesn't have the sub, add it (on ready)",
			ready:     true,
			subID:     "foo",
			partition: 0,
			originalKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: map[string]sets.Int32{"bar": sets.NewInt32(0)},
			},
			desiredKafkaSub: &KafkaSubscription{
				subs: sets.NewString(),
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
					"foo": sets.NewInt32(0),
				},
			},
		},
		{
			name:      "has the sub but not the partition, add it (on ready)",
			ready:     true,
			subID:     "foo",
			partition: 1,
			originalKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
					"foo": sets.NewInt32(0),
				},
			},
			desiredKafkaSub: &KafkaSubscription{
				subs: sets.NewString(),
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
					"foo": sets.NewInt32(0, 1),
				},
			},
		},
		{
			name:      "has the sub and partition already (on ready)",
			ready:     true,
			subID:     "foo",
			partition: 0,
			originalKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
					"foo": sets.NewInt32(0),
				}},
			desiredKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
					"foo": sets.NewInt32(0),
				}},
		},
		{
			name:      "has the sub with two partition, delete one (on !ready)",
			ready:     false,
			subID:     "foo",
			partition: 1,
			originalKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
					"foo": sets.NewInt32(0, 1),
				},
			},
			desiredKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
					"foo": sets.NewInt32(0),
				},
			},
		},
		{
			name:      "has the sub with one partition, delete sub (on !ready)",
			ready:     false,
			subID:     "foo",
			partition: 0,
			originalKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
					"foo": sets.NewInt32(0),
				},
			},
			desiredKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
				},
			},
		},
		{
			name:      "doesn't have the sub to delete (on !ready)",
			ready:     false,
			subID:     "foo",
			partition: 0,
			originalKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
				}},
			desiredKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: map[string]sets.Int32{
					"bar": sets.NewInt32(0),
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Running %s", t.Name())
			tc.originalKafkaSub.logger = logger
			tc.originalKafkaSub.SetReady(tc.subID, tc.partition, tc.ready)
			if diff := cmp.Diff(tc.desiredKafkaSub.channelReadySubscriptions, tc.originalKafkaSub.channelReadySubscriptions); diff != "" {
				t.Errorf("unexpected ChannelReadySubscription (-want, +got) = %v", diff)
			}
		})
	}
}

var sortStrings = cmpopts.SortSlices(func(x, y string) bool {
	return x < y
})
