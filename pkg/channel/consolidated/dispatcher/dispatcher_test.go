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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
	"knative.dev/eventing-kafka/pkg/common/consumer"
	eventingchannels "knative.dev/eventing/pkg/channel"
	"knative.dev/eventing/pkg/channel/fanout"
	_ "knative.dev/pkg/system/testing"
)

// ----- Mocks

type mockKafkaConsumerFactory struct {
	// createErr will return an error when creating a consumer
	createErr bool
}

func (c mockKafkaConsumerFactory) StartConsumerGroup(groupID string, topics []string, logger *zap.SugaredLogger, handler consumer.KafkaConsumerHandler) (sarama.ConsumerGroup, error) {
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

// test util for various config checks
func (d *KafkaDispatcher) checkConfigAndUpdate(config *Config) error {
	if config == nil {
		return errors.New("nil config")
	}

	if _, err := d.UpdateKafkaConsumers(config); err != nil {
		// failed to update dispatchers consumers
		return err
	}
	if err := d.UpdateHostToChannelMap(config); err != nil {
		return err
	}

	return nil
}

func TestDispatcher_UpdateConfig(t *testing.T) {
	subscriber, _ := url.Parse("http://test/subscriber")

	testCases := []struct {
		name             string
		oldConfig        *Config
		newConfig        *Config
		subscribes       []string
		unsubscribes     []string
		createErr        string
		oldHostToChanMap map[string]eventingchannels.ChannelReference
		newHostToChanMap map[string]eventingchannels.ChannelReference
	}{
		{
			name:             "nil config",
			oldConfig:        &Config{},
			newConfig:        nil,
			createErr:        "nil config",
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{},
		},
		{
			name:             "same config",
			oldConfig:        &Config{},
			newConfig:        &Config{},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{},
		},
		{
			name:      "config with no subscription",
			oldConfig: &Config{},
			newConfig: &Config{
				ChannelConfigs: []ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel",
						HostName:  "a.b.c.d",
					},
				},
			},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel", Namespace: "default"},
			},
		},
		{
			name:      "single channel w/ new subscriptions",
			oldConfig: &Config{},
			newConfig: &Config{
				ChannelConfigs: []ChannelConfig{
					{
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
			oldConfig: &Config{
				ChannelConfigs: []ChannelConfig{
					{
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
				},
			},
			newConfig: &Config{
				ChannelConfigs: []ChannelConfig{
					{
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
		{
			name: "multi channel w/old and new subscriptions",
			oldConfig: &Config{
				ChannelConfigs: []ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel-1",
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
				},
			},
			newConfig: &Config{
				ChannelConfigs: []ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel-1",
						HostName:  "a.b.c.d",
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
				},
			},
			subscribes:   []string{"subscription-1", "subscription-3", "subscription-4"},
			unsubscribes: []string{"subscription-2"},
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel-1", Namespace: "default"},
			},
			newHostToChanMap: map[string]eventingchannels.ChannelReference{
				"a.b.c.d": {Name: "test-channel-1", Namespace: "default"},
				"e.f.g.h": {Name: "test-channel-2", Namespace: "default"},
			},
		},
		{
			name:      "Duplicate hostnames",
			oldConfig: &Config{},
			newConfig: &Config{
				ChannelConfigs: []ChannelConfig{
					{
						Namespace: "default",
						Name:      "test-channel-1",
						HostName:  "a.b.c.d",
					},
					{
						Namespace: "default",
						Name:      "test-channel-2",
						HostName:  "a.b.c.d",
					},
				},
			},
			createErr:        "duplicate hostName found. Each channel must have a unique host header. HostName:a.b.c.d, channel:default.test-channel-2, channel:default.test-channel-1",
			oldHostToChanMap: map[string]eventingchannels.ChannelReference{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Running %s", t.Name())
			d := &KafkaDispatcher{
				kafkaConsumerFactory: &mockKafkaConsumerFactory{},
				channelSubscriptions: make(map[eventingchannels.ChannelReference]*KafkaSubscription),
				subsConsumerGroups:   make(map[types.UID]sarama.ConsumerGroup),
				subscriptions:        make(map[types.UID]Subscription),
				topicFunc:            utils.TopicName,
				logger:               zaptest.NewLogger(t).Sugar(),
			}
			d.setHostToChannelMap(map[string]eventingchannels.ChannelReference{})

			// Initialize using oldConfig
			err := d.checkConfigAndUpdate(tc.oldConfig)
			if err != nil {

				t.Errorf("unexpected error: %v", err)
			}
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
			err = d.checkConfigAndUpdate(tc.newConfig)
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

func TestSubscribeError(t *testing.T) {
	cf := &mockKafkaConsumerFactory{createErr: true}
	d := &KafkaDispatcher{
		kafkaConsumerFactory: cf,
		logger:               zap.NewNop().Sugar(),
		topicFunc:            utils.TopicName,
	}

	channelRef := eventingchannels.ChannelReference{
		Name:      "test-channel",
		Namespace: "test-ns",
	}

	subRef := Subscription{
		UID:          "test-sub",
		Subscription: fanout.Subscription{},
	}
	err := d.subscribe(channelRef, subRef)
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

	channelRef := eventingchannels.ChannelReference{
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
		ClientID:  "kafka-ch-dispatcher",
		Brokers:   []string{"localhost:10000"},
		TopicFunc: utils.TopicName,
		Logger:    nil,
	}
	_, err := NewDispatcher(context.TODO(), args)
	if err == nil {
		t.Errorf("Expected error want %s, got %s", "message receiver is not set", err)
	}
}

func TestSetReady(t *testing.T) {
	testCases := []struct {
		name             string
		ready            bool
		subID            types.UID
		originalKafkaSub *KafkaSubscription
		desiredKafkaSub  *KafkaSubscription
	}{
		{
			name:  "doesn't have the sub, add it (on ready)",
			ready: true,
			subID: "foo",
			originalKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: sets.String{"bar": sets.Empty{}},
			},
			desiredKafkaSub: &KafkaSubscription{
				subs:                      []types.UID{},
				channelReadySubscriptions: sets.String{"bar": sets.Empty{}, "foo": sets.Empty{}},
			},
		},
		{
			name:  "has the sub already (on ready)",
			ready: true,
			subID: "foo",
			originalKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: sets.String{"foo": sets.Empty{}, "bar": sets.Empty{}},
			},
			desiredKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: sets.String{"foo": sets.Empty{}, "bar": sets.Empty{}},
			},
		},
		{
			name:  "has the sub, delete it (on !ready)",
			ready: false,
			subID: "foo",
			originalKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: sets.String{"foo": sets.Empty{}, "bar": sets.Empty{}},
			},
			desiredKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: sets.String{"bar": sets.Empty{}},
			},
		},
		{
			name:  "doesn't have the sub to delete (on !ready)",
			ready: false,
			subID: "foo",
			originalKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: sets.String{"bar": sets.Empty{}},
			},
			desiredKafkaSub: &KafkaSubscription{
				channelReadySubscriptions: sets.String{"bar": sets.Empty{}},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Running %s", t.Name())
			tc.originalKafkaSub.SetReady(tc.subID, tc.ready)
			if diff := cmp.Diff(tc.desiredKafkaSub.channelReadySubscriptions, tc.originalKafkaSub.channelReadySubscriptions); diff != "" {
				t.Errorf("unexpected ChannelReadySubscription (-want, +got) = %v", diff)
			}
		})
	}
}

func TestServeHTTP(t *testing.T) {

	httpGet := "GET"
	httpPost := "POST"
	testCases := []struct {
		name               string
		responseReturnCode int
		desiredJson        []byte
		channelSubs        map[eventingchannels.ChannelReference]*KafkaSubscription
		requestURI         string
		httpMethod         string
	}{
		{
			name:               "channelref not found",
			httpMethod:         httpGet,
			responseReturnCode: http.StatusNotFound,
			desiredJson:        []byte{},
			requestURI:         "/exist/thisDoesNot",
		}, {
			name:               "nop",
			httpMethod:         httpGet,
			responseReturnCode: http.StatusNotFound,
			desiredJson:        []byte{},
			requestURI:         "///",
		}, {
			name:               "no ready subscribers",
			httpMethod:         httpGet,
			responseReturnCode: http.StatusOK,
			desiredJson:        []byte(`{"bar/foo":[]}`),
			channelSubs: map[eventingchannels.ChannelReference]*KafkaSubscription{
				{Name: "foo", Namespace: "bar"}: {
					subs:                      []types.UID{},
					channelReadySubscriptions: sets.String{},
				},
			},
			requestURI: "/bar/foo",
		}, {
			name:               "different channelref called from populated channref (different ns)",
			httpMethod:         httpGet,
			desiredJson:        []byte{},
			responseReturnCode: http.StatusNotFound,
			channelSubs: map[eventingchannels.ChannelReference]*KafkaSubscription{
				{Name: "foo", Namespace: "baz"}: {
					subs:                      []types.UID{"a", "b"},
					channelReadySubscriptions: sets.String{"a": sets.Empty{}, "b": sets.Empty{}},
				},
			},
			requestURI: "/bar/foo",
		}, {
			name:               "return correct subscription",
			httpMethod:         httpGet,
			desiredJson:        []byte(`{"bar/foo":["a","b"]}`),
			responseReturnCode: http.StatusOK,
			channelSubs: map[eventingchannels.ChannelReference]*KafkaSubscription{
				{Name: "foo", Namespace: "bar"}: {
					subs:                      []types.UID{"a", "b"},
					channelReadySubscriptions: sets.String{"a": sets.Empty{}, "b": sets.Empty{}},
				},
			},
			requestURI: "/bar/foo",
		}, {
			name:               "return correct subscription from multiple chanrefs",
			httpMethod:         httpGet,
			desiredJson:        []byte(`{"bar/foo":["a","b"]}`),
			responseReturnCode: http.StatusOK,
			channelSubs: map[eventingchannels.ChannelReference]*KafkaSubscription{
				{Name: "table", Namespace: "flip"}: {
					subs:                      []types.UID{"c", "d"},
					channelReadySubscriptions: sets.String{"c": sets.Empty{}},
				},
				{Name: "foo", Namespace: "bar"}: {
					subs:                      []types.UID{"a", "b"},
					channelReadySubscriptions: sets.String{"a": sets.Empty{}, "b": sets.Empty{}},
				},
			},
			requestURI: "/bar/foo",
		}, {
			name:               "bad request uri",
			httpMethod:         httpGet,
			desiredJson:        []byte{},
			responseReturnCode: http.StatusNotFound,
			requestURI:         "/here/be/dragons/there/are/too/many/slashes",
		}, {
			name:               "bad request method (POST)",
			httpMethod:         httpPost,
			desiredJson:        []byte{},
			responseReturnCode: http.StatusMethodNotAllowed,
		},
	}
	d := &KafkaDispatcher{
		channelSubscriptions: make(map[eventingchannels.ChannelReference]*KafkaSubscription),
		logger:               zaptest.NewLogger(t).Sugar(),
	}
	ts := httptest.NewServer(d)
	defer ts.Close()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Running %s", t.Name())
			d.channelSubscriptions = tc.channelSubs

			request, _ := http.NewRequest(tc.httpMethod, fmt.Sprintf("%s%s", ts.URL, tc.requestURI), nil)
			//			resp, err := http.Get(fmt.Sprintf("%s%s", ts.URL, tc.requestURI))
			resp, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Errorf("Could not send request to subscriber endpoint: %v", err)
			}
			if resp.StatusCode != tc.responseReturnCode {
				t.Errorf("unepxected status returned: want: %d, got: %d", tc.responseReturnCode, resp.StatusCode)
			}
			respBody, err := ioutil.ReadAll(resp.Body)
			defer resp.Body.Close()
			if err != nil {
				t.Errorf("Could not read response from subscriber endpoint: %v", err)
			}
			if testing.Verbose() && len(respBody) > 0 {
				t.Logf("http response: %s\n", string(respBody))
			}
			if diff := cmp.Diff(tc.desiredJson, respBody); diff != "" {
				t.Errorf("unexpected readysubscriber status response: (-want, +got) = %v", diff)
			}
		})
	}
}

var sortStrings = cmpopts.SortSlices(func(x, y string) bool {
	return x < y
})
