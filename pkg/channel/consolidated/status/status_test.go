/*
Copyright 2019 The Knative Authors.

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

package status

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap/zaptest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
)

var (
	channelTemplate = &v1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "chan4prober",
		},
		Spec: v1beta1.KafkaChannelSpec{
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}
	subscriptionTemplate = eventingduckv1.SubscriberSpec{
		UID:           types.UID("90713ffd-f527-42bf-b158-57630b68ebe2"),
		Generation:    1,
		SubscriberURI: getURL("http://subscr.ns.local"),
	}
)

const dispatcherReadySubHeader = "K-Subscriber-Status"

func getURL(s string) *apis.URL {
	u, _ := apis.ParseURL(s)
	return u
}

func handleProbe(t *testing.T) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		channelRefName := channelTemplate.ObjectMeta.Name
		channelRefNamespace := channelTemplate.ObjectMeta.Namespace
		var subscriptions = map[string][]int{
			string(subscriptionTemplate.UID): {0},
		}
		w.Header().Set(dispatcherReadySubHeader, channelRefName)
		jsonResult, err := json.Marshal(subscriptions)
		if err != nil {
			t.Fatalf("Error marshalling json for sub-status channelref: %s/%s, %v", channelRefNamespace, channelRefName, err)
		}
		_, err = w.Write(jsonResult)
		if err != nil {
			t.Fatalf("Error writing jsonResult to serveHTTP writer: %v", err)
		}
	}
}

type ReadyPair struct {
	c v1beta1.KafkaChannel
	s eventingduckv1.SubscriberSpec
}

func TestProbeSinglePod(t *testing.T) {
	var succeed atomic.Bool

	ch := channelTemplate.DeepCopy()
	sub := subscriptionTemplate.DeepCopy()

	probeHandler := http.HandlerFunc(handleProbe(t))

	// Probes only succeed if succeed is true
	probeRequests := make(chan *http.Request)
	finalHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probeRequests <- r
		if !succeed.Load() {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		probeHandler.ServeHTTP(w, r)
	})

	ts := httptest.NewServer(finalHandler)
	defer ts.Close()
	tsURL, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("Failed to parse URL %q: %v", ts.URL, err)
	}
	port, err := strconv.Atoi(tsURL.Port())
	if err != nil {
		t.Fatalf("Failed to parse port %q: %v", tsURL.Port(), err)
	}
	hostname := tsURL.Hostname()

	ready := make(chan *ReadyPair)

	prober := NewProber(
		zaptest.NewLogger(t).Sugar(),
		fakeProbeTargetLister{
			PodIPs:  sets.NewString(hostname),
			PodPort: strconv.Itoa(port),
			URL:     tsURL,
		},
		func(c v1beta1.KafkaChannel, s eventingduckv1.SubscriberSpec) {
			ready <- &ReadyPair{
				c,
				s,
			}
		})

	done := make(chan struct{})
	cancelled := prober.Start(done)
	defer func() {
		close(done)
		<-cancelled
	}()

	// The first call to IsReady must succeed and return false
	ok, err := prober.IsReady(context.Background(), *ch, *sub)
	if err != nil {
		t.Fatal("IsReady failed:", err)
	}
	if ok {
		t.Fatal("IsReady() returned true")
	}

	select {
	case <-ready:
		// Since succeed is still false and we don't return 200, the prober shouldn't be ready
		t.Fatal("Prober shouldn't be ready")
	case <-time.After(1 * time.Second):
		// Not ideal but it gives time to the prober to write to ready
		break
	}

	// Make probes to hostB succeed
	succeed.Store(true)

	// Just drain the requests in the channel to not block the handler
	go func() {
		for range probeRequests {
		}
	}()

	select {
	case <-ready:
		// Wait for the probing to eventually succeed
	case <-time.After(5 * time.Second):
		t.Error("Timed out waiting for probing to succeed.")
	}
}

func TestProbeListerFail(t *testing.T) {
	ch := channelTemplate.DeepCopy()
	sub := subscriptionTemplate.DeepCopy()

	ready := make(chan *ReadyPair)
	defer close(ready)
	prober := NewProber(
		zaptest.NewLogger(t).Sugar(),
		notFoundLister{},
		func(c v1beta1.KafkaChannel, s eventingduckv1.SubscriberSpec) {
			ready <- &ReadyPair{
				c,
				s,
			}
		})

	// If we can't list, this  must fail and return false
	ok, err := prober.IsReady(context.Background(), *ch, *sub)
	if err == nil {
		t.Fatal("IsReady returned unexpected success")
	}
	if ok {
		t.Fatal("IsReady() returned true")
	}
}

type fakeProbeTargetLister ProbeTarget

func (l fakeProbeTargetLister) ListProbeTargets(ctx context.Context, kc messagingv1beta1.KafkaChannel) (*ProbeTarget, error) {
	t := ProbeTarget(l)
	return &t, nil
}

type notFoundLister struct{}

func (l notFoundLister) ListProbeTargets(ctx context.Context, kc messagingv1beta1.KafkaChannel) (*ProbeTarget, error) {
	return nil, errors.New("not found")
}
