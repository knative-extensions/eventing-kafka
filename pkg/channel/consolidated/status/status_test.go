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

var channelObjectMeta = metav1.ObjectMeta{
	Namespace: "default",
	Name:      "chan4prober",
}

const dispatcherReadySubHeader = "K-Subscriber-Status"

type ReadyPair struct {
	c v1beta1.KafkaChannel
	s eventingduckv1.SubscriberSpec
}

func TestProbeSinglePod(t *testing.T) {
	ch := getChannel(1)
	sub := getSubscription()
	var subscriptions = map[string][]int{
		string(sub.UID): {0},
	}

	// This should be called when we want the dispatcher to return a successful result
	successHandler := http.HandlerFunc(readyJSONHandler(t, subscriptions))

	// Probes only succeed if succeed is true
	var succeed atomic.Bool

	// This is a latch channel that will lock the handler goroutine until we drain it
	probeRequests := make(chan *http.Request)
	handler := func(w http.ResponseWriter, r *http.Request) {
		probeRequests <- r
		if !succeed.Load() {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		successHandler.ServeHTTP(w, r)
	}

	ts := getDispatcherServer(handler)
	defer ts.Close()

	lister := fakeProbeTargetLister{
		target: getTargetLister(t, ts.URL),
	}

	prober, ready := getProber(t, &lister)

	done := make(chan struct{})
	cancelled := prober.Start(done)
	defer func() {
		close(done)
		<-cancelled
	}()

	assertEventuallyReady(t, prober, ch, sub, ready, &succeed, &probeRequests)
}

func TestProbeListerFail(t *testing.T) {
	ch := getChannel(1)
	sub := getSubscription()

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

func TestSucceedAfterRefreshPodProbing(t *testing.T) {
	// We have a channel with three partitions
	ch := getChannel(3)
	sub := getSubscription()

	// Dispatcher D1 will return only one ready partition
	var subsD1 = map[string][]int{
		string(sub.UID): {0},
	}

	// Dispatcher D2 will return the three ready partitions
	var subsD2 = map[string][]int{
		string(sub.UID): {0, 1, 2},
	}

	// The success handler for dispatcher D1, will return one partition only
	successHandlerD1 := http.HandlerFunc(readyJSONHandler(t, subsD1))

	// This is a latch channel that will lock the handler goroutine until we drain it
	probeRequestsD1 := make(chan *http.Request)

	handlerD1 := func(w http.ResponseWriter, r *http.Request) {
		probeRequestsD1 <- r
		successHandlerD1.ServeHTTP(w, r)
	}

	serverD1 := getDispatcherServer(handlerD1)
	defer serverD1.Close()

	// Probes only succeed if succeed is true
	var succeed atomic.Bool

	// The success handler for dispatcher D2, will return all three needed partitions
	probeHandlerD2 := http.HandlerFunc(readyJSONHandler(t, subsD2))

	// This is a latch channel that will lock the handler goroutine until we drain it
	probeRequestsD2 := make(chan *http.Request)
	handlerD2 := func(w http.ResponseWriter, r *http.Request) {
		probeRequestsD2 <- r
		if !succeed.Load() {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		probeHandlerD2.ServeHTTP(w, r)
	}

	serverD2 := getDispatcherServer(handlerD2)
	defer serverD2.Close()

	// Initially, lister points to d1
	lister := fakeProbeTargetLister{
		target: getTargetLister(t, serverD1.URL),
	}

	prober, ready := getProber(t, &lister)

	done := make(chan struct{})
	cancelled := prober.Start(done)
	defer func() {
		close(done)
		<-cancelled
	}()

	// Assert we're not ready.
	assertNeverReady(t, prober, ch, sub, ready, &probeRequestsD1)

	// Switch to new dispatcher
	lister.target = getTargetLister(t, serverD2.URL)

	// Assert we're still not ready
	assertNeverReady(t, prober, ch, sub, ready, &probeRequestsD1)

	// Refresh the pod probing, now the prober should probe the new disptacher
	prober.RefreshPodProbing(context.Background())

	// Assert that probing will be successful eventually
	assertEventuallyReady(t, prober, ch, sub, ready, &succeed, &probeRequestsD2)
}

func assertNeverReady(t *testing.T, prober *Prober, ch *messagingv1beta1.KafkaChannel, sub *eventingduckv1.SubscriberSpec, ready chan *ReadyPair, probeRequests *chan *http.Request) {
	// The first call to IsReady must succeed and return false
	ok, err := prober.IsReady(context.Background(), *ch, *sub)
	if err != nil {
		t.Fatal("IsReady failed:", err)
	}
	if ok {
		t.Fatal("IsReady() returned true")
	}

	// Just drain the requests in the channel to not block the handler
	go func() {
		for range *probeRequests {
		}
	}()

	select {
	case <-ready:
		// Prober shouldn't be ready
		t.Fatal("Prober shouldn't be ready")
	case <-time.After(1 * time.Second):
		// Not ideal but it gives time to the prober to write to ready
		break
	}
}

func assertEventuallyReady(t *testing.T, prober *Prober, ch *messagingv1beta1.KafkaChannel, sub *eventingduckv1.SubscriberSpec, ready chan *ReadyPair, succeed *atomic.Bool, probeRequests *chan *http.Request) {

	// Since succeed is still false the prober shouldn't be ready
	assertNeverReady(t, prober, ch, sub, ready, probeRequests)

	// Make probes succeed
	succeed.Store(true)

	// Just drain the requests in the channel to not block the handler
	go func() {
		for range *probeRequests {
		}
	}()

	select {
	case <-ready:
		// Wait for the probing to eventually succeed
	case <-time.After(5 * time.Second):
		t.Error("Timed out waiting for probing to succeed.")
	}
}

func getDispatcherServer(handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(handler))
	return ts
}

func getChannel(partitionsNum int32) *v1beta1.KafkaChannel {
	return (&v1beta1.KafkaChannel{
		ObjectMeta: channelObjectMeta,
		Spec: v1beta1.KafkaChannelSpec{
			NumPartitions:     partitionsNum,
			ReplicationFactor: 1,
		},
	}).DeepCopy()
}

func getSubscription() *eventingduckv1.SubscriberSpec {
	return (&eventingduckv1.SubscriberSpec{
		UID:           types.UID("90713ffd-f527-42bf-b158-57630b68ebe2"),
		Generation:    1,
		SubscriberURI: getURL("http://subscr.ns.local"),
	}).DeepCopy()
}

func getURL(s string) *apis.URL {
	u, _ := apis.ParseURL(s)
	return u
}

// readyJSONHandler is a factory for a handler which responds with a JSON of the ready subscriptions
func readyJSONHandler(t *testing.T, subscriptions map[string][]int) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		channelRefName := channelObjectMeta.Name
		channelRefNamespace := channelObjectMeta.Namespace
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

func getTargetLister(t *testing.T, dURL string) *ProbeTarget {
	tsURL, err := url.Parse(dURL)
	if err != nil {
		t.Fatalf("Failed to parse URL %q: %v", dURL, err)
	}
	port, err := strconv.Atoi(tsURL.Port())
	if err != nil {
		t.Fatalf("Failed to parse port %q: %v", tsURL.Port(), err)
	}
	hostname := tsURL.Hostname()
	return &ProbeTarget{
		PodIPs:  sets.NewString(hostname),
		PodPort: strconv.Itoa(port),
		URL:     tsURL,
	}
}

func getProber(t *testing.T, lister ProbeTargetLister) (*Prober, chan *ReadyPair) {
	ready := make(chan *ReadyPair)
	prober := NewProber(
		zaptest.NewLogger(t).Sugar(),
		lister,
		func(c v1beta1.KafkaChannel, s eventingduckv1.SubscriberSpec) {
			ready <- &ReadyPair{
				c,
				s,
			}
		})
	return prober, ready
}

type fakeProbeTargetLister struct {
	target *ProbeTarget
}

func (l fakeProbeTargetLister) ListProbeTargets(ctx context.Context, kc messagingv1beta1.KafkaChannel) (*ProbeTarget, error) {
	return l.target, nil
}

type notFoundLister struct{}

func (l notFoundLister) ListProbeTargets(ctx context.Context, kc messagingv1beta1.KafkaChannel) (*ProbeTarget, error) {
	return nil, errors.New("not found")
}
