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
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"

	v12 "knative.dev/eventing/pkg/apis/duck/v1"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"

	"knative.dev/networking/pkg/prober"
	"knative.dev/pkg/logging"
)

const (
	// probeConcurrency defines how many probing calls can be issued simultaneously
	probeConcurrency = 15
	// probeTimeout defines the maximum amount of time a request will wait
	probeTimeout = 1 * time.Second
	// initialDelay defines the delay before enqueuing a probing request the first time.
	// It gives times for the change to propagate and prevents unnecessary retries.
	initialDelay = 200 * time.Millisecond
)

var dialContext = (&net.Dialer{Timeout: probeTimeout}).DialContext

// targetState represents the probing state of a subscription
type targetState struct {
	hash string
	sub  v12.SubscriberSpec
	ch   v1beta1.KafkaChannel

	// pendingCount is the number of pods that haven't been successfully probed yet
	pendingCount atomic.Int32
	lastAccessed time.Time

	cancel func()
}

// podState represents the probing state of a Pod (for a specific subscription)
type podState struct {
	// pendingCount is the number of probes for the Pod
	pendingCount atomic.Int32

	cancel func()
}

// cancelContext is a pair of a Context and its cancel function
type cancelContext struct {
	context context.Context
	cancel  func()
}

type workItem struct {
	targetStates *targetState
	podState     *podState
	context      context.Context
	url          *url.URL
	podIP        string
	podPort      string
	logger       *zap.SugaredLogger
}

// ProbeTarget contains the URLs to probes for a set of Pod IPs serving out of the same port.
type ProbeTarget struct {
	PodIPs  sets.String
	PodPort string
	Port    string
	URLs    []*url.URL
}

// ProbeTargetLister lists all the targets that requires probing.
type ProbeTargetLister interface {
	// ListProbeTargets returns a list of targets to be probed
	ListProbeTargets(ctx context.Context, ch v1beta1.KafkaChannel) ([]ProbeTarget, error)
}

// Prober provides a way to check if a VirtualService is ready by probing the Envoy pods
// handling that VirtualService.
type Prober struct {
	logger *zap.SugaredLogger

	// mu guards targetStates and podContexts
	mu           sync.Mutex
	targetStates map[types.UID]*targetState
	podContexts  map[string]cancelContext

	workQueue workqueue.RateLimitingInterface

	targetLister ProbeTargetLister

	readyCallback func(v1beta1.KafkaChannel, v12.SubscriberSpec)

	probeConcurrency int

	opts []interface{}
}

// NewProber creates a new instance of Prober
func NewProber(
	logger *zap.SugaredLogger,
	targetLister ProbeTargetLister,
	readyCallback func(v1beta1.KafkaChannel, v12.SubscriberSpec), opts ...interface{}) *Prober {
	return &Prober{
		logger:       logger,
		targetStates: make(map[types.UID]*targetState),
		podContexts:  make(map[string]cancelContext),
		workQueue: workqueue.NewNamedRateLimitingQueue(
			workqueue.NewMaxOfRateLimiter(
				// Per item exponential backoff
				workqueue.NewItemExponentialFailureRateLimiter(50*time.Millisecond, 30*time.Second),
				// Global rate limiter
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(50), 100)},
			),
			"ProbingQueue"),
		targetLister:     targetLister,
		readyCallback:    readyCallback,
		probeConcurrency: probeConcurrency,
		opts:             opts,
	}
}

func computeHash(sub v12.SubscriberSpec) ([sha256.Size]byte, error) {
	bytes, err := json.Marshal(sub)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("failed to serialize Subscription: %w", err)
	}
	return sha256.Sum256(bytes), nil
}

func (m *Prober) IsReady(ctx context.Context, ch v1beta1.KafkaChannel, sub v12.SubscriberSpec) (bool, error) {
	subscriptionKey := sub.UID
	logger := logging.FromContext(ctx)

	bytes, err := computeHash(sub)
	if err != nil {
		return false, fmt.Errorf("failed to compute the hash of the Subscription: %w", err)
	}
	hash := fmt.Sprintf("%x", bytes)

	if ready, ok := func() (bool, bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if state, ok := m.targetStates[subscriptionKey]; ok {
			if state.hash == hash {
				state.lastAccessed = time.Now()
				return state.pendingCount.Load() == 0, true
			}

			// Cancel the polling for the outdated version
			state.cancel()
			delete(m.targetStates, subscriptionKey)
		}
		return false, false
	}(); ok {
		return ready, nil
	}

	subCtx, cancel := context.WithCancel(context.Background())
	subscriptionState := &targetState{
		hash:         hash,
		sub:          sub,
		ch:           ch,
		lastAccessed: time.Now(),
		cancel:       cancel,
	}

	// Get the probe targets and group them by IP
	targets, err := m.targetLister.ListProbeTargets(ctx, ch)
	if err != nil {
		return false, err
	}
	workItems := make(map[string][]*workItem)
	for _, target := range targets {
		for ip := range target.PodIPs {
			for _, url := range target.URLs {
				workItems[ip] = append(workItems[ip], &workItem{
					targetStates: subscriptionState,
					url:          url,
					podIP:        ip,
					podPort:      target.PodPort,
					logger:       logger,
				})
			}
		}
	}

	subscriptionState.pendingCount.Store(int32(len(workItems)))

	for ip, ipWorkItems := range workItems {
		// Get or create the context for that IP
		ipCtx := func() context.Context {
			m.mu.Lock()
			defer m.mu.Unlock()
			cancelCtx, ok := m.podContexts[ip]
			if !ok {
				ctx, cancel := context.WithCancel(context.Background())
				cancelCtx = cancelContext{
					context: ctx,
					cancel:  cancel,
				}
				m.podContexts[ip] = cancelCtx
			}
			return cancelCtx.context
		}()

		podCtx, cancel := context.WithCancel(subCtx)
		podState := &podState{
			pendingCount: *atomic.NewInt32(int32(len(ipWorkItems))),
			cancel:       cancel,
		}

		// Quick and dirty way to join two contexts (i.e. podCtx is cancelled when either subCtx or ipCtx are cancelled)
		go func() {
			select {
			case <-podCtx.Done():
				// This is the actual context, there is nothing to do except
				// break to avoid leaking this goroutine.
				break
			case <-ipCtx.Done():
				// Cancel podCtx
				cancel()
			}
		}()

		// Update the states when probing is cancelled
		go func() {
			<-podCtx.Done()
			m.onProbingCancellation(subscriptionState, podState)
		}()

		for _, wi := range ipWorkItems {
			wi.podState = podState
			wi.context = podCtx
			m.workQueue.AddAfter(wi, initialDelay)
			logger.Infof("Queuing probe for %s, IP: %s:%s (depth: %d)",
				wi.url, wi.podIP, wi.podPort, m.workQueue.Len())
		}
	}

	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.targetStates[subscriptionKey] = subscriptionState
	}()
	return len(workItems) == 0, nil
}

// Start starts the Manager background operations
func (m *Prober) Start(done <-chan struct{}) chan struct{} {
	var wg sync.WaitGroup

	// Start the worker goroutines
	for i := 0; i < m.probeConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m.processWorkItem() {
			}
		}()
	}

	// Stop processing the queue when cancelled
	go func() {
		<-done
		m.workQueue.ShutDown()
	}()

	// Return a channel closed when all work is done
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

// CancelProbing cancels probing of the provided Subscription
func (m *Prober) CancelProbing(sub v12.SubscriberSpec) {
	key := sub.UID
	m.mu.Lock()
	defer m.mu.Unlock()
	if state, ok := m.targetStates[key]; ok {
		state.cancel()
		delete(m.targetStates, key)
	}
}

// CancelPodProbing cancels probing of the provided Pod IP.
//
// TODO(#6269): make this cancellation based on Pod x port instead of just Pod.
func (m *Prober) CancelPodProbing(obj interface{}) {
	if pod, ok := obj.(*corev1.Pod); ok {
		m.mu.Lock()
		defer m.mu.Unlock()

		if ctx, ok := m.podContexts[pod.Status.PodIP]; ok {
			ctx.cancel()
			delete(m.podContexts, pod.Status.PodIP)
		}
	}
}

// processWorkItem processes a single work item from workQueue.
// It returns false when there is no more items to process, true otherwise.
func (m *Prober) processWorkItem() bool {
	obj, shutdown := m.workQueue.Get()
	if shutdown {
		return false
	}

	defer m.workQueue.Done(obj)

	// Crash if the item is not of the expected type
	item, ok := obj.(*workItem)
	if !ok {
		m.logger.Fatalf("Unexpected work item type: want: %s, got: %s\n",
			reflect.TypeOf(&workItem{}).Name(), reflect.TypeOf(obj).Name())
	}
	item.logger.Infof("Processing probe for %s, IP: %s:%s (depth: %d)",
		item.url, item.podIP, item.podPort, m.workQueue.Len())

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		//nolint:gosec
		// We only want to know that the Gateway is configured, not that the configuration is valid.
		// Therefore, we can safely ignore any TLS certificate validation.
		InsecureSkipVerify: true,
	}
	transport.DialContext = func(ctx context.Context, network, addr string) (conn net.Conn, e error) {
		// Requests with the IP as hostname and the Host header set do no pass client-side validation
		// because the HTTP client validates that the hostname (not the Host header) matches the server
		// TLS certificate Common Name or Alternative Names. Therefore, http.Request.URL is set to the
		// hostname and it is substituted it here with the target IP.
		return dialContext(ctx, network, net.JoinHostPort(item.podIP, item.podPort))
	}

	probeURL := deepCopy(item.url)

	ctx, cancel := context.WithTimeout(item.context, probeTimeout)
	defer cancel()
	var opts []interface{}
	opts = append(opts, m.opts...)
	opts = append(opts, m.probeVerifier(item))

	ok, err := prober.Do(
		ctx,
		transport,
		probeURL.String(),
		opts...)

	// In case of cancellation, drop the work item
	select {
	case <-item.context.Done():
		m.workQueue.Forget(obj)
		return true
	default:
	}

	if err != nil || !ok {
		// In case of error, enqueue for retry
		m.workQueue.AddRateLimited(obj)
		item.logger.Errorf("Probing of %s failed, IP: %s:%s, ready: %t, error: %v (depth: %d)",
			item.url, item.podIP, item.podPort, ok, err, m.workQueue.Len())
	} else {
		m.onProbingSuccess(item.targetStates, item.podState)
	}
	return true
}

func (m *Prober) onProbingSuccess(subscriptionState *targetState, podState *podState) {
	// The last probe call for the Pod succeeded, the Pod is ready
	if podState.pendingCount.Dec() == 0 {
		// Unlock the goroutine blocked on <-podCtx.Done()
		podState.cancel()

		// This is the last pod being successfully probed, the subscription is ready
		if subscriptionState.pendingCount.Dec() == 0 {
			m.readyCallback(subscriptionState.ch, subscriptionState.sub)
		}
	}
}

func (m *Prober) onProbingCancellation(subscriptionState *targetState, podState *podState) {
	for {
		pendingCount := podState.pendingCount.Load()
		if pendingCount <= 0 {
			// Probing succeeded, nothing to do
			return
		}

		// Attempt to set pendingCount to 0.
		if podState.pendingCount.CAS(pendingCount, 0) {
			// This is the last pod being successfully probed, the subscription is ready
			if subscriptionState.pendingCount.Dec() == 0 {
				m.readyCallback(subscriptionState.ch, subscriptionState.sub)
			}
			return
		}
	}
}

func (m *Prober) probeVerifier(item *workItem) prober.Verifier {
	return func(r *http.Response, b []byte) (bool, error) {
		//TODO Check if we need to use a hash
		switch r.StatusCode {
		case http.StatusOK:
			/**
			{"my-kafka-channel":["90713ffd-f527-42bf-b158-57630b68ebe2","a2041ec2-3295-4cd8-ac31-e699ab08273e","d3d70a79-8528-4df6-a812-3b559380cf08","db536b74-45f8-41cd-ab3e-7e3f60ed9e35","eb3aeee9-7cb5-4cad-b4c4-424e436dac9f"]}
			*/
			m.logger.Debug("Verifying response")
			var subscriptions = make(map[string][]string)
			err := json.Unmarshal(b, &subscriptions)
			if err != nil {
				m.logger.Errorw("Error unmarshaling", err)
				return false, err
			}
			m.logger.Debugw("Got response", zap.Any("Response", b))
			m.logger.Debugw("Got list", zap.Any("Unmarshaled", subscriptions))
			uid := string(item.targetStates.sub.UID)
			m.logger.Debugf("want %s", uid)
			key := fmt.Sprintf("%s/%s", item.targetStates.ch.Namespace, item.targetStates.ch.Name)
			if subs, ok := subscriptions[key]; ok && sets.NewString(subs...).Has(uid) {

				return true, nil
			} else {
				//TODO return and error if the channel doesn't exist?
				return false, nil
			}
		case http.StatusNotFound, http.StatusServiceUnavailable:
			return false, fmt.Errorf("unexpected status code: want %v, got %v", http.StatusOK, r.StatusCode)
		default:
			item.logger.Errorf("Probing of %s abandoned, IP: %s:%s: the response status is %v, expected one of: %v",
				item.url, item.podIP, item.podPort, r.StatusCode,
				[]int{http.StatusOK, http.StatusNotFound, http.StatusServiceUnavailable})
			return true, nil
		}
	}
}

// deepCopy copies a URL into a new one
func deepCopy(in *url.URL) *url.URL {
	// Safe to ignore the error since this is a deep copy
	newURL, _ := url.Parse(in.String())
	return newURL
}
