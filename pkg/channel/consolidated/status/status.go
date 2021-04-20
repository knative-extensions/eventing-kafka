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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"

	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/networking/pkg/prober"
	"knative.dev/pkg/logging"
)

const (
	// probeConcurrency defines how many probing calls can be issued simultaneously
	probeConcurrency = 100
	// probeTimeout defines the maximum amount of time a request will wait
	probeTimeout = 1 * time.Second
	// initialDelay defines the delay before enqueuing a probing request the first time.
	// It gives times for the change to propagate and prevents unnecessary retries.
	initialDelay = 200 * time.Millisecond
)

var dialContext = (&net.Dialer{Timeout: probeTimeout}).DialContext

// targetState represents the probing state of a subscription
type targetState struct {
	sub eventingduckv1.SubscriberSpec
	ch  messagingv1beta1.KafkaChannel

	readyLock sync.RWMutex
	// pendingCount is the number of pods that haven't been successfully probed yet
	pendingCount atomic.Int32
	// readyCount is the number of pods that have the subscription ready
	readyPartitions sets.Int
	probedPods      sets.String
	lastAccessed    time.Time
	ready           bool
	cancel          func()
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
	URL     *url.URL
}

// ProbeTargetLister lists all the targets that requires probing.
type ProbeTargetLister interface {
	// ListProbeTargets returns a list of targets to be probed
	ListProbeTargets(ctx context.Context, ch messagingv1beta1.KafkaChannel) (*ProbeTarget, error)
}

// Manager provides a way to check if a Subscription is ready
type Manager interface {
	IsReady(ctx context.Context, ch messagingv1beta1.KafkaChannel, sub eventingduckv1.SubscriberSpec) (bool, error)
	CancelProbing(sub eventingduckv1.SubscriberSpec)
	CancelPodProbing(pod corev1.Pod)
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

	readyCallback func(messagingv1beta1.KafkaChannel, eventingduckv1.SubscriberSpec)

	probeConcurrency int

	opts []interface{}
}

// NewProber creates a new instance of Prober
func NewProber(
	logger *zap.SugaredLogger,
	targetLister ProbeTargetLister,
	readyCallback func(messagingv1beta1.KafkaChannel, eventingduckv1.SubscriberSpec), opts ...interface{}) *Prober {
	return &Prober{
		logger:       logger,
		targetStates: make(map[types.UID]*targetState),
		podContexts:  make(map[string]cancelContext),
		workQueue: workqueue.NewNamedRateLimitingQueue(
			workqueue.NewMaxOfRateLimiter(
				// Per item exponential backoff
				workqueue.NewItemExponentialFailureRateLimiter(100*time.Millisecond, 5*time.Minute),
				// Global rate limiter
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(50), 1000)},
			),
			"ProbingQueue"),
		targetLister:     targetLister,
		readyCallback:    readyCallback,
		probeConcurrency: probeConcurrency,
		opts:             opts,
	}
}

func (m *Prober) checkReadiness(state *targetState) bool {
	state.readyLock.Lock()
	defer state.readyLock.Unlock()
	partitions := state.ch.Spec.NumPartitions
	if !state.ready {
		state.ready = state.readyPartitions.Len() == int(partitions)
	}
	m.logger.Debugw("Checking subscription readiness",
		zap.Any("subscription", state.sub.UID),
		zap.Any("channel", state.ch.Name),
		zap.Any("pod ips", state.probedPods),
		zap.Any("channel partitions", partitions),
		zap.Any("ready partitions", state.readyPartitions.List()),
		zap.Bool("ready", state.ready),
	)
	return state.ready
}

func (m *Prober) IsReady(ctx context.Context, ch messagingv1beta1.KafkaChannel, sub eventingduckv1.SubscriberSpec) (bool, error) {
	logger := logging.FromContext(ctx)

	// Get the probe targets
	target, err := m.targetLister.ListProbeTargets(ctx, ch)
	if err != nil {
		logger.Errorw("Error listing probe targets", zap.Error(err),
			zap.Any("subscription", sub.UID))
		return false, err
	}
	// get the state while locking for very short scope
	state, ok := func() (*targetState, bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		s, o := m.targetStates[sub.UID]
		return s, o
	}()
	if ok {
		if !isOutdatedTargetState(state, sub, target.PodIPs) {
			return m.checkReadiness(state), nil
		}
		m.ejectStateUnsafe(sub)
	}
	m.probeTarget(ctx, ch, sub, target)
	return false, nil
}

func (m *Prober) probeTarget(ctx context.Context, ch messagingv1beta1.KafkaChannel, sub eventingduckv1.SubscriberSpec, target *ProbeTarget) {
	subscriptionKey := sub.UID
	logger := logging.FromContext(ctx)
	subCtx, cancel := context.WithCancel(context.Background())
	subscriptionState := &targetState{
		sub:          sub,
		ch:           ch,
		lastAccessed: time.Now(),
		cancel:       cancel,
	}

	// Group the probe targets by IP
	workItems := make(map[string][]*workItem)
	for ip := range target.PodIPs {
		workItems[ip] = append(workItems[ip], &workItem{
			targetStates: subscriptionState,
			url:          target.URL,
			podIP:        ip,
			podPort:      target.PodPort,
			logger:       logger,
		})
	}

	subscriptionState.probedPods = target.PodIPs
	subscriptionState.pendingCount.Store(int32(len(workItems)))
	subscriptionState.readyPartitions = sets.Int{}

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
func (m *Prober) CancelProbing(sub eventingduckv1.SubscriberSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ejectStateUnsafe(sub)
}

// ejectStateUnsafe ejects a state from Cache, it's not safe for concurrent access and is meant for internal use only under proper locking.
func (m *Prober) ejectStateUnsafe(sub eventingduckv1.SubscriberSpec) {
	if state, ok := m.targetStates[sub.UID]; ok {
		m.logger.Debugw("Canceling state", zap.Any("subscription", sub))
		state.cancel()
		delete(m.targetStates, sub.UID)
	}
}

// CancelPodProbing cancels probing of the provided Pod IP.
func (m *Prober) CancelPodProbing(pod corev1.Pod) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx, ok := m.podContexts[pod.Status.PodIP]; ok {
		ctx.cancel()
		delete(m.podContexts, pod.Status.PodIP)
	}
}

// RefreshPodProbing lists probe targets and invalidates any in-flight (non-ready) states whose initial probed targets changed from the
// current ones.
func (m *Prober) RefreshPodProbing(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	logger := logging.FromContext(ctx)
	for _, state := range m.targetStates {
		if !m.checkReadiness(state) {
			// This is an in-flight state
			sub := state.sub
			ch := state.ch
			// Get the probe targets
			target, err := m.targetLister.ListProbeTargets(ctx, ch)
			if err != nil {
				logger.Errorw("Error listing probe targets", zap.Error(err),
					zap.Any("subscription", sub.UID))
				return
			}
			m.ejectStateUnsafe(sub)
			func() {
				// probeTarget requires an unlocked mutex.
				m.mu.Unlock()
				defer m.mu.Lock()
				m.probeTarget(ctx, ch, sub, target)
			}()
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

	transport.DialContext = func(ctx context.Context, network, addr string) (conn net.Conn, e error) {
		// http.Request.URL is set to the hostname and it is substituted in here with the target IP.
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

	if err != nil {
		// In case of error, enqueue for retry
		m.workQueue.AddRateLimited(obj)
		item.logger.Errorw("Error occurred while probing",
			zap.Any("url", item.url), zap.Any("IP", item.podIP),
			zap.Any("port", item.podPort), zap.Bool("ready", ok), zap.Error(err),
			zap.Int("depth", m.workQueue.Len()))
	} else if !ok {
		// No error, but verification failed, enqueue for retry
		m.workQueue.AddRateLimited(obj)
		item.logger.Debugw("Verification of pod response failed.",
			zap.Any("url", item.url), zap.Any("IP", item.podIP),
			zap.Any("port", item.podPort), zap.Bool("ready", ok), zap.Error(err),
			zap.Int("depth", m.workQueue.Len()))
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
		// This is the last pod being successfully probed, the subscription might ready
		if m.checkReadiness(subscriptionState) {
			subscriptionState.cancel()
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
			// This is the last pod being successfully probed, the subscription might be ready
			if subscriptionState.pendingCount.Dec() == 0 {
				if m.checkReadiness(subscriptionState) {
					m.readyCallback(subscriptionState.ch, subscriptionState.sub)
				}
			}
			return
		}
	}
}

func (m *Prober) probeVerifier(item *workItem) prober.Verifier {
	return func(r *http.Response, b []byte) (bool, error) {
		m.logger.Debugw("Verifying response", zap.Int("status code", r.StatusCode),
			zap.ByteString("body", b), zap.Any("subscription", item.targetStates.sub.UID),
			zap.Any("channel", item.targetStates.ch))
		switch r.StatusCode {
		case http.StatusOK:
			var subscriptions = make(map[string][]int)
			err := json.Unmarshal(b, &subscriptions)
			if err != nil {
				m.logger.Errorw("error unmarshaling", err)
				return false, err
			}
			uid := string(item.targetStates.sub.UID)
			key := fmt.Sprintf("%s/%s", item.targetStates.ch.Namespace, item.targetStates.ch.Name)
			m.logger.Debugw("Received proper probing response from target",
				zap.Any("found subscriptions", subscriptions),
				zap.String("pod ip", item.podIP),
				zap.String("want channel", key),
				zap.String("want subscription", uid),
			)
			if partitions, ok := subscriptions[uid]; ok {
				func() {
					item.targetStates.readyLock.Lock()
					defer item.targetStates.readyLock.Unlock()
					item.targetStates.readyPartitions.Insert(partitions...)
				}()
				return m.checkReadiness(item.targetStates), nil
			} else {
				return false, nil
			}
		case http.StatusNotFound, http.StatusServiceUnavailable:
			m.logger.Errorf("unexpected status code: want %v, got %v", http.StatusOK, r.StatusCode)
			return false, fmt.Errorf("unexpected status code: want %v, got %v", http.StatusOK, r.StatusCode)
		default:
			item.logger.Errorf("Probing of %s abandoned, IP: %s:%s: the response status is %v, expected one of: %v",
				item.url, item.podIP, item.podPort, r.StatusCode,
				[]int{http.StatusOK, http.StatusNotFound, http.StatusServiceUnavailable})
			return true, nil
		}
	}
}

// A target state is outdated if the generation is different or if the target IPs change before it becomes
// ready.
func isOutdatedTargetState(state *targetState, sub eventingduckv1.SubscriberSpec, podIPs sets.String) bool {
	state.readyLock.RLock()
	defer state.readyLock.RUnlock()
	return state.sub.Generation != sub.Generation || (!state.ready && !state.probedPods.Equal(podIPs))
}

// deepCopy copies a URL into a new one
func deepCopy(in *url.URL) *url.URL {
	// Safe to ignore the error since this is a deep copy
	newURL, _ := url.Parse(in.String())
	return newURL
}
