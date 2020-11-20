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

package controller

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	"knative.dev/eventing-kafka/pkg/channel/consolidated/kafka"
)

var (
	watchersMtx sync.RWMutex
	cacheMtx    sync.RWMutex
	// Hooks into the poll logic for testing
	after = time.After
	done  = func() {}
)

type CGCallBack func()
type Matcher func(string) bool

type ConsumerGroupWatcher interface {
	// Start instructs the watcher to start polling for the consumer groups and
	// notify any observers on the event of any changes
	Start() error

	// Watch registers callback on the event of any changes observed
	// on the consumer groups. watcherID is an arbitrary string the user provides
	// that will be used to identify his callbacks when provided to Forget(watcherID).
	// Successive calls with the same watcherID will queue the callbacks successfully.
	//
	//To ensure this is event-triggered, level-driven,
	// we don't pass the updates to the callback, instead the observer is expected
	// to use List() to get the updated list of ConsumerGroups.
	Watch(watcherID string, callback CGCallBack) error

	// Forget removes all callbacks that correspond to the watcherID
	Forget(watcherID string)

	// List returns all the cached consumer groups that match matcher.
	// It will return an empty slice if none matched or the cache is empty
	List(matcher Matcher) []string
}

type KafkaWatcher struct {
	logger *zap.SugaredLogger
	//TODO name?
	watchers     map[string][]CGCallBack
	cgCache      []string
	ac           kafka.AdminClient
	pollDuration time.Duration
}

func NewKafkaWatcher(ctx context.Context, ac kafka.AdminClient, pollDuration time.Duration) KafkaWatcher {
	w := KafkaWatcher{
		logger:       logging.FromContext(ctx),
		ac:           ac,
		pollDuration: pollDuration,
		watchers:     make(map[string][]CGCallBack),
		cgCache:      make([]string, 0),
	}
	return w
}

func (w *KafkaWatcher) Start() error {
	w.logger.Info("KafkaWatcher starting. Polling for consumer groups", zap.Duration("poll duration", w.pollDuration))
	go func() {
		for {
			select {
			case <-after(w.pollDuration):
				cgs, err := w.ac.ListConsumerGroups()
				if err != nil {
					w.logger.Error("error while listing consumer groups", zap.Error(err))
					continue
				}
				var notify bool
				var cg string
				// Look for observed CGs
				for _, c := range cgs {
					if !Find(w.cgCache, c) {
						// This is the first appearance.
						w.logger.Debug("Consumer group observed. Caching.",
							zap.String("consumer group", c))
						cg = c
						notify = true
						break
					}
				}
				// Look for disappeared CGs
				for _, c := range w.cgCache {
					if !Find(cgs, c) {
						// This CG was cached but it's no longer there.
						w.logger.Debug("Consumer group deleted.",
							zap.String("consumer group", c))
						cg = c
						notify = true
						break
					}
				}
				if notify {
					cacheMtx.Lock()
					w.cgCache = cgs
					cacheMtx.Unlock()
					w.notify(cg)
				}
				done()
			}
		}
	}()
	return nil
}

// TODO explore returning a channel instead of a taking callback
func (w *KafkaWatcher) Watch(watcherID string, cb CGCallBack) error {
	w.logger.Debug("Adding a new watcher", zap.String("watcherID", watcherID))
	watchersMtx.Lock()
	defer watchersMtx.Unlock()
	cbs, ok := w.watchers[watcherID]
	if !ok {
		cbs = []CGCallBack{}
	}
	w.watchers[watcherID] = append(cbs, cb)
	// notify at least once to get the current state
	cb()
	return nil
}

func (w *KafkaWatcher) Forget(watcherID string) {
	w.logger.Debug("Forgetting watcher", zap.String("watcherID", watcherID))
	delete(w.watchers, watcherID)
}

func (w *KafkaWatcher) List(matcher Matcher) []string {
	w.logger.Debug("Listing consumer groups")
	cacheMtx.RLock()
	defer cacheMtx.RUnlock()
	cgs := make([]string, 0)
	for _, cg := range w.cgCache {
		if matcher(cg) {
			cgs = append(cgs, cg)
		}
	}
	return cgs
}

func (w *KafkaWatcher) notify(cg string) {
	watchersMtx.RLock()
	cacheMtx.RLock()
	defer watchersMtx.RUnlock()
	defer cacheMtx.RUnlock()

	for _, cbs := range w.watchers {
		for _, w := range cbs {
			w()
		}
	}
}

func Find(list []string, item string) bool {
	for _, i := range list {
		if i == item {
			return true
		}
	}
	return false
}
