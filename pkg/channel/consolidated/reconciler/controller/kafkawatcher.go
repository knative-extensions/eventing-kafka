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
	"sync"
	"time"
)

var watchersMtx sync.RWMutex
var cacheMtx sync.RWMutex

type CGCallBack func()
type Matcher func(string) bool

type ConsumerGroupsLister interface {
	ListConsumerGroups() (map[string]string, error)
}

type KafkaWatcher struct {
	//TODO name?
	watchers []CGCallBack
	cgCache  []string
	lister   ConsumerGroupsLister
	closed   chan bool
}

func NewKafkaWatcher(l ConsumerGroupsLister) KafkaWatcher {
	w := KafkaWatcher{
		lister:   l,
		watchers: make([]CGCallBack, 0, 5),
		cgCache:  make([]string, 0),
		closed:   make(chan bool, 1),
	}
	return w
}

func (w *KafkaWatcher) Start() error {
	go func() {
		for {
			select {
			case <-time.After(2 * time.Second):
				//TODO log error
				cgs, _ := w.lister.ListConsumerGroups()
				var notify bool
				var cg string
				// Look for observed CGs
				for c := range cgs {
					if !Find(w.cgCache, c) {
						// This is the first appearance.
						cg = c
						notify = true
					}
				}
				// Look for disappeared CGs
				for _, c := range w.cgCache {
					if _, ok := cgs[c]; !ok {
						// This CG was cached but it's no longer there.
						cg = c
						notify = true
					}
				}
				if notify {
					cacheMtx.Lock()
					w.cgCache = keys(cgs)
					cacheMtx.Unlock()
					w.notify(cg)
				}
			case <-w.closed:
				break
			}
		}
	}()
	return nil
}

func (w *KafkaWatcher) notify(cg string) {
	watchersMtx.RLock()
	cacheMtx.RLock()
	defer watchersMtx.RUnlock()
	defer cacheMtx.RUnlock()

	for _, w := range w.watchers {
		w()
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

func (w *KafkaWatcher) Stop() error {
	//TODO handle if closed
	close(w.closed)
	return nil
}

// TODO Add remove watcher with key
// TODO explore returning a channel instead of a taking callback
func (w *KafkaWatcher) WatchConsumerGroup(cb CGCallBack) error {
	//TODO handle if exists
	watchersMtx.Lock()
	defer watchersMtx.Unlock()
	w.watchers = append(w.watchers, cb)
	// notify at least once to get the current state
	cb()
	return nil
}

func (w *KafkaWatcher) ListConsumerGroups(matcher Matcher) []string {
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

func keys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k, _ := range m {
		keys = append(keys, k)
	}
	return keys
}
