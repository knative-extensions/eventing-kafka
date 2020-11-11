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

type CGEvent int

const (
	CGObserved CGEvent = iota
	CGDeleted
)

var rwMutex sync.RWMutex

type CGCallBack func(CGEvent)

type ConsumerGroupsLister interface {
	ListConsumerGroups() (map[string]string, error)
}

type KafkaWatcher struct {
	watchers map[string]CGCallBack
	cgCache  []string
	lister   ConsumerGroupsLister
	closed   chan bool
}

type CallBack func(event CGEvent)

func NewKafkaWatcher(l ConsumerGroupsLister) KafkaWatcher {
	w := KafkaWatcher{
		lister:   l,
		watchers: make(map[string]CGCallBack),
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
				// Look for observed CGs
				for c := range cgs {
					if !Find(w.cgCache, c) {
						// This is the first appearance. Cache it and trigger
						// that it was added
						w.cgCache = append(w.cgCache, c)
						w.notify(c, CGObserved)
					}
				}
				// Look for disappeared CGs
				for _, c := range w.cgCache {
					if _, ok := cgs[c]; !ok {
						// This CG was cached but it's no longer there, we need
						// to trigget CGDeleted
						w.notify(c, CGDeleted)
					}
				}
			case <-w.closed:
				break
			}
		}
	}()
	return nil
}

func (w *KafkaWatcher) notify(cg string, event CGEvent) {
	rwMutex.RLock()
	cb, ok := w.watchers[cg]
	rwMutex.RUnlock()
	if ok {
		cb(event)
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

// TODO Add batch notification
func (w *KafkaWatcher) WatchCosumerGroup(cgname string, cb CGCallBack) error {
	//TODO handle if exists
	rwMutex.Lock()
	w.watchers[cgname] = cb
	rwMutex.Unlock()
	return nil
}
