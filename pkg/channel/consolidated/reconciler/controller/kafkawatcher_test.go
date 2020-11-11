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
	"testing"
	"time"
)

//TODO how to mock the sarama AdminClient
type FakeClusterAdmin struct {
	lister func() (map[string]string, error)
}

func (fake *FakeClusterAdmin) ListConsumerGroups() (map[string]string, error) {
	return fake.lister()
}

func TestKafkaWatcher(t *testing.T) {
	cgname := "kafka.event-example.default-kne-trigger.0d9c4383-1e68-42b5-8c3a-3788274404c5"
	cgs := map[string]string{
		cgname: "consumer",
	}
	ca := FakeClusterAdmin{
		func() (map[string]string, error) {
			return cgs, nil
		},
	}
	ch := make(chan CGEvent, 1)

	w := NewKafkaWatcher(&ca)
	w.WatchCosumerGroup(cgname, func(event CGEvent) {
		ch <- event
	})

	w.Start()
	waitForEvent(t, ch, CGObserved, cgname)
	delete(cgs, cgname)
	waitForEvent(t, ch, CGDeleted, cgname)
}

func waitForEvent(t *testing.T, ch chan CGEvent, event CGEvent, cgname string) {
	select {
	case e := <-ch:
		if e != event {
			t.Errorf("unexpected event received. got %v expected %v", e, event)
		}
	case <-time.After(6 * time.Second):
		t.Errorf("timedout waiting for event %v for ConsumerGroup %s", event, cgname)
	}
}
