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

package testing

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	fakeclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned/fake"
	"knative.dev/pkg/controller"
	. "knative.dev/pkg/reconciler/testing"
)

const (
	// maxEventBufferSize is the estimated max number of event notifications that
	// can be buffered during reconciliation.
	maxEventBufferSize = 10
)

// Ctor functions create a k8s controller with given params.
type Ctor func(listers *Listers, kafkaClient versioned.Interface, eventRecorder record.EventRecorder) controller.Reconciler

// MakeFactory creates a reconciler factory with fake clients and controller created by `ctor`.
func MakeFactory(ctor Ctor) Factory {
	return func(t *testing.T, r *TableRow) (controller.Reconciler, ActionRecorderList, EventList) {
		ls := NewListers(r.Objects)

		client := fakeclientset.NewSimpleClientset(ls.GetMessagingObjects()...)

		dynamicScheme := runtime.NewScheme()
		for _, addTo := range clientSetSchemes {
			_ = addTo(dynamicScheme)
		}

		eventRecorder := record.NewFakeRecorder(maxEventBufferSize)

		// Set up our Controller from the fakes.
		c := ctor(&ls, client, eventRecorder)

		actionRecorderList := ActionRecorderList{client}
		eventList := EventList{Recorder: eventRecorder}

		return c, actionRecorderList, eventList
	}
}
