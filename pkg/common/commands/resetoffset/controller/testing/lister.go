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

package testing

import (
	"k8s.io/apimachinery/pkg/runtime"
	fakekubeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	fakeeventingclientset "knative.dev/eventing/pkg/client/clientset/versioned/fake"
	fakeeventsclientset "knative.dev/eventing/pkg/client/clientset/versioned/fake"
	"knative.dev/pkg/reconciler/testing"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	fakekafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned/fake"
	resetoffsetlisters "knative.dev/eventing-kafka/pkg/client/listers/kafka/v1alpha1"
)

var clientSetSchemes = []func(*runtime.Scheme) error{
	fakekubeclientset.AddToScheme,
	fakeeventsclientset.AddToScheme,
	fakekafkaclientset.AddToScheme,
	fakeeventingclientset.AddToScheme,
}

type Listers struct {
	sorter testing.ObjectSorter
}

func NewListers(objs []runtime.Object) Listers {

	scheme := runtime.NewScheme()

	for _, addTo := range clientSetSchemes {
		addTo(scheme)
	}

	ls := Listers{
		sorter: testing.NewObjectSorter(scheme),
	}

	ls.sorter.AddObjects(objs...)

	return ls
}

func (l *Listers) indexerFor(obj runtime.Object) cache.Indexer {
	return l.sorter.IndexerForObjectType(obj)
}

func (l *Listers) GetKubeObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakekubeclientset.AddToScheme)
}

func (l *Listers) GetEventingObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakeeventingclientset.AddToScheme)
}

func (l *Listers) GetEventsObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakeeventsclientset.AddToScheme)
}

func (l *Listers) GetResetOffsetObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakekafkaclientset.AddToScheme)
}

func (l *Listers) GetAllObjects() []runtime.Object {
	all := l.GetResetOffsetObjects()
	all = append(all, l.GetEventsObjects()...)
	all = append(all, l.GetKubeObjects()...)
	return all
}

func (l *Listers) GetResetOffsetLister() resetoffsetlisters.ResetOffsetLister {
	return resetoffsetlisters.NewResetOffsetLister(l.indexerFor(&kafkav1alpha1.ResetOffset{}))
}
