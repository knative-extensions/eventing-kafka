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
	"errors"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	kafkalisters "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
)

//
// Mock KafkaChannel Lister
//

var _ kafkalisters.KafkaChannelLister = &MockKafkaChannelLister{}

type MockKafkaChannelLister struct {
	name      string
	namespace string
	exists    bool
	ready     corev1.ConditionStatus
	err       bool
}

func NewMockKafkaChannelLister(name string, namespace string, exists bool, ready corev1.ConditionStatus, err bool) MockKafkaChannelLister {
	return MockKafkaChannelLister{
		name:      name,
		namespace: namespace,
		exists:    exists,
		ready:     ready,
		err:       err,
	}
}

func (m MockKafkaChannelLister) List(_ labels.Selector) (ret []*kafkav1beta1.KafkaChannel, err error) {
	panic("implement me")
}

func (m MockKafkaChannelLister) KafkaChannels(namespace string) kafkalisters.KafkaChannelNamespaceLister {
	return NewMockKafkaChannelNamespaceLister(m.name, namespace, m.exists, m.ready, m.err)
}

//
// Mock KafkaChannel NamespaceLister
//

var _ kafkalisters.KafkaChannelNamespaceLister = &MockKafkaChannelNamespaceLister{}

type MockKafkaChannelNamespaceLister struct {
	name      string
	namespace string
	exists    bool
	ready     corev1.ConditionStatus
	err       bool
}

func NewMockKafkaChannelNamespaceLister(name string, namespace string, exists bool, ready corev1.ConditionStatus, err bool) MockKafkaChannelNamespaceLister {
	return MockKafkaChannelNamespaceLister{
		name:      name,
		namespace: namespace,
		exists:    exists,
		ready:     ready,
		err:       err,
	}
}

func (m MockKafkaChannelNamespaceLister) List(_ labels.Selector) (ret []*kafkav1beta1.KafkaChannel, err error) {
	panic("implement me")
}

func (m MockKafkaChannelNamespaceLister) Get(name string) (*kafkav1beta1.KafkaChannel, error) {
	if m.err {
		return nil, k8serrors.NewInternalError(errors.New("expected Unit Test error from MockKafkaChannelNamespaceLister"))
	} else if m.exists {
		return CreateKafkaChannel(m.name, m.namespace, m.ready), nil
	} else {
		return nil, k8serrors.NewNotFound(kafkav1beta1.Resource("KafkaChannel"), name)
	}
}
