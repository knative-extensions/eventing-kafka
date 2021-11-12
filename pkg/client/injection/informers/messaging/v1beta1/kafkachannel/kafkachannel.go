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

// Code generated by injection-gen. DO NOT EDIT.

package kafkachannel

import (
	context "context"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	cache "k8s.io/client-go/tools/cache"
	apismessagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	versioned "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	v1beta1 "knative.dev/eventing-kafka/pkg/client/informers/externalversions/messaging/v1beta1"
	client "knative.dev/eventing-kafka/pkg/client/injection/client"
	factory "knative.dev/eventing-kafka/pkg/client/injection/informers/factory"
	messagingv1beta1 "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	controller "knative.dev/pkg/controller"
	injection "knative.dev/pkg/injection"
	logging "knative.dev/pkg/logging"
)

func init() {
	injection.Default.RegisterInformer(withInformer)
	injection.Dynamic.RegisterDynamicInformer(withDynamicInformer)
}

// Key is used for associating the Informer inside the context.Context.
type Key struct{}

func withInformer(ctx context.Context) (context.Context, controller.Informer) {
	f := factory.Get(ctx)
	inf := f.Messaging().V1beta1().KafkaChannels()
	return context.WithValue(ctx, Key{}, inf), inf.Informer()
}

func withDynamicInformer(ctx context.Context) context.Context {
	inf := &wrapper{client: client.Get(ctx), resourceVersion: injection.GetResourceVersion(ctx)}
	return context.WithValue(ctx, Key{}, inf)
}

// Get extracts the typed informer from the context.
func Get(ctx context.Context) v1beta1.KafkaChannelInformer {
	untyped := ctx.Value(Key{})
	if untyped == nil {
		logging.FromContext(ctx).Panic(
			"Unable to fetch knative.dev/eventing-kafka/pkg/client/informers/externalversions/messaging/v1beta1.KafkaChannelInformer from context.")
	}
	return untyped.(v1beta1.KafkaChannelInformer)
}

type wrapper struct {
	client versioned.Interface

	namespace string

	resourceVersion string
}

var _ v1beta1.KafkaChannelInformer = (*wrapper)(nil)
var _ messagingv1beta1.KafkaChannelLister = (*wrapper)(nil)

func (w *wrapper) Informer() cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(nil, &apismessagingv1beta1.KafkaChannel{}, 0, nil)
}

func (w *wrapper) Lister() messagingv1beta1.KafkaChannelLister {
	return w
}

func (w *wrapper) KafkaChannels(namespace string) messagingv1beta1.KafkaChannelNamespaceLister {
	return &wrapper{client: w.client, namespace: namespace, resourceVersion: w.resourceVersion}
}

// SetResourceVersion allows consumers to adjust the minimum resourceVersion
// used by the underlying client.  It is not accessible via the standard
// lister interface, but can be accessed through a user-defined interface and
// an implementation check e.g. rvs, ok := foo.(ResourceVersionSetter)
func (w *wrapper) SetResourceVersion(resourceVersion string) {
	w.resourceVersion = resourceVersion
}

func (w *wrapper) List(selector labels.Selector) (ret []*apismessagingv1beta1.KafkaChannel, err error) {
	lo, err := w.client.MessagingV1beta1().KafkaChannels(w.namespace).List(context.TODO(), v1.ListOptions{
		LabelSelector:   selector.String(),
		ResourceVersion: w.resourceVersion,
	})
	if err != nil {
		return nil, err
	}
	for idx := range lo.Items {
		ret = append(ret, &lo.Items[idx])
	}
	return ret, nil
}

func (w *wrapper) Get(name string) (*apismessagingv1beta1.KafkaChannel, error) {
	return w.client.MessagingV1beta1().KafkaChannels(w.namespace).Get(context.TODO(), name, v1.GetOptions{
		ResourceVersion: w.resourceVersion,
	})
}
