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

package client

import (
	context "context"
	json "encoding/json"
	errors "errors"
	fmt "fmt"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	runtime "k8s.io/apimachinery/pkg/runtime"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	discovery "k8s.io/client-go/discovery"
	dynamic "k8s.io/client-go/dynamic"
	rest "k8s.io/client-go/rest"
	v1beta1 "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	v1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	versioned "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	typedbindingsv1beta1 "knative.dev/eventing-kafka/pkg/client/clientset/versioned/typed/bindings/v1beta1"
	typedkafkav1alpha1 "knative.dev/eventing-kafka/pkg/client/clientset/versioned/typed/kafka/v1alpha1"
	typedmessagingv1beta1 "knative.dev/eventing-kafka/pkg/client/clientset/versioned/typed/messaging/v1beta1"
	typedsourcesv1beta1 "knative.dev/eventing-kafka/pkg/client/clientset/versioned/typed/sources/v1beta1"
	injection "knative.dev/pkg/injection"
	dynamicclient "knative.dev/pkg/injection/clients/dynamicclient"
	logging "knative.dev/pkg/logging"
)

func init() {
	injection.Default.RegisterClient(withClientFromConfig)
	injection.Default.RegisterClientFetcher(func(ctx context.Context) interface{} {
		return Get(ctx)
	})
	injection.Dynamic.RegisterDynamicClient(withClientFromDynamic)
}

// Key is used as the key for associating information with a context.Context.
type Key struct{}

func withClientFromConfig(ctx context.Context, cfg *rest.Config) context.Context {
	return context.WithValue(ctx, Key{}, versioned.NewForConfigOrDie(cfg))
}

func withClientFromDynamic(ctx context.Context) context.Context {
	return context.WithValue(ctx, Key{}, &wrapClient{dyn: dynamicclient.Get(ctx)})
}

// Get extracts the versioned.Interface client from the context.
func Get(ctx context.Context) versioned.Interface {
	untyped := ctx.Value(Key{})
	if untyped == nil {
		if injection.GetConfig(ctx) == nil {
			logging.FromContext(ctx).Panic(
				"Unable to fetch knative.dev/eventing-kafka/pkg/client/clientset/versioned.Interface from context. This context is not the application context (which is typically given to constructors via sharedmain).")
		} else {
			logging.FromContext(ctx).Panic(
				"Unable to fetch knative.dev/eventing-kafka/pkg/client/clientset/versioned.Interface from context.")
		}
	}
	return untyped.(versioned.Interface)
}

type wrapClient struct {
	dyn dynamic.Interface
}

var _ versioned.Interface = (*wrapClient)(nil)

func (w *wrapClient) Discovery() discovery.DiscoveryInterface {
	panic("Discovery called on dynamic client!")
}

func convert(from interface{}, to runtime.Object) error {
	bs, err := json.Marshal(from)
	if err != nil {
		return fmt.Errorf("Marshal() = %w", err)
	}
	if err := json.Unmarshal(bs, to); err != nil {
		return fmt.Errorf("Unmarshal() = %w", err)
	}
	return nil
}

// BindingsV1beta1 retrieves the BindingsV1beta1Client
func (w *wrapClient) BindingsV1beta1() typedbindingsv1beta1.BindingsV1beta1Interface {
	return &wrapBindingsV1beta1{
		dyn: w.dyn,
	}
}

type wrapBindingsV1beta1 struct {
	dyn dynamic.Interface
}

func (w *wrapBindingsV1beta1) RESTClient() rest.Interface {
	panic("RESTClient called on dynamic client!")
}

func (w *wrapBindingsV1beta1) KafkaBindings(namespace string) typedbindingsv1beta1.KafkaBindingInterface {
	return &wrapBindingsV1beta1KafkaBindingImpl{
		dyn: w.dyn.Resource(schema.GroupVersionResource{
			Group:    "bindings.knative.dev",
			Version:  "v1beta1",
			Resource: "kafkabindings",
		}),

		namespace: namespace,
	}
}

type wrapBindingsV1beta1KafkaBindingImpl struct {
	dyn dynamic.NamespaceableResourceInterface

	namespace string
}

var _ typedbindingsv1beta1.KafkaBindingInterface = (*wrapBindingsV1beta1KafkaBindingImpl)(nil)

func (w *wrapBindingsV1beta1KafkaBindingImpl) Create(ctx context.Context, in *v1beta1.KafkaBinding, opts v1.CreateOptions) (*v1beta1.KafkaBinding, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "bindings.knative.dev",
		Version: "v1beta1",
		Kind:    "KafkaBinding",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Create(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1beta1.KafkaBinding{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapBindingsV1beta1KafkaBindingImpl) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return w.dyn.Namespace(w.namespace).Delete(ctx, name, opts)
}

func (w *wrapBindingsV1beta1KafkaBindingImpl) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	return w.dyn.Namespace(w.namespace).DeleteCollection(ctx, opts, listOpts)
}

func (w *wrapBindingsV1beta1KafkaBindingImpl) Get(ctx context.Context, name string, opts v1.GetOptions) (*v1beta1.KafkaBinding, error) {
	uo, err := w.dyn.Namespace(w.namespace).Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	out := &v1beta1.KafkaBinding{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapBindingsV1beta1KafkaBindingImpl) List(ctx context.Context, opts v1.ListOptions) (*v1beta1.KafkaBindingList, error) {
	uo, err := w.dyn.Namespace(w.namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := &v1beta1.KafkaBindingList{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapBindingsV1beta1KafkaBindingImpl) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1beta1.KafkaBinding, err error) {
	uo, err := w.dyn.Namespace(w.namespace).Patch(ctx, name, pt, data, opts)
	if err != nil {
		return nil, err
	}
	out := &v1beta1.KafkaBinding{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapBindingsV1beta1KafkaBindingImpl) Update(ctx context.Context, in *v1beta1.KafkaBinding, opts v1.UpdateOptions) (*v1beta1.KafkaBinding, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "bindings.knative.dev",
		Version: "v1beta1",
		Kind:    "KafkaBinding",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Update(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1beta1.KafkaBinding{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapBindingsV1beta1KafkaBindingImpl) UpdateStatus(ctx context.Context, in *v1beta1.KafkaBinding, opts v1.UpdateOptions) (*v1beta1.KafkaBinding, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "bindings.knative.dev",
		Version: "v1beta1",
		Kind:    "KafkaBinding",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).UpdateStatus(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1beta1.KafkaBinding{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapBindingsV1beta1KafkaBindingImpl) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return nil, errors.New("NYI: Watch")
}

// KafkaV1alpha1 retrieves the KafkaV1alpha1Client
func (w *wrapClient) KafkaV1alpha1() typedkafkav1alpha1.KafkaV1alpha1Interface {
	return &wrapKafkaV1alpha1{
		dyn: w.dyn,
	}
}

type wrapKafkaV1alpha1 struct {
	dyn dynamic.Interface
}

func (w *wrapKafkaV1alpha1) RESTClient() rest.Interface {
	panic("RESTClient called on dynamic client!")
}

func (w *wrapKafkaV1alpha1) ResetOffsets(namespace string) typedkafkav1alpha1.ResetOffsetInterface {
	return &wrapKafkaV1alpha1ResetOffsetImpl{
		dyn: w.dyn.Resource(schema.GroupVersionResource{
			Group:    "kafka.eventing.knative.dev",
			Version:  "v1alpha1",
			Resource: "resetoffsets",
		}),

		namespace: namespace,
	}
}

type wrapKafkaV1alpha1ResetOffsetImpl struct {
	dyn dynamic.NamespaceableResourceInterface

	namespace string
}

var _ typedkafkav1alpha1.ResetOffsetInterface = (*wrapKafkaV1alpha1ResetOffsetImpl)(nil)

func (w *wrapKafkaV1alpha1ResetOffsetImpl) Create(ctx context.Context, in *v1alpha1.ResetOffset, opts v1.CreateOptions) (*v1alpha1.ResetOffset, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kafka.eventing.knative.dev",
		Version: "v1alpha1",
		Kind:    "ResetOffset",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Create(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ResetOffset{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKafkaV1alpha1ResetOffsetImpl) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return w.dyn.Namespace(w.namespace).Delete(ctx, name, opts)
}

func (w *wrapKafkaV1alpha1ResetOffsetImpl) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	return w.dyn.Namespace(w.namespace).DeleteCollection(ctx, opts, listOpts)
}

func (w *wrapKafkaV1alpha1ResetOffsetImpl) Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.ResetOffset, error) {
	uo, err := w.dyn.Namespace(w.namespace).Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ResetOffset{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKafkaV1alpha1ResetOffsetImpl) List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.ResetOffsetList, error) {
	uo, err := w.dyn.Namespace(w.namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ResetOffsetList{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKafkaV1alpha1ResetOffsetImpl) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.ResetOffset, err error) {
	uo, err := w.dyn.Namespace(w.namespace).Patch(ctx, name, pt, data, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ResetOffset{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKafkaV1alpha1ResetOffsetImpl) Update(ctx context.Context, in *v1alpha1.ResetOffset, opts v1.UpdateOptions) (*v1alpha1.ResetOffset, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kafka.eventing.knative.dev",
		Version: "v1alpha1",
		Kind:    "ResetOffset",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Update(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ResetOffset{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKafkaV1alpha1ResetOffsetImpl) UpdateStatus(ctx context.Context, in *v1alpha1.ResetOffset, opts v1.UpdateOptions) (*v1alpha1.ResetOffset, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kafka.eventing.knative.dev",
		Version: "v1alpha1",
		Kind:    "ResetOffset",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).UpdateStatus(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &v1alpha1.ResetOffset{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapKafkaV1alpha1ResetOffsetImpl) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return nil, errors.New("NYI: Watch")
}

// MessagingV1beta1 retrieves the MessagingV1beta1Client
func (w *wrapClient) MessagingV1beta1() typedmessagingv1beta1.MessagingV1beta1Interface {
	return &wrapMessagingV1beta1{
		dyn: w.dyn,
	}
}

type wrapMessagingV1beta1 struct {
	dyn dynamic.Interface
}

func (w *wrapMessagingV1beta1) RESTClient() rest.Interface {
	panic("RESTClient called on dynamic client!")
}

func (w *wrapMessagingV1beta1) KafkaChannels(namespace string) typedmessagingv1beta1.KafkaChannelInterface {
	return &wrapMessagingV1beta1KafkaChannelImpl{
		dyn: w.dyn.Resource(schema.GroupVersionResource{
			Group:    "messaging.knative.dev",
			Version:  "v1beta1",
			Resource: "kafkachannels",
		}),

		namespace: namespace,
	}
}

type wrapMessagingV1beta1KafkaChannelImpl struct {
	dyn dynamic.NamespaceableResourceInterface

	namespace string
}

var _ typedmessagingv1beta1.KafkaChannelInterface = (*wrapMessagingV1beta1KafkaChannelImpl)(nil)

func (w *wrapMessagingV1beta1KafkaChannelImpl) Create(ctx context.Context, in *messagingv1beta1.KafkaChannel, opts v1.CreateOptions) (*messagingv1beta1.KafkaChannel, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "messaging.knative.dev",
		Version: "v1beta1",
		Kind:    "KafkaChannel",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Create(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &messagingv1beta1.KafkaChannel{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapMessagingV1beta1KafkaChannelImpl) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return w.dyn.Namespace(w.namespace).Delete(ctx, name, opts)
}

func (w *wrapMessagingV1beta1KafkaChannelImpl) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	return w.dyn.Namespace(w.namespace).DeleteCollection(ctx, opts, listOpts)
}

func (w *wrapMessagingV1beta1KafkaChannelImpl) Get(ctx context.Context, name string, opts v1.GetOptions) (*messagingv1beta1.KafkaChannel, error) {
	uo, err := w.dyn.Namespace(w.namespace).Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	out := &messagingv1beta1.KafkaChannel{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapMessagingV1beta1KafkaChannelImpl) List(ctx context.Context, opts v1.ListOptions) (*messagingv1beta1.KafkaChannelList, error) {
	uo, err := w.dyn.Namespace(w.namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := &messagingv1beta1.KafkaChannelList{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapMessagingV1beta1KafkaChannelImpl) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *messagingv1beta1.KafkaChannel, err error) {
	uo, err := w.dyn.Namespace(w.namespace).Patch(ctx, name, pt, data, opts)
	if err != nil {
		return nil, err
	}
	out := &messagingv1beta1.KafkaChannel{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapMessagingV1beta1KafkaChannelImpl) Update(ctx context.Context, in *messagingv1beta1.KafkaChannel, opts v1.UpdateOptions) (*messagingv1beta1.KafkaChannel, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "messaging.knative.dev",
		Version: "v1beta1",
		Kind:    "KafkaChannel",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Update(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &messagingv1beta1.KafkaChannel{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapMessagingV1beta1KafkaChannelImpl) UpdateStatus(ctx context.Context, in *messagingv1beta1.KafkaChannel, opts v1.UpdateOptions) (*messagingv1beta1.KafkaChannel, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "messaging.knative.dev",
		Version: "v1beta1",
		Kind:    "KafkaChannel",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).UpdateStatus(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &messagingv1beta1.KafkaChannel{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapMessagingV1beta1KafkaChannelImpl) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return nil, errors.New("NYI: Watch")
}

// SourcesV1beta1 retrieves the SourcesV1beta1Client
func (w *wrapClient) SourcesV1beta1() typedsourcesv1beta1.SourcesV1beta1Interface {
	return &wrapSourcesV1beta1{
		dyn: w.dyn,
	}
}

type wrapSourcesV1beta1 struct {
	dyn dynamic.Interface
}

func (w *wrapSourcesV1beta1) RESTClient() rest.Interface {
	panic("RESTClient called on dynamic client!")
}

func (w *wrapSourcesV1beta1) KafkaSources(namespace string) typedsourcesv1beta1.KafkaSourceInterface {
	return &wrapSourcesV1beta1KafkaSourceImpl{
		dyn: w.dyn.Resource(schema.GroupVersionResource{
			Group:    "sources.knative.dev",
			Version:  "v1beta1",
			Resource: "kafkasources",
		}),

		namespace: namespace,
	}
}

type wrapSourcesV1beta1KafkaSourceImpl struct {
	dyn dynamic.NamespaceableResourceInterface

	namespace string
}

var _ typedsourcesv1beta1.KafkaSourceInterface = (*wrapSourcesV1beta1KafkaSourceImpl)(nil)

func (w *wrapSourcesV1beta1KafkaSourceImpl) Create(ctx context.Context, in *sourcesv1beta1.KafkaSource, opts v1.CreateOptions) (*sourcesv1beta1.KafkaSource, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "sources.knative.dev",
		Version: "v1beta1",
		Kind:    "KafkaSource",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Create(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &sourcesv1beta1.KafkaSource{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapSourcesV1beta1KafkaSourceImpl) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return w.dyn.Namespace(w.namespace).Delete(ctx, name, opts)
}

func (w *wrapSourcesV1beta1KafkaSourceImpl) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	return w.dyn.Namespace(w.namespace).DeleteCollection(ctx, opts, listOpts)
}

func (w *wrapSourcesV1beta1KafkaSourceImpl) Get(ctx context.Context, name string, opts v1.GetOptions) (*sourcesv1beta1.KafkaSource, error) {
	uo, err := w.dyn.Namespace(w.namespace).Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	out := &sourcesv1beta1.KafkaSource{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapSourcesV1beta1KafkaSourceImpl) List(ctx context.Context, opts v1.ListOptions) (*sourcesv1beta1.KafkaSourceList, error) {
	uo, err := w.dyn.Namespace(w.namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := &sourcesv1beta1.KafkaSourceList{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapSourcesV1beta1KafkaSourceImpl) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *sourcesv1beta1.KafkaSource, err error) {
	uo, err := w.dyn.Namespace(w.namespace).Patch(ctx, name, pt, data, opts)
	if err != nil {
		return nil, err
	}
	out := &sourcesv1beta1.KafkaSource{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapSourcesV1beta1KafkaSourceImpl) Update(ctx context.Context, in *sourcesv1beta1.KafkaSource, opts v1.UpdateOptions) (*sourcesv1beta1.KafkaSource, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "sources.knative.dev",
		Version: "v1beta1",
		Kind:    "KafkaSource",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).Update(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &sourcesv1beta1.KafkaSource{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapSourcesV1beta1KafkaSourceImpl) UpdateStatus(ctx context.Context, in *sourcesv1beta1.KafkaSource, opts v1.UpdateOptions) (*sourcesv1beta1.KafkaSource, error) {
	in.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "sources.knative.dev",
		Version: "v1beta1",
		Kind:    "KafkaSource",
	})
	uo := &unstructured.Unstructured{}
	if err := convert(in, uo); err != nil {
		return nil, err
	}
	uo, err := w.dyn.Namespace(w.namespace).UpdateStatus(ctx, uo, opts)
	if err != nil {
		return nil, err
	}
	out := &sourcesv1beta1.KafkaSource{}
	if err := convert(uo, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *wrapSourcesV1beta1KafkaSourceImpl) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return nil, errors.New("NYI: Watch")
}

func (w *wrapSourcesV1beta1KafkaSourceImpl) GetScale(ctx context.Context, name string, opts v1.GetOptions) (*autoscalingv1.Scale, error) {
	panic("NYI")
}
