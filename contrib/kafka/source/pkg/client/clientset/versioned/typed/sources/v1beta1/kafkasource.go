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

// Code generated by client-gen. DO NOT EDIT.

package v1beta1

import (
	"context"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
	v1beta1 "knative.dev/eventing-kafka/contrib/kafka/source/pkg/apis/sources/v1beta1"
	scheme "knative.dev/eventing-kafka/contrib/kafka/source/pkg/client/clientset/versioned/scheme"
)

// KafkaSourcesGetter has a method to return a KafkaSourceInterface.
// A group's client should implement this interface.
type KafkaSourcesGetter interface {
	KafkaSources(namespace string) KafkaSourceInterface
}

// KafkaSourceInterface has methods to work with KafkaSource resources.
type KafkaSourceInterface interface {
	Create(ctx context.Context, kafkaSource *v1beta1.KafkaSource, opts v1.CreateOptions) (*v1beta1.KafkaSource, error)
	Update(ctx context.Context, kafkaSource *v1beta1.KafkaSource, opts v1.UpdateOptions) (*v1beta1.KafkaSource, error)
	UpdateStatus(ctx context.Context, kafkaSource *v1beta1.KafkaSource, opts v1.UpdateOptions) (*v1beta1.KafkaSource, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1beta1.KafkaSource, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1beta1.KafkaSourceList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1beta1.KafkaSource, err error)
	KafkaSourceExpansion
}

// kafkaSources implements KafkaSourceInterface
type kafkaSources struct {
	client rest.Interface
	ns     string
}

// newKafkaSources returns a KafkaSources
func newKafkaSources(c *SourcesV1beta1Client, namespace string) *kafkaSources {
	return &kafkaSources{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the kafkaSource, and returns the corresponding kafkaSource object, and an error if there is any.
func (c *kafkaSources) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1beta1.KafkaSource, err error) {
	result = &v1beta1.KafkaSource{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("kafkasources").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of KafkaSources that match those selectors.
func (c *kafkaSources) List(ctx context.Context, opts v1.ListOptions) (result *v1beta1.KafkaSourceList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1beta1.KafkaSourceList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("kafkasources").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested kafkaSources.
func (c *kafkaSources) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("kafkasources").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a kafkaSource and creates it.  Returns the server's representation of the kafkaSource, and an error, if there is any.
func (c *kafkaSources) Create(ctx context.Context, kafkaSource *v1beta1.KafkaSource, opts v1.CreateOptions) (result *v1beta1.KafkaSource, err error) {
	result = &v1beta1.KafkaSource{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("kafkasources").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(kafkaSource).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a kafkaSource and updates it. Returns the server's representation of the kafkaSource, and an error, if there is any.
func (c *kafkaSources) Update(ctx context.Context, kafkaSource *v1beta1.KafkaSource, opts v1.UpdateOptions) (result *v1beta1.KafkaSource, err error) {
	result = &v1beta1.KafkaSource{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("kafkasources").
		Name(kafkaSource.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(kafkaSource).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *kafkaSources) UpdateStatus(ctx context.Context, kafkaSource *v1beta1.KafkaSource, opts v1.UpdateOptions) (result *v1beta1.KafkaSource, err error) {
	result = &v1beta1.KafkaSource{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("kafkasources").
		Name(kafkaSource.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(kafkaSource).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the kafkaSource and deletes it. Returns an error if one occurs.
func (c *kafkaSources) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("kafkasources").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *kafkaSources) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("kafkasources").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched kafkaSource.
func (c *kafkaSources) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1beta1.KafkaSource, err error) {
	result = &v1beta1.KafkaSource{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("kafkasources").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}
