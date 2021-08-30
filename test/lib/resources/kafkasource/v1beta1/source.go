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

package v1beta1

import (
	"context"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/naming"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	kafkabindingv1beta1 "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	kafkasourcev1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	ktestlib "knative.dev/eventing-kafka/test/lib"
)

var GVR = schema.GroupVersionResource{Group: "sources.knative.dev", Version: "v1beta1", Resource: "kafkasources"}

// Install creates a minimal KafkaSource object, installs it and wait for it to be ready
func Install(c *testlib.Client, bootstrapServer string, topicName string, ref *duckv1.KReference) string {
	name := naming.MakeRandomK8sName("kafkasource")
	ks := New(name, bootstrapServer, topicName, ref)
	Create(c, ks)

	Eventually(IsReady(c, ks)).Should(BeTrue())
	return name
}

// New creates a minimal KafkaSource object
func New(name string, bootstrapServer string, topicName string, ref *duckv1.KReference) *kafkasourcev1beta1.KafkaSource {
	source := &kafkasourcev1beta1.KafkaSource{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: kafkasourcev1beta1.KafkaSourceSpec{
			KafkaAuthSpec: kafkabindingv1beta1.KafkaAuthSpec{
				BootstrapServers: []string{bootstrapServer},
			},
			Topics: []string{topicName},
			SourceSpec: duckv1.SourceSpec{
				Sink: duckv1.Destination{
					Ref: ref,
				},
			},
		},
	}

	return source
}

// Get the KafkaSource of the given name
func Get(c *testlib.Client, name string) *kafkasourcev1beta1.KafkaSource {
	kafkaSourceClientSet, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaSource client: %v", err)
	}

	obj, err := kafkaSourceClientSet.SourcesV1beta1().KafkaSources(c.Namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		c.T.Fatalf("Failed to get v1beta1 KafkaSource %q: %v", name, err)
	}

	return obj
}

// Create the given source on the cluster
func Create(c *testlib.Client, source *kafkasourcev1beta1.KafkaSource) {
	kafkaSourceClientSet, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaSource client: %v", err)
	}

	kSources := kafkaSourceClientSet.SourcesV1beta1().KafkaSources(c.Namespace)
	obj, err := kSources.Create(context.Background(), source, metav1.CreateOptions{})

	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaSource %q: %v", source.Name, err)
	}

	c.Tracker.AddObj(obj)
}

// Update the given source on the cluster
func Update(c *testlib.Client, source *kafkasourcev1beta1.KafkaSource) {
	kafkaSourceClientSet, err := kafkaclientset.NewForConfig(c.Config)
	if err != nil {
		c.T.Fatalf("Failed to create v1beta1 KafkaSource client: %v", err)
	}

	kSources := kafkaSourceClientSet.SourcesV1beta1().KafkaSources(c.Namespace)
	source, err = kSources.Update(context.Background(), source, metav1.UpdateOptions{})

	if err != nil {
		c.T.Fatalf("Failed to update v1beta1 KafkaSource %q: %v", source.Name, err)
	}
}

// WithTLSEnabled enables TLS
func WithTLSEnabled(source *kafkasourcev1beta1.KafkaSource, secretName string) {
	source.Spec.KafkaAuthSpec.Net.TLS.Enable = true
	if secretName != "" {
		source.Spec.KafkaAuthSpec.Net.TLS.CACert.SecretKeyRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: secretName,
			},
			Key: "ca.crt",
		}
		source.Spec.KafkaAuthSpec.Net.TLS.Cert.SecretKeyRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: secretName,
			},
			Key: "user.crt",
		}
		source.Spec.KafkaAuthSpec.Net.TLS.Key.SecretKeyRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: secretName,
			},
			Key: "user.key",
		}
	}
}

// WithSASLEnabled enables SASL
func WithSASLEnabled(source *kafkasourcev1beta1.KafkaSource, secretName string) {
	source.Spec.KafkaAuthSpec.Net.SASL.Enable = true
	source.Spec.KafkaAuthSpec.Net.SASL.User.SecretKeyRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: secretName,
		},
		Key: "user",
	}
	source.Spec.KafkaAuthSpec.Net.SASL.Password.SecretKeyRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: secretName,
		},
		Key: "password",
	}
	source.Spec.KafkaAuthSpec.Net.SASL.Type.SecretKeyRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: secretName,
		},
		Key: "saslType",
	}
	source.Spec.KafkaAuthSpec.Net.TLS.Enable = true
	source.Spec.KafkaAuthSpec.Net.TLS.CACert.SecretKeyRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: secretName,
		},
		Key: "ca.crt",
	}
}

// HasGenerationBeenObserved returns true when the observed generation matches the source generation
func HasGenerationBeenObserved(c *testlib.Client, source *kafkasourcev1beta1.KafkaSource) func() (bool, error) {
	return func() (bool, error) {
		return ktestlib.HasGenerationBeenObserved(c, c.Namespace, source.Name, GVR)
	}
}

// IsReady returns true when the source is ready
func IsReady(c *testlib.Client, source *kafkasourcev1beta1.KafkaSource) func() (bool, error) {
	return func() (bool, error) {
		return ktestlib.IsReady(c, c.Namespace, source.Name, GVR)
	}
}
