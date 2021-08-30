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

package v1beta2

import (
	"context"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/reconciler-test/pkg/feature"

	ktestlib "knative.dev/eventing-kafka/test/lib"
	testlib "knative.dev/eventing/test/lib"
)

const (
	strimziApiGroup      = "kafka.strimzi.io"
	strimziApiVersion    = "v1beta2"
	strimziTopicResource = "kafkatopics"
)

var topicGVR = schema.GroupVersionResource{Group: strimziApiGroup, Version: strimziApiVersion, Resource: strimziTopicResource}

func Install(c *testlib.Client, kafkaClusterName, kafkaClusterNamespace string) string {
	topicName := feature.MakeRandomK8sName("topic")

	topic := New(kafkaClusterName, kafkaClusterNamespace, topicName, 1)
	Create(c, topic)

	Eventually(IsReady(c, topic)).Should(BeTrue())

	return topicName
}

func New(clusterName, clusterNamespace, topicName string, partitions int) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": topicGVR.GroupVersion().String(),
			"kind":       "KafkaTopic",
			"metadata": map[string]interface{}{
				"name":      topicName,
				"namespace": clusterNamespace,
				"labels": map[string]interface{}{
					"strimzi.io/cluster": clusterName,
				},
			},
			"spec": map[string]interface{}{
				"partitions": partitions,
				"replicas":   1,
			},
		},
	}
}

func Create(c *testlib.Client, topic *unstructured.Unstructured) {
	meta := topic.Object["metadata"].(map[string]interface{})
	name := meta["name"].(string)
	namespace := meta["namespace"].(string)

	// TODO: retries
	_, err := c.Dynamic.Resource(topicGVR).Namespace(namespace).Create(context.Background(), topic, metav1.CreateOptions{})

	if err != nil {
		c.T.Fatalf("Error while creating the topic %s: %v", name, err)
	}

	c.Tracker.Add(topicGVR.Group, topicGVR.Version, topicGVR.Resource, namespace, name)
}

func IsReady(c *testlib.Client, topic *unstructured.Unstructured) func() (bool, error) {
	return func() (bool, error) {
		meta := topic.Object["metadata"].(map[string]interface{})
		name := meta["name"].(string)
		namespace := meta["namespace"].(string)

		return ktestlib.IsReady(c, namespace, name, topicGVR)
	}
}
