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

package lib

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

func IsReady(c *testlib.Client, namespace, name string, gvr schema.GroupVersionResource) (bool, error) {
	like := &duckv1.KResource{}

	us, err := c.Dynamic.Resource(gvr).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.T.Log(namespace, name, err)
			// keep polling
			return false, nil
		}

		return false, err
	}
	obj := like.DeepCopy()
	if err = runtime.DefaultUnstructuredConverter.FromUnstructured(us.Object, obj); err != nil {
		c.T.Fatalf("Error DefaultUnstructured.DynamicConverter. %v", err)
	}
	obj.ResourceVersion = gvr.Version
	obj.APIVersion = gvr.GroupVersion().String()

	// First see if the resource has conditions.
	if len(obj.Status.Conditions) == 0 {
		return false, nil
	}

	// Ready type.
	ready := obj.Status.GetCondition(apis.ConditionReady)
	if ready != nil {
		// Succeeded type.
		ready = obj.Status.GetCondition(apis.ConditionSucceeded)
	}
	// Test Ready or Succeeded.
	if ready != nil {
		if !ready.IsTrue() {
			msg := fmt.Sprintf("%s is not %s, %s: %s", name, ready.Type, ready.Reason, ready.Message)
			c.T.Log(msg)

			return ready.IsFalse(), nil
		}

		c.T.Logf("%s is %s, %s: %s\n", name, ready.Type, ready.Reason, ready.Message)
		return ready.IsTrue(), nil
	}

	// Last resort, look at all conditions.
	// As a side-effect of this test,
	//   if a resource has no conditions, then it is ready.
	allReady := true
	for _, cd := range obj.Status.Conditions {
		if !cd.IsTrue() {
			msg := fmt.Sprintf("%s is not %s, %s: %s", name, cd.Type, cd.Reason, cd.Message)
			c.T.Logf(msg)
			allReady = false
			break
		}
	}
	return allReady, nil
}

// IsPodRunning tells whether the given pod to be in running state or not
func IsPodRunning(c *testlib.Client, ctx context.Context, podName string) (bool, error) {
	pod, err := c.Kube.CoreV1().Pods(c.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	return pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodSucceeded, nil
}

// HasGenerationBeenObserved returns true when `status.ObservedGeneration` matches the KResource generation.
func HasGenerationBeenObserved(c *testlib.Client, namespace, name string, gvr schema.GroupVersionResource) (bool, error) {
	obj, err := c.Dynamic.Resource(gvr).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.T.Log(namespace, name, err)
			// keep polling
			return false, nil
		}

		return false, err
	}

	kr := new(duckv1.KResource)
	if err = runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, kr); err != nil {
		c.T.Fatalf("Error DefaultUnstructured.DynamicConverter. %v", err)
	}

	return kr.Status.ObservedGeneration == kr.Generation, nil

}
