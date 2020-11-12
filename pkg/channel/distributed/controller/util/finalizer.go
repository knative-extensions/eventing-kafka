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

package util

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
)

//
// Get A Kubernetes "Qualified" Finalizer Name With The Specified Suffix
//
// When adding finalizers to Kubernetes built-in components (Secrets, ConfigMaps, Services, etc...)
// it is necessary to "qualify" the finalizer name by including a "/".  Kubernetes performs validation
// on the finalizer names and only accepts the following values if NOT qualified ...
//
//     FinalizerOrphanDependents string = "orphan"
//     FinalizerDeleteDependents string = "foregroundDeletion"
//	   FinalizerKubernetes FinalizerName = "kubernetes"
//
// ... and will produce the following error message ...
//
//     metadata.finalizers[0]: Invalid value: \"externaltarget-controller\": name is neither a standard finalizer name nor is it fully qualified
//
// This is in contrast with Finalizers on a Custom Resource which can just be any arbitrary string.
// There was little to no documentation on this and it required tracing the Kubernetes pkg/apis/core
// source code to decipher what a "qualified" name format would be.
//
func KubernetesResourceFinalizerName(finalizerSuffix string) string {
	return constants.EventingKafkaFinalizerPrefix + finalizerSuffix
}

// Determine Whether ObjectMeta Contains Specified Finalizer
func HasFinalizer(finalizer string, objectMeta *metav1.ObjectMeta) bool {
	if objectMeta != nil {
		finalizers := sets.NewString(objectMeta.Finalizers...)
		return finalizers.Has(finalizer)
	} else {
		return false
	}
}

// Remove The Specified Finalizer From The Object
func RemoveFinalizer(finalizer string, objectMeta *metav1.ObjectMeta) {
	if objectMeta != nil {
		finalizers := sets.NewString(objectMeta.Finalizers...)
		finalizers.Delete(finalizer)
		objectMeta.Finalizers = finalizers.List()
	}
}
