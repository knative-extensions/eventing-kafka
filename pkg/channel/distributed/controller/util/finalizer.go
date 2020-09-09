package util

import (
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
