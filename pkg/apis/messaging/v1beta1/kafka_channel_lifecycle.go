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

package v1beta1

import (
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

// A default ConditionSet containing base configuration only for testing.
// The consolidated and distributed KafkaChannel implementations require
// differentiated ConditionSets in order to accurately reflect their varied
// runtime architectures.  One of the "Register..." functions below should
// be called via an init() in the main() of associated components.
var kc = apis.NewLivingConditionSet(
	KafkaChannelConditionAddressable,
	KafkaChannelConditionConfigReady,
	KafkaChannelConditionTopicReady,
	KafkaChannelConditionChannelServiceReady)
var channelCondSetLock = sync.RWMutex{}

const (

	//
	// Common KafkaChannel Conditions
	//

	// KafkaChannelConditionReady has status True when all sub-conditions below have been set to True.
	KafkaChannelConditionReady = apis.ConditionReady

	// KafkaChannelConditionAddressable has status true when this KafkaChannel meets
	// the Addressable contract and has a non-empty URL.
	KafkaChannelConditionAddressable apis.ConditionType = "Addressable"

	// KafkaChannelConditionConfigReady has status True when the Kafka configuration to use by the channel
	// exists and is valid (i.e. the connection has been established).
	KafkaChannelConditionConfigReady apis.ConditionType = "ConfigurationReady"

	// KafkaChannelConditionTopicReady has status True when the Kafka topic to use by the channel exists.
	KafkaChannelConditionTopicReady apis.ConditionType = "TopicReady"

	// KafkaChannelConditionChannelServiceReady has status True when the K8S Service representing the channel
	// is ready. Because this uses ExternalName, there are no endpoints to check.
	KafkaChannelConditionChannelServiceReady apis.ConditionType = "ChannelServiceReady"

	//
	// Consolidated KafkaChannel Conditions
	//

	// KafkaChannelConditionDispatcherReady has status True when a Dispatcher deployment is ready
	// Keyed off appsv1.DeploymentAvailable, which means minimum available replicas required are up
	// and running for at least minReadySeconds.
	KafkaChannelConditionDispatcherReady apis.ConditionType = "DispatcherReady"

	// KafkaChannelConditionServiceReady has status True when a k8s Service is ready. This
	// basically just means it exists because there's no meaningful status in Service. See Endpoints
	// below.
	KafkaChannelConditionServiceReady apis.ConditionType = "ServiceReady"

	// KafkaChannelConditionEndpointsReady has status True when a k8s Service Endpoints are backed
	// by at least one endpoint.
	KafkaChannelConditionEndpointsReady apis.ConditionType = "EndpointsReady"

	//
	// Distributed KafkaChannel Conditions
	//

	// KafkaChannelConditionReceiverServiceReady has status True when the Receiver's K8S Service is ready.
	// This basically just means it exists because there's no meaningful status in Service.
	KafkaChannelConditionReceiverServiceReady apis.ConditionType = "ReceiverServiceReady"

	// KafkaChannelConditionReceiverDeploymentReady has status True when the Receiver's K8S Deployment is ready.
	KafkaChannelConditionReceiverDeploymentReady apis.ConditionType = "ReceiverDeploymentReady"

	// KafkaChannelConditionDispatcherServiceReady has status True when the Dispatcher's K8S Service is ready.
	// This basically just means it exists because there's no meaningful status in Service.
	// Note - Dispatcher Service is only exposed for Metrics collection and is not an active part of the data-plane.
	KafkaChannelConditionDispatcherServiceReady apis.ConditionType = "DispatcherServiceReady"

	// KafkaChannelConditionDispatcherDeploymentReady has status True when the Receiver's K8S Deployment is ready.
	KafkaChannelConditionDispatcherDeploymentReady apis.ConditionType = "DispatcherDeploymentReady"
)

// RegisterConsolidatedKafkaChannelConditionSet initializes the ConditionSet to those pertaining to the consolidated KafkaChannel.
func RegisterConsolidatedKafkaChannelConditionSet() {
	RegisterAlternateKafkaChannelConditionSet(
		apis.NewLivingConditionSet(
			KafkaChannelConditionAddressable,
			KafkaChannelConditionConfigReady,
			KafkaChannelConditionTopicReady,
			KafkaChannelConditionChannelServiceReady,
			KafkaChannelConditionDispatcherReady,
			KafkaChannelConditionServiceReady,
			KafkaChannelConditionEndpointsReady,
		),
	)
}

// RegisterDistributedKafkaChannelConditionSet initializes the ConditionSet to those pertaining to the distributed KafkaChannel.
func RegisterDistributedKafkaChannelConditionSet() {
	RegisterAlternateKafkaChannelConditionSet(
		apis.NewLivingConditionSet(
			KafkaChannelConditionAddressable,
			KafkaChannelConditionConfigReady,
			KafkaChannelConditionTopicReady,
			KafkaChannelConditionChannelServiceReady,
			KafkaChannelConditionReceiverServiceReady,
			KafkaChannelConditionReceiverDeploymentReady,
			KafkaChannelConditionDispatcherServiceReady,
			KafkaChannelConditionDispatcherDeploymentReady,
		),
	)
}

// RegisterAlternateKafkaChannelConditionSet register a different apis.ConditionSet.
func RegisterAlternateKafkaChannelConditionSet(conditionSet apis.ConditionSet) {
	channelCondSetLock.Lock()
	defer channelCondSetLock.Unlock()

	kc = conditionSet
}

// GetConditionSet retrieves the condition set for this resource. Implements the KRShaped interface.
func (*KafkaChannel) GetConditionSet() apis.ConditionSet {
	channelCondSetLock.RLock()
	defer channelCondSetLock.RUnlock()

	return kc
}

// GetConditionSet retrieves the condition set for this resource.
func (*KafkaChannelStatus) GetConditionSet() apis.ConditionSet {
	channelCondSetLock.RLock()
	defer channelCondSetLock.RUnlock()

	return kc
}

// GetCondition returns the condition currently associated with the given type, or nil.
func (kcs *KafkaChannelStatus) GetCondition(t apis.ConditionType) *apis.Condition {
	return kcs.GetConditionSet().Manage(kcs).GetCondition(t)
}

// IsReady returns true if the resource is ready overall.
func (kcs *KafkaChannelStatus) IsReady() bool {
	return kcs.GetConditionSet().Manage(kcs).IsHappy()
}

//
// Common  KafkaChannel Condition Markers
//

// InitializeConditions sets relevant unset conditions to Unknown state.
func (kcs *KafkaChannelStatus) InitializeConditions() {
	kcs.GetConditionSet().Manage(kcs).InitializeConditions()
}

// SetAddress sets the address (as part of Addressable contract) and marks the correct condition.
func (kcs *KafkaChannelStatus) SetAddress(url *apis.URL) {
	if kcs.Address == nil {
		kcs.Address = &duckv1.Addressable{}
	}
	if url != nil {
		kcs.Address.URL = url
		kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionAddressable)
	} else {
		kcs.Address.URL = nil
		kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionAddressable, "EmptyURL", "URL is nil")
	}
}

func (kcs *KafkaChannelStatus) MarkConfigTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionConfigReady)
}

func (kcs *KafkaChannelStatus) MarkConfigFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionConfigReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkTopicTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionTopicReady)
}

func (kcs *KafkaChannelStatus) MarkTopicFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionTopicReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkChannelServiceFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionChannelServiceReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkChannelServiceTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionChannelServiceReady)
}

//
// Consolidated KafkaChannel Condition Markers
//

func (kcs *KafkaChannelStatus) MarkDispatcherFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionDispatcherReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkDispatcherUnknown(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkUnknown(KafkaChannelConditionDispatcherReady, reason, messageFormat, messageA...)
}

// TODO: Unify this with the ones from Eventing. Say: Broker, Trigger.
func (kcs *KafkaChannelStatus) PropagateDispatcherStatus(ds *appsv1.DeploymentStatus) {
	for _, cond := range ds.Conditions {
		if cond.Type == appsv1.DeploymentAvailable {
			if cond.Status == corev1.ConditionTrue {
				kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionDispatcherReady)
			} else if cond.Status == corev1.ConditionFalse {
				kcs.MarkDispatcherFailed("DispatcherDeploymentFalse", "The status of Dispatcher Deployment is False: %s : %s", cond.Reason, cond.Message)
			} else if cond.Status == corev1.ConditionUnknown {
				kcs.MarkDispatcherUnknown("DispatcherDeploymentUnknown", "The status of Dispatcher Deployment is Unknown: %s : %s", cond.Reason, cond.Message)
			}
		}
	}
}

func (kcs *KafkaChannelStatus) MarkServiceFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionServiceReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkServiceUnknown(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkUnknown(KafkaChannelConditionServiceReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkServiceTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionServiceReady)
}

func (kcs *KafkaChannelStatus) MarkEndpointsFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionEndpointsReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkEndpointsTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionEndpointsReady)
}

//
// Distributed KafkaChannel Condition Markers
//

func (kcs *KafkaChannelStatus) MarkReceiverServiceFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionReceiverServiceReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkReceiverServiceTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionReceiverServiceReady)
}

func (kcs *KafkaChannelStatus) MarkReceiverDeploymentFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionReceiverDeploymentReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkReceiverDeploymentTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionReceiverDeploymentReady)
}

func (kcs *KafkaChannelStatus) MarkDispatcherServiceFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionDispatcherServiceReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkDispatcherServiceTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionDispatcherServiceReady)
}

func (kcs *KafkaChannelStatus) MarkDispatcherServiceUnknown(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkUnknown(KafkaChannelConditionDispatcherServiceReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkDispatcherDeploymentFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionDispatcherDeploymentReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkDispatcherDeploymentUnknown(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkUnknown(KafkaChannelConditionDispatcherDeploymentReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) PropagateDispatcherDeploymentStatus(ds *appsv1.DeploymentStatus) {
	for _, cond := range ds.Conditions {
		if cond.Type == appsv1.DeploymentAvailable {
			if cond.Status == corev1.ConditionTrue {
				kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionDispatcherDeploymentReady)
			} else if cond.Status == corev1.ConditionFalse {
				kcs.MarkDispatcherDeploymentFailed("DispatcherDeploymentFalse", "The status of Dispatcher Deployment is False: %s : %s", cond.Reason, cond.Message)
			} else if cond.Status == corev1.ConditionUnknown {
				kcs.MarkDispatcherDeploymentUnknown("DispatcherDeploymentUnknown", "The status of Dispatcher Deployment is Unknown: %s : %s", cond.Reason, cond.Message)
			}
		}
	}
}
