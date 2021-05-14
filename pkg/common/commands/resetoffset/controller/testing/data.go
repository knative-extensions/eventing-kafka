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

package testing

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
)

const (
	ResetOffsetNamespace = "resetoffset-namespace"
	ResetOffsetName      = "resetoffset-name"
	ResetOffsetKey       = ResetOffsetNamespace + "/" + ResetOffsetName
	ResetOffsetFinalizer = "resetoffsets.kafka.eventing.knative.dev"

	RefAPIVersion = "ref-apiversion"
	RefKind       = "ref-kind"
	RefNamespace  = "ref-namespace"
	RefName       = "ref-name"

	Brokers = "TestKafkaBrokers"

	TopicName = "TestTopicName"
	GroupId   = "TestGroupId"
)

//
// ResetOffset Resources
//

// ResetOffsetOption allow for customizing a ResetOffset
type ResetOffsetOption func(resetOffset *kafkav1alpha1.ResetOffset)

// NewResetOffset creates a custom ResetOffset
func NewResetOffset(options ...ResetOffsetOption) *kafkav1alpha1.ResetOffset {

	// Create The Base ResetOffset
	resetOffset := &kafkav1alpha1.ResetOffset{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ResetOffset",
			APIVersion: kafkav1alpha1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ResetOffsetNamespace,
			Name:      ResetOffsetName,
		},
		Spec: kafkav1alpha1.ResetOffsetSpec{
			Offset: kafkav1alpha1.OffsetSpec{
				Time: kafkav1alpha1.OffsetEarliest,
			},
			Ref: duckv1.KReference{
				APIVersion: RefAPIVersion,
				Kind:       RefKind,
				Namespace:  RefNamespace,
				Name:       RefName,
			},
		},
	}

	// Apply The Specified Customizations
	for _, option := range options {
		option(resetOffset)
	}

	// Return The Custom ResetOffset
	return resetOffset
}

func WithSpecOffsetTime(time string) ResetOffsetOption {
	return func(resetOffset *kafkav1alpha1.ResetOffset) {
		resetOffset.Spec.Offset.Time = time
	}
}

func WithSpecRef(ref *duckv1.KReference) ResetOffsetOption {
	return func(resetOffset *kafkav1alpha1.ResetOffset) {
		resetOffset.Spec.Ref = *ref
	}
}

func WithFinalizer(resetOffset *kafkav1alpha1.ResetOffset) {
	resetOffset.ObjectMeta.Finalizers = []string{ResetOffsetFinalizer}
}

func WithStatusTopic(topic string) ResetOffsetOption {
	return func(resetOffset *kafkav1alpha1.ResetOffset) {
		resetOffset.Status.Topic = topic
	}
}

func WithStatusGroup(group string) ResetOffsetOption {
	return func(resetOffset *kafkav1alpha1.ResetOffset) {
		resetOffset.Status.Group = group
	}
}

func WithStatusPartitions(partitions []kafkav1alpha1.OffsetMapping) ResetOffsetOption {
	return func(resetOffset *kafkav1alpha1.ResetOffset) {
		resetOffset.Status.Partitions = partitions
	}
}

func WithStatusInitialized(resetOffset *kafkav1alpha1.ResetOffset) {
	resetOffset.Status.InitializeConditions()
}

func WithStatusRefMapped(state bool, failed ...string) ResetOffsetOption {
	return func(resetOffset *kafkav1alpha1.ResetOffset) {
		if state {
			resetOffset.Status.MarkRefMappedTrue()
		} else {
			resetOffset.Status.MarkRefMappedFailed(failed[0], failed[1])
		}
	}
}

func WithStatusResetInitiated(state bool, failed ...string) ResetOffsetOption {
	return func(resetOffset *kafkav1alpha1.ResetOffset) {
		if state {
			resetOffset.Status.MarkResetInitiatedTrue()
		} else {
			resetOffset.Status.MarkResetInitiatedFailed(failed[0], failed[1])
		}
	}
}

func WithStatusConsumerGroupsStopped(state bool, failed ...string) ResetOffsetOption {
	return func(resetOffset *kafkav1alpha1.ResetOffset) {
		if state {
			resetOffset.Status.MarkConsumerGroupsStoppedTrue()
		} else {
			resetOffset.Status.MarkConsumerGroupsStoppedFailed(failed[0], failed[1])
		}
	}
}

func WithStatusOffsetsUpdated(state bool, failed ...string) ResetOffsetOption {
	return func(resetOffset *kafkav1alpha1.ResetOffset) {
		if state {
			resetOffset.Status.MarkOffsetsUpdatedTrue()
		} else {
			resetOffset.Status.MarkOffsetsUpdatedFailed(failed[0], failed[1])
		}
	}
}

func WithStatusConsumerGroupsStarted(state bool, failed ...string) ResetOffsetOption {
	return func(resetOffset *kafkav1alpha1.ResetOffset) {
		if state {
			resetOffset.Status.MarkConsumerGroupsStartedTrue()
		} else {
			resetOffset.Status.MarkConsumerGroupsStartedFailed(failed[0], failed[1])
		}
	}
}
