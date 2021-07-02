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

package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
)

func TestDefaultTypeMap(t *testing.T) {
	assert.Len(t, types, 1)
	typeEntry := types[messagingv1beta1.SchemeGroupVersion.WithKind("KafkaChannel")]
	assert.NotNil(t, typeEntry)
	assert.IsType(t, &messagingv1beta1.KafkaChannel{}, typeEntry)
}

func TestDefaultCallbacksMap(t *testing.T) {
	assert.Len(t, callbacks, 0)
}

func TestIncludeResetOffset(t *testing.T) {
	IncludeResetOffset()
	assert.Len(t, types, 2)

	kcTypeEntry := types[messagingv1beta1.SchemeGroupVersion.WithKind("KafkaChannel")]
	assert.NotNil(t, kcTypeEntry)
	assert.IsType(t, &messagingv1beta1.KafkaChannel{}, kcTypeEntry)

	roTypeEntry := types[kafkav1alpha1.SchemeGroupVersion.WithKind("ResetOffset")]
	assert.NotNil(t, roTypeEntry)
	assert.IsType(t, &kafkav1alpha1.ResetOffset{}, roTypeEntry)
}
