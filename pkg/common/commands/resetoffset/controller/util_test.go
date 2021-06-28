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

package controller

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
)

func TestGenerateLockToken(t *testing.T) {

	// Test Data
	reconcilerId := types.UID(uuid.NewString())
	resetOffsetId := types.UID(uuid.NewString())

	// Perform The Test
	actualLockToken := GenerateLockToken(reconcilerId, resetOffsetId)

	// Verify The Results
	expectedLockToken := fmt.Sprintf("%s-%s", string(reconcilerId), string(resetOffsetId))
	assert.Equal(t, expectedLockToken, actualLockToken)
}

func TestGenerateCommandId(t *testing.T) {

	// Test Data
	uid := types.UID("TestUID")
	generation := int64(5)
	resetOffset := &kafkav1alpha1.ResetOffset{
		ObjectMeta: metav1.ObjectMeta{
			UID:        uid,
			Generation: generation,
		},
	}
	podIP := "TestPodIP"
	opCode := commands.StopConsumerGroupOpCode

	// Perform The Test
	actualCommandId, err := GenerateCommandId(resetOffset, podIP, opCode)

	// Verify The Results
	assert.Nil(t, err)
	assert.Equal(t, int64(3448459854), actualCommandId)
}
