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
	"hash/fnv"

	"k8s.io/apimachinery/pkg/types"
	ctrl "knative.dev/control-protocol/pkg"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
)

// GenerateLockToken returns a string representing a Lock.Token unique to the specified Reconciler and ResetOffset
func GenerateLockToken(reconcilerId types.UID, resetOffsetId types.UID) string {
	return fmt.Sprintf("%s-%s", string(reconcilerId), string(resetOffsetId))
}

// GenerateCommandId returns an int64 hash based on the specified ResetOffset.
func GenerateCommandId(resetOffset *kafkav1alpha1.ResetOffset, podIP string, opCode ctrl.OpCode) (int64, error) {
	hash := fnv.New32a()
	_, err := hash.Write([]byte(fmt.Sprintf("%s-%d-%s-%d", string(resetOffset.UID), resetOffset.Generation, podIP, opCode)))
	if err != nil {
		return -1, err
	}
	return int64(hash.Sum32()), nil
}
