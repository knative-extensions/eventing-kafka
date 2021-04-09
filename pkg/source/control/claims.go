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

package control

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"

	ctrl "knative.dev/control-protocol/pkg"
)

// This just contains the different opcodes
const (
	NotifySetupClaimsOpCode   ctrl.OpCode = 1
	NotifyCleanupClaimsOpCode ctrl.OpCode = 2
)

type Claims map[string][]int32

func ClaimsParser(payload []byte) (interface{}, error) {
	var claims Claims
	err := (&claims).UnmarshalBinary(payload)
	if err != nil {
		return nil, err
	}
	return claims, nil
}

func ClaimsMerger(old interface{}, new interface{}) interface{} {
	oldClaims := old.(Claims)
	newClaims := new.(Claims)
	result := oldClaims.copy()

	for topic, partitions := range result {
		if newPartitions, ok := newClaims[topic]; ok {
			result[topic] = sets.NewInt32(partitions...).Insert(newPartitions...).List() // Merge partitions
			delete(newClaims, topic)
		}
	}
	for newTopic, newPartitions := range newClaims {
		result[newTopic] = newPartitions
	}

	return result
}

func ClaimsDifference(old interface{}, new interface{}) interface{} {
	oldClaims := old.(Claims)
	cleanedClaims := new.(Claims)
	result := oldClaims.copy()

	for topic, partitions := range result {
		if cleanedPartitions, ok := cleanedClaims[topic]; ok {
			newSet := sets.NewInt32(partitions...).Delete(cleanedPartitions...).List()
			if len(newSet) == 0 {
				delete(result, topic)
			} else {
				result[topic] = newSet
			}
		}
	}

	return result
}

func (c Claims) String() string {
	strs := make([]string, 0, len(c))
	for topic, partitions := range c {
		strs = append(strs, fmt.Sprintf("'%s': %v", topic, partitions))
	}
	return strings.Join(strs, ", ")
}

func (c Claims) MarshalBinary() (data []byte, err error) {
	return json.Marshal(c)
}

func (c *Claims) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c Claims) copy() Claims {
	res := make(Claims, len(c))

	for k, v := range c {
		res[k] = v
	}

	return res
}
