package control

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
)

// This just contains the different opcodes
const (
	NotifySetupClaimsOpCode   uint8 = 1
	NotifyCleanupClaimsOpCode uint8 = 2
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
	var strs []string
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
