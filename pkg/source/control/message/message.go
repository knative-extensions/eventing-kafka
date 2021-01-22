package control

import (
	"encoding/json"
	"fmt"
	"strings"
)

// This just contains the different opcodes
const (
	SetupClaimsOpCode   uint8 = 1
	CleanupClaimsOpCode uint8 = 2
)

type Claims map[string][]int32

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
