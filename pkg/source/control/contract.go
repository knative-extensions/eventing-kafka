package control

import (
	"encoding/json"

	ctrl "knative.dev/control-protocol/pkg"
)

const (
	SetContract ctrl.OpCode = 3
)

type KafkaSourceContract struct {
	Topics        []string `json:"topics" required:"true"`
	ConsumerGroup string   `json:"consumerGroup" required:"true"`
	KeyType       string   `json:"keyType" required:"false"`
}

func (k KafkaSourceContract) MarshalBinary() (data []byte, err error) {
	return json.Marshal(k)
}

func (k *KafkaSourceContract) UnmarshalBinary(data []byte) error {
	// We might use something better than json here
	return json.Unmarshal(data, k)
}
