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

	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/message"
)

const (
	SetContractCommand    ctrl.OpCode = 3
	NotifyContractUpdated ctrl.OpCode = 4
)

type KafkaSourceContract struct {
	Generation       int64    `json:"generation" required:"true"`
	BootstrapServers []string `json:"bootstrapServers" required:"true"`
	Topics           []string `json:"topics" required:"true"`
	ConsumerGroup    string   `json:"consumerGroup" required:"true"`
	KeyType          string   `json:"keyType" required:"false"`
}

func (k KafkaSourceContract) SerializedId() []byte {
	return message.Int64CommandId(k.Generation)
}

func (k KafkaSourceContract) MarshalBinary() (data []byte, err error) {
	return json.Marshal(k)
}

func (k *KafkaSourceContract) UnmarshalBinary(data []byte) error {
	// We might use something better than json here
	return json.Unmarshal(data, k)
}
