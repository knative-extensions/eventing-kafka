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

package continual

import (
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"knative.dev/eventing/test/upgrade/prober/wathola/sender"
)

// BuildKafkaSender will create a wathola sender that sends events to Kafka
// topic directly.
func BuildKafkaSender() sender.EventSender {
	return &kafkaSender{}
}

type kafkaSender struct {}

func (k *kafkaSender) Supports(endpoint interface{}) bool {
	panic("implement me")
}

func (k *kafkaSender) SendEvent(ce cloudevents.Event, endpoint interface{}) error {
	panic("implement me")
}

