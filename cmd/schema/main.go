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

package main

import (
	bindingv1beta1 "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	sourcev1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"log"

	"knative.dev/hack/schema/commands"
	"knative.dev/hack/schema/registry"

)

// schema is a tool to dump the schema for Eventing resources.
func main() {
	registry.Register(&messagingv1beta1.KafkaChannel{})
	registry.Register(&sourcev1beta1.KafkaSource{})
	registry.Register(&bindingv1beta1.KafkaBinding{})

	if err := commands.New("knative.dev/eventing-kafka").Execute(); err != nil {
		log.Fatal("Error during command execution: ", err)
	}
}
