/*
Copyright 2020 The Knative Authors

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

package util

import (
	"fmt"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
)

// Create A DNS Safe Name For The Specified KafkaChannel Suitable For Use With K8S Services
func DispatcherDnsSafeName(channel *kafkav1beta1.KafkaChannel) string {

	// In order for the resulting name to be a valid DNS component is 63 characters.  We are appending 13 characters to
	// separate the components and to indicate this is a Dispatcher, and adding 8 hash characters, which further reduces
	// the available length to 42.
	// We will allocate 26 characters to the channel and 16 to the namespace, leaving some extra buffer.
	safeChannelName := GenerateValidDnsName(channel.Name, 26, true, false)
	safeChannelNamespace := GenerateValidDnsName(channel.Namespace, 16, false, false)
	hash := GenerateHash(channel.Name+channel.Namespace, 8)
	return fmt.Sprintf("%s-%s-%s-dispatcher", safeChannelName, safeChannelNamespace, hash)
}
