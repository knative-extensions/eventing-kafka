package util

import (
	"fmt"
	kafkav1beta1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1beta1"
)

// Create A DNS Safe Name For The Specified KafkaChannel Suitable For Use With K8S Services
func DispatcherDnsSafeName(channel *kafkav1beta1.KafkaChannel) string {

	// In order for the resulting name to be a valid DNS component is 63 characters.  We are appending 12 characters to separate
	// the components and to indicate this is a Dispatcher which further reduces the available length to 51.  We will allocate 30
	// characters to the channel and 20 to the namespace, leaving some extra buffer.
	safeChannelName := GenerateValidDnsName(channel.Name, 30, true, false)
	safeChannelNamespace := GenerateValidDnsName(channel.Namespace, 20, false, false)
	return fmt.Sprintf("%s-%s-dispatcher", safeChannelName, safeChannelNamespace)
}
