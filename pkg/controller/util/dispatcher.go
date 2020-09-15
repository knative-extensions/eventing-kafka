package util

import (
	"crypto/md5"
	"fmt"
	kafkav1beta1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1beta1"
)

// Create A DNS Safe Name For The Specified KafkaChannel Suitable For Use With K8S Services
func DispatcherDnsSafeName(channel *kafkav1beta1.KafkaChannel) string {

	// In order for the resulting name to be a valid DNS component is 63 characters.  We are appending 13 characters to
	// separate the components and to indicate this is a Dispatcher, and adding 8 hash characters, which further reduces
	// the available length to 42.
	// We will allocate 26 characters to the channel and 16 to the namespace, leaving some extra buffer.
	safeChannelName := GenerateValidDnsName(channel.Name, 26, true, false)
	safeChannelNamespace := GenerateValidDnsName(channel.Namespace, 16, false, false)
	hash := GenerateHash(channel.Name, 4) + GenerateHash(channel.Namespace, 4)
	return fmt.Sprintf("%s-%s-%s-dispatcher", safeChannelName, safeChannelNamespace, hash)
}

// Generate An MD5 Hash Of A String And Return Desired Number Of Characters
func GenerateHash(stringToHash string, length int) string {
	// Create an MD5 hash and return however many characters the caller wants (note that the max is 32 for MD5)
	return fmt.Sprintf(fmt.Sprintf("%%.%ds", length), fmt.Sprintf("%x", md5.Sum([]byte(stringToHash))))
}
