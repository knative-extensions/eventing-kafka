package util

import (
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"testing"
)

// Test The KubernetesResourceFinalizerName() Functionality
func TestKubernetesResourceFinalizerName(t *testing.T) {
	const suffix = "TestSuffix"
	result := KubernetesResourceFinalizerName(suffix)
	assert.Equal(t, constants.EventingKafkaFinalizerPrefix+suffix, result)
}
