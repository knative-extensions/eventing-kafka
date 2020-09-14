package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
)

// Test The KubernetesResourceFinalizerName() Functionality
func TestKubernetesResourceFinalizerName(t *testing.T) {
	const suffix = "TestSuffix"
	result := KubernetesResourceFinalizerName(suffix)
	assert.Equal(t, constants.EventingKafkaFinalizerPrefix+suffix, result)
}
