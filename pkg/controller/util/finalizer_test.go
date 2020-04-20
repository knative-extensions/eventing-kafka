package util

import (
	"github.com/kyma-incubator/knative-kafka/pkg/controller/constants"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Test The KubernetesResourceFinalizerName() Functionality
func TestKubernetesResourceFinalizerName(t *testing.T) {
	const suffix = "TestSuffix"
	result := KubernetesResourceFinalizerName(suffix)
	assert.Equal(t, constants.KnativeKafkaFinalizerPrefix+suffix, result)
}
