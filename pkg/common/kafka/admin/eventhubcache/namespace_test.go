package eventhubcache

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	logtesting "knative.dev/pkg/logging/testing"
	"testing"
)

// Test The NewNamespace() Constructor
func TestNewNamespace(t *testing.T) {

	// Test Data
	name := "TestName"
	username := "TestUsername"
	password := "TestPassword"
	secret := "TestSecret"
	count := 99

	// Create A Mock HubManager
	mockHubManager := &MockHubManager{}

	// Replace The NewHubManagerFromConnectionString Wrapper To Provide Mock Implementation & Defer Reset
	newHubManagerFromConnectionStringWrapperPlaceholder := NewHubManagerFromConnectionStringWrapper
	NewHubManagerFromConnectionStringWrapper = func(connectionString string) (managerInterface HubManagerInterface, e error) {
		return mockHubManager, nil
	}
	defer func() { NewHubManagerFromConnectionStringWrapper = newHubManagerFromConnectionStringWrapperPlaceholder }()

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Perform The Test
	namespace, err := NewNamespace(logger, name, username, password, secret, count)

	// Verify Results
	assert.NotNil(t, namespace)
	assert.Nil(t, err)
	assert.Equal(t, name, namespace.Name)
	assert.Equal(t, username, namespace.Username)
	assert.Equal(t, password, namespace.Password)
	assert.Equal(t, secret, namespace.Secret)
	assert.Equal(t, count, namespace.Count)
	assert.Equal(t, mockHubManager, namespace.HubManager)
}

// Test The NewNamespace() Constructor With HubManager Error Handling
func TestNewNamespaceError(t *testing.T) {

	// Test Data
	name := "TestName"
	username := "TestUsername"
	password := "TestPassword"
	secret := "TestSecret"
	count := 99

	// Replace The NewHubManagerFromConnectionString Wrapper To Return Error & Defer Reset
	newHubManagerFromConnectionStringWrapperPlaceholder := NewHubManagerFromConnectionStringWrapper
	NewHubManagerFromConnectionStringWrapper = func(connectionString string) (managerInterface HubManagerInterface, e error) {
		return nil, fmt.Errorf("expected test error")
	}
	defer func() { NewHubManagerFromConnectionStringWrapper = newHubManagerFromConnectionStringWrapperPlaceholder }()

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Perform The Test
	namespace, err := NewNamespace(logger, name, username, password, secret, count)

	// Verify Results
	assert.Nil(t, namespace)
	assert.NotNil(t, err)
}

// Test The NewNamespaceFromKafkaSecret() Constructor
func TestNewNamespaceFromKafkaSecret(t *testing.T) {

	// Test Data
	username := "TestUsername"
	password := "TestPassword"
	secret := "TestSecret"
	eventhubNamespace := "TestNamespace"
	count := 0

	kafkaSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secret},
		Data: map[string][]byte{
			constants.KafkaSecretKeyNamespace: []byte(eventhubNamespace),
			constants.KafkaSecretKeyUsername:  []byte(username),
			constants.KafkaSecretKeyPassword:  []byte(password),
		},
	}

	// Create A Mock HubManager
	mockHubManager := &MockHubManager{}

	// Replace The NewHubManagerFromConnectionString Wrapper To Provide Mock Implementation & Defer Reset
	newHubManagerFromConnectionStringWrapperPlaceholder := NewHubManagerFromConnectionStringWrapper
	NewHubManagerFromConnectionStringWrapper = func(connectionString string) (managerInterface HubManagerInterface, e error) {
		return mockHubManager, nil
	}
	defer func() { NewHubManagerFromConnectionStringWrapper = newHubManagerFromConnectionStringWrapperPlaceholder }()

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Perform The Test
	namespace, err := NewNamespaceFromKafkaSecret(logger, kafkaSecret)

	// Verify Results
	assert.NotNil(t, namespace)
	assert.Nil(t, err)
	assert.Equal(t, eventhubNamespace, namespace.Name)
	assert.Equal(t, username, namespace.Username)
	assert.Equal(t, password, namespace.Password)
	assert.Equal(t, secret, namespace.Secret)
	assert.Equal(t, count, namespace.Count)
	assert.Equal(t, mockHubManager, namespace.HubManager)
}
