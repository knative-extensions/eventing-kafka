package channel

import (
	"context"
	channelhealth "knative.dev/eventing-kafka/pkg/channel/health"
	"knative.dev/eventing-kafka/pkg/channel/test"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	kafkaclientset "knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned"
	fakeclientset "knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned/fake"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
	"testing"
)

// Test The InitializeKafkaChannelLister() Functionality
func TestInitializeKafkaChannelLister(t *testing.T) {

	// Stub The K8S Client Creation Wrapper With Test Version Returning The Fake KafkaClient Clientset
	getKafkaClient = func(ctx context.Context, masterUrl string, kubeconfigPath string) (kafkaclientset.Interface, error) {
		return fakeclientset.NewSimpleClientset(), nil
	}

	// Create A Context With Test Logger
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t))

	// Perform The Test
	healthServer := channelhealth.NewChannelHealthServer("12345")
	err := InitializeKafkaChannelLister(ctx, "", "", healthServer)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, kafkaChannelLister)
	assert.Equal(t, true, healthServer.ChannelReady())
}

// Test All The ValidateKafkaChannel() Functionality
func TestValidateKafkaChannel(t *testing.T) {

	// Set The Package Level Logger To A Test Logger
	logger = logtesting.TestLogger(t).Desugar()

	// Test Data
	channelName := "TestChannelName"
	channelNamespace := "TestChannelNamespace"

	// Test All Permutations Of KafkaChannel Validation
	performValidateKafkaChannelTest(t, "", channelNamespace, false, corev1.ConditionFalse, true)
	performValidateKafkaChannelTest(t, channelName, "", false, corev1.ConditionFalse, true)
	performValidateKafkaChannelTest(t, channelName, channelNamespace, true, corev1.ConditionTrue, false)
	performValidateKafkaChannelTest(t, channelName, channelNamespace, true, corev1.ConditionFalse, true)
	performValidateKafkaChannelTest(t, channelName, channelNamespace, true, corev1.ConditionTrue, true)
	performValidateKafkaChannelTest(t, channelName, channelNamespace, false, corev1.ConditionFalse, true)
}

// Utility Function To Perform A Single Instance Of The ValidateKafkaChannel Test
func performValidateKafkaChannelTest(t *testing.T, channelName string, channelNamespace string, exists bool, ready corev1.ConditionStatus, err bool) {

	// Create The Channel Reference To Test
	channelReference := test.CreateChannelReference(channelName, channelNamespace)

	// Mock The Package Level KafkaChannel Lister For The Specified Use Case
	kafkaChannelLister = test.NewMockKafkaChannelLister(channelReference.Name, channelReference.Namespace, exists, ready, err)

	// Perform The Test
	validationError := ValidateKafkaChannel(channelReference)

	// Verify The Results
	assert.Equal(t, err, validationError != nil)
}

// Test The Close() Functionality
func TestClose(t *testing.T) {

	// Set The Package Level Logger To A Test Logger
	logger = logtesting.TestLogger(t).Desugar()

	// Test With Nil stopChan Instance
	Close()

	// Initialize The stopChan Instance
	stopChan = make(chan struct{})

	// Close In The Background
	go Close()

	// Block On The stopChan
	_, ok := <-stopChan

	// Verify stopChan Was Closed
	assert.False(t, ok)
}
