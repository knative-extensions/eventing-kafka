package config

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/common/constants"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"
)

// The watcher handler sets this variable to indicate that it was called
var watchedConfigMap *corev1.ConfigMap

// Test The InitializeObservability() Functionality
func TestInitializeConfigWatcher(t *testing.T) {

	// Test Data
	ctx := context.TODO()

	// Obtain a Test Logger (Required By be InitializeConfigWatcher function)
	logger := logtesting.TestLogger(t)

	// Setup Environment
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))

	// Create A Test Observability ConfigMap For The InitializeObservability() Call To Watch
	configMap := commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, commontesting.TestEKConfig)

	// Create The Fake K8S Client And Add It To The ConfigMap
	fakeK8sClient := fake.NewSimpleClientset(configMap)

	// Add The Fake K8S Client To The Context (Required By InitializeObservability)
	ctx = context.WithValue(ctx, injectionclient.Key{}, fakeK8sClient)

	// Perform The Test (Initialize The Observability Watcher)
	err := InitializeConfigWatcher(logger, ctx, configWatcherHandler)
	assert.Nil(t, err)

	testConfigMap, err := fakeK8sClient.CoreV1().ConfigMaps(system.Namespace()).Get(SettingsConfigMapName, v1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, testConfigMap.Data["sarama"], commontesting.OldSaramaConfig)

	// Change the config map and verify the handler is called
	testConfigMap.Data["sarama"] = commontesting.NewSaramaConfig

	// The configWatcherHandler should change this to a valid ConfigMap
	watchedConfigMap = nil

	testConfigMap, err = fakeK8sClient.CoreV1().ConfigMaps(system.Namespace()).Update(testConfigMap)
	assert.Nil(t, err)
	assert.Equal(t, testConfigMap.Data["sarama"], commontesting.NewSaramaConfig)

	// Wait for the configWatcherHandler to be called (happens pretty quickly; loop usually only runs once)
	for try := 0; watchedConfigMap == nil && try < 100; try++ {
		time.Sleep(5 * time.Millisecond)
	}
	assert.NotNil(t, watchedConfigMap)
	assert.Equal(t, watchedConfigMap.Data["sarama"], commontesting.NewSaramaConfig)
}

// Handler function for the ConfigMap watcher
func configWatcherHandler(configMap *corev1.ConfigMap) {
	// Set this package variable to indicate that the test watcher was called
	watchedConfigMap = configMap
}

func TestLoadSettingsConfigMap(t *testing.T) {
	// Not much to this function; just set up a configmap and make sure it gets loaded
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, constants.KnativeEventingNamespace))
	configMap := commontesting.GetTestSaramaConfigMap(commontesting.OldSaramaConfig, commontesting.TestEKConfig)
	fakeK8sClient := fake.NewSimpleClientset(configMap)

	getConfigMap, err := LoadSettingsConfigMap(fakeK8sClient)
	assert.Nil(t, err)
	assert.Equal(t, configMap.Data[SaramaSettingsConfigKey], getConfigMap.Data[SaramaSettingsConfigKey])
}
