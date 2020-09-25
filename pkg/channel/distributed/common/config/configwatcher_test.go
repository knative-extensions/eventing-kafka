package config

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/constants"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"
)

// The watcher handler sets this variable to indicate that it was called
var (
	watchedConfigMap *corev1.ConfigMap
	configMapMutex   = sync.Mutex{} // Don't trip up the data race examiner during tests
)

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

	// The configWatcherHandler should change the nil "watchedConfigMap" to a valid ConfigMap when the watcher triggers

	testConfigMap, err := fakeK8sClient.CoreV1().ConfigMaps(system.Namespace()).Get(ctx, SettingsConfigMapName, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, testConfigMap.Data["sarama"], commontesting.OldSaramaConfig)

	// Perform The Test (Initialize The Observability Watcher)
	err = InitializeConfigWatcher(ctx, logger, configWatcherHandler)
	assert.Nil(t, err)

	// Wait for the configWatcherHandler to be called (happens pretty quickly; loop usually only runs once)
	for try := 0; getWatchedMap() == nil && try < 100; try++ {
		time.Sleep(5 * time.Millisecond)
	}

	assert.Equal(t, getWatchedMap().Data["sarama"], commontesting.OldSaramaConfig)

	// Change the config map and verify the handler is called
	testConfigMap.Data["sarama"] = commontesting.NewSaramaConfig

	// The configWatcherHandler should change this back to a valid ConfigMap
	setWatchedMap(nil)

	testConfigMap, err = fakeK8sClient.CoreV1().ConfigMaps(system.Namespace()).Update(ctx, testConfigMap, metav1.UpdateOptions{})
	assert.Nil(t, err)
	assert.Equal(t, testConfigMap.Data["sarama"], commontesting.NewSaramaConfig)

	// Wait for the configWatcherHandler to be called (happens pretty quickly; loop usually only runs once)
	for try := 0; getWatchedMap() == nil && try < 100; try++ {
		time.Sleep(5 * time.Millisecond)
	}
	assert.NotNil(t, getWatchedMap())
	assert.Equal(t, getWatchedMap().Data["sarama"], commontesting.NewSaramaConfig)
}

func getWatchedMap() *corev1.ConfigMap {
	configMapMutex.Lock()
	defer configMapMutex.Unlock()
	return watchedConfigMap
}

func setWatchedMap(configMap *corev1.ConfigMap) {
	configMapMutex.Lock()
	watchedConfigMap = configMap
	defer configMapMutex.Unlock()
}

// Handler function for the ConfigMap watcher
func configWatcherHandler(configMap *corev1.ConfigMap) {
	// Set the package variable to indicate that the test watcher was called
	setWatchedMap(configMap)
}
