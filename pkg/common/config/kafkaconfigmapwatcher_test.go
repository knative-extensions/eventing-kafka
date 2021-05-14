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

package config

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/injection/sharedmain"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"
)

// The watcher handler sets this variable to indicate that it was called
var (
	watchedConfigMap *corev1.ConfigMap
	configMapMutex   = sync.Mutex{} // Don't trip up the data race examiner during tests
)

// Test The InitializeKafkaConfigMapWatcher() Functionality
func TestInitializeKafkaConfigMapWatcher(t *testing.T) {

	// Obtain a Test Logger (Required By be InitializeKafkaConfigMapWatcher function)
	logger := logtesting.TestLogger(t)

	// Setup Environment
	commontesting.SetTestEnvironment(t)

	// Create A Test Sarama ConfigMap For The InitializeKafkaConfigMapWatcher() Call To Watch
	configMap := commontesting.GetTestSaramaConfigMap(sarama.CurrentConfigVersion, commontesting.OldSaramaConfig, commontesting.TestEKConfig)

	// Create The Fake K8S Client And Add It To The ConfigMap
	fakeK8sClient := fake.NewSimpleClientset(configMap)

	// Add The Fake K8S Client To The Context (Required By InitializeKafkaConfigMapWatcher)
	ctx := context.WithValue(context.TODO(), injectionclient.Key{}, fakeK8sClient)

	// The configWatcherHandler should change the nil "watchedConfigMap" to a valid ConfigMap when the watcher triggers

	testConfigMap, err := fakeK8sClient.CoreV1().ConfigMaps(system.Namespace()).Get(ctx, constants.SettingsConfigMapName, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, testConfigMap.Data["sarama"], commontesting.OldSaramaConfig)

	cmw := sharedmain.SetupConfigMapWatchOrDie(ctx, logger)

	// Perform The Test (Initialize The Config Watcher)
	err = InitializeKafkaConfigMapWatcher(ctx, cmw, logger, configWatcherHandler, system.Namespace())
	assert.Nil(t, err)

	if err := cmw.Start(ctx.Done()); err != nil {
		logger.Fatal("Failed to start configmap watcher", zap.Error(err))
	}

	// Note that this initial change to the watched map is not part of the default Kubernetes watching logic.
	// The underlying KNative InformedWatcher Start() function that is called as part of the
	// SetupConfigMapWatchOrDie code "pretends" that all watched resources were just created.

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
func configWatcherHandler(_ context.Context, configMap *corev1.ConfigMap) {
	// Set the package variable to indicate that the test watcher was called
	setWatchedMap(configMap)
}
