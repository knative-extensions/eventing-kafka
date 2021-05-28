/*
Copyright 2021 The Knative Authors

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

	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
)

// The watcher handler sets this variable to indicate that it was called
var (
	watchedSecret *corev1.Secret
	secretMutex   = sync.Mutex{} // Don't trip up the data race examiner during tests
)

// Test The InitializeSecretWatcher() Functionality
func TestInitializeSecretWatcher(t *testing.T) {

	// Add a Secret to a fake Kubernetes Client
	commontesting.SetTestEnvironment(t)
	secret := commontesting.GetTestSaramaSecret(
		commontesting.SecretName,
		commontesting.OldAuthUsername,
		commontesting.OldAuthPassword,
		commontesting.OldAuthNamespace,
		commontesting.OldAuthSaslType)
	fakeK8sClient := fake.NewSimpleClientset(secret)
	ctx := context.WithValue(context.TODO(), injectionclient.Key{}, fakeK8sClient)
	ctx, cancel := context.WithCancel(ctx)

	// The secretWatcherHandler should change the nil "watchedSecret" to a valid Secret when the watcher triggers

	testSecret, err := fakeK8sClient.CoreV1().Secrets(system.Namespace()).Get(ctx, commontesting.SecretName, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, string(testSecret.Data[commonconstants.KafkaSecretKeyUsername]), commontesting.OldAuthUsername)
	assert.Equal(t, string(testSecret.Data[commonconstants.KafkaSecretKeyPassword]), commontesting.OldAuthPassword)
	assert.Equal(t, string(testSecret.Data[commonconstants.KafkaSecretKeyNamespace]), commontesting.OldAuthNamespace)
	assert.Equal(t, string(testSecret.Data[commonconstants.KafkaSecretKeySaslType]), commontesting.OldAuthSaslType)

	// Perform The Test (Initialize The Secret Watcher)
	err = InitializeSecretWatcher(ctx, system.Namespace(), commontesting.SecretName, 10*time.Second, secretWatcherHandler)
	assert.Nil(t, err)

	// The secretWatcherHandler should change this back to a valid Secret after the watcher is triggered
	setWatchedSecret(nil)

	// Change the data in the secret
	testSecret.Data[commonconstants.KafkaSecretKeyUsername] = []byte(commontesting.NewAuthUsername)
	testSecret.Data[commonconstants.KafkaSecretKeyPassword] = []byte(commontesting.NewAuthPassword)
	testSecret.Data[commonconstants.KafkaSecretKeyNamespace] = []byte(commontesting.NewAuthNamespace)
	testSecret.Data[commonconstants.KafkaSecretKeySaslType] = []byte(commontesting.NewAuthSaslType)

	// Update the secret in the Kubernetes client
	testSecret, err = fakeK8sClient.CoreV1().Secrets(system.Namespace()).Update(ctx, testSecret, metav1.UpdateOptions{})
	assert.Nil(t, err)
	assert.Equal(t, string(testSecret.Data[commonconstants.KafkaSecretKeyPassword]), commontesting.NewAuthPassword)

	// Wait for the secretWatcherHandler to be called
	assert.Eventually(t, func() bool { return getWatchedSecret() != nil }, 500*time.Millisecond, 5*time.Millisecond)

	assert.NotNil(t, getWatchedSecret())
	assert.Equal(t, string(testSecret.Data[commonconstants.KafkaSecretKeyUsername]), commontesting.NewAuthUsername)
	assert.Equal(t, string(testSecret.Data[commonconstants.KafkaSecretKeyPassword]), commontesting.NewAuthPassword)
	assert.Equal(t, string(testSecret.Data[commonconstants.KafkaSecretKeyNamespace]), commontesting.NewAuthNamespace)
	assert.Equal(t, string(testSecret.Data[commonconstants.KafkaSecretKeySaslType]), commontesting.NewAuthSaslType)

	// End the watcher
	cancel()
}

func getWatchedSecret() *corev1.Secret {
	secretMutex.Lock()
	defer secretMutex.Unlock()
	return watchedSecret
}

func setWatchedSecret(secret *corev1.Secret) {
	secretMutex.Lock()
	watchedSecret = secret
	defer secretMutex.Unlock()
}

// Handler function for the Secret watcher
func secretWatcherHandler(_ context.Context, secret *corev1.Secret) {
	// Set the package variable to indicate that the test watcher was called
	setWatchedSecret(secret)
}
