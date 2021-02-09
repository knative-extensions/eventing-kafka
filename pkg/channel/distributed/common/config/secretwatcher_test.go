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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	kafkaconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	"knative.dev/eventing-kafka/pkg/common/constants"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
)

// The watcher handler sets this variable to indicate that it was called
var (
	watchedSecret *corev1.Secret
	secretMutex   = sync.Mutex{} // Don't trip up the data race examiner during tests
)

func TestGetAuthConfigFromKubernetes_Valid(t *testing.T) {
	ctx := context.WithValue(context.TODO(), injectionclient.Key{}, clientWithTestSecret(t, commontesting.SecretName))
	kafkaAuth, err := GetAuthConfigFromKubernetes(ctx, commontesting.SecretName, commontesting.SystemNamespace)
	assert.Nil(t, err)
	assert.NotNil(t, kafkaAuth)
	assert.Equal(t, kafkaAuth.Brokers, commontesting.OldAuthBrokers)
	assert.Equal(t, kafkaAuth.SASL.User, commontesting.OldAuthUsername)
	assert.Equal(t, kafkaAuth.SASL.Password, commontesting.OldAuthPassword)
	assert.Equal(t, kafkaAuth.SASL.SaslType, commontesting.OldAuthSaslType)
}

func TestGetAuthConfigFromKubernetes_NotFound(t *testing.T) {
	ctx := context.WithValue(context.TODO(), injectionclient.Key{}, clientWithTestSecret(t, commontesting.SecretName))
	kafkaAuth, err := GetAuthConfigFromKubernetes(ctx, "invalid-secret-name", commontesting.SystemNamespace)
	assert.NotNil(t, err)
	assert.Nil(t, kafkaAuth)
}

func TestGetConfigFromSecret_Valid(t *testing.T) {
	commontesting.SetTestEnvironment(t)
	kafkaAuth := GetAuthConfigFromSecret(getSaramaTestSecret(constants.SettingsSecretName))

	assert.Equal(t, commontesting.OldAuthBrokers, kafkaAuth.Brokers)
	assert.Equal(t, commontesting.OldAuthUsername, kafkaAuth.SASL.User)
	assert.Equal(t, commontesting.OldAuthPassword, kafkaAuth.SASL.Password)
	assert.Equal(t, commontesting.OldAuthSaslType, kafkaAuth.SASL.SaslType)
}

func TestGetConfigFromSecret_Invalid(t *testing.T) {
	assert.Nil(t, GetAuthConfigFromSecret(nil))
}

// Test The InitializeSecretWatcher() Functionality
func TestInitializeSecretWatcher(t *testing.T) {
	fakeK8sClient := clientWithTestSecret(t, constants.SettingsSecretName)
	ctx := context.WithValue(context.TODO(), injectionclient.Key{}, fakeK8sClient)
	ctx, cancel := context.WithCancel(ctx)

	// The secretWatcherHandler should change the nil "watchedSecret" to a valid Secret when the watcher triggers

	testSecret, err := fakeK8sClient.CoreV1().Secrets(system.Namespace()).Get(ctx, constants.SettingsSecretName, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeyBrokers]), commontesting.OldAuthBrokers)
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeyUsername]), commontesting.OldAuthUsername)
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeyPassword]), commontesting.OldAuthPassword)
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeyNamespace]), commontesting.OldAuthNamespace)
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeySaslType]), commontesting.OldAuthSaslType)

	// Perform The Test (Initialize The Secret Watcher)
	err = InitializeSecretWatcher(ctx, system.Namespace(), constants.SettingsSecretName, secretWatcherHandler)
	assert.Nil(t, err)

	// Change the secret and verify the handler is called
	testSecret.Data[kafkaconstants.KafkaSecretKeyBrokers] = []byte(commontesting.NewAuthBrokers)
	testSecret.Data[kafkaconstants.KafkaSecretKeyUsername] = []byte(commontesting.NewAuthUsername)
	testSecret.Data[kafkaconstants.KafkaSecretKeyPassword] = []byte(commontesting.NewAuthPassword)
	testSecret.Data[kafkaconstants.KafkaSecretKeyNamespace] = []byte(commontesting.NewAuthNamespace)
	testSecret.Data[kafkaconstants.KafkaSecretKeySaslType] = []byte(commontesting.NewAuthSaslType)

	// The secretWatcherHandler should change this back to a valid Secret
	setWatchedSecret(nil)

	testSecret, err = fakeK8sClient.CoreV1().Secrets(system.Namespace()).Update(ctx, testSecret, metav1.UpdateOptions{})
	assert.Nil(t, err)
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeyPassword]), commontesting.NewAuthPassword)

	// Wait for the secretWatcherHandler to be called (happens pretty quickly; loop usually only runs once)
	for try := 0; getWatchedSecret() == nil && try < 100; try++ {
		time.Sleep(5 * time.Millisecond)
	}
	assert.NotNil(t, getWatchedSecret())
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeyBrokers]), commontesting.NewAuthBrokers)
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeyUsername]), commontesting.NewAuthUsername)
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeyPassword]), commontesting.NewAuthPassword)
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeyNamespace]), commontesting.NewAuthNamespace)
	assert.Equal(t, string(testSecret.Data[kafkaconstants.KafkaSecretKeySaslType]), commontesting.NewAuthSaslType)

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

func getSaramaTestSecret(name string) *corev1.Secret {
	return commontesting.GetTestSaramaSecret(
		name,
		commontesting.OldAuthBrokers,
		commontesting.OldAuthUsername,
		commontesting.OldAuthPassword,
		commontesting.OldAuthNamespace,
		commontesting.OldAuthSaslType,
	)
}

func clientWithTestSecret(t *testing.T, name string) *fake.Clientset {
	commontesting.SetTestEnvironment(t)
	return fake.NewSimpleClientset(getSaramaTestSecret(name))
}
