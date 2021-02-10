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
	"time"

	"k8s.io/apimachinery/pkg/fields"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/client"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
)

type SecretObserver func(ctx context.Context, secret *corev1.Secret)

//
// Initialize The Specified Context With A Secret Watcher
//
func InitializeSecretWatcher(ctx context.Context, namespace string, name string, observer SecretObserver) error {

	logger := logging.FromContext(ctx)
	secrets := kubeclient.Get(ctx).CoreV1().Secrets(namespace)
	watcher, err := secrets.Watch(ctx, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String()})
	if err != nil {
		logger.Error("Failed to start secret watcher", zap.Error(err))
	}

	go func() {
		defer watcher.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("Stopped Secret Watcher")
				return
			case event := <-watcher.ResultChan():
				// We only care if the secret was modified or added
				if event.Type == watch.Added || event.Type == watch.Modified {
					if secret, ok := event.Object.(*corev1.Secret); ok {
						observer(ctx, secret)
					} else {
						logger.Error("Could not convert watched object to Secret", zap.Any("event.Object", event.Object))
					}
				}
			case <-time.After(5 * time.Second):
				// Don't hang a shutdown waiting for an event
			}
		}

	}()

	return nil
}

// Look Up And Return Kafka Auth ConfigAnd Brokers From Named Secret
func GetAuthConfigFromKubernetes(ctx context.Context, secretName string, secretNamespace string) (*client.KafkaAuthConfig, string, error) {
	secrets := kubeclient.Get(ctx).CoreV1().Secrets(secretNamespace)
	secret, err := secrets.Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return nil, "", err
	}
	kafkaAuthCfg, brokers := GetAuthConfigFromSecret(secret)
	return kafkaAuthCfg, brokers, nil
}

// Look Up And Return Kafka Auth Config And Brokers From Provided Secret
func GetAuthConfigFromSecret(secret *corev1.Secret) (*client.KafkaAuthConfig, string) {
	if secret == nil || secret.Data == nil {
		return nil, ""
	}

	return &client.KafkaAuthConfig{
			SASL: &client.KafkaSaslConfig{
				User:     string(secret.Data[constants.KafkaSecretKeyUsername]),
				Password: string(secret.Data[constants.KafkaSecretKeyPassword]),
				SaslType: string(secret.Data[constants.KafkaSecretKeySaslType]),
			},
		},
		string(secret.Data[constants.KafkaSecretKeyBrokers])

}
