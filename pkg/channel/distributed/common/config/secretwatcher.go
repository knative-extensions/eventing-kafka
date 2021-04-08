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

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
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
	watcherOptions := metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String()}
	secrets := kubeclient.Get(ctx).CoreV1().Secrets(namespace)
	watcher, err := secrets.Watch(ctx, watcherOptions)
	if err != nil {
		logger.Error("Failed to start secret watcher", zap.Error(err))
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("Stopped Secret Watcher")
				watcher.Stop()
				return
			case event, ok := <-watcher.ResultChan():
				if !ok {
					// Channel was closed; this typically happens because watchers have a default
					// timeout that is randomly set between 30 and 60 minutes if there is no value
					// (or zero) in the watcherOptions.TimeoutSeconds.  Regardless of the particular
					// value of the timeout, we need to restart the watcher when this happens.
					logger.Debug("Watcher channel closed; restarting Secret Watcher")
					watcher, err = secrets.Watch(ctx, watcherOptions)
					if err != nil {
						logger.Error("Failed to restart Secret Watcher", zap.Error(err))
						return
					}
				}
				// We only care if the secret was modified or added
				if event.Type == watch.Added || event.Type == watch.Modified {
					if secret, ok := event.Object.(*corev1.Secret); ok {
						observer(ctx, secret)
					} else {
						logger.Error("Could not convert watched object to Secret", zap.Any("event.Object", event.Object))
					}
				}
			}
		}
	}()

	return nil
}

// Look Up And Return Kafka Auth ConfigAnd Brokers From Named Secret
func GetAuthConfigFromKubernetes(ctx context.Context, secretName string, secretNamespace string) (*client.KafkaAuthConfig, error) {
	secrets := kubeclient.Get(ctx).CoreV1().Secrets(secretNamespace)
	secret, err := secrets.Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	kafkaAuthCfg := GetAuthConfigFromSecret(secret)
	return kafkaAuthCfg, nil
}

// Look Up And Return Kafka Auth Config And Brokers From Provided Secret
func GetAuthConfigFromSecret(secret *corev1.Secret) *client.KafkaAuthConfig {
	if secret == nil || secret.Data == nil {
		return nil
	}

	// If we don't convert the empty string to the "PLAIN" default, the client.HasSameSettings()
	// function will assume that they should be treated as differences and needlessly reconfigure
	saslType := string(secret.Data[constants.KafkaSecretKeySaslType])
	if saslType == "" {
		saslType = sarama.SASLTypePlaintext
	}

	return &client.KafkaAuthConfig{
		SASL: &client.KafkaSaslConfig{
			User:     string(secret.Data[constants.KafkaSecretKeyUsername]),
			Password: string(secret.Data[constants.KafkaSecretKeyPassword]),
			SaslType: saslType,
		},
	}
}
