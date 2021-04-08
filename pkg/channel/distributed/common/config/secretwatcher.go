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
	"fmt"
	"time"

	"k8s.io/client-go/informers"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/client"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
)

type SecretObserver func(ctx context.Context, secret *corev1.Secret)

//
// Initialize The Specified Context With A Secret Informer
//
func InitializeSecretWatcher(ctx context.Context, namespace string, name string, resyncTime time.Duration, observer SecretObserver) error {

	logger := logging.FromContext(ctx)

	// Create A New SharedInformerFactory
	secretsInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		kubeclient.Get(ctx), resyncTime, informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(listOptions *metav1.ListOptions) {
			listOptions.FieldSelector = fmt.Sprintf("metadata.name=%s", name)
		}))

	// Create A Secrets Informer That Calls Our Observer Function
	secretsInformer := secretsInformerFactory.Core().V1().Secrets().Informer()
	secretsInformer.AddEventHandler(controller.HandleAll(func(object interface{}) {
		secret, ok := object.(*corev1.Secret)
		if ok {
			observer(ctx, secret)
		}
	}))

	// Calling Informer.Run() instead of InformerFactory.Start() allows us to more easily
	// log a message if the informer is stopped.
	go func() {
		secretsInformer.Run(ctx.Done())
		logger.Info("Stopped Secret Watcher")
	}()

	logger.Info("Started Secret Watcher")
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
