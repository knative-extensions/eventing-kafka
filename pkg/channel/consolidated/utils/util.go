/*
Copyright 2019 The Knative Authors

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

package utils

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/pkg/configmap"

	"knative.dev/eventing-kafka/pkg/common/config"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	BrokerConfigMapKey           = "bootstrapServers"
	AuthSecretName               = "authSecretName"
	AuthSecretNamespace          = "authSecretNamespace"
	MaxIdleConnectionsKey        = "maxIdleConns"
	MaxIdleConnectionsPerHostKey = "maxIdleConnsPerHost"

	TlsEnabled   = "tls.enabled"
	TlsCacert    = "ca.crt"
	TlsUsercert  = "user.crt"
	TlsUserkey   = "user.key"
	SaslUser     = "user"
	SaslPassword = "password"
	SaslType     = "saslType"

	KafkaChannelSeparator = "."

	knativeKafkaTopicPrefix = "knative-messaging-kafka"
)

type KafkaConfig struct {
	Brokers       []string
	EventingKafka *config.EventingKafkaConfig
}

func parseTls(secret *corev1.Secret, kafkaAuthConfig *client.KafkaAuthConfig) {

	// self-signed CERTs we need CA CERT, USER CERT and KEy
	if string(secret.Data[TlsCacert]) != "" {
		// We have a self-signed TLS cert
		tls := &client.KafkaTlsConfig{
			Cacert:   string(secret.Data[TlsCacert]),
			Usercert: string(secret.Data[TlsUsercert]),
			Userkey:  string(secret.Data[TlsUserkey]),
		}
		kafkaAuthConfig.TLS = tls
	} else {
		// Public CERTS from a proper CA do not need this,
		// we can just say `tls.enabled: true`
		tlsEnabled, err := strconv.ParseBool(string(secret.Data[TlsEnabled]))
		if err != nil {
			tlsEnabled = false
		}
		if tlsEnabled {
			// Looks like TLS is desired/enabled:
			kafkaAuthConfig.TLS = &client.KafkaTlsConfig{}
		}
	}
}

func parseSasl(secret *corev1.Secret, kafkaAuthConfig *client.KafkaAuthConfig) {
	if string(secret.Data[SaslUser]) != "" {
		sasl := &client.KafkaSaslConfig{
			User:     string(secret.Data[SaslUser]),
			Password: string(secret.Data[SaslPassword]),
			SaslType: string(secret.Data[SaslType]),
		}
		kafkaAuthConfig.SASL = sasl
	}
}

func GetKafkaAuthData(ctx context.Context, secretname string, secretNS string) *client.KafkaAuthConfig {

	k8sClient := kubeclient.Get(ctx)
	secret, err := k8sClient.CoreV1().Secrets(secretNS).Get(ctx, secretname, metav1.GetOptions{})

	if err != nil || secret == nil {
		logging.FromContext(ctx).Errorf("Referenced Auth Secret not found")
		return nil
	}

	kafkaAuthConfig := &client.KafkaAuthConfig{}

	// check for TLS and SASL options
	parseTls(secret, kafkaAuthConfig)
	parseSasl(secret, kafkaAuthConfig)

	return kafkaAuthConfig
}

// GetKafkaConfig returns the details of the Kafka cluster.
func GetKafkaConfig(ctx context.Context, configMap map[string]string, getAuth sarama.GetAuth) (*KafkaConfig, error) {
	if len(configMap) == 0 {
		return nil, fmt.Errorf("missing configuration")
	}

	eventingKafkaConfig := &config.EventingKafkaConfig{
		CloudEvents: config.EKCloudEventConfig{
			MaxIdleConns:        constants.DefaultMaxIdleConns,
			MaxIdleConnsPerHost: constants.DefaultMaxIdleConnsPerHost,
		},
	}
	var err error

	if configMap[constants.VersionConfigKey] != sarama.CurrentConfigVersion {
		// Backwards-compatibility: Support old configmap format
		err = configmap.Parse(configMap,
			configmap.AsString(BrokerConfigMapKey, &eventingKafkaConfig.Kafka.Brokers),
			configmap.AsString(AuthSecretName, &eventingKafkaConfig.Kafka.AuthSecretName),
			configmap.AsString(AuthSecretNamespace, &eventingKafkaConfig.Kafka.AuthSecretNamespace),
			configmap.AsInt(MaxIdleConnectionsKey, &eventingKafkaConfig.CloudEvents.MaxIdleConns),
			configmap.AsInt(MaxIdleConnectionsPerHostKey, &eventingKafkaConfig.CloudEvents.MaxIdleConnsPerHost),
		)
	} else {
		eventingKafkaConfig, err = sarama.LoadSettings(ctx, "kafka-ch-dispatcher", configMap, getAuth)
	}
	if err != nil {
		return nil, err
	}

	if eventingKafkaConfig.Kafka.Brokers == "" {
		return nil, errors.New("missing or empty brokers in configuration")
	}
	bootstrapServersSplitted := strings.Split(eventingKafkaConfig.Kafka.Brokers, ",")
	for _, s := range bootstrapServersSplitted {
		if len(s) == 0 {
			return nil, errors.New("empty brokers value in configuration")
		}
	}

	return &KafkaConfig{
		Brokers:       bootstrapServersSplitted,
		EventingKafka: eventingKafkaConfig,
	}, nil
}

func TopicName(separator, namespace, name string) string {
	topic := []string{knativeKafkaTopicPrefix, namespace, name}
	return strings.Join(topic, separator)
}

func FindContainer(d *appsv1.Deployment, containerName string) *corev1.Container {
	for i := range d.Spec.Template.Spec.Containers {
		if d.Spec.Template.Spec.Containers[i].Name == containerName {
			return &d.Spec.Template.Spec.Containers[i]
		}
	}

	return nil
}
