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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing-kafka/pkg/common/constants"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"knative.dev/pkg/configmap"
)

const (
	BrokerConfigMapKey           = "bootstrapServers"
	AuthSecretName               = "authSecretName"
	AuthSecretNamespace          = "authSecretNamespace"
	MaxIdleConnectionsKey        = "maxIdleConns"
	MaxIdleConnectionsPerHostKey = "maxIdleConnsPerHost"

	TlsEnabled  = "tls.enabled"
	TlsCacert    = "ca.crt"
	TlsUsercert  = "user.crt"
	TlsUserkey   = "user.key"
	SaslUser     = "user"
	SaslPassword = "password"
	SaslType     = "saslType"

	KafkaChannelSeparator = "."

	knativeKafkaTopicPrefix = "knative-messaging-kafka"

	DefaultMaxIdleConns        = 1000
	DefaultMaxIdleConnsPerHost = 100
)

type KafkaConfig struct {
	Brokers                  []string
	MaxIdleConns             int32
	MaxIdleConnsPerHost      int32
	AuthSecretName           string
	AuthSecretNamespace      string
	SaramaSettingsYamlString string
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
		tlsEnabled, err :=  strconv.ParseBool(string(secret.Data[TlsEnabled]))
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
func GetKafkaConfig(configMap map[string]string) (*KafkaConfig, error) {
	if len(configMap) == 0 {
		return nil, fmt.Errorf("missing configuration")
	}

	config := &KafkaConfig{
		MaxIdleConns:        DefaultMaxIdleConns,
		MaxIdleConnsPerHost: DefaultMaxIdleConnsPerHost,
	}

	var bootstrapServers string
	var authSecretNamespace string
	var authSecretName string

	err := configmap.Parse(configMap,
		configmap.AsString(BrokerConfigMapKey, &bootstrapServers),
		configmap.AsString(AuthSecretName, &authSecretName),
		configmap.AsString(AuthSecretNamespace, &authSecretNamespace),
		configmap.AsInt32(MaxIdleConnectionsKey, &config.MaxIdleConns),
		configmap.AsInt32(MaxIdleConnectionsPerHostKey, &config.MaxIdleConnsPerHost),
		configmap.AsString(constants.SaramaSettingsConfigKey, &config.SaramaSettingsYamlString),
	)
	if err != nil {
		return nil, err
	}

	if bootstrapServers == "" {
		return nil, errors.New("missing or empty key bootstrapServers in configuration")
	}
	bootstrapServersSplitted := strings.Split(bootstrapServers, ",")
	for _, s := range bootstrapServersSplitted {
		if len(s) == 0 {
			return nil, fmt.Errorf("empty %s value in configuration", BrokerConfigMapKey)
		}
	}
	config.Brokers = bootstrapServersSplitted
	config.AuthSecretName = authSecretName
	config.AuthSecretNamespace = authSecretNamespace

	return config, nil
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
