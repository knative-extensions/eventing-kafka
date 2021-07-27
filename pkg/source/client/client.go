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

package client

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetes "k8s.io/client-go/kubernetes"

	"github.com/Shopify/sarama"
	"github.com/kelseyhightower/envconfig"

	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/client"
)

type AdapterSASL struct {
	Enable   bool   `envconfig:"KAFKA_NET_SASL_ENABLE" required:"false"`
	User     string `envconfig:"KAFKA_NET_SASL_USER" required:"false"`
	Password string `envconfig:"KAFKA_NET_SASL_PASSWORD" required:"false"`
	Type     string `envconfig:"KAFKA_NET_SASL_TYPE" required:"false"`
}

type AdapterTLS struct {
	Enable bool   `envconfig:"KAFKA_NET_TLS_ENABLE" required:"false"`
	Cert   string `envconfig:"KAFKA_NET_TLS_CERT" required:"false"`
	Key    string `envconfig:"KAFKA_NET_TLS_KEY" required:"false"`
	CACert string `envconfig:"KAFKA_NET_TLS_CA_CERT" required:"false"`
}

type AdapterNet struct {
	SASL AdapterSASL
	TLS  AdapterTLS
}

type KafkaConfig struct {
	SaramaYamlString string
}

type KafkaEnvConfig struct {
	// KafkaConfigJson is the environment variable that's passed to adapter by the controller.
	// It contains configuration from the Kafka configmap.
	KafkaConfigJson  string                `envconfig:"K_KAFKA_CONFIG"`
	BootstrapServers []string              `envconfig:"KAFKA_BOOTSTRAP_SERVERS" required:"true"`
	InitialOffset    sourcesv1beta1.Offset `envconfig:"INITIAL_OFFSET" `
	Net              AdapterNet
}

// NewConfig extracts the Kafka configuration from the environment.
func NewConfigFromEnv(ctx context.Context) ([]string, *sarama.Config, error) {
	var env KafkaEnvConfig
	if err := envconfig.Process("", &env); err != nil {
		return nil, nil, err
	}

	return NewConfigWithEnv(ctx, &env)
}

// NewConfig extracts the Kafka configuration from the environment.
func NewConfigWithEnv(ctx context.Context, env *KafkaEnvConfig) ([]string, *sarama.Config, error) {
	kafkaAuthConfig := &client.KafkaAuthConfig{}

	if env.Net.TLS.Enable {
		kafkaAuthConfig.TLS = &client.KafkaTlsConfig{
			Cacert:   env.Net.TLS.CACert,
			Usercert: env.Net.TLS.Cert,
			Userkey:  env.Net.TLS.Key,
		}
	}

	if env.Net.SASL.Enable {
		kafkaAuthConfig.SASL = &client.KafkaSaslConfig{
			User:     env.Net.SASL.User,
			Password: env.Net.SASL.Password,
			SaslType: env.Net.SASL.Type,
		}
	}

	configBuilder := client.NewConfigBuilder().
		WithDefaults().
		WithAuth(kafkaAuthConfig).
		WithInitialOffset(env.InitialOffset)

	if env.KafkaConfigJson != "" {
		kafkaCfg := &KafkaConfig{}
		err := json.Unmarshal([]byte(env.KafkaConfigJson), kafkaCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing Kafka config from environment: %w", err)
		}

		configBuilder = configBuilder.FromYaml(kafkaCfg.SaramaYamlString)
	}

	cfg, err := configBuilder.Build(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating Sarama config: %w", err)
	}

	return env.BootstrapServers, cfg, nil
}

// NewConfig extracts the Kafka configuration from a KafkaSource spec.
func NewConfigFromSpec(ctx context.Context, kc kubernetes.Interface, obj *sourcesv1beta1.KafkaSource) ([]string, *sarama.Config, error) {
	envConfig, err := NewEnvConfigFromSpec(ctx, kc, obj)
	if err != nil {
		return nil, nil, err
	}
	return NewConfigWithEnv(ctx, &envConfig)
}

// NewEnvConfigFromSpec validates and creates a KafkaEnvConfig from a KafkaSource
func NewEnvConfigFromSpec(ctx context.Context, kc kubernetes.Interface, obj *sourcesv1beta1.KafkaSource) (KafkaEnvConfig, error) {
	saslUser, err := resolveSecret(ctx, kc, obj.Namespace, obj.Spec.Net.SASL.User.SecretKeyRef)
	if err != nil {
		return KafkaEnvConfig{}, err
	}

	saslPassword, err := resolveSecret(ctx, kc, obj.Namespace, obj.Spec.Net.SASL.Password.SecretKeyRef)
	if err != nil {
		return KafkaEnvConfig{}, err
	}

	saslType, err := resolveSecret(ctx, kc, obj.Namespace, obj.Spec.Net.SASL.Type.SecretKeyRef)
	if err != nil {
		return KafkaEnvConfig{}, err
	}

	tlsCert, err := resolveSecret(ctx, kc, obj.Namespace, obj.Spec.Net.TLS.Cert.SecretKeyRef)
	if err != nil {
		return KafkaEnvConfig{}, err
	}

	tlsKey, err := resolveSecret(ctx, kc, obj.Namespace, obj.Spec.Net.TLS.Key.SecretKeyRef)
	if err != nil {
		return KafkaEnvConfig{}, err
	}

	tlsCACert, err := resolveSecret(ctx, kc, obj.Namespace, obj.Spec.Net.TLS.CACert.SecretKeyRef)
	if err != nil {
		return KafkaEnvConfig{}, err
	}

	config := KafkaEnvConfig{
		BootstrapServers: obj.Spec.BootstrapServers,
		InitialOffset:    obj.Spec.InitialOffset,
		Net: AdapterNet{
			SASL: AdapterSASL{
				Enable:   obj.Spec.Net.SASL.Enable,
				User:     saslUser,
				Password: saslPassword,
				Type:     saslType,
			},
			TLS: AdapterTLS{
				Enable: obj.Spec.Net.TLS.Enable,
				Cert:   tlsCert,
				Key:    tlsKey,
				CACert: tlsCACert,
			},
		},
	}

	return config, nil
}

// NewProducer is a helper method for constructing a client for producing kafka methods.
func NewProducer(ctx context.Context) (sarama.Client, error) {
	bs, cfg, err := NewConfigFromEnv(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: specific here. can we do this for all?
	cfg.Producer.Return.Errors = true

	return sarama.NewClient(bs, cfg)
}

// NewProducer is a helper method for constructing an admin client
func MakeAdminClient(ctx context.Context, env *KafkaEnvConfig) (sarama.ClusterAdmin, error) {
	bs, cfg, err := NewConfigWithEnv(ctx, env)
	if err != nil {
		return nil, err
	}
	return sarama.NewClusterAdmin(bs, cfg)
}

// resolveSecret resolves the secret reference
func resolveSecret(ctx context.Context, kc kubernetes.Interface, ns string, ref *corev1.SecretKeySelector) (string, error) {
	if ref == nil {
		return "", nil
	}
	secret, err := kc.CoreV1().Secrets(ns).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to read secret (%v)", err)
	}

	if value, ok := secret.Data[ref.Key]; ok && len(value) > 0 {
		return string(value), nil
	}

	return "", fmt.Errorf("missing secret key or empty secret value (%s/%s)", ref.Name, ref.Key)
}
