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

package source

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kelseyhightower/envconfig"

	"knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
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

type KafkaEnvConfig struct {
	BootstrapServers []string `envconfig:"KAFKA_BOOTSTRAP_SERVERS" required:"true"`
	Net              AdapterNet
}

// NewConfig extracts the Kafka configuration from the environment.
func NewConfig(ctx context.Context) ([]string, *sarama.Config, error) {
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

	cfg, err := client.NewConfigBuilder().
		WithDefaults().
		WithAuth(kafkaAuthConfig).
		WithVersion(&sarama.V2_0_0_0).
		Build(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating Sarama config: %w", err)
	}

	return env.BootstrapServers, cfg, nil
}

// NewProducer is a helper method for constructing a client for producing kafka methods.
func NewProducer(ctx context.Context) (sarama.Client, error) {
	bs, cfg, err := NewConfig(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: specific here. can we do this for all?
	cfg.Producer.Return.Errors = true

	return sarama.NewClient(bs, cfg)
}

func MakeAdminClient(ctx context.Context, clientID string, kafkaAuthCfg *client.KafkaAuthConfig, kafkaConfig *utils.KafkaConfig) (sarama.ClusterAdmin, error) {
	saramaConf, err := client.NewConfigBuilder().
		WithDefaults().
		WithAuth(kafkaAuthCfg).
		WithClientId(clientID).
		Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating admin client Sarama config: %w", err)
	}

	return sarama.NewClusterAdmin(kafkaConfig.Brokers, saramaConf)
}
