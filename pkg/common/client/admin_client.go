package client

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
)

func MakeAdminClient(ctx context.Context, clientID string, kafkaAuthCfg *KafkaAuthConfig, kafkaConfig *utils.KafkaConfig) (sarama.ClusterAdmin, error) {
	saramaConf, err := NewConfigBuilder().
		WithDefaults().
		WithAuth(kafkaAuthCfg).
		WithClientId(clientID).
		FromYaml(kafkaConfig.SaramaSettingsYamlString).
		Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating admin client Sarama config: %w", err)
	}

	return sarama.NewClusterAdmin(kafkaConfig.Brokers, saramaConf)
}
