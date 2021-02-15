package client

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
)

func MakeAdminClient(ctx context.Context, clientID string, kafkaAuthCfg *KafkaAuthConfig, saramaSettingsYamlString string, brokers []string) (sarama.ClusterAdmin, error) {
	saramaConf, err := NewConfigBuilder().
		WithDefaults().
		WithAuth(kafkaAuthCfg).
		WithClientId(clientID).
		FromYaml(saramaSettingsYamlString).
		Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating admin client Sarama config: %w", err)
	}

	return sarama.NewClusterAdmin(brokers, saramaConf)
}
