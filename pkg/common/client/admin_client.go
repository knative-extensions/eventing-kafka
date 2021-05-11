package client

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func MakeAdminClient(saramaConf *sarama.Config, brokers []string) (sarama.ClusterAdmin, error) {
	if saramaConf == nil {
		return nil, fmt.Errorf("error creating admin client: Sarama config is nil")
	}

	return sarama.NewClusterAdmin(brokers, saramaConf)
}
