package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	"knative.dev/eventing-kafka/pkg/common/kafka/offset"
)

const (
	OffsetCheckRetryTimeout  = 60 * time.Second
	OffsetCheckRetryInterval = 5 * time.Second
)

// wrapper functions for the Sarama functions, to facilitate unit testing
var newSaramaClient = sarama.NewClient
var newClusterAdminFromClient = sarama.NewClusterAdminFromClient

type ConsumerGroupOffsetsChecker interface {
	WaitForOffsetsInitialization(ctx context.Context, groupID string, topics []string, logger *zap.SugaredLogger, addrs []string, config *sarama.Config) error
}

type NoopConsumerGroupOffsetsChecker struct {
}

func (c *NoopConsumerGroupOffsetsChecker) WaitForOffsetsInitialization(ctx context.Context, groupID string, topics []string, logger *zap.SugaredLogger, addrs []string, config *sarama.Config) error {
	return nil
}

type KafkaConsumerGroupOffsetsChecker struct {
}

func (k *KafkaConsumerGroupOffsetsChecker) WaitForOffsetsInitialization(ctx context.Context, groupID string, topics []string, logger *zap.SugaredLogger, addrs []string, config *sarama.Config) error {
	logger.Debugw("checking if all offsets are initialized", zap.Any("topics", topics), zap.Any("groupID", groupID))

	client, err := newSaramaClient(addrs, config)
	if err != nil {
		logger.Errorw("unable to create Kafka client", zap.Any("topics", topics), zap.String("groupId", groupID), zap.Error(err))
		return err
	}
	defer client.Close()

	clusterAdmin, err := newClusterAdminFromClient(client)
	if err != nil {
		logger.Errorw("unable to create Kafka cluster admin client", zap.Any("topics", topics), zap.String("groupId", groupID), zap.Error(err))
		return err
	}
	defer clusterAdmin.Close()

	check := func() (bool, error) {
		if initialized, err := offset.CheckIfAllOffsetsInitialized(client, clusterAdmin, topics, groupID); err == nil {
			if initialized {
				return true, nil
			} else {
				logger.Debugw("offsets not yet initialized, going to try again")
				return false, nil
			}
		} else {
			return false, fmt.Errorf("error checking if offsets are initialized. stopping trying. %w", err)
		}
	}
	err = wait.PollImmediate(OffsetCheckRetryInterval, OffsetCheckRetryTimeout, check)
	if err != nil {
		return fmt.Errorf("failed to check if offsets are initialized %w", err)
	}
	return nil
}
