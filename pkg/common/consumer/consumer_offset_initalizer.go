package consumer

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"
	"knative.dev/eventing-kafka/pkg/common/kafka/offset"
)

// wrapper functions for the Sarama functions, to facilitate unit testing
var newSaramaClient = sarama.NewClient
var newSaramaClusterAdmin = sarama.NewClusterAdmin

type ConsumerOffsetInitializer interface {
	checkOffsetsInitialized(ctx context.Context, groupID string, topics []string, logger *zap.SugaredLogger, addrs []string, config *sarama.Config) error
}

type NoopConsumerOffsetInitializer struct {
}

func (coi *NoopConsumerOffsetInitializer) checkOffsetsInitialized(ctx context.Context, groupID string, topics []string, logger *zap.SugaredLogger, addrs []string, config *sarama.Config) error {
	return nil
}

type KafkaConsumerOffsetInitializer struct {
}

func (kcoi *KafkaConsumerOffsetInitializer) checkOffsetsInitialized(ctx context.Context, groupID string, topics []string, logger *zap.SugaredLogger, addrs []string, config *sarama.Config) error {
	logger.Infow("Checking if all offsets are initialized", zap.Any("topics", topics), zap.Any("groupID", groupID))

	client, err := newSaramaClient(addrs, config)
	if err != nil {
		logger.Errorw("Unable to create Kafka client", zap.Error(err))
		return err
	}
	defer client.Close()

	clusterAdmin, err := newSaramaClusterAdmin(addrs, config)
	if err != nil {
		logger.Errorw("Unable to create Kafka cluster admin client", zap.Error(err))
		return err
	}
	defer clusterAdmin.Close()

	check := func() (bool, error) {
		if initialized, err := offset.CheckIfAllOffsetsInitialized(client, clusterAdmin, topics, groupID); err == nil {
			if initialized {
				return true, nil
			} else {
				logger.Infow("Offsets not yet initialized, going to try again")
				return false, nil
			}
		} else {
			return true, fmt.Errorf("error checking if offsets are initialized. stopping trying. %w", err)
		}
	}
	pollCtx, pollCtxCancel := context.WithTimeout(ctx, OffsetInitRetryTimeout)
	err = wait.PollUntil(OffsetInitRetryInterval, check, pollCtx.Done())
	defer pollCtxCancel()

	if err != nil {
		return fmt.Errorf("failed to check if offsets are initialized %w", err)
	}
	return nil
}
