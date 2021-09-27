package consumer

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"
	"knative.dev/eventing-kafka/pkg/common/kafka/offset"
)

type consumerOffsetInitializer interface {
	checkOffsetsInitialized(ctx context.Context, groupID string, topics []string, logger *zap.SugaredLogger, client sarama.Client, clusterAdmin sarama.ClusterAdmin) error
}

type kafkaConsumerOffsetInitializer struct {
}

func (kcoi *kafkaConsumerOffsetInitializer) checkOffsetsInitialized(ctx context.Context, groupID string, topics []string, logger *zap.SugaredLogger, client sarama.Client, clusterAdmin sarama.ClusterAdmin) error {
	logger.Infow("Checking if all offsets are initialized", zap.Any("topics", topics), zap.Any("groupID", groupID))

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
	err := wait.PollUntil(OffsetInitRetryInterval, check, pollCtx.Done())
	defer pollCtxCancel()

	if err != nil {
		return fmt.Errorf("failed to check if offsets are initialized %w", err)
	}
	return nil
}
