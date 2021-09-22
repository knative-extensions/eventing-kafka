/*
Copyright 2021 The Knative Authors

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

package common

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"

	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"

	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/pkg/source/client"
)

func FinalizeKind(ctx context.Context, kubeClient kubernetes.Interface, src *v1beta1.KafkaSource) reconciler.Event {
	bs, config, err := client.NewConfigFromSpec(ctx, kubeClient, src)
	if err != nil {
		return err
	}

	// Version must be at least 1.1
	config.Version = sarama.V1_1_0_0

	c, err := sarama.NewClusterAdmin(bs, config)
	if err != nil {
		logging.FromContext(ctx).Errorw("unable to create a kafka client", zap.Error(err))
		return err
	}
	defer c.Close()

	if err := c.DeleteConsumerGroup(src.Spec.ConsumerGroup); err != nil && !errors.Is(sarama.ErrGroupIDNotFound, err) {
		logging.FromContext(ctx).Errorw("unable to delete the consumer group", zap.Error(err))
		return err
	}

	logging.FromContext(ctx).Infow("consumer group deleted", zap.String("id", src.Spec.ConsumerGroup))
	return nil
}
