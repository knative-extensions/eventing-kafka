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

package channel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	eventingChannel "knative.dev/eventing/pkg/channel"
	"knative.dev/pkg/logging"

	messaging "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/health"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	kafkainformers "knative.dev/eventing-kafka/pkg/client/informers/externalversions"
	kafkalisters "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
)

// Package Variables
var (
	logger             *zap.Logger
	kafkaChannelLister kafkalisters.KafkaChannelLister
	stopChan           chan struct{}
)

// InitializeKafkaChannelLister initializes the KafkaChannel Lister singleton.
func InitializeKafkaChannelLister(ctx context.Context, client kafkaclientset.Interface, healthServer *health.Server, resyncDuration time.Duration) error {
	// Get The Logger From The Provided Context
	logger = logging.FromContext(ctx).Desugar()

	// Create A New KafkaChannel SharedInformerFactory For ALL Namespaces (Default Resync Is 10 Hrs)
	sharedInformerFactory := kafkainformers.NewSharedInformerFactory(client, resyncDuration)

	// Initialize The Stop Channel (Close If Previously Created)
	Close()
	stopChan = make(chan struct{})

	// Get A KafkaChannel Informer From The SharedInformerFactory - Start The Informer & Wait For It
	kafkaChannelInformer := sharedInformerFactory.Messaging().V1beta1().KafkaChannels()
	go kafkaChannelInformer.Informer().Run(stopChan)
	sharedInformerFactory.WaitForCacheSync(stopChan)

	// Get A KafkaChannel Lister From The Informer
	kafkaChannelLister = kafkaChannelInformer.Lister()

	// Return Success
	logger.Info("Successfully Initialized KafkaChannel Lister")
	healthServer.SetChannelReady(true)
	return nil
}

// ValidateKafkaChannel verifies the specified ChannelReference refers to a KafkaChannel capable of receiving events.
// This ensures the KafkaChannel exists and has Status indicating the Kafka Topic has been created and the shared
// Receiver Service and Deployment are READY.  The KafkaChannel's overall Status might not be READY depending on
// Dispatcher state, but the ingress pathway is viable.
func ValidateKafkaChannel(channelReference eventingChannel.ChannelReference) error {

	// Enhance Logger With ChannelReference
	logger := logger.With(zap.String("ChannelReference", channelReference.String()))

	// Validate The Specified Channel Reference
	if channelReference.Name == "" || channelReference.Namespace == "" {
		logger.Warn("Invalid KafkaChannel - Invalid ChannelReference")
		return errors.New("invalid ChannelReference specified")
	}

	// Attempt To Get The KafkaChannel From The KafkaChannel Lister
	kafkaChannel, err := kafkaChannelLister.KafkaChannels(channelReference.Namespace).Get(channelReference.Name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Warn("Invalid KafkaChannel - Not Found")
			return &eventingChannel.UnknownChannelError{Channel: channelReference}
		} else {
			logger.Error("Invalid KafkaChannel - Failed To Find", zap.Error(err))
			return err
		}
	}

	// Verify The Topic And Receiver Status Indicates Ingress Is READY
	if !kafkaChannel.Status.GetCondition(messaging.KafkaChannelConditionTopicReady).IsTrue() ||
		!kafkaChannel.Status.GetCondition(messaging.KafkaChannelConditionReceiverServiceReady).IsTrue() ||
		!kafkaChannel.Status.GetCondition(messaging.KafkaChannelConditionReceiverDeploymentReady).IsTrue() {
		logger.Info("Invalid KafkaChannel - Ingress (Topic / Receiver) Not READY")
		return fmt.Errorf("ingress not READY: Kafka Topic=%t, Receiver Service=%t, Receiver Deployment=%t",
			kafkaChannel.Status.GetCondition(messaging.KafkaChannelConditionTopicReady).IsTrue(),
			kafkaChannel.Status.GetCondition(messaging.KafkaChannelConditionReceiverServiceReady).IsTrue(),
			kafkaChannel.Status.GetCondition(messaging.KafkaChannelConditionReceiverServiceReady).IsTrue())
	}

	// Return Valid KafkaChannel
	logger.Debug("Valid KafkaChannel - Found & READY")
	return nil
}

// Close The Channel Lister (Stop Processing)
func Close() {
	if stopChan != nil {
		logger.Info("Closing Informer's Stop Channel")
		close(stopChan)
	}
}
