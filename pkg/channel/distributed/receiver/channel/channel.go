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
	"time"

	"go.uber.org/zap"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	k8sclientcmd "k8s.io/client-go/tools/clientcmd"
	"knative.dev/eventing-kafka/pkg/channel/distributed/receiver/health"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	kafkainformers "knative.dev/eventing-kafka/pkg/client/informers/externalversions"
	kafkalisters "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	eventingChannel "knative.dev/eventing/pkg/channel"
	"knative.dev/pkg/logging"
)

// Package Variables
var (
	logger             *zap.Logger
	kafkaChannelLister kafkalisters.KafkaChannelLister
	stopChan           chan struct{}
)

// Wrapper Around Kafka Client Creation To Facilitate Unit Testing
var getKafkaClient = func(ctx context.Context, serverUrl string, kubeconfigPath string) (kafkaclientset.Interface, error) {

	// Create The K8S Configuration (In-Cluster With Cmd Line Flags For Out-Of-Cluster Usage)
	k8sConfig, err := k8sclientcmd.BuildConfigFromFlags(serverUrl, kubeconfigPath)
	if err != nil {
		logging.FromContext(ctx).Error("Failed To Build Kubernetes Config", zap.Error(err))
		return nil, err
	}

	// Create A New Kafka Client From The K8S Config & Return The Result
	return kafkaclientset.NewForConfigOrDie(k8sConfig), nil
}

// Initialize The KafkaChannel Lister Singleton
func InitializeKafkaChannelLister(ctx context.Context, serverUrl string, kubeconfigPath string, healthServer *health.Server, resyncDuration time.Duration) error {

	// Get The Logger From The Provided Context
	logger = logging.FromContext(ctx).Desugar()

	// Get The K8S Kafka Client For KafkaChannels
	client, err := getKafkaClient(ctx, serverUrl, kubeconfigPath)
	if err != nil {
		logger.Error("Failed To Create Kafka Client", zap.Error(err))
		return err
	}

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

// Validate The Specified ChannelReference Is For A Valid (Existing / READY) KafkaChannel
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
			// Note - Returning Knative UnknownChannelError Type For EventReceiver.StartHTTP()
			//        Ideally we'd be able to populate the UnknownChannelError's ChannelReference
			//        but once again Knative has made this private.
			logger.Warn("Invalid KafkaChannel - Not Found")
			return &eventingChannel.UnknownChannelError{}
		} else {
			logger.Error("Invalid KafkaChannel - Failed To Find", zap.Error(err))
			return err
		}
	}

	// Check KafkaChannel READY Status
	if !kafkaChannel.Status.IsReady() {
		logger.Info("Invalid KafkaChannel - Not READY")
		return errors.New("channel status not READY")
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
