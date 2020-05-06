package channel

import (
	"context"
	"errors"
	"go.uber.org/zap"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	k8sclientcmd "k8s.io/client-go/tools/clientcmd"
	kafkaclientset "knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned"
	kafkainformers "knative.dev/eventing-contrib/kafka/channel/pkg/client/informers/externalversions"
	kafkalisters "knative.dev/eventing-contrib/kafka/channel/pkg/client/listers/messaging/v1alpha1"
	"knative.dev/eventing-kafka/pkg/channel/health"
	eventingChannel "knative.dev/eventing/pkg/channel"
	knativecontroller "knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
)

// Package Variables
var (
	logger             *zap.Logger
	kafkaChannelLister kafkalisters.KafkaChannelLister
	stopChan           chan struct{}
)

// Wrapper Around Kafka Client Creation To Facilitate Unit Testing
var getKafkaClient = func(ctx context.Context, masterUrl string, kubeconfigPath string) (kafkaclientset.Interface, error) {

	// Create The K8S Configuration (In-Cluster With Cmd Line Flags For Out-Of-Cluster Usage)
	k8sConfig, err := k8sclientcmd.BuildConfigFromFlags(masterUrl, kubeconfigPath)
	if err != nil {
		logging.FromContext(ctx).Error("Failed To Build Kubernetes Config", zap.Error(err))
		return nil, err
	}

	// Create A New Kafka Client From The K8S Config & Return The Result
	return kafkaclientset.NewForConfigOrDie(k8sConfig), nil
}

// Initialize The KafkaChannel Lister Singleton
func InitializeKafkaChannelLister(ctx context.Context, masterUrl string, kubeconfigPath string, healthServer *health.Server) error {

	// Get The Logger From The Provided Context
	logger = logging.FromContext(ctx).Desugar()

	// Get The K8S Kafka Client For KafkaChannels
	client, err := getKafkaClient(ctx, masterUrl, kubeconfigPath)
	if err != nil {
		logger.Error("Failed To Create Kafka Client", zap.Error(err))
		return err
	}

	// Create A New KafkaChannel SharedInformerFactory For ALL Namespaces (Default Resync Is 10 Hrs)
	sharedInformerFactory := kafkainformers.NewSharedInformerFactory(client, knativecontroller.DefaultResyncPeriod)

	// Initialize The Stop Channel (Close If Previously Created)
	Close()
	stopChan = make(chan struct{})

	// Get A KafkaChannel Informer From The SharedInformerFactory - Start The Informer & Wait For It
	kafkaChannelInformer := sharedInformerFactory.Messaging().V1alpha1().KafkaChannels()
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
