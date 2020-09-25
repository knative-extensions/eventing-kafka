package config

import (
	"context"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/system"
)

// The EventingKafkaConfig and these EK sub-structs contain our custom configuration settings,
// stored in the config-eventing-kafka configmap.  The sub-structs are explicitly declared so that they
// can have their own JSON tags in the overall EventingKafkaConfig
type EKKubernetesConfig struct {
	CpuLimit      resource.Quantity `json:"cpuLimit,omitempty"`
	CpuRequest    resource.Quantity `json:"cpuRequest,omitempty"`
	MemoryLimit   resource.Quantity `json:"memoryLimit,omitempty"`
	MemoryRequest resource.Quantity `json:"memoryRequest,omitempty"`
	Replicas      int               `json:"replicas,omitempty"`
}

// The Channel config has nothing in it except the base Kubernetes fields (Cpu, Memory, Replicas)
type EKChannelConfig struct {
	EKKubernetesConfig
}

// The Dispatcher config has the base Kubernetes fields and some retry settings
type EKDispatcherConfig struct {
	EKKubernetesConfig
	RetryInitialIntervalMillis int64 `json:"retryInitialIntervalMillis,omitempty"`
	RetryTimeMillis            int64 `json:"retryTimeMillis,omitempty"`
	RetryExponentialBackoff    *bool `json:"retryExponentialBackoff,omitempty"`
}

// EKKafkaTopicConfig contains some defaults that are only used if not provided by the channel spec
type EKKafkaTopicConfig struct {
	DefaultNumPartitions     int32 `json:"defaultNumPartitions,omitempty"`
	DefaultReplicationFactor int16 `json:"defaultReplicationFactor,omitempty"`
	DefaultRetentionMillis   int64 `json:"defaultRetentionMillis,omitempty"`
}

// EKKafkaConfig contains items relevant to Kafka specifically
type EKKafkaConfig struct {
	Topic     EKKafkaTopicConfig `json:"topic,omitempty"`
	AdminType string             `json:"adminType,omitempty"`
}

// EventingKafkaConfig is the main struct that holds the Channel, Dispatcher, and Kafka sub-items
type EventingKafkaConfig struct {
	Channel    EKChannelConfig    `json:"channel,omitempty"`
	Dispatcher EKDispatcherConfig `json:"dispatcher,omitempty"`
	Kafka      EKKafkaConfig      `json:"kafka,omitempty"`
}

//
// Initialize The Specified Context With A ConfigMap Watcher
// Much Of This Function Is Taken From The knative.dev sharedmain Package
//
func InitializeConfigWatcher(ctx context.Context, logger *zap.SugaredLogger, handler configmap.Observer) error {

	// Create A Watcher On The Configuration Settings ConfigMap & Dynamically Update Configuration
	// Since this is designed to be called by the main() function, the default KNative package behavior here
	// is a fatal exit if the watch cannot be set up.
	watcher := sharedmain.SetupConfigMapWatchOrDie(ctx, logger)

	// Start The ConfigMap Watcher
	// Taken from knative.dev/pkg/injection/sharedmain/main.go::WatchObservabilityConfigOrDie
	if _, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get(ctx, SettingsConfigMapName, metav1.GetOptions{}); err == nil {
		watcher.Watch(SettingsConfigMapName, handler)
	} else if !apierrors.IsNotFound(err) {
		logger.Error("Error reading ConfigMap "+SettingsConfigMapName, zap.Error(err))
		return err
	}

	if err := watcher.Start(ctx.Done()); err != nil {
		logger.Error("Failed to start configuration watcher", zap.Error(err))
		return err
	}

	return nil
}
