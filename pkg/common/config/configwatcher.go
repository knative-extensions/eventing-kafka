package config

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
	"golang.org/x/net/proxy"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	commonutil "knative.dev/eventing-kafka/pkg/common/kafka/util"
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
	Image         string            `json:"image,omitempty"`
}

type EKChannelConfig struct {
	EKKubernetesConfig
}

type EKDispatcherConfig struct {
	EKKubernetesConfig
	RetryInitialIntervalMillis int64 `json:"retryInitialIntervalMillis,omitempty"`
	RetryTimeMillis            int64 `json:"retryTimeMillis,omitempty"`
	RetryExponentialBackoff    *bool `json:"retryExponentialBackoff,omitempty"`
}

type EKKafkaTopicConfig struct {
	Name                     string `json:"name,omitempty"`
	DefaultNumPartitions     int32  `json:"defaultNumPartitions,omitempty"`
	DefaultReplicationFactor int16  `json:"defaultReplicationFactor,omitempty"`
	DefaultRetentionMillis   int64  `json:"defaultRetentionMillis,omitempty"`
}

type EKKafkaConfig struct {
	Topic       EKKafkaTopicConfig `json:"topic,omitempty"`
	Provider    string             `json:"provider,omitempty"`
	Password    string             `json:"password,omitempty"`
	Brokers     string             `json:"brokers,omitempty"`
	Secret      string             `json:"secret,omitempty"`
	Username    string             `json:"username,omitempty"`
	ChannelKey  string             `json:"channelKey,omitempty"`
	ServiceName string             `json:"serviceName,omitempty"`
}

type EKMetricsConfig struct {
	Domain string `json:"domain,omitempty"`
	Port   int    `json:"port,omitempty"`
}

type EKHealthConfig struct {
	Port int `json:"port,omitempty"`
}

type EventingKafkaConfig struct {
	Channel        EKChannelConfig    `json:"channel,omitempty"`
	Dispatcher     EKDispatcherConfig `json:"dispatcher,omitempty"`
	Kafka          EKKafkaConfig      `json:"kafka,omitempty"`
	Metrics        EKMetricsConfig    `json:"metrics,omitempty"`
	Health         EKHealthConfig     `json:"health,omitempty"`
	ServiceAccount string             `json:"serviceAccount,omitempty"`
}

// This is a copy of the sarama.Config struct with tags that remove function pointers and any
// other problematic fields, in order to make comparisons to new settings easier.  The downside
// to this mechanism is that this does require that this structure be updated whenever we update to
// a new sarama.Config object, or the custom MarshalJSON() call will fail.
// On the brighter side, a change to the sarama.Config struct will cause this to fail at compile-time
// with "cannot convert *config (type sarama.Config) to type CompareSaramaConfig" so the chances of
// causing unintended runtime issues are very low.
type CompareSaramaConfig struct {
	Admin struct {
		Retry struct {
			Max     int
			Backoff time.Duration
		}
		Timeout time.Duration
	}
	Net struct {
		MaxOpenRequests int
		DialTimeout     time.Duration
		ReadTimeout     time.Duration
		WriteTimeout    time.Duration
		TLS             struct {
			Enable bool
			Config *tls.Config `json:"-"`
		}
		SASL struct {
			Enable                   bool
			Mechanism                sarama.SASLMechanism
			Version                  int16
			Handshake                bool
			AuthIdentity             string
			User                     string
			Password                 string
			SCRAMAuthzID             string
			SCRAMClientGeneratorFunc func() sarama.SCRAMClient `json:"-"`
			TokenProvider            sarama.AccessTokenProvider
			GSSAPI                   sarama.GSSAPIConfig
		}
		KeepAlive time.Duration
		LocalAddr net.Addr
		Proxy     struct {
			Enable bool
			Dialer proxy.Dialer
		}
	}
	Metadata struct {
		Retry struct {
			Max         int
			Backoff     time.Duration
			BackoffFunc func(retries, maxRetries int) time.Duration `json:"-"`
		}
		RefreshFrequency time.Duration
		Full             bool
		Timeout          time.Duration
	}
	Producer struct {
		MaxMessageBytes  int
		RequiredAcks     sarama.RequiredAcks
		Timeout          time.Duration
		Compression      sarama.CompressionCodec
		CompressionLevel int
		Partitioner      sarama.PartitionerConstructor `json:"-"`
		Idempotent       bool
		Return           struct {
			Successes bool
			Errors    bool
		}
		Flush struct {
			Bytes       int
			Messages    int
			Frequency   time.Duration
			MaxMessages int
		}
		Retry struct {
			Max         int
			Backoff     time.Duration
			BackoffFunc func(retries, maxRetries int) time.Duration `json:"-"`
		}
	}
	Consumer struct {
		Group struct {
			Session struct {
				Timeout time.Duration
			}
			Heartbeat struct {
				Interval time.Duration
			}
			Rebalance struct {
				Strategy sarama.BalanceStrategy `json:"-"`
				Timeout  time.Duration
				Retry    struct {
					Max     int
					Backoff time.Duration
				}
			}
			Member struct {
				UserData []byte
			}
		}
		Retry struct {
			Backoff     time.Duration
			BackoffFunc func(retries int) time.Duration `json:"-"`
		}
		Fetch struct {
			Min     int32
			Default int32
			Max     int32
		}
		MaxWaitTime       time.Duration
		MaxProcessingTime time.Duration
		Return            struct {
			Errors bool
		}
		Offsets struct {
			CommitInterval time.Duration
			AutoCommit     struct {
				Enable   bool
				Interval time.Duration
			}
			Initial   int64
			Retention time.Duration
			Retry     struct {
				Max int
			}
		}
		IsolationLevel sarama.IsolationLevel
	}
	ClientID          string
	RackID            string
	ChannelBufferSize int
	Version           sarama.KafkaVersion
	MetricRegistry    metrics.Registry `json:"-"`
}

// This custom marshaller converts a sarama.Config struct to our CompareSaramaConfig using the ability of go
// to initialize one struct with another identical one (aside from field tags).  It then uses the CompareSaramaConfig
// field tags to skip the fields that cause problematic JSON output (such as function pointers).
// An error of "cannot convert *config (type sarama.Config) to type CompareSaramaConfig" indicates that the
// CompareSaramaConfig structure above needs to be brought in sync with the current config.Sarama struct.
func MarshalSaramaJSON(config *sarama.Config) (string, error) {
	bytes, err := json.Marshal(CompareSaramaConfig(*config))
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// SaramaConfigEqual is a convenience function to determine if two given sarama.Config structs are identical aside
// from unserializable fields (e.g. function pointers).  This function treats any marshalling errors as
// "the structs are not equal"
func SaramaConfigEqual(config1, config2 *sarama.Config) bool {
	configString1, err := MarshalSaramaJSON(config1)
	if err != nil {
		return false
	}
	configString2, err := MarshalSaramaJSON(config2)
	if err != nil {
		return false
	}
	return configString1 == configString2
}

//
// Initialize The Specified Context With A ConfigMap Watcher
// Much Of This Function Is Taken From The knative.dev sharedmain Package
//
func InitializeConfigWatcher(logger *zap.SugaredLogger, ctx context.Context, handler configmap.Observer) error {

	// Create A Watcher On The Configuration Settings ConfigMap & Dynamically Update Configuration
	// Since this is designed to be called by the main() function, the default KNative package behavior here
	// is a fatal exit if the watch cannot be set up.
	watcher := sharedmain.SetupConfigMapWatchOrDie(ctx, logger)

	// Start The ConfigMap Watcher
	// Taken from knative.dev/pkg/injection/sharedmain/main.go::WatchObservabilityConfigOrDie
	if _, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get(SettingsConfigMapName,
		metav1.GetOptions{}); err == nil {
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

func LoadEventingKafkaSettings(ctx context.Context) (*sarama.Config, *EventingKafkaConfig, error) {
	configMap, err := LoadSettingsConfigMap(kubeclient.Get(ctx))
	if err != nil {
		return nil, nil, err
	}
	config := commonutil.NewSaramaConfig()
	err = MergeSaramaSettings(config, configMap)
	if err != nil {
		return nil, nil, err
	}

	if configMap.Data == nil {
		return nil, nil, errors.New("attempted to load eventing-kafka settings with empty configmap")
	}
	var eventingKafkaConfig EventingKafkaConfig
	eventingKafkaSettings := configMap.Data[EventingKafkaSettingsConfigKey]
	jsonSettings, err := yaml.YAMLToJSON([]byte(eventingKafkaSettings))
	if err != nil {
		return nil, nil, fmt.Errorf("ConfigMap's value could not be converted to JSON: %s : %v", err, eventingKafkaSettings)
	}
	err = json.Unmarshal(jsonSettings, &eventingKafkaConfig)
	if err != nil {
		return nil, nil, err
	}

	return config, &eventingKafkaConfig, nil
}

// Convenience function to load our configmap
func LoadSettingsConfigMap(k8sClient kubernetes.Interface) (*corev1.ConfigMap, error) {
	return k8sClient.CoreV1().ConfigMaps(system.Namespace()).Get(SettingsConfigMapName, metav1.GetOptions{})
}

// Extract The Sarama-Specific Settings From A ConfigMap And Merge Them With Existing Settings
func MergeSaramaSettings(config *sarama.Config, configMap *corev1.ConfigMap) error {
	if configMap.Data == nil {
		return errors.New("attempted to merge sarama settings with empty configmap")
	}
	// Merge The ConfigMap Settings Into The Provided Config
	saramaSettings := configMap.Data[SaramaSettingsConfigKey]
	jsonSettings, err := yaml.YAMLToJSON([]byte(saramaSettings))
	if err != nil {
		return fmt.Errorf("ConfigMap's value could not be converted to JSON: %s : %v", err, saramaSettings)
	}
	err = json.Unmarshal(jsonSettings, &config)
	if err != nil {
		return err
	}

	return nil
}
