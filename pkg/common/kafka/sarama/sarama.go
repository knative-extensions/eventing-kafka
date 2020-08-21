package sarama

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/rcrowley/go-metrics"
	"golang.org/x/net/proxy"
	corev1 "k8s.io/api/core/v1"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
)

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

// Utility Function For Enabling Sarama Logging (Debugging)
func EnableSaramaLogging() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
}

// Creates A sarama.Config With Some Default Settings
func NewSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	enforceSaramaConfig(config)
	return config
}

// Forces Some Sarama Settings To Have Mandatory Values
func enforceSaramaConfig(config *sarama.Config) {

	// Latest version, seems to work with EventHubs as well.
	config.Version = constants.ConfigKafkaVersion

	// We Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// We Want "Message Produced" Success Messages For Use With SyncProducer
	config.Producer.Return.Successes = true
}

// Utility Function For Configuring Common Settings For Admin/Producer/Consumer
func UpdateSaramaConfig(config *sarama.Config, clientId string, username string, password string) {

	// Set The ClientID For Logging
	config.ClientID = clientId

	// Update Config With With Additional SASL Auth If Specified
	if len(username) > 0 && len(password) > 0 {
		config.Net.SASL.Version = constants.ConfigNetSaslVersion
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			ClientAuth: tls.NoClientCert,
		}
	}

	// Do not permit changing of some particular settings
	enforceSaramaConfig(config)
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

// ConfigEqual is a convenience function to determine if two given sarama.Config structs are identical aside
// from unserializable fields (e.g. function pointers).  This function treats any marshalling errors as
// "the structs are not equal"
func ConfigEqual(config1, config2 *sarama.Config) bool {
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

// Extract The Sarama-Specific Settings From A ConfigMap And Merge Them With Existing Settings
func MergeSaramaSettings(config *sarama.Config, configMap *corev1.ConfigMap) error {
	if configMap.Data == nil {
		return errors.New("attempted to merge sarama settings with empty configmap")
	}
	// Merge The ConfigMap Settings Into The Provided Config
	saramaSettings := configMap.Data[commontesting.SaramaSettingsConfigKey]
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

func LoadConfigFromMap(configMap *corev1.ConfigMap) (*sarama.Config, *commonconfig.EventingKafkaConfig, error) {
	config := NewSaramaConfig()
	err := MergeSaramaSettings(config, configMap)
	if err != nil {
		return nil, nil, err
	}

	if configMap.Data == nil {
		return nil, nil, errors.New("attempted to load eventing-kafka settings with empty configmap")
	}
	var eventingKafkaConfig commonconfig.EventingKafkaConfig
	eventingKafkaSettings := configMap.Data[commonconfig.EventingKafkaSettingsConfigKey]
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

func LoadSettings(ctx context.Context) (*sarama.Config, *commonconfig.EventingKafkaConfig, error) {
	configMap, err := commonconfig.LoadSettingsConfigMap(kubeclient.Get(ctx))
	if err != nil {
		return nil, nil, err
	}
	return LoadConfigFromMap(configMap)
}
