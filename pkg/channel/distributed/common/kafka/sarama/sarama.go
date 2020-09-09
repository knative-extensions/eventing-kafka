package sarama

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/rcrowley/go-metrics"
	"golang.org/x/net/proxy"
	corev1 "k8s.io/api/core/v1"
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
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
		Interceptors []sarama.ProducerInterceptor
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
		Interceptors   []sarama.ConsumerInterceptor
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

	// Start With Base Sarama Defaults
	config := sarama.NewConfig()

	// Use Our Default Minimum Version
	config.Version = constants.ConfigKafkaVersionDefault

	// Enforce eventing-kafka Required Config
	enforceSaramaConfig(config)

	// Return Base Sarama Config
	return config
}

//
// Extract (Parse & Remove) Top Level Kafka Version From Specified Sarama Confirm YAML String
//
// The Sarama.Config struct contains a top-level 'Version' field of type sarama.KafkaVersion.
// This type contains a package scoped [4]uint field called 'version' which we cannot unmarshal
// the yaml into.  Therefore, we instead support the user providing a 'Version' string of the
// format "major.minor.patch" (e.g. "2.3.0") which we will "extract" out of the YAML string
// and return as an actual sarama.KafkaVersion.  In the case where the user has NOT specified
// a Version string we will return a default version.
//
func extractKafkaVersion(saramaConfigYamlString string) (string, sarama.KafkaVersion, error) {

	// Define Inline Struct To Marshall The Top Level Sarama Config "Kafka" Version Into
	type saramaConfigShell struct {
		Version string
	}

	// Unmarshal The Sarama Config Into The Shell
	shell := &saramaConfigShell{}
	err := yaml.Unmarshal([]byte(saramaConfigYamlString), shell)
	if err != nil {
		return saramaConfigYamlString, constants.ConfigKafkaVersionDefault, err
	}

	// Return Default Kafka Version If Not Specified
	if len(shell.Version) <= 0 {
		return saramaConfigYamlString, constants.ConfigKafkaVersionDefault, nil
	}

	// Attempt To Parse The Version String Into A Sarama.KafkaVersion
	kafkaVersion, err := sarama.ParseKafkaVersion(shell.Version)
	if err != nil {
		return saramaConfigYamlString, constants.ConfigKafkaVersionDefault, err
	}

	// Remove The Version From The Sarama Config YAML String
	regex, err := regexp.Compile("\\s*Version:\\s*" + shell.Version + "\\s*")
	if err != nil {
		return saramaConfigYamlString, constants.ConfigKafkaVersionDefault, err
	} else {
		updatedSaramaConfigYamlBytes := regex.ReplaceAll([]byte(saramaConfigYamlString), []byte{})
		return string(updatedSaramaConfigYamlBytes), kafkaVersion, nil
	}
}

/* Extract (Parse & Remove) TLS.Config Level RootPEMs From Specified Sarama Confirm YAML String

The Sarama.Config struct contains Net.TLS.Config which is a *tls.Config which cannot be parsed.
due to it being from another package and containing lots of func()s.  We do need the ability
however to provide Root Certificates in order to avoid having to disable verification via the
InsecureSkipVerify field.  Therefore, we support a custom 'RootPEMs' field which is an array
of strings containing the PEM file content.  This function will "extract" that content out
of the YAML string and return as a populated *x509.CertPool which can be assigned to the
Sarama.Config.Net.TLS.Config.RootCAs field.  In the case where the user has NOT specified
any PEM files we will return nil.

In order for the PEM file content to pass cleanly from the YAML string, which is itself
already a YAML string inside the K8S ConfigMap, it must be formatted and spaced correctly.
The following shows an example of the expected usage...

apiVersion: v1
kind: ConfigMap
metadata:
  name: config-eventing-kafka
  namespace: knative-eventing
data:
  sarama: |
    Admin:
      Timeout: 10000000000
    Net:
      KeepAlive: 30000000000
      TLS:
        Enable: true
        Config:
          RootCaPems:
          - |-
            -----BEGIN CERTIFICATE-----
            MIIGBDCCA+ygAwIBAgIJAKi1aEV58cQ1MA0GCSqGSIb3DQEBCwUAMIGOMQswCQYD
            ...
            2wk9rLRZaQnhspt6MhlmU0qkaEZpYND3emR2XZ07m51jXqDUgTjXYCSggImUsARs
            NAehp9bMeco=
            -----END CERTIFICATE-----
      SASL:
        Enable: true

...where you should make sure to use the YAML string syntax of "|-" in order to
prevent trailing linefeed. The indentation of the PEM content is also important
and must be aligned as shown.
*/
func extractRootCerts(saramaConfigYamlString string) (string, *x509.CertPool, error) {

	// Define Inline Struct To Marshall The TLS Config 'RootPEMs' Into
	type tlsConfigShell struct {
		Net struct {
			TLS struct {
				Config struct {
					RootPEMs []string
				}
			}
		}
	}

	// Unmarshal The TLS Config Into The Shell
	shell := &tlsConfigShell{}
	err := yaml.Unmarshal([]byte(saramaConfigYamlString), shell)
	if err != nil {
		return saramaConfigYamlString, nil, err
	}

	// Convenience Variable For The RootPEMs
	rootPEMs := shell.Net.TLS.Config.RootPEMs

	// Exit Early If No RootCert PEMs
	if rootPEMs == nil || len(rootPEMs) < 1 {
		return saramaConfigYamlString, nil, nil
	}

	// Create A New CertPool To Contain The Root PEMs
	certPool := x509.NewCertPool()

	// Populate The CertPool With The PEMs
	for _, rootPEM := range rootPEMs {
		ok := certPool.AppendCertsFromPEM([]byte(rootPEM))
		if !ok {
			return saramaConfigYamlString, nil, fmt.Errorf("failed to parse root certificate PEM: %s", rootPEM)
		}
	}

	// Remove The RootPEMs From The Sarama YAML String (Multi-Line / Greedy To Collect All PEMs)
	regex, err := regexp.Compile("(?s)\\s*RootPEMs:.*-----END CERTIFICATE-----")
	if err != nil {
		return saramaConfigYamlString, nil, err
	} else {
		updatedSaramaConfigYamlBytes := regex.ReplaceAll([]byte(saramaConfigYamlString), []byte{})
		return string(updatedSaramaConfigYamlBytes), certPool, nil
	}
}

// Forces Some Sarama Settings To Have Mandatory Values
func enforceSaramaConfig(config *sarama.Config) {

	// We Always Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// We Always Want Success Messages From Producer
	config.Producer.Return.Successes = true
}

// Utility Function For Configuring Common Settings For Admin/Producer/Consumer
func UpdateSaramaConfig(config *sarama.Config, clientId string, username string, password string) {

	// Set The ClientID For Logging
	config.ClientID = clientId

	// Set The SASL Username / Password
	config.Net.SASL.User = username
	config.Net.SASL.Password = password

	// Force Required Configuration
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

	// Validate The ConfigMap Data
	if configMap.Data == nil {
		return fmt.Errorf("attempted to merge sarama settings with empty configmap")
	}

	// Merge The ConfigMap Settings Into The Provided Config
	saramaSettingsYamlString := configMap.Data[testing.SaramaSettingsConfigKey]

	// Extract (Remove) The KafkaVersion From The Sarama Config YAML
	saramaSettingsYamlString, kafkaVersion, err := extractKafkaVersion(saramaSettingsYamlString)
	if err != nil {
		return fmt.Errorf("failed to extract KafkaVersion from Sarama Config YAML: err=%s : config=%+v", err, saramaSettingsYamlString)
	}

	// Extract (Remove) Any TLS.Config RootCAs & Set In Sarama.Config
	saramaSettingsYamlString, certPool, err := extractRootCerts(saramaSettingsYamlString)
	if err != nil {
		return fmt.Errorf("failed to extract RootPEMs from Sarama Config YAML: err=%s : config=%+v", err, saramaSettingsYamlString)
	}

	// Unmarshall The Sarama Config Yaml Into The Provided Sarama.Config Object
	err = yaml.Unmarshal([]byte(saramaSettingsYamlString), &config)
	if err != nil {
		return fmt.Errorf("ConfigMap's sarama value could not be converted to a Sarama.Config struct: %s : %v", err, saramaSettingsYamlString)
	}

	// Override The Custom Parsed KafkaVersion
	config.Version = kafkaVersion

	// Override Any Custom Parsed TLS.Config.RootCAs
	if certPool != nil && len(certPool.Subjects()) > 0 {
		config.Net.TLS.Config = &tls.Config{RootCAs: certPool}
	}

	// Return Success
	return nil
}

// Load The Sarama & EventingKafka Configuration From The ConfigMap
func LoadConfigFromMap(configMap *corev1.ConfigMap) (*sarama.Config, *commonconfig.EventingKafkaConfig, error) {

	// Validate The ConfigMap Data
	if configMap.Data == nil {
		return nil, nil, fmt.Errorf("attempted to load configuration from empty configmap")
	}

	// Create A New Base Sarama Config To Start From
	saramaConfig := NewSaramaConfig()

	// Merge The Sarama Settings In The ConfigMap Into the New/Base Sarama Config
	err := MergeSaramaSettings(saramaConfig, configMap)
	if err != nil {
		return nil, nil, err
	}

	// Unmarshal The Eventing-Kafka ConfigMap YAML Into A EventingKafkaSettings Struct
	eventingKafkaSettings := configMap.Data[commonconfig.EventingKafkaSettingsConfigKey]
	eventingKafkaConfig := &commonconfig.EventingKafkaConfig{}
	err = yaml.Unmarshal([]byte(eventingKafkaSettings), &eventingKafkaConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("ConfigMap's eventing-kafka value could not be converted to an EventingKafkaConfig struct: %s : %v", err, eventingKafkaSettings)
	}

	return saramaConfig, eventingKafkaConfig, nil
}

// Load The Sarama & EventingKafka Configuration From The ConfigMap
func LoadSettings(ctx context.Context) (*sarama.Config, *commonconfig.EventingKafkaConfig, error) {
	configMap, err := commonconfig.LoadSettingsConfigMap(kubeclient.Get(ctx))
	if err != nil {
		return nil, nil, err
	}
	return LoadConfigFromMap(configMap)
}
