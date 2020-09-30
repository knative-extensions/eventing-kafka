package sarama

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
)

// Regular Expression To Find All Certificates In Net.TLS.Config.RootPEMs Field
var regexRootPEMs = regexp.MustCompile(`(?s)\s*RootPEMs:.*-----END CERTIFICATE-----`)

// Utility Function For Enabling Sarama Logging (Debugging)
func EnableSaramaLogging() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
}

// Utility Function For Configuring Common Settings For Admin/Producer/Consumer
func UpdateSaramaConfig(config *sarama.Config, clientId string, username string, password string) {

	// Set The ClientID For Logging
	config.ClientID = clientId

	// Set The SASL Username / Password
	config.Net.SASL.User = username
	config.Net.SASL.Password = password

	// We Always Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// We Always Want Success Messages From Producer
	config.Producer.Return.Successes = true
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
	updatedSaramaConfigYamlBytes := regexRootPEMs.ReplaceAll([]byte(saramaConfigYamlString), []byte{})
	return string(updatedSaramaConfigYamlBytes), certPool, nil
}

// ConfigEqual is a convenience function to determine if two given sarama.Config structs are identical aside
// from unserializable fields (e.g. function pointers).  To ignore parts of the sarama.Config struct, pass
// them in as the "ignore" parameter.
func ConfigEqual(config1, config2 *sarama.Config, ignore ...interface{}) bool {
	// If some of the types in the sarama.Config struct are not ignored, these kinds of errors will appear:
	// panic: cannot handle unexported field at {*sarama.Config}.Consumer.Group.Rebalance.Strategy.(*sarama.balanceStrategy).name

	// Note that using the types directly from config1 is convenient (it allows us to call IgnoreTypes instead of the
	// more complicated IgnoreInterfaces), but it will fail if, for example, config1.Consumer.Group.Rebalance is nil

	// However, the sarama.NewConfig() function sets all of these values to a non-nil default, so the risk
	// is minimal and should be caught by one of the several unit tests for this function if the sarama vendor
	// code is updated and these defaults become something invalid at that time)

	ignoreTypeList := append([]interface{}{
		config1.Consumer.Group.Rebalance.Strategy,
		config1.MetricRegistry,
		config1.Producer.Partitioner},
		ignore...)
	ignoredTypes := cmpopts.IgnoreTypes(ignoreTypeList...)

	// If some interfaces are not included in the "IgnoreUnexported" list, these kinds of errors will appear:
	// panic: cannot handle unexported field at {*sarama.Config}.Net.TLS.Config.mutex: "crypto/tls".Config

	// Note that x509.CertPool and tls.Config are created here explicitly because config1/config2 may not
	// have those fields, and results in a nil pointer panic if used in the IgnoreUnexported list indirectly
	// like config1.Version is (Version is required to be present in a sarama.Config struct).

	ignoredUnexported := cmpopts.IgnoreUnexported(config1.Version, x509.CertPool{}, tls.Config{})

	// Compare the two sarama config structs, ignoring types and unexported fields as specified
	return cmp.Equal(config1, config2, ignoredTypes, ignoredUnexported)
}

// Extract The Sarama-Specific Settings From A ConfigMap And Merge Them With Existing Settings
// If config Is nil, A New sarama.Config Struct Will Be Created With Default Values
func MergeSaramaSettings(config *sarama.Config, configMap *corev1.ConfigMap) (*sarama.Config, error) {

	// Validate The ConfigMap Data
	if configMap.Data == nil {
		return nil, fmt.Errorf("attempted to merge sarama settings with empty configmap")
	}

	// Merging To A Nil Config Requires Creating An Default One First
	if config == nil {
		// Start With Base Sarama Defaults
		config = sarama.NewConfig()

		// Use Our Default Minimum Version
		config.Version = constants.ConfigKafkaVersionDefault

		// Add Any Required Settings
		UpdateSaramaConfig(config, config.ClientID, "", "")
	}

	// Merge The ConfigMap Settings Into The Provided Config
	saramaSettingsYamlString := configMap.Data[testing.SaramaSettingsConfigKey]

	// Extract (Remove) The KafkaVersion From The Sarama Config YAML
	saramaSettingsYamlString, kafkaVersion, err := extractKafkaVersion(saramaSettingsYamlString)
	if err != nil {
		return nil, fmt.Errorf("failed to extract KafkaVersion from Sarama Config YAML: err=%s : config=%+v", err, saramaSettingsYamlString)
	}

	// Extract (Remove) Any TLS.Config RootCAs & Set In Sarama.Config
	saramaSettingsYamlString, certPool, err := extractRootCerts(saramaSettingsYamlString)
	if err != nil {
		return nil, fmt.Errorf("failed to extract RootPEMs from Sarama Config YAML: err=%s : config=%+v", err, saramaSettingsYamlString)
	}

	// Unmarshall The Sarama Config Yaml Into The Provided Sarama.Config Object
	err = yaml.Unmarshal([]byte(saramaSettingsYamlString), &config)
	if err != nil {
		return nil, fmt.Errorf("ConfigMap's sarama value could not be converted to a Sarama.Config struct: %s : %v", err, saramaSettingsYamlString)
	}

	// Override The Custom Parsed KafkaVersion
	config.Version = kafkaVersion

	// Override Any Custom Parsed TLS.Config.RootCAs
	if certPool != nil && len(certPool.Subjects()) > 0 {
		config.Net.TLS.Config = &tls.Config{RootCAs: certPool}
	}

	// Return Success
	return config, nil
}

// Load The Sarama & EventingKafka Configuration From The ConfigMap
// The Provided Context Must Have A Kubernetes Client Associated With It
func LoadSettings(ctx context.Context) (*sarama.Config, *commonconfig.EventingKafkaConfig, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("attempted to load settings from a nil context")
	}

	configMap, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get(ctx, commonconfig.SettingsConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}

	// Validate The ConfigMap Data
	if configMap.Data == nil {
		return nil, nil, fmt.Errorf("attempted to load configuration from empty configmap")
	}

	// Unmarshal The Eventing-Kafka ConfigMap YAML Into A EventingKafkaSettings Struct
	eventingKafkaConfigString := configMap.Data[commonconfig.EventingKafkaSettingsConfigKey]
	eventingKafkaConfig := &commonconfig.EventingKafkaConfig{}
	err = yaml.Unmarshal([]byte(eventingKafkaConfigString), &eventingKafkaConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("ConfigMap's eventing-kafka value could not be converted to an EventingKafkaConfig struct: %s : %v", err, eventingKafkaConfigString)
	}

	// Merge The Sarama Settings In The ConfigMap Into A New Base Sarama Config
	saramaConfig, err := MergeSaramaSettings(nil, configMap)

	return saramaConfig, eventingKafkaConfig, err
}
