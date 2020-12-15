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

package sarama

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	commonconfig "knative.dev/eventing-kafka/pkg/channel/distributed/common/config"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	"knative.dev/eventing-kafka/pkg/common/client"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
)

// Utility Function For Enabling Sarama Logging (Debugging)
func EnableSaramaLogging(enable bool) {
	if enable {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	} else {
		sarama.Logger = log.New(ioutil.Discard, "[Sarama] ", log.LstdFlags)
	}
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
	saramaSettingsYamlString, kafkaVersion, err := client.ExtractKafkaVersion(saramaSettingsYamlString)
	if err != nil {
		return nil, fmt.Errorf("failed to extract KafkaVersion from Sarama Config YAML: err=%s : config=%+v", err, saramaSettingsYamlString)
	}

	// Extract (Remove) Any TLS.Config RootCAs & Set In Sarama.Config
	saramaSettingsYamlString, certPool, err := client.ExtractRootCerts(saramaSettingsYamlString)
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

	eventingKafkaConfig, err := LoadEventingKafkaSettings(configMap)
	if err != nil {
		return nil, nil, err
	}

	// Merge The Sarama Settings In The ConfigMap Into A New Base Sarama Config
	saramaConfig, err := MergeSaramaSettings(nil, configMap)

	return saramaConfig, eventingKafkaConfig, err
}

func LoadEventingKafkaSettings(configMap *corev1.ConfigMap) (*commonconfig.EventingKafkaConfig, error) {
	// Validate The ConfigMap Data
	if configMap == nil || configMap.Data == nil {
		return nil, fmt.Errorf("attempted to load configuration from empty configmap")
	}

	// Unmarshal The Eventing-Kafka ConfigMap YAML Into A EventingKafkaSettings Struct
	eventingKafkaConfig := &commonconfig.EventingKafkaConfig{}
	err := yaml.Unmarshal([]byte(configMap.Data[commonconfig.EventingKafkaSettingsConfigKey]), &eventingKafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("ConfigMap's eventing-kafka value could not be converted to an EventingKafkaConfig struct: %s : %v", err, configMap.Data[commonconfig.EventingKafkaSettingsConfigKey])
	}

	return eventingKafkaConfig, nil
}
