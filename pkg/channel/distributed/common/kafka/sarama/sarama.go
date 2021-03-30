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
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/common/client"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
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

// Load The Sarama & EventingKafka Configuration From The ConfigMap
// The Provided Context Must Have A Kubernetes Client Associated With It
func LoadSettings(ctx context.Context, clientId string, kafkaAuthConfig *client.KafkaAuthConfig) (*sarama.Config, *commonconfig.EventingKafkaConfig, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("attempted to load settings from a nil context")
	}

	configMap, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get(ctx, constants.SettingsConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}

	eventingKafkaConfig, err := LoadEventingKafkaSettings(configMap)
	if err != nil {
		return nil, nil, err
	}

	// Validate The ConfigMap Data
	if configMap.Data == nil {
		return nil, nil, fmt.Errorf("attempted to merge sarama settings with empty configmap")
	}

	// Merge The ConfigMap Settings Into The Provided Config
	saramaSettingsYamlString := configMap.Data[constants.SaramaSettingsConfigKey]

	if kafkaAuthConfig != nil && kafkaAuthConfig.SASL.User == "" {
		// The config builder expects the entire config object to be nil if not using auth
		// (Otherwise it will end up with "PLAIN" SASL by default and fail due to having no user/password)
		kafkaAuthConfig = nil
	}

	// Merge The Sarama Settings In The ConfigMap Into A New Base Sarama Config
	saramaConfig, err := client.NewConfigBuilder().
		WithDefaults().
		FromYaml(saramaSettingsYamlString).
		WithAuth(kafkaAuthConfig).
		WithClientId(clientId).
		Build(ctx)

	return saramaConfig, eventingKafkaConfig, err
}

func LoadEventingKafkaSettings(configMap *corev1.ConfigMap) (*commonconfig.EventingKafkaConfig, error) {
	// Validate The ConfigMap Data
	if configMap == nil || configMap.Data == nil {
		return nil, fmt.Errorf("attempted to load configuration from empty configmap")
	}

	// Unmarshal The Eventing-Kafka ConfigMap YAML Into A EventingKafkaSettings Struct
	eventingKafkaConfig := &commonconfig.EventingKafkaConfig{}
	err := yaml.Unmarshal([]byte(configMap.Data[constants.EventingKafkaSettingsConfigKey]), &eventingKafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("ConfigMap's eventing-kafka value could not be converted to an EventingKafkaConfig struct: %s : %v", err, configMap.Data[constants.EventingKafkaSettingsConfigKey])
	}

	if eventingKafkaConfig != nil {
		// If Any Config Was Provided, Set Some Default Values If Missing

		// Increase The Idle Connection Limits From Transport Defaults If Not Provided (see net/http/DefaultTransport)
		if eventingKafkaConfig.CloudEvents.MaxIdleConns == 0 {
			eventingKafkaConfig.CloudEvents.MaxIdleConns = constants.DefaultMaxIdleConns
		}
		if eventingKafkaConfig.CloudEvents.MaxIdleConnsPerHost == 0 {
			eventingKafkaConfig.CloudEvents.MaxIdleConnsPerHost = constants.DefaultMaxIdleConnsPerHost
		}
	}

	return eventingKafkaConfig, nil
}

// AuthFromSarama creates a KafkaAuthConfig using the SASL settings from
// a given Sarama config, or nil if there is no SASL user in that config
func AuthFromSarama(config *sarama.Config) *client.KafkaAuthConfig {
	// Use the SASL settings from the provided Sarama config only if the user is non-empty
	if config.Net.SASL.User != "" {
		return &client.KafkaAuthConfig{
			SASL: &client.KafkaSaslConfig{
				User:     config.Net.SASL.User,
				Password: config.Net.SASL.Password,
				SaslType: string(config.Net.SASL.Mechanism),
			},
		}
	} else {
		// If the user is empty, return explicitly nil authentication
		return nil
	}
}

// Utility function to convert []byte headers to string ones for logging purposes
func StringifyHeaders(headers []sarama.RecordHeader) map[string][]string {
	stringHeaders := make(map[string][]string)
	for _, header := range headers {
		key := string(header.Key)
		stringHeaders[key] = append(stringHeaders[key], string(header.Value))
	}
	return stringHeaders
}

// Pointer-version of the StringifyHeaders function
func StringifyHeaderPtrs(headers []*sarama.RecordHeader) map[string][]string {
	stringHeaders := make(map[string][]string)
	for _, header := range headers {
		key := string(header.Key)
		stringHeaders[key] = append(stringHeaders[key], string(header.Value))
	}
	return stringHeaders
}
