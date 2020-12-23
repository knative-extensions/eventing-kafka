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

package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
)

// BuildSaramaConfig builds the Sarama config using 3 things: an existing Sarama config,
// a Sarama config YAML and a KafkaAuthConfig.
// Listed parameters above have precedence in the reverse order. So, the function
// first gets the existing config, then applies the config in the YAML and finally it applies the
// config in the KafkaAuthConfig.
//
// If existing config is nil, it creates a new sarama.Config object, initializes it with
// our defaults and builds it.
// If YAML string or kafkaAuthConfig are null, they're simply ignored.
func BuildSaramaConfig(config *sarama.Config, saramaSettingsYamlString string, kafkaAuthCfg *KafkaAuthConfig) (*sarama.Config, error) {
	// Merging To A Nil Config Requires Creating An Default One First
	if config == nil {
		// Start With Base Sarama Defaults
		config = sarama.NewConfig()

		// Use Our Default Minimum Version
		config.Version = constants.ConfigKafkaVersionDefault

		// Add Any Required Settings
		UpdateConfigWithDefaults(config)
	}

	if saramaSettingsYamlString != "" {
		// Extract (Remove) The KafkaVersion From The Sarama Config YAML as we can't marshal it regularly
		saramaSettingsYamlString, kafkaVersionInYaml, err := extractKafkaVersion(saramaSettingsYamlString)
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

		// Override The Custom Parsed KafkaVersion, if it is specified in the YAML
		if kafkaVersionInYaml != nil {
			config.Version = *kafkaVersionInYaml
		}

		// Override Any Custom Parsed TLS.Config.RootCAs
		if certPool != nil && len(certPool.Subjects()) > 0 {
			config.Net.TLS.Config = &tls.Config{RootCAs: certPool}
		}
	}

	err := UpdateSaramaConfigWithKafkaAuthConfig(config, kafkaAuthCfg)
	if err != nil {
		return nil, err
	}

	// Return Success
	return config, nil
}

// UpdateConfigWithDefaults updates the given Sarama config with
// some defaults that we always use in eventing-kafka.
func UpdateConfigWithDefaults(config *sarama.Config) {
	// We Always Want To Know About Consumer Errors
	config.Consumer.Return.Errors = true

	// We Always Want Success Messages From Producer
	config.Producer.Return.Successes = true
}

// UpdateSaramaConfigWithKafkaAuthConfig updates the TLS and SASL of Sarama config
// based on the values in the given KafkaAuthConfig.
func UpdateSaramaConfigWithKafkaAuthConfig(saramaConf *sarama.Config, kafkaAuthCfg *KafkaAuthConfig) error {
	if kafkaAuthCfg != nil {
		// tls
		if kafkaAuthCfg.TLS != nil {
			saramaConf.Net.TLS.Enable = true
			tlsConfig, err := newTLSConfig(kafkaAuthCfg.TLS.Usercert, kafkaAuthCfg.TLS.Userkey, kafkaAuthCfg.TLS.Cacert)
			if err != nil {
				return fmt.Errorf("Error creating TLS config: %w", err)
			}
			saramaConf.Net.TLS.Config = tlsConfig
		}
		// SASL
		if kafkaAuthCfg.SASL != nil {
			saramaConf.Net.SASL.Enable = true
			saramaConf.Net.SASL.Handshake = true

			// if SaslType is not provided we are defaulting to PLAIN
			saramaConf.Net.SASL.Mechanism = sarama.SASLTypePlaintext

			if kafkaAuthCfg.SASL.SaslType == sarama.SASLTypeSCRAMSHA256 {
				saramaConf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
				saramaConf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			}

			if kafkaAuthCfg.SASL.SaslType == sarama.SASLTypeSCRAMSHA512 {
				saramaConf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
				saramaConf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			}
			saramaConf.Net.SASL.User = kafkaAuthCfg.SASL.User
			saramaConf.Net.SASL.Password = kafkaAuthCfg.SASL.Password
		}
	}
	return nil
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

type KafkaAuthConfig struct {
	TLS  *KafkaTlsConfig
	SASL *KafkaSaslConfig
}

type KafkaTlsConfig struct {
	Cacert   string
	Usercert string
	Userkey  string
}

type KafkaSaslConfig struct {
	User     string
	Password string
	SaslType string
}

// verifyCertSkipHostname verifies certificates in the same way that the
// default TLS handshake does, except it skips hostname verification. It must
// be used with InsecureSkipVerify.
func verifyCertSkipHostname(roots *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(certs [][]byte, _ [][]*x509.Certificate) error {
		opts := x509.VerifyOptions{
			Roots:         roots,
			CurrentTime:   time.Now(),
			Intermediates: x509.NewCertPool(),
		}

		leaf, err := x509.ParseCertificate(certs[0])
		if err != nil {
			return err
		}

		for _, asn1Data := range certs[1:] {
			cert, err := x509.ParseCertificate(asn1Data)
			if err != nil {
				return err
			}

			opts.Intermediates.AddCert(cert)
		}

		_, err = leaf.Verify(opts)
		return err
	}
}

// NewTLSConfig returns a *tls.Config using the given ceClient cert, ceClient key,
// and CA certificate. If none are appropriate, a nil *tls.Config is returned.
func newTLSConfig(clientCert, clientKey, caCert string) (*tls.Config, error) {
	valid := false

	config := &tls.Config{}

	if clientCert != "" && clientKey != "" {
		cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{cert}
		valid = true
	}

	if caCert != "" {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(caCert))
		config.RootCAs = caCertPool
		// The CN of Heroku Kafka certs do not match the hostname of the
		// broker, but Go's default TLS behavior requires that they do.
		config.VerifyPeerCertificate = verifyCertSkipHostname(caCertPool)
		config.InsecureSkipVerify = true
		valid = true
	}

	if !valid {
		config = nil
	}

	return config, nil
}
