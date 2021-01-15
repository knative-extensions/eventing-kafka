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

package source

import (
	"context"
	"os"
	"testing"

	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/utils"

	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	// Increasing coverage
	defaultBootstrapServer := "my-cluster-kafka-bootstrap.my-kafka-namespace:9092"
	defaultSASLUser := "secret-user"
	defaultSASLPassword := "super-seekrit-password"
	testCases := map[string]struct {
		env             map[string]string
		enabledTLS      bool
		enabledSASL     bool
		wantErr         bool
		saslMechanism   string
		bootstrapServer string
		saslUser        string
		saslPassword    string
	}{
		"Just bootstrap Server": {
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": defaultBootstrapServer,
			},
			bootstrapServer: defaultBootstrapServer,
		},
		"Incorrect bootstrap Server": {
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": defaultBootstrapServer,
			},
			bootstrapServer: "ImADoctorNotABootstrapServerJim!",
			wantErr:         true,
		},
		/*
			TODO
			"Multiple bootstrap servers": {
				env: map[string]string{

				},

			},*/
		"No Auth": {
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": defaultBootstrapServer,
			},
			enabledTLS:      false,
			enabledSASL:     false,
			bootstrapServer: defaultBootstrapServer,
		},
		"Defaulting to SASL-Plain Auth (none specified)": {
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": defaultBootstrapServer,
				"KAFKA_NET_SASL_ENABLE":   "true",
			},
			enabledSASL:     true,
			saslMechanism:   sarama.SASLTypePlaintext,
			bootstrapServer: defaultBootstrapServer,
		},
		"Only SASL-PLAIN Auth": {
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": defaultBootstrapServer,
				"KAFKA_NET_SASL_ENABLE":   "true",
				"KAFKA_NET_SASL_USER":     defaultSASLUser,
				"KAFKA_NET_SASL_PASSWORD": defaultSASLPassword,
			},
			enabledSASL:     true,
			saslUser:        defaultSASLUser,
			saslPassword:    defaultSASLPassword,
			saslMechanism:   sarama.SASLTypePlaintext,
			bootstrapServer: defaultBootstrapServer,
		},
		"SASL-PLAIN Auth, Forgot User": {
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": defaultBootstrapServer,
				"KAFKA_NET_SASL_ENABLE":   "true",
				"KAFKA_NET_SASL_PASSWORD": defaultSASLPassword,
			},
			enabledSASL:     true,
			wantErr:         true,
			saslUser:        defaultSASLUser,
			saslPassword:    defaultSASLPassword,
			saslMechanism:   sarama.SASLTypePlaintext,
			bootstrapServer: defaultBootstrapServer,
		},
		"SASL-PLAIN Auth, Forgot Password": {
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": defaultBootstrapServer,
				"KAFKA_NET_SASL_ENABLE":   "true",
				"KAFKA_NET_SASL_USER":     defaultSASLUser,
			},
			enabledSASL:     true,
			wantErr:         true,
			saslUser:        defaultSASLUser,
			saslPassword:    defaultSASLPassword,
			saslMechanism:   sarama.SASLTypePlaintext,
			bootstrapServer: defaultBootstrapServer,
		},
		"Only SASL-SCRAM-SHA-256 Auth": {
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": defaultBootstrapServer,
				"KAFKA_NET_SASL_ENABLE":   "true",
				"KAFKA_NET_SASL_USER":     defaultSASLUser,
				"KAFKA_NET_SASL_PASSWORD": defaultSASLPassword,
				"KAFKA_NET_SASL_TYPE":     sarama.SASLTypeSCRAMSHA256,
			},
			enabledSASL:     true,
			saslUser:        defaultSASLUser,
			saslPassword:    defaultSASLPassword,
			saslMechanism:   sarama.SASLTypeSCRAMSHA256,
			bootstrapServer: defaultBootstrapServer,
		},
		"Only SASL-SCRAM-SHA-512 Auth": {
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": defaultBootstrapServer,
				"KAFKA_NET_SASL_ENABLE":   "true",
				"KAFKA_NET_SASL_USER":     defaultSASLUser,
				"KAFKA_NET_SASL_PASSWORD": defaultSASLPassword,
				"KAFKA_NET_SASL_TYPE":     sarama.SASLTypeSCRAMSHA512,
			},
			enabledSASL:     true,
			saslUser:        defaultSASLUser,
			saslPassword:    defaultSASLPassword,
			saslMechanism:   sarama.SASLTypeSCRAMSHA512,
			bootstrapServer: defaultBootstrapServer,
		},
	}
	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			for k, v := range tc.env {
				_ = os.Setenv(k, v)
			}
			servers, config, err := NewConfig(context.Background())
			if err != nil && tc.wantErr != true {
				t.Fatal(err)
			}
			if servers[0] != tc.bootstrapServer && tc.wantErr != true {
				t.Fatalf("Incorrect bootstrapServers, got: %s vs want: %s", servers[0], tc.bootstrapServer)
			}
			if tc.enabledSASL {
				if tc.saslMechanism != string(config.Net.SASL.Mechanism) {
					t.Fatalf("Incorrect SASL mechanism, got: %s vs want: %s", string(config.Net.SASL.Mechanism), tc.saslMechanism)
				}

				if config.Net.SASL.Enable != true {
					t.Fatal("Incorrect SASL Configuration (not enabled)")
				}

				if config.Net.SASL.User != tc.saslUser && !tc.wantErr {
					t.Fatalf("Incorrect SASL User, got: %s vs want: %s", config.Net.SASL.User, tc.saslUser)
				}
				if config.Net.SASL.Password != tc.saslPassword && !tc.wantErr {
					t.Fatalf("Incorrect SASL Password, got: %s vs want: %s", config.Net.SASL.Password, tc.saslPassword)
				}
			}
			require.NotNil(t, config)
			for k := range tc.env {
				_ = os.Unsetenv(k)
			}
		})
	}
}

func TestAdminClient(t *testing.T) {

	seedBroker := sarama.NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	// mock broker does not support TLS ...
	admin, err := MakeAdminClient("test-client", nil, &utils.KafkaConfig{Brokers: []string{seedBroker.Addr()}})
	if err != nil {
		t.Fatal(err)
	}

	require.NoError(t, err)
	require.NotNil(t, admin)
}
