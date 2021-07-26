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
	"context"
	"os"
	"testing"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	bindingsv1beta1 "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/ptr"

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
		initialOffset   int64
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
				"INITIAL_OFFSET":          "-1",
			},
			enabledTLS:      false,
			enabledSASL:     false,
			bootstrapServer: defaultBootstrapServer,
			initialOffset:   -1,
		},
		"Custom offset": {
			env: map[string]string{
				"KAFKA_BOOTSTRAP_SERVERS": defaultBootstrapServer,
				"INITIAL_OFFSET":          "100",
			},
			enabledTLS:      false,
			enabledSASL:     false,
			bootstrapServer: defaultBootstrapServer,
			initialOffset:   100,
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
			servers, config, err := NewConfigFromEnv(context.Background())
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
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	seedBroker := sarama.NewMockBroker(t, 1)
	defer seedBroker.Close()

	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(seedBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()),
	})

	// mock broker does not support TLS ...
	saramaConf, err := client.NewConfigBuilder().
		WithDefaults().
		WithAuth(nil).
		WithClientId("test-client").
		WithInitialOffset(ptr.Int64(-1)).
		FromYaml("").
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sarama.NewClient([]string{seedBroker.Addr()}, saramaConf)
	if err != nil {
		t.Fatal(err)
	}

	require.NoError(t, err)
	require.NotNil(t, admin)
}

func TestNewEnvConfigFromSpec(t *testing.T) {
	defaultSASLUser := "secret-user"
	defaultSASLPassword := "super-seekrit-password"
	defaultSASLType := "PLAIN"
	caCert := `-----BEGIN CERTIFICATE-----
MIICkDCCAfmgAwIBAgIUWxBoN+HbX3Cp97QkWedg5nnRw8swDQYJKoZIhvcNAQEL
BQAwWjELMAkGA1UEBhMCVFIxETAPBgNVBAgMCElzdGFuYnVsMREwDwYDVQQHDAhJ
c3RhbmJ1bDERMA8GA1UECgwIS25hdGl2ZTIxEjAQBgNVBAsMCUV2ZW50aW5nMjAe
Fw0yMDEyMTkyMDU4MzRaFw0yMTEyMTkyMDU4MzRaMFoxCzAJBgNVBAYTAlRSMREw
DwYDVQQIDAhJc3RhbmJ1bDERMA8GA1UEBwwISXN0YW5idWwxETAPBgNVBAoMCEtu
YXRpdmUyMRIwEAYDVQQLDAlFdmVudGluZzIwgZ8wDQYJKoZIhvcNAQEBBQADgY0A
MIGJAoGBANIjPQtvYpfHRtrjRiPwiJnRFCs7M+i1Y1lXiyYGFKIJgFOteBKluHH5
ZJ4Le37lxkaS4LoEgc5PiN7Z7aM4qBsh1XWV2rQ2/0mZCvxarfb7oD5DIjZOGVoW
rr6mDArdqaxNSDe0+Jch8ZSlFivtk7af3m00BDmQSUqmhYvaWph7AgMBAAGjUzBR
MB0GA1UdDgQWBBTvm9UMcoH0nWvWPCAQz4GG9KfxOjAfBgNVHSMEGDAWgBTvm9UM
coH0nWvWPCAQz4GG9KfxOjAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUA
A4GBAAqUoCQ2w04W12eHUs6xCVccmDYgdECLaiZ5s8A7YI9BipMK46T0lKtRDgbF
GmW1otglU9VjOHrwh02z7uUGu5EuLa7OWHc/dIrGYLSO1Wr8f7+WWxjmiasBK4Sj
G3wZl+h4Fd3/ARnFshcCgp1WCvwGPArwP2l4ePfmIjjA94lo
-----END CERTIFICATE-----
`
	userCert := `-----BEGIN CERTIFICATE-----
MIICiDCCAfGgAwIBAgIUemseKHPaNxCypHhZ+QcBX15FLtUwDQYJKoZIhvcNAQEL
BQAwVjELMAkGA1UEBhMCVFIxEDAOBgNVBAgMB0theXNlcmkxEDAOBgNVBAcMB0th
eXNlcmkxEDAOBgNVBAoMB0tuYXRpdmUxETAPBgNVBAsMCEV2ZW50aW5nMB4XDTIw
MTIxOTIwNTQ0OVoXDTMwMTIxNzIwNTQ0OVowVjELMAkGA1UEBhMCVFIxEDAOBgNV
BAgMB0theXNlcmkxEDAOBgNVBAcMB0theXNlcmkxEDAOBgNVBAoMB0tuYXRpdmUx
ETAPBgNVBAsMCEV2ZW50aW5nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDq
391sRAlMjJ9B63VwuMPvSPwWf3Hnx+sJY9/q1bN6VuYkwWkoFjbqT3iKGLq00LHX
RWmDYbdIZ8gccS1sE7K/p1StBSEAXDLYw8bZLTmHrnUXYxxamiXLlOV3Mw+D5zvP
Qp0nlX3LYn87RRCYNwcilqijzL0DhtQ+/fBcxjlgFwIDAQABo1MwUTAdBgNVHQ4E
FgQUDubgADC9pQTaFr74JwzBM07l760wHwYDVR0jBBgwFoAUDubgADC9pQTaFr74
JwzBM07l760wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOBgQDgmhgY
Kq5PKDkGykk1SUlvcJl0py+CDGhBKpVoPstui7YleU+LcFZx9DmfuRW3k0AasAky
ms8iaTuT5nrQkmoqTB+SYUezLujuXHayVCNMNF5qJcUWBmkLedI5ZRMcSrnPcN76
JyOW+qTVy8PUsysnSdEe3BOTacbNzNpL0Nibjg==
-----END CERTIFICATE-----`
	userKey := `-----BEGIN RSA PRIVATE KEY-----
MIICWwIBAAKBgQDq391sRAlMjJ9B63VwuMPvSPwWf3Hnx+sJY9/q1bN6VuYkwWko
FjbqT3iKGLq00LHXRWmDYbdIZ8gccS1sE7K/p1StBSEAXDLYw8bZLTmHrnUXYxxa
miXLlOV3Mw+D5zvPQp0nlX3LYn87RRCYNwcilqijzL0DhtQ+/fBcxjlgFwIDAQAB
AoGAdCjbPVw4rR8u9E8a+fCnFoSmCApnrxX0a+R1LZMa/HpVv//Xnfe+mQtMth+c
1ygPjEPL9yowlyKcmVRv/m+Pir7gNkJ0DUus3K/QuCuBtNj1yM+POqW1m/61ZWRa
1rSsSQFQeTV4zZW395ORvat2UCcRIxOn4LeGKAVnaS5z98ECQQD+7vyfl+GUpkCa
16cqDtqjR6me8Hj1xzwS/j5jYe7M228OfHJvsmWHZI+JTXDGZqG29E6o6LLNR5di
BsrpN2wPAkEA69tlf+XRGAge/pabqIrf40EhBAViJ+vtRg38bDiBXHImR9SMSD8x
zUkiXqWKmgOlYn2AWm5EJ8mBw4jn2GrjeQJACOF0ZW7aCd6cw4gdp6Zq0WNOsl24
KP+uxQ6cR8QCmJpQTRXiuqdhSA0lvue2tQKgQYpTLykkCWikCmMoMGWg2wJAH4/S
e1UDsBWWIDeDSQCciUqz4lfeFL2LmO5SMyE0nmxgFwioZRqfzXrV8Jhyfb2zKgTl
YjSTRke+562waNOU8QJAfCZkNR12+RF1ntIDEFYpNMj+VySQ8R0Xgz8DGfwhhx7Q
sny569QyyWHk2+FZoWDfjxFZ7CvIdgLJBHc3qUXLsg==
-----END RSA PRIVATE KEY-----
`
	testCases := map[string]struct {
		src            *v1beta1.KafkaSource
		runtimeObjects []runtime.Object
	}{
		"Simple Source": {
			src: &v1beta1.KafkaSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "source-name",
					Namespace: "source-namespace",
				},
				Spec: v1beta1.KafkaSourceSpec{
					Topics: []string{"topic1,topic2"},
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
						BootstrapServers: []string{"server1"},
					},
					ConsumerGroup: "group",
				},
			},
		},
		"source with all auth options": {
			runtimeObjects: []runtime.Object{
				constructSecret("the-user-secret", "user", defaultSASLUser),
				constructSecret("the-password-secret", "password", defaultSASLPassword),
				constructSecret("the-sasltype-secret", "saslType", defaultSASLType),
				constructSecret("the-cert-secret", "tls.crt", userCert),
				constructSecret("the-key-secret", "tls.key", userKey),
				constructSecret("the-ca-cert-secret", "tls.crt", caCert),
			},
			src: &v1beta1.KafkaSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "source-name",
					Namespace: "source-namespace",
				},
				Spec: v1beta1.KafkaSourceSpec{
					Topics:        []string{"topic1,topic2"},
					ConsumerGroup: "group",
					InitialOffset: ptr.Int64(-2),
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
						BootstrapServers: []string{"server1,server2"},
						Net: bindingsv1beta1.KafkaNetSpec{
							SASL: bindingsv1beta1.KafkaSASLSpec{
								Enable: true,
								User: bindingsv1beta1.SecretValueFromSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "the-user-secret",
										},
										Key: "user",
									},
								},
								Password: bindingsv1beta1.SecretValueFromSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "the-password-secret",
										},
										Key: "password",
									},
								},
								Type: bindingsv1beta1.SecretValueFromSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "the-sasltype-secret",
										},
										Key: "saslType",
									},
								},
							},
							TLS: bindingsv1beta1.KafkaTLSSpec{
								Enable: true,
								Cert: bindingsv1beta1.SecretValueFromSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "the-cert-secret",
										},
										Key: "tls.crt",
									},
								},
								Key: bindingsv1beta1.SecretValueFromSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "the-key-secret",
										},
										Key: "tls.key",
									},
								},
								CACert: bindingsv1beta1.SecretValueFromSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "the-ca-cert-secret",
										},
										Key: "tls.crt",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			servers, config, err := NewConfigFromSpec(context.Background(), fake.NewSimpleClientset((tc.runtimeObjects)...), tc.src)
			if err != nil {
				t.Fatal(err)
			}
			if servers[0] != tc.src.Spec.KafkaAuthSpec.BootstrapServers[0] {
				t.Fatalf("Incorrect bootstrapServers, got: %s vs want: %s", servers[0], tc.src.Spec.KafkaAuthSpec.BootstrapServers[0])
			}
			initialOffset := int64(-1)
			if tc.src.Spec.InitialOffset != nil {
				initialOffset = *tc.src.Spec.InitialOffset
			}
			if config.Consumer.Offsets.Initial != initialOffset {
				t.Fatalf("Incorrect initial offset, got: %d vs want: %d", config.Consumer.Offsets.Initial, tc.src.Spec.InitialOffset)
			}
			if tc.src.Spec.KafkaAuthSpec.Net.SASL.Enable {
				if config.Net.SASL.User != defaultSASLUser {
					t.Fatalf("Incorrect SASL User, got: %s vs want: %s", config.Net.SASL.User, defaultSASLUser)
				}
				if config.Net.SASL.Password != defaultSASLPassword {
					t.Fatalf("Incorrect SASL Password, got: %s vs want: %s", config.Net.SASL.Password, defaultSASLPassword)
				}
			}
			if tc.src.Spec.KafkaAuthSpec.Net.TLS.Enable {
				if len(config.Net.TLS.Config.Certificates) != 1 {
					t.Fatalf("tls cert not found but expected")
				}
				if len(config.Net.TLS.Config.RootCAs.Subjects()) != 1 {
					t.Fatalf("ca certs not found but expected")
				}
			}
		})
	}
}

func constructSecret(name, key, secret string) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{Kind: "Secret", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "source-namespace"},
		Data:       map[string][]byte{key: []byte(secret)},
	}
}
