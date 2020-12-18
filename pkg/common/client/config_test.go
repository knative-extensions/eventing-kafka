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
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
)

const (
	EKDefaultSaramaConfigWithInsecureSkipVerify = `
Net:
  TLS:
    Enable: true
    Config:
      InsecureSkipVerify: true
  SASL:
    Mechanism: PLAIN
    Version: 1
Metadata:
  RefreshFrequency: 300000000000
Consumer:
  Offsets:
    AutoCommit:
        Interval: 5000000000
    Retention: 604800000000000
  Return:
    Errors: true
`
)

// Verify that the JSON fragment can be loaded into a sarama.Config struct
func TestBuildSaramaConfig(t *testing.T) {
	// Setup Environment
	commontesting.SetTestEnvironment(t)

	// Get a default Sarama config for verification that we don't overwrite settings when we merge
	defaultConfig := sarama.NewConfig()

	// Verify a few settings in different parts of two separate sarama.Config structures
	// Since it's a simple JSON merge we don't need to test every possible value.
	config, err := BuildSaramaConfig(nil, commontesting.OldSaramaConfig, nil)
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.True(t, config.Net.SASL.Enable)
	assert.Equal(t, commontesting.OldUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	config, err = BuildSaramaConfig(config, commontesting.NewSaramaConfig, nil)
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, sarama.V2_3_0_0, config.Version)
	assert.True(t, config.Net.SASL.Enable)
	assert.Equal(t, commontesting.NewUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	// Verify error when an invalid Version is provided
	regexVersion := regexp.MustCompile(`Version:\s*\d*\.[\d.]*`) // Must have at least one period or it will match the "Version: 1" in Net.SASL
	config, err = BuildSaramaConfig(config, regexVersion.ReplaceAllString(commontesting.NewSaramaConfig, "Version: INVALID"), nil)
	assert.NotNil(t, err)

	// Verify error when an invalid RootPEMs is provided
	config, err = BuildSaramaConfig(config, strings.Replace(EKDefaultSaramaConfigWithRootCert, "-----BEGIN CERTIFICATE-----", "INVALID CERT DATA", -1), nil)
	assert.NotNil(t, err)

	// Verify that the RootPEMs section is merged properly
	config, err = BuildSaramaConfig(config, EKDefaultSaramaConfigWithRootCert, nil)
	assert.Nil(t, err)
	assert.NotNil(t, config.Net.TLS.Config.RootCAs)

	// Verify that the InsecureSkipVerify flag can be set properly
	config, err = BuildSaramaConfig(config, EKDefaultSaramaConfigWithInsecureSkipVerify, nil)
	assert.Nil(t, err)
	assert.True(t, config.Net.TLS.Config.InsecureSkipVerify)
}

// Verify that comparisons of sarama config structs function as expected
func TestSaramaConfigEqual(t *testing.T) {
	config1 := sarama.NewConfig()
	config2 := sarama.NewConfig()

	// Change some of the values back and forth and verify that the comparison function is correctly evaluated
	assert.True(t, ConfigEqual(config1, config2))

	config1.Admin = sarama.Config{}.Admin // Zero out the entire Admin sub-struct
	assert.False(t, ConfigEqual(config1, config2))

	config2.Admin = sarama.Config{}.Admin // Zero out the entire Admin sub-struct
	assert.True(t, ConfigEqual(config1, config2))

	config1.Net.SASL.Version = 12345
	assert.False(t, ConfigEqual(config1, config2))

	config2.Net.SASL.Version = 12345
	assert.True(t, ConfigEqual(config1, config2))

	config1.Metadata.RefreshFrequency = 1234 * time.Second
	assert.False(t, ConfigEqual(config1, config2))

	config2.Metadata.RefreshFrequency = 1234 * time.Second
	assert.True(t, ConfigEqual(config1, config2))

	config1.Producer.Flush.Bytes = 12345678
	assert.False(t, ConfigEqual(config1, config2))

	config2.Producer.Flush.Bytes = 12345678
	assert.True(t, ConfigEqual(config1, config2))

	config1.RackID = "New Rack ID"
	assert.False(t, ConfigEqual(config1, config2))

	config2.RackID = "New Rack ID"
	assert.True(t, ConfigEqual(config1, config2))

	// Change a boolean flag in the TLS.Config struct (which is not Sarama-specific) and make sure the compare function
	// works with those sub-structs as well.
	config1.Net.TLS.Config = &tls.Config{}
	config2.Net.TLS.Config = &tls.Config{}

	config1.Net.TLS.Config.InsecureSkipVerify = true
	config2.Net.TLS.Config.InsecureSkipVerify = false
	assert.False(t, ConfigEqual(config1, config2))
	config2.Net.TLS.Config.InsecureSkipVerify = true
	assert.True(t, ConfigEqual(config1, config2))
	config1.Net.TLS.Config.InsecureSkipVerify = false
	assert.False(t, ConfigEqual(config1, config2))
	config2.Net.TLS.Config.InsecureSkipVerify = false
	assert.True(t, ConfigEqual(config1, config2))

	// Test config with TLS struct
	config1, err := BuildSaramaConfig(nil, EKDefaultSaramaConfigWithRootCert, nil)
	assert.Nil(t, err)
	config2, err = BuildSaramaConfig(nil, EKDefaultSaramaConfigWithRootCert, nil)
	assert.Nil(t, err)
	assert.True(t, ConfigEqual(config1, config2))
}

func TestUpdateSaramaConfigWithKafkaAuthConfig(t *testing.T) {

	cert, key := generateCert(t)

	testCases := map[string]struct {
		kafkaAuthCfg  *KafkaAuthConfig
		enabledTLS    bool
		enabledSASL   bool
		salsMechanism string
	}{
		"No Auth": {
			enabledTLS:  false,
			enabledSASL: false,
		},
		"Only SASL-PLAIN Auth": {
			kafkaAuthCfg: &KafkaAuthConfig{
				SASL: &KafkaSaslConfig{
					User:     "my-user",
					Password: "super-secret",
					SaslType: sarama.SASLTypePlaintext,
				},
			},
			enabledTLS:    false,
			enabledSASL:   true,
			salsMechanism: sarama.SASLTypePlaintext,
		},
		"Only SASL-PLAIN Auth (not specified, defaulted)": {
			kafkaAuthCfg: &KafkaAuthConfig{
				SASL: &KafkaSaslConfig{
					User:     "my-user",
					Password: "super-secret",
				},
			},
			enabledTLS:    false,
			enabledSASL:   true,
			salsMechanism: sarama.SASLTypePlaintext,
		},
		"Only SASL-SCRAM-SHA-256 Auth": {
			kafkaAuthCfg: &KafkaAuthConfig{
				SASL: &KafkaSaslConfig{
					User:     "my-user",
					Password: "super-secret",
					SaslType: sarama.SASLTypeSCRAMSHA256,
				},
			},
			enabledTLS:    false,
			enabledSASL:   true,
			salsMechanism: sarama.SASLTypeSCRAMSHA256,
		},
		"Only SASL-SCRAM-SHA-512 Auth": {
			kafkaAuthCfg: &KafkaAuthConfig{
				SASL: &KafkaSaslConfig{
					User:     "my-user",
					Password: "super-secret",
					SaslType: sarama.SASLTypeSCRAMSHA512,
				},
			},
			enabledTLS:    false,
			enabledSASL:   true,
			salsMechanism: sarama.SASLTypeSCRAMSHA512,
		},
		"Only TLS Auth": {
			kafkaAuthCfg: &KafkaAuthConfig{
				TLS: &KafkaTlsConfig{
					Cacert:   cert,
					Usercert: cert,
					Userkey:  key,
				},
			},
			enabledTLS:  true,
			enabledSASL: false,
		},
		"SASL and TLS Auth": {
			kafkaAuthCfg: &KafkaAuthConfig{
				SASL: &KafkaSaslConfig{
					User:     "my-user",
					Password: "super-secret",
					SaslType: sarama.SASLTypeSCRAMSHA512,
				},
				TLS: &KafkaTlsConfig{
					Cacert:   cert,
					Usercert: cert,
					Userkey:  key,
				},
			},
			enabledTLS:    true,
			enabledSASL:   true,
			salsMechanism: sarama.SASLTypeSCRAMSHA512,
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {

			// Perform The Test
			config := sarama.NewConfig()
			err := UpdateSaramaConfigWithKafkaAuthConfig(config, tc.kafkaAuthCfg)
			if err != nil {
				t.Errorf("error configuring Sarama config with auth :%e", err)
			}

			saslEnabled := config.Net.SASL.Enable
			if saslEnabled != tc.enabledSASL {
				t.Errorf("SASL config is wrong")
			}
			if saslEnabled {
				if tc.salsMechanism != string(config.Net.SASL.Mechanism) {
					t.Errorf("SASL Mechanism is wrong, want: %s vs got %s", tc.salsMechanism, string(config.Net.SASL.Mechanism))
				}
			}

			tlsEnabled := config.Net.TLS.Enable
			if tlsEnabled != tc.enabledTLS {
				t.Errorf("TLS config is wrong")
			}
			if tlsEnabled {
				if config.Net.TLS.Config == nil {
					t.Errorf("TLS config is wrong")
				}
			}
		})
	}
}

func TestNewTLSConfig(t *testing.T) {
	cert, key := generateCert(t)

	for _, tt := range []struct {
		name       string
		cert       string
		key        string
		caCert     string
		wantErr    bool
		wantNil    bool
		wantClient bool
		wantServer bool
	}{{
		name:    "all empty",
		wantNil: true,
	}, {
		name:    "bad input",
		cert:    "x",
		key:     "y",
		caCert:  "z",
		wantErr: true,
	}, {
		name:    "only cert",
		cert:    cert,
		wantNil: true,
	}, {
		name:    "only key",
		key:     key,
		wantNil: true,
	}, {
		name:       "cert and key",
		cert:       cert,
		key:        key,
		wantClient: true,
	}, {
		name:       "only caCert",
		caCert:     cert,
		wantServer: true,
	}, {
		name:       "cert, key, and caCert",
		cert:       cert,
		key:        key,
		caCert:     cert,
		wantClient: true,
		wantServer: true,
	}} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := newTLSConfig(tt.cert, tt.key, tt.caCert)
			if tt.wantErr {
				if err == nil {
					t.Fatal("wanted error")
				}
				return
			}

			if tt.wantNil {
				if c != nil {
					t.Fatal("wanted non-nil config")
				}
				return
			}

			var wantCertificates int
			if tt.wantClient {
				wantCertificates = 1
			} else {
				wantCertificates = 0
			}
			if got, want := len(c.Certificates), wantCertificates; got != want {
				t.Errorf("got %d Certificates, wanted %d", got, want)
			}

			if tt.wantServer {
				if c.RootCAs == nil {
					t.Error("wanted non-nil RootCAs")
				}

				if c.VerifyPeerCertificate == nil {
					t.Error("wanted non-nil VerifyPeerCertificate")
				}

				if !c.InsecureSkipVerify {
					t.Error("wanted InsecureSkipVerify")
				}
			} else {
				if c.RootCAs != nil {
					t.Error("wanted nil RootCAs")
				}

				if c.VerifyPeerCertificate != nil {
					t.Error("wanted nil VerifyPeerCertificate")
				}

				if c.InsecureSkipVerify {
					t.Error("wanted false InsecureSkipVerify")
				}
			}
		})
	}

}

func TestVerifyCertSkipHostname(t *testing.T) {
	cert, _ := generateCert(t)
	certPem, _ := pem.Decode([]byte(cert))

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(cert))

	v := verifyCertSkipHostname(caCertPool)

	err := v([][]byte{certPem.Bytes}, nil)
	if err != nil {
		t.Fatal(err)
	}

	cert2, _ := generateCert(t)
	cert2Pem, _ := pem.Decode([]byte(cert2))

	err = v([][]byte{cert2Pem.Bytes}, nil)
	// Error expected as we're still verifying with the first cert.
	if err == nil {
		t.Fatal("wanted error")
	}
}

// Lifted from the RSA path of https://golang.org/src/crypto/tls/generate_cert.go.
func generateCert(t *testing.T) (string, string) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	notBefore := time.Now().Add(-5 * time.Minute)
	notAfter := notBefore.Add(time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)

	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)

	if err != nil {
		t.Fatal(err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}

	var certOut bytes.Buffer
	if err := pem.Encode(&certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		t.Fatal(err)
	}

	var keyOut bytes.Buffer
	if err := pem.Encode(&keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}); err != nil {
		t.Fatal(err)
	}

	return certOut.String(), keyOut.String()
}
