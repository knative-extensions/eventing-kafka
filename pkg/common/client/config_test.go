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
	"context"
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
	"github.com/ghodss/yaml"
	"github.com/stretchr/testify/assert"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
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

const EKDefaultSaramaConfigWithoutCerts = `
Net:
  TLS:
    Enable: true
    Config:
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

const EKDefaultSaramaConfigWithRootCert = `
Net:
  TLS:
    Enable: true
    Config:
      RootPEMs: # Array of Root Certificate PEM Files As Strings (Mind indentation and use '|-' Syntax To Avoid Terminating \n)
      - |-
        -----BEGIN CERTIFICATE-----
        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
        NAehp9bMeco=
        -----END CERTIFICATE-----
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

const EKDefaultSaramaConfigWithTwoRootCerts = `
Net:
  TLS:
    Enable: true
    Config:
      RootPEMs: # Array of Root Certificate PEM Files As Strings (Mind indentation and use '|-' Syntax To Avoid Terminating \n)
      - |-
        -----BEGIN CERTIFICATE-----
        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
        NAehp9bMeco=
        -----END CERTIFICATE-----
        -----BEGIN CERTIFICATE-----
        MIIGDzCCA/egAwIBAgIUWq6j7u/25wPQiNMPZqL6Vy0rkvQwDQYJKoZIhvcNAQEL
        BQAwgY4xCzAJBgNVBAYTAkRFMRswGQYDVQQIDBJCYWRlbi1XdWVydHRlbWJlcmcx
        ETAPBgNVBAcMCFdhbGxkb3JmMQ8wDQYDVQQKDAZTQVAgU0UxHzAdBgNVBAsMFlNB
        UCBDUCBEYXRhIE1hbmFnZW1lbnQxHTAbBgNVBAMMFFNBUCBTRSBLYWZrYSBSb290
        IENBMB4XDTIxMDIxNzE0MjgyOVoXDTI0MDYwMTE0MjgyOVowgY4xCzAJBgNVBAYT
        AkRFMRswGQYDVQQIDBJCYWRlbi1XdWVydHRlbWJlcmcxETAPBgNVBAcMCFdhbGxk
        b3JmMQ8wDQYDVQQKDAZTQVAgU0UxHzAdBgNVBAsMFlNBUCBDUCBEYXRhIE1hbmFn
        ZW1lbnQxHTAbBgNVBAMMFFNBUCBTRSBLYWZrYSBSb290IENBMIICIjANBgkqhkiG
        9w0BAQEFAAOCAg8AMIICCgKCAgEAoLH563sTndme/L5sUH/LggOO5sgRk6D6eYJi
        YgPZFX/o8a0dctgMTBK574kPAZeniwvOIUUeGhKM/Ho4ou/B3XDD/8WtoOIZt4lj
        QGyeSlZVFtCI3Ip/ENj5gIuFhO6N209mpw9wIgjhCdCTU+g6uaAe440aqCMhVO5I
        54bHp4hSHwkLdNZnsRjSopufEKxWbJm52w7lKHwBwNc0cAckiagne6FbBTliF4K/
        bgmfRRwP9fzjcmgno+GGgKastNkJVh6Ku/2NCpOKSzQiGNAQAvqbZbpQIkCt4/PK
        Nj4c5pjbRSajwt8JWNHRJ9muJz2Db7ZceQF7H/vcAwlu/IK/ARHygfA72IY164du
        QN1qJyi1DkPs1qm5cuA8WbfL8KSqIFeiKoInHL524zeK2Yt5b89sS00ydtLSYbri
        1kd31McUCHcj66DDX5xBbNg7bgRx09DNpfwKlUnwC7tphz1DLJprJ9OvhjEZi0w7
        TNrL21yETIKGG0qSgebIhLPWTG650i2hEM2dF1t9RfVB4qdhaxS194rkwQPZ2lQ8
        74hWnuqiZ3L4XhMp1AO86UoFEWyeCbIFsZ4YHnCNfBOO4t8LSPpUiV9bTIwqG549
        upI9tWMZMVV59GiuA4M9oit7vYa1eNxpUhKWpqAVCA66tETln04umAKsywt06pUn
        0udI1OsCAwEAAaNjMGEwHQYDVR0OBBYEFGRMbSqCPP1qeNX3hEYn4YAClp/cMB8G
        A1UdIwQYMBaAFGRMbSqCPP1qeNX3hEYn4YAClp/cMA8GA1UdEwEB/wQFMAMBAf8w
        DgYDVR0PAQH/BAQDAgGGMA0GCSqGSIb3DQEBCwUAA4ICAQBRNkbWuq13OZVAC+KG
        tjQJWkAym80St+5Ta2XiethvjnBi4TJGP8pjmMJhbUcQ6HNHQlHSFUDE5RKZkZvl
        EggAwIhKv49MuguiDUhe2aguQvcUV3VHj96/l6ryHsMz4tT6zQBhEaw2Ejgf6zDO
        RHBMh2AiEo8CDGaS9TOpSQQSmZf3xWdOAagjxbMKWYjbU2EMQV2vA0+lv2RGtZGW
        WUMaj9kToYCYqFsWxVBQd8EQ/9lIVrEyi9raI5X06ofiKlpG93oEtiL8SQnp27Wa
        ynQVBA0iS5BD34uXLGT7K6QnijR+FFKWeTFTCCKtiVrD5S3+zs29d2stQ9pvPo8x
        hfUV76mik+i3XX6/rpyoCtOHNHW/38LPMHzwyQQ1DgJ3TJ/0WHuWZvtzFIUwl5Ie
        4lJ3tfrh5sXyHOMclWM5Sl9TwJvGJIo4XAP+qnFInL8zSN72rzliJdZMbJDHM6Vz
        sGz3KWmgntiTDg+1hLvBD/D6Le1axC8qg7j/QUL8D/YFVy4AZnhuMYdVJBM5oNs5
        gec0L0mhPTD8fqVpzr6nZ/K2ZL5xlOSujE5+w9gmjtuGZwVpI7wOmNSgaG6/u7CP
        31OCuKAlbmDBYuwjaCIOWZL+BBj7bQcI6hZEP4oj+vSWe0xUPLvH2u2Jr26arNEs
        771uezZAFqd1GLLL8ZYRmCsAMg==
        -----END CERTIFICATE-----
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

// Verify that the JSON fragment can be loaded into a sarama.Config struct
func TestBuildSaramaConfig(t *testing.T) {
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Setup Environment
	commontesting.SetTestEnvironment(t)

	// Get a default Sarama config for verification that we don't overwrite settings when we merge
	defaultConfig := sarama.NewConfig()

	// Verify a few settings in different parts of two separate sarama.Config structures
	// Since it's a simple JSON merge we don't need to test every possible value.
	config, err := NewConfigBuilder().
		WithDefaults().
		FromYaml(extractSaramaConfig(t, commontesting.OldSaramaConfig)).
		Build(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.True(t, config.Net.SASL.Enable)
	assert.Equal(t, commontesting.OldUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	config, err = NewConfigBuilder().
		WithDefaults().
		FromYaml(extractSaramaConfig(t, commontesting.NewSaramaConfig)).
		Build(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, sarama.V2_3_0_0, config.Version)
	assert.True(t, config.Net.SASL.Enable)
	assert.Equal(t, commontesting.NewUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	// Verify error when an invalid Version is provided
	regexVersion := regexp.MustCompile(`Version:\s*\d*\.[\d.]*`) // Must have at least one period or it will match the "Version: 1" in Net.SASL
	_, err = NewConfigBuilder().
		WithDefaults().
		FromYaml(extractSaramaConfig(t, regexVersion.ReplaceAllString(commontesting.NewSaramaConfig, "Version: INVALID"))).
		Build(ctx)
	assert.NotNil(t, err)

	// Verify error when an invalid RootPEMs is provided
	_, err = NewConfigBuilder().
		WithDefaults().
		FromYaml(strings.Replace(EKDefaultSaramaConfigWithRootCert, "-----BEGIN CERTIFICATE-----", "INVALID CERT DATA", -1)).
		Build(ctx)
	assert.NotNil(t, err)

	// Verify that the RootPEMs section is merged properly
	config, err = NewConfigBuilder().
		WithDefaults().
		FromYaml(EKDefaultSaramaConfigWithRootCert).
		Build(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, config.Net.TLS.Config.RootCAs)

	// Verify that the InsecureSkipVerify flag can be set properly
	config, err = NewConfigBuilder().
		WithDefaults().
		FromYaml(EKDefaultSaramaConfigWithInsecureSkipVerify).
		Build(ctx)
	assert.Nil(t, err)
	assert.True(t, config.Net.TLS.Config.InsecureSkipVerify)

	// Verify precedence
	existing := sarama.NewConfig()
	existing.ClientID = "toBeOverriddenClientId"
	existing.Version = sarama.V1_0_0_0
	config, err = NewConfigBuilder().
		WithDefaults().
		WithExisting(existing).
		FromYaml(EKDefaultSaramaConfigWithRootCert).
		WithAuth(&KafkaAuthConfig{
			SASL: &KafkaSaslConfig{
				User: "foo",
			},
		}).
		WithVersion(&sarama.V2_0_0_0).
		WithClientId("newClientId").
		Build(ctx)
	assert.Nil(t, err)
	assert.Equal(t, "foo", config.Net.SASL.User)
	assert.Equal(t, "newClientId", config.ClientID)
	assert.Equal(t, sarama.V2_0_0_0, config.Version)
}

func extractSaramaConfig(t *testing.T, saramaConfigField string) string {
	saramaShell := &struct {
		EnableLogging bool   `json:"enableLogging"`
		Config        string `json:"config"`
	}{}
	assert.Nil(t, yaml.Unmarshal([]byte(saramaConfigField), &saramaShell))
	return saramaShell.Config
}

func TestBuildSaramaConfigWithTLSAuth(t *testing.T) {
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Setup Environment
	commontesting.SetTestEnvironment(t)

	// Verify that auth config is merged
	noAuthSaramaYaml := `
Net:
  TLS:
    Enable: false
  SASL:
    Enable: true
    Mechanism: PLAIN
    Version: 1
Metadata:
  RefreshFrequency: 300000000000
`
	kafkaAuthCfg := &KafkaAuthConfig{
		TLS: &KafkaTlsConfig{
			Cacert: `-----BEGIN CERTIFICATE-----
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
`,
			Usercert: `-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----`,
			Userkey: `-----BEGIN RSA PRIVATE KEY-----
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
`,
		},
	}

	config, err := NewConfigBuilder().
		WithDefaults().
		FromYaml(noAuthSaramaYaml).
		WithAuth(kafkaAuthCfg).
		Build(ctx)
	assert.Nil(t, err)

	// Make sure TLS settings are applied from the KafkaAuthConfig
	assert.True(t, config.Net.TLS.Enable)
	assert.True(t, config.Net.TLS.Config.InsecureSkipVerify)
	assert.Len(t, config.Net.TLS.Config.Certificates, 1)
	assert.Len(t, config.Net.TLS.Config.RootCAs.Subjects(), 1)

	// Make sure SASL settings are untouched
	assert.True(t, config.Net.SASL.Enable)
	assert.Equal(t, sarama.SASLMechanism("PLAIN"), config.Net.SASL.Mechanism)
	assert.Equal(t, int16(1), config.Net.SASL.Version)
}

func TestBuildSaramaConfigWithSASLAuth(t *testing.T) {
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

	// Setup Environment
	commontesting.SetTestEnvironment(t)

	// Verify that auth config is merged
	noAuthSaramaYaml := `
Net:
  TLS:
    Enable: false
  SASL:
    Enable: true
    Mechanism: PLAIN
    Version: 1
Metadata:
  RefreshFrequency: 300000000000
`
	kafkaAuthCfg := &KafkaAuthConfig{
		SASL: &KafkaSaslConfig{
			User:     "USERNAME",
			Password: "PASSWORD",
			SaslType: "SCRAM-SHA-256",
		},
	}

	config, err := NewConfigBuilder().
		WithDefaults().
		FromYaml(noAuthSaramaYaml).
		WithAuth(kafkaAuthCfg).
		Build(ctx)
	assert.Nil(t, err)

	// Make sure SASL settings are applied from the KafkaAuthConfig
	assert.True(t, config.Net.SASL.Enable)
	assert.True(t, config.Net.SASL.Handshake)
	assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256), config.Net.SASL.Mechanism)
	assert.Equal(t, "USERNAME", config.Net.SASL.User)
	assert.Equal(t, "PASSWORD", config.Net.SASL.Password)

	// Make sure TLS settings are untouched
	assert.False(t, config.Net.TLS.Enable)
}

// Verify that comparisons of sarama config structs function as expected
func TestSaramaConfigEqual(t *testing.T) {
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)

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
	config1, err := NewConfigBuilder().
		WithDefaults().
		FromYaml(EKDefaultSaramaConfigWithRootCert).
		Build(ctx)
	assert.Nil(t, err)
	config2, err = NewConfigBuilder().
		WithDefaults().
		FromYaml(EKDefaultSaramaConfigWithRootCert).
		Build(ctx)
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
			logger := logtesting.TestLogger(t)
			ctx := logging.WithLogger(context.TODO(), logger)

			// Perform The Test
			config, err := NewConfigBuilder().
				WithAuth(tc.kafkaAuthCfg).
				Build(ctx)

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

func TestHasSameSettings(t *testing.T) {
	authConfig := &KafkaAuthConfig{SASL: &KafkaSaslConfig{User: "user1", Password: "password1", SaslType: sarama.SASLTypeOAuth}}
	saramaConfig := sarama.NewConfig()

	assert.False(t, authConfig.SASL.HasSameSettings(saramaConfig))
	saramaConfig.Net.SASL.User = "user1"
	assert.False(t, authConfig.SASL.HasSameSettings(saramaConfig))
	saramaConfig.Net.SASL.Password = "password1"
	assert.False(t, authConfig.SASL.HasSameSettings(saramaConfig))
	saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	assert.True(t, authConfig.SASL.HasSameSettings(saramaConfig))
}

func TestHasSameBrokers(t *testing.T) {

	// Define The TestCase Struct
	type TestCase struct {
		name  string
		str   string
		slice []string
		same  bool
	}

	const brokers123 = "broker1,broker2,broker3"

	// Create The TestCases
	testCases := []TestCase{
		{name: "3 vs. Identical Slice", str: brokers123, slice: []string{"broker1", "broker2", "broker3"}, same: true},
		{name: "3 vs. Empty Slice", str: brokers123, slice: []string{}},
		{name: "3 vs. 1-element Slice", str: brokers123, slice: []string{"broker1"}},
		{name: "3 vs. 2-element Slice", str: brokers123, slice: []string{"broker1", "broker2"}},
		{name: "3 vs. 4-element Slice", str: brokers123, slice: []string{"broker1", "broker2", "broker3", "broker4"}},
		{name: "3 vs. Reversed Slice", str: brokers123, slice: []string{"broker3", "broker2", "broker1"}},
		{name: "1 vs. 1-element Slice", str: "broker1", slice: []string{"broker1"}, same: true},
		{name: "0 vs. Empty Slice", str: "", slice: []string{}, same: true},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.same, HasSameBrokers(testCase.str, testCase.slice))
		})
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
