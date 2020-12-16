package client

import (
	"regexp"
	"strings"
	"testing"

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
func TestMergeSaramaSettings(t *testing.T) {
	// Setup Environment
	commontesting.SetTestEnvironment(t)

	// Get a default Sarama config for verification that we don't overwrite settings when we merge
	defaultConfig := sarama.NewConfig()

	// Verify a few settings in different parts of two separate sarama.Config structures
	// Since it's a simple JSON merge we don't need to test every possible value.
	config, err := MergeSaramaSettings(nil, commontesting.OldSaramaConfig)
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, commontesting.OldUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	config, err = MergeSaramaSettings(config, commontesting.NewSaramaConfig)
	assert.Nil(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, sarama.V2_3_0_0, config.Version)
	assert.Equal(t, commontesting.NewUsername, config.Net.SASL.User)
	assert.Equal(t, defaultConfig.Producer.Timeout, config.Producer.Timeout)
	assert.Equal(t, defaultConfig.Consumer.MaxProcessingTime, config.Consumer.MaxProcessingTime)

	// Verify error when an invalid Version is provided
	regexVersion := regexp.MustCompile(`Version:\s*\d*\.[\d.]*`) // Must have at least one period or it will match the "Version: 1" in Net.SASL
	config, err = MergeSaramaSettings(config, regexVersion.ReplaceAllString(commontesting.NewSaramaConfig, "Version: INVALID"))
	assert.NotNil(t, err)

	// Verify error when an invalid RootPEMs is provided
	config, err = MergeSaramaSettings(config, strings.Replace(EKDefaultSaramaConfigWithRootCert, "-----BEGIN CERTIFICATE-----", "INVALID CERT DATA", -1))
	assert.NotNil(t, err)

	// Verify that the RootPEMs section is merged properly
	config, err = MergeSaramaSettings(config, EKDefaultSaramaConfigWithRootCert)
	assert.Nil(t, err)
	assert.NotNil(t, config.Net.TLS.Config.RootCAs)

	// Verify that the InsecureSkipVerify flag can be set properly
	config, err = MergeSaramaSettings(config, EKDefaultSaramaConfigWithInsecureSkipVerify)
	assert.Nil(t, err)
	assert.True(t, config.Net.TLS.Config.InsecureSkipVerify)
}

// Test The UpdateSaramaConfig() Functionality
func TestUpdateSaramaConfig(t *testing.T) {

	// Test Data
	clientId := "TestClientId"
	username := "TestUsername"
	password := "TestPassword"

	// Perform The Test
	config := sarama.NewConfig()
	UpdateSaramaConfig(config, clientId, username, password)

	// Verify The Results
	assert.Equal(t, clientId, config.ClientID)
	assert.Equal(t, username, config.Net.SASL.User)
	assert.Equal(t, password, config.Net.SASL.Password)
	assert.Equal(t, sarama.V1_0_0_0, config.Version)
	assert.Nil(t, config.Net.TLS.Config)
}
