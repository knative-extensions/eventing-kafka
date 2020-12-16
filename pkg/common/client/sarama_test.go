package client

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

// Test The ExtractRoots() Functionality
func TestExtractRootCerts(t *testing.T) {

	// The Sarama Config YAML String To Test
	beforeSaramaConfigYaml := EKDefaultSaramaConfigWithRootCert

	// Perform The Test (Extract The RootCert)
	afterSaramaConfigYaml, certPool, err := extractRootCerts(beforeSaramaConfigYaml)

	// Verify The RootCert Was Extracted Successfully & Returned In CertPool
	assert.NotNil(t, afterSaramaConfigYaml)
	assert.NotEqual(t, beforeSaramaConfigYaml, afterSaramaConfigYaml)
	assert.False(t, strings.Contains(afterSaramaConfigYaml, "RootPEMs"))
	assert.False(t, strings.Contains(afterSaramaConfigYaml, "-----BEGIN CERTIFICATE-----"))
	assert.False(t, strings.Contains(afterSaramaConfigYaml, "-----END CERTIFICATE-----"))
	assert.NotNil(t, certPool)
	assert.Nil(t, err)
	subjects := certPool.Subjects()
	assert.NotNil(t, subjects)
	assert.Len(t, subjects, 1)

	// Attempt To Extract Again (Now That There Arent' Any RootPEMs)
	finalSaramaConfigYaml, certPool, err := extractRootCerts(afterSaramaConfigYaml)

	// Verify The YAML String Is Unchanged And CertPool Is Nil
	assert.NotNil(t, finalSaramaConfigYaml)
	assert.Equal(t, afterSaramaConfigYaml, finalSaramaConfigYaml)
	assert.Nil(t, certPool)
	assert.Nil(t, err)
}
