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
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

// Test The ExtractRoots() Functionality
func TestExtractRootCerts(t *testing.T) {
	tt := []struct {
		name                  string
		yamlBefore            string
		expectedYamlAfter     string
		expectedRootCertCount int
	}{
		{
			name:                  "single cert",
			yamlBefore:            EKDefaultSaramaConfigWithRootCert,
			expectedYamlAfter:     EKDefaultSaramaConfigWithoutCerts,
			expectedRootCertCount: 1,
		},
		{
			name:                  "no cert",
			yamlBefore:            EKDefaultSaramaConfigWithoutCerts,
			expectedYamlAfter:     EKDefaultSaramaConfigWithoutCerts,
			expectedRootCertCount: 0,
		},
		{
			name:                  "multiple certs",
			yamlBefore:            EKDefaultSaramaConfigWithTwoRootCerts,
			expectedYamlAfter:     EKDefaultSaramaConfigWithoutCerts,
			expectedRootCertCount: 2,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			// Perform The Test (Extract The RootCert)
			yamlAfter, certPool, err := extractRootCerts(tc.yamlBefore)

			// Verify The RootCert Was Extracted Successfully & Returned In CertPool
			assert.Nil(t, err)

			assert.Equal(t, tc.expectedYamlAfter, yamlAfter)

			assert.False(t, strings.Contains(yamlAfter, "RootPEMs"))
			assert.False(t, strings.Contains(yamlAfter, "-----BEGIN CERTIFICATE-----"))
			assert.False(t, strings.Contains(yamlAfter, "-----END CERTIFICATE-----"))

			if tc.expectedRootCertCount == 0 {
				assert.True(t, certPool == nil)
			} else {
				assert.NotNil(t, certPool)
			}
		})
	}
}

// Test The extractKafkaVersion() Functionality
func TestExtractKafkaVersion(t *testing.T) {
	tt := []struct {
		name              string
		yamlBefore        string
		error             bool
		expectedYamlAfter string
		expectedVersion   *sarama.KafkaVersion
	}{
		{
			name: "version exists with 3 numbers",
			yamlBefore: `
Net:
  TLS:
    Enable: false
Version: 1.0.0
Metadata:
  RefreshFrequency: 300000000000
`,
			error: false,
			expectedYamlAfter: `
Net:
  TLS:
    Enable: false
Metadata:
  RefreshFrequency: 300000000000
`,
			expectedVersion: &sarama.V1_0_0_0,
		},
		{
			// Sarama special thing: only versions starting with 0 can have 4 digits
			name: "version exists with 4 numbers",
			yamlBefore: `
Net:
  TLS:
    Enable: false
Version: 0.8.2.0
Metadata:
  RefreshFrequency: 300000000000
`,
			error: false,
			expectedYamlAfter: `
Net:
  TLS:
    Enable: false
Metadata:
  RefreshFrequency: 300000000000
`,
			expectedVersion: &sarama.V0_8_2_0,
		},
		{
			name: "invalid version",
			yamlBefore: `
Net:
  TLS:
    Enable: false
Version: ABC
Metadata:
  RefreshFrequency: 300000000000
`,
			error: true,
			expectedYamlAfter: `
Net:
  TLS:
    Enable: false
Version: ABC
Metadata:
  RefreshFrequency: 300000000000
`,
			expectedVersion: nil,
		},
		{
			name: "version doesn't exist",
			yamlBefore: `
Net:
  TLS:
    Enable: false
Metadata:
  RefreshFrequency: 300000000000
`,
			error: false,
			expectedYamlAfter: `
Net:
  TLS:
    Enable: false
Metadata:
  RefreshFrequency: 300000000000
`,
			expectedVersion: nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			// Perform The Test (Extract The Version)
			yamlAfter, version, err := extractKafkaVersion(tc.yamlBefore)

			if !tc.error {
				assert.Nil(t, err)
				assert.False(t, strings.Contains(yamlAfter, "Version: "))
			} else {
				assert.NotNil(t, err)
			}

			assert.Equal(t, tc.expectedYamlAfter, yamlAfter)
			assert.Equal(t, tc.expectedVersion, version)
		})
	}
}
