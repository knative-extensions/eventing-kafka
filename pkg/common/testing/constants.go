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

package testing

const (
	SystemNamespace = "eventing-test-ns"
	SecretName      = "test-secret-name"

	NewClientId = "TestNewClientId"
	OldUsername = "TestOldUsername"
	NewUsername = "TestNewUsername"
	OldPassword = "TestOldPassword"
	NewPassword = "TestNewPassword"

	ReceiverCpuLimit        = "501m"
	ReceiverCpuRequest      = "301m"
	ReceiverMemoryLimit     = "129Mi"
	ReceiverMemoryRequest   = "51Mi"
	ReceiverReplicas        = "4"
	DispatcherReplicas      = "3"
	DispatcherRetryInitial  = "5000"
	DispatcherRetry         = "500000"
	DispatcherCpuLimit      = "500m"
	DispatcherCpuRequest    = "300m"
	DispatcherMemoryLimit   = "128Mi"
	DispatcherMemoryRequest = "50Mi"
	BrokerString            = "test-broker-string"

	TestEKConfig = `
receiver:
  cpuRequest: ` + ReceiverCpuRequest + `
  memoryRequest: ` + ReceiverMemoryRequest + `
  cpuLimit: ` + ReceiverCpuLimit + `
  memoryLimit: ` + ReceiverMemoryLimit + `
  replicas: ` + ReceiverReplicas + `
dispatcher:
  cpuRequest: ` + DispatcherCpuRequest + `
  memoryRequest: ` + DispatcherMemoryRequest + `
  cpuLimit: ` + DispatcherCpuLimit + `
  memoryLimit: ` + DispatcherMemoryLimit + `
  replicas: ` + DispatcherReplicas + `
  retryInitialIntervalMillis: ` + DispatcherRetryInitial + `
  retryTimeMillis: ` + DispatcherRetry + `
  retryExponentialBackoff: true
kafka:
  brokers: ` + BrokerString + `
  enableSaramaLogging: false
  topic:
    defaultNumPartitions: 4
    defaultReplicationFactor: 1 # Cannot exceed the number of Kafka Brokers!
    defaultRetentionMillis: 604800000  # 1 week
  adminType: kafka # One of "kafka", "azure", "custom"
`

	OldSaramaConfig = `
Net:
  TLS:
    Enable: true
  SASL:
    Enable: true
    Mechanism: PLAIN
    Version: 1
    User: ` + OldUsername + `
    Password: ` + OldPassword + `
Metadata:
  RefreshFrequency: 300000000000
`

	NewSaramaConfig = `
Version: 2.3.0
Net:
  TLS:
    Enable: true
  SASL:
    Enable: true
    Mechanism: PLAIN
    Version: 1
    User: ` + NewUsername + `
    Password: ` + NewPassword + `
Metadata:
  RefreshFrequency: 300000000000
ClientID: ` + NewClientId + `
`

	OldAuthNamespace = "TestOldNamespace"
	OldAuthPassword  = "TestOldPassword"
	OldAuthSaslType  = "TestOldAuthSaslType"
	OldAuthUsername  = "TestOldAuthUsername"

	NewAuthNamespace = "TestNewNamespace"
	NewAuthPassword  = "TestNewPassword"
	NewAuthSaslType  = "TestNewAuthSaslType"
	NewAuthUsername  = "TestNewAuthUsername"
)
