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

	NewClientId = "TestNewClientId"
	OldUsername = "TestOldUsername"
	NewUsername = "TestNewUsername"
	OldPassword = "TestOldPassword"
	NewPassword = "TestNewPassword"

	DispatcherReplicas     = "3"
	DispatcherRetryInitial = "5000"
	DispatcherRetry        = "500000"

	TestEKConfig = `
dispatcher:
  cpuLimit: 500m
  cpuRequest: 300m
  memoryLimit: 128Mi
  memoryRequest: 50Mi
  replicas: ` + DispatcherReplicas + `
  retryInitialIntervalMillis: ` + DispatcherRetryInitial + `
  retryTimeMillis: ` + DispatcherRetry + `
  retryExponentialBackoff: true
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

	OldAuthBrokers   = "TestOldBrokers"
	OldAuthNamespace = "TestOldNamespace"
	OldAuthPassword  = "TestOldPassword"
	OldAuthSaslType  = "TestOldAuthSaslType"
	OldAuthUsername  = "TestOldAuthUsername"

	NewAuthBrokers   = "TestNewBrokers"
	NewAuthNamespace = "TestNewNamespace"
	NewAuthPassword  = "TestNewPassword"
	NewAuthSaslType  = "TestNewAuthSaslType"
	NewAuthUsername  = "TestNewAuthUsername"
)
