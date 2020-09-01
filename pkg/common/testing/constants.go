package testing

const (
	OldClientId = "TestOldClientId"
	NewClientId = "TestNewClientId"
	OldUsername = "TestOldUsername"
	NewUsername = "TestNewUsername"
	OldPassword = "TestOldPassword"
	NewPassword = "TestNewPassword"

	KnativeEventingNamespace       = "knative-eventing"
	SettingsConfigMapName          = "config-eventing-kafka"
	SaramaSettingsConfigKey        = "sarama"
	EventingKafkaSettingsConfigKey = "eventing-kafka"

	DispatcherReplicas     = "3"
	DispatcherRetryInitial = "5000"
	DispatcherRetry        = "500000"

	// These constants are used here to make sure that the CreateConsumerGroup() call doesn't have problems,
	// but they aren't non-testing defaults since most settings are now in 200-eventing-kafka-configmap.yaml
	ConfigAdminTimeout                      = "10000000000"
	ConfigNetKeepAlive                      = "30000000000"
	ConfigMetadataRefreshFrequency          = "300000000000"
	ConfigConsumerOffsetsAutoCommitInterval = "5000000000"
	ConfigConsumerOffsetsRetention          = "604800000000000"
	ConfigProducerIdempotent                = "false"
	ConfigProducerRequiredAcks              = "-1"

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

	SaramaDefaultConfigYaml = `
Admin:
  Timeout: ` + ConfigAdminTimeout + `
Net:
  KeepAlive: ` + ConfigNetKeepAlive + `
Metadata:
  RefreshFrequency: ` + ConfigMetadataRefreshFrequency + `
Consumer:
  Offsets:
    AutoCommit:
      Interval: ` + ConfigConsumerOffsetsAutoCommitInterval + `
    Retention: ` + ConfigConsumerOffsetsRetention + `
  Return:
    Errors: true
Producer:
  Idempotent: ` + ConfigProducerIdempotent + `
  RequiredAcks: ` + ConfigProducerRequiredAcks + `
  Return:
    Successes: true
`
)
