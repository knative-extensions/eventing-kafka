package config

// Package Constants
const (
	// The name of the configmap used to hold eventing-kafka settings
	SettingsConfigMapName = "config-eventing-kafka"
	// The name of the key in the Data section of the eventing-kafka configmap that holds a Sarama JSON settings fragment
	SaramaSettingsConfigKey        = "sarama"
	EventingKafkaSettingsConfigKey = "eventing-kafka"
)
