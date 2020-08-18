package config

// Package Constants
const (
	// The name of the configmap used to hold eventing-kafka settings
	SettingsConfigMapName = "config-eventing-kafka"
	
	// The name of the keys in the Data section of the eventing-kafka configmap that holds Sarama and Eventing-Kafka configuration YAML
	SaramaSettingsConfigKey        = "sarama"
	EventingKafkaSettingsConfigKey = "eventing-kafka"
)
