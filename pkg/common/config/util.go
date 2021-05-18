package config

import (
	"context"
	"fmt"
	"hash/crc32"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing-kafka/pkg/common/constants"
)

func ConfigmapDataCheckSum(configMapData map[string]string) string {
	if configMapData == nil {
		return ""
	}
	configMapDataStr := fmt.Sprintf("%v", configMapData)
	checksum := fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(configMapDataStr)))
	return checksum
}

// GetAuthConfigFromKubernetes Looks Up And Returns Kafka Auth ConfigAnd Brokers From Named Secret
func GetAuthConfigFromKubernetes(ctx context.Context, secretName string, secretNamespace string) (*client.KafkaAuthConfig, error) {
	secrets := kubeclient.Get(ctx).CoreV1().Secrets(secretNamespace)
	secret, err := secrets.Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	kafkaAuthCfg := GetAuthConfigFromSecret(secret)
	return kafkaAuthCfg, nil
}

// GetAuthConfigFromSecret Looks Up And Returns Kafka Auth Config And Brokers From Provided Secret
func GetAuthConfigFromSecret(secret *corev1.Secret) *client.KafkaAuthConfig {
	if secret == nil || secret.Data == nil {
		return nil
	}

	// If we don't convert the empty string to the "PLAIN" default, the client.HasSameSettings()
	// function will assume that they should be treated as differences and needlessly reconfigure
	saslType := string(secret.Data[constants.KafkaSecretKeySaslType])
	if saslType == "" {
		saslType = sarama.SASLTypePlaintext
	}

	return &client.KafkaAuthConfig{
		SASL: &client.KafkaSaslConfig{
			User:     string(secret.Data[constants.KafkaSecretKeyUsername]),
			Password: string(secret.Data[constants.KafkaSecretKeyPassword]),
			SaslType: saslType,
		},
	}
}

// NumPartitions Gets The NumPartitions - First From Channel Spec And Then From ConfigMap-Provided Settings
func NumPartitions(channel *kafkav1beta1.KafkaChannel, configuration *EventingKafkaConfig, logger *zap.SugaredLogger) int32 {
	value := channel.Spec.NumPartitions
	if value <= 0 && configuration != nil {
		logger.Debug("Kafka Channel Spec 'NumPartitions' Not Specified - Using Default", zap.Int32("Value", configuration.Kafka.Topic.DefaultNumPartitions))
		value = configuration.Kafka.Topic.DefaultNumPartitions
	}
	return value
}

// ReplicationFactor Gets The ReplicationFactor - First From Channel Spec And Then From ConfigMap-Provided Settings
func ReplicationFactor(channel *kafkav1beta1.KafkaChannel, configuration *EventingKafkaConfig, logger *zap.SugaredLogger) int16 {
	value := channel.Spec.ReplicationFactor
	if value <= 0 && configuration != nil {
		logger.Debug("Kafka Channel Spec 'ReplicationFactor' Not Specified - Using Default", zap.Int16("Value", configuration.Kafka.Topic.DefaultReplicationFactor))
		value = configuration.Kafka.Topic.DefaultReplicationFactor
	}
	return value
}

// RetentionMillis Gets The RetentionMillis - First From Channel Spec And Then From ConfigMap-Provided Settings
func RetentionMillis(channel *kafkav1beta1.KafkaChannel, configuration *EventingKafkaConfig, logger *zap.SugaredLogger) int64 {
	//
	// TODO - The eventing-contrib KafkaChannel CRD does not include RetentionMillis so we're
	//        currently just using the default value specified in Controller Environment Variables.
	//
	//value := channel.Spec.RetentionMillis
	//if value <= 0 && configuration != nil {
	//	logger.Debug("Kafka Channel Spec 'RetentionMillis' Not Specified - Using Default", zap.Int64("Value", environment.DefaultRetentionMillis))
	//	value = environment.DefaultRetentionMillis
	//}
	//return value
	return configuration.Kafka.Topic.DefaultRetentionMillis
}
