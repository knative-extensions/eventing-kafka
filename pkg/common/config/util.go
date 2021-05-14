package config

import (
	"context"
	"fmt"
	"hash/crc32"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing-kafka/pkg/common/constants"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
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
