package eventhubcache

import (
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
)

// Azure EventHubs Namespace Struct
type Namespace struct {
	Name       string
	Username   string
	Password   string
	Secret     string
	HubManager HubManagerInterface
	Count      int
}

// Namespace Complete Argument Constructor
func NewNamespace(logger *zap.Logger, name string, username string, password string, secret string, count int) (*Namespace, error) {

	// Create A New HubManager For The Specified ConnectionString (Password)
	hubManager, err := NewHubManagerFromConnectionStringWrapper(password)
	if err != nil {
		logger.Error("Failed To Create New HubManager For Azure EventHubs Namespace", zap.Error(err))
		return nil, err
	}

	// Create & Return A New Namespace With Specified Configuration & Initialized HubManager
	return &Namespace{
		Name:       name,
		Username:   username,
		Password:   password,
		Secret:     secret,
		HubManager: hubManager,
		Count:      count,
	}, nil
}

// Namespace Secret Constructor
func NewNamespaceFromKafkaSecret(logger *zap.Logger, kafkaSecret *corev1.Secret) (*Namespace, error) {

	// Extract The Relevant Data From The Kafka Secret
	data := kafkaSecret.Data
	username := string(data[constants.KafkaSecretKeyUsername])
	password := string(data[constants.KafkaSecretKeyPassword])
	namespace := string(data[constants.KafkaSecretKeyNamespace])
	secret := kafkaSecret.Name

	// Create A New Namespace From The Secret
	return NewNamespace(logger, namespace, username, password, secret, 0)
}
