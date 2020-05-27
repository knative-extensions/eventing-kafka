package admin

import (
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	util2 "knative.dev/eventing-kafka/pkg/common/kafka/admin/util"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
)

//
// This is an implementation of the AdminClient interface backed by the Confluent API. This is largely a pass-through
// to the Confluent client with some additional functionality layered on top.  Primarily this layer exists
//

// Ensure The KafkaAdminClient Struct Implements The AdminClientInterface
var _ AdminClientInterface = &KafkaAdminClient{}

// Kafka AdminClient Definition
type KafkaAdminClient struct {
	logger      *zap.Logger
	namespace   string
	kafkaSecret string
	adminClient ConfluentAdminClientInterface
}

// Confluent AdminClient Interface - Adding Our Own Wrapping Interface To Facilitate Testing
type ConfluentAdminClientInterface interface {
	CreateTopics(ctx context.Context, topicSpecifications []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error)
	DeleteTopics(ctx context.Context, topics []string, options ...kafka.DeleteTopicsAdminOption) ([]kafka.TopicResult, error)
	Close()
}

// Verify The Confluent AdminClient Implements Our Interface
var _ ConfluentAdminClientInterface = &kafka.AdminClient{}

// Create A New Kafka (Confluent, etc...) AdminClient Based On The Kafka Secret In The Specified K8S Namespace
func NewKafkaAdminClient(ctx context.Context, namespace string) (AdminClientInterface, error) {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx).Desugar()

	// Get The K8S Client From The Context
	k8sClient := kubeclient.Get(ctx)

	// Get A List Of The Kafka Secrets
	kafkaSecrets, err := util2.GetKafkaSecrets(k8sClient, namespace)
	if err != nil {
		logger.Error("Failed To Get Kafka Authentication Secrets", zap.Error(err))
		return nil, err
	}

	// Handle Various Numbers Of Kafka Secrets - Currently Only Support One!
	if len(kafkaSecrets.Items) != 1 {
		logger.Warn(fmt.Sprintf("Expected 1 Kafka Secret But Found %d - Kafka AdminClient Will Not Be Functional!", len(kafkaSecrets.Items)))
		return &KafkaAdminClient{logger: logger, namespace: namespace}, nil
	} else {
		logger.Info("Found 1 Kafka Secret", zap.String("Secret", kafkaSecrets.Items[0].Name))
	}

	// Extract The Relevant Data From The Kafka Secret
	kafkaSecret := kafkaSecrets.Items[0]
	brokers := string(kafkaSecret.Data[constants.KafkaSecretKeyBrokers])
	username := string(kafkaSecret.Data[constants.KafkaSecretKeyUsername])
	password := string(kafkaSecret.Data[constants.KafkaSecretKeyPassword])

	// Validate Secret Data
	if !validateKafkaSecret(logger, &kafkaSecret) {
		err = errors.New("invalid Kafka Secret found")
		return nil, err
	}

	// Create The Kafka Consumer Configuration
	configMap := getBaseAdminConfigMap(brokers)

	// Append SASL Authentication If Specified
	if len(username) > 0 && len(password) > 0 {
		util.AddSaslAuthentication(configMap, constants.ConfigPropertySaslMechanismsPlain, username, password)
	}

	// Create A New Kafka AdminClient From ConfigMap & Return Results
	adminClient, err := kafka.NewAdminClient(configMap)
	if err != nil {
		logger.Error("Failed To Create Kafka AdminClient", zap.Any("ConfigMap", configMap), zap.Error(err))
		return nil, err
	}

	// Create And Return A New Kafka AdminClient With Admin Client
	return &KafkaAdminClient{
		logger:      logger,
		namespace:   namespace,
		kafkaSecret: kafkaSecret.Name,
		adminClient: adminClient,
	}, nil
}

// CreateTopics - Confluent Pass-Through Function
func (c KafkaAdminClient) CreateTopics(ctx context.Context, topicSpecifications []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {
	if c.adminClient == nil {
		c.logger.Error("Unable To Create Topics Due To Invalid AdminClient - Check Kafka Authorization Secret")
		return nil, fmt.Errorf("unable to create topics due to invalid AdminClient - check Kafka authorization secrets")
	} else {
		return c.adminClient.CreateTopics(ctx, topicSpecifications, options...)
	}
}

// DeleteTopics - Confluent Pass-Through Function
func (c KafkaAdminClient) DeleteTopics(ctx context.Context, topics []string, options ...kafka.DeleteTopicsAdminOption) ([]kafka.TopicResult, error) {
	if c.adminClient == nil {
		c.logger.Error("Unable To Delete Topics Due To Invalid AdminClient - Check Kafka Authorization Secret")
		return nil, fmt.Errorf("unable to delete topics due to invalid AdminClient - check Kafka authorization secrets")
	} else {
		return c.adminClient.DeleteTopics(ctx, topics, options...)
	}
}

// Close - Confluent Pass-Through Function
func (c KafkaAdminClient) Close() {
	if c.adminClient == nil {
		c.logger.Error("Unable To Close Invalid AdminClient - Check Kafka Authorization Secret")
	} else {
		c.adminClient.Close()
	}
}

// Get The K8S Secret With Kafka Credentials For The Specified Topic
func (c KafkaAdminClient) GetKafkaSecretName(topicName string) string {
	return c.kafkaSecret
}

// Utility Function For Returning The Base/Common Kafka ConfigMap (Values Shared By All Connections)
func getBaseAdminConfigMap(brokers string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		constants.ConfigPropertyBootstrapServers: brokers,
		constants.ConfigPropertyRequestTimeoutMs: constants.ConfigPropertyRequestTimeoutMsValue,
	}
}

// Utility Function For Validating Kafka Secret
func validateKafkaSecret(logger *zap.Logger, secret *corev1.Secret) bool {

	// Assume Invalid Until Proven Otherwise
	valid := false

	// Validate The Kafka Secret
	if secret != nil {

		// Extract The Relevant Data From The Kafka Secret
		brokers := string(secret.Data[constants.KafkaSecretKeyBrokers])
		username := string(secret.Data[constants.KafkaSecretKeyUsername])
		password := string(secret.Data[constants.KafkaSecretKeyPassword])

		// Validate Kafka Secret Data (Allowing for Kafka not having Authentication enabled)
		if len(brokers) > 0 && len(username) >= 0 && len(password) >= 0 {

			// Mark Kafka Secret As Valid
			valid = true

		} else {

			// Invalid Kafka Secret - Log State
			pwdString := ""
			if len(password) > 0 {
				pwdString = "********"
			}
			logger.Error("Kafka Secret Contains Invalid Data",
				zap.String("Name", secret.Name),
				zap.String("Brokers", brokers),
				zap.String("Username", username),
				zap.String("Password", pwdString))
		}
	}

	// Return Kafka Secret Validity
	return valid
}
