package admin

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	adminutil "knative.dev/eventing-kafka/pkg/common/kafka/admin/util"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/util"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	"strings"
)

//
// This is an implementation of the AdminClient interface backed by the Sarama API. This is largely
// a pass-through to the Sarama ClusterAdmin with some additional functionality layered on top.
//

// Ensure The KafkaAdminClient Struct Implements The AdminClientInterface
var _ AdminClientInterface = &KafkaAdminClient{}

// Kafka AdminClient Definition
type KafkaAdminClient struct {
	logger       *zap.Logger
	namespace    string
	kafkaSecret  string
	clientId     string
	clusterAdmin sarama.ClusterAdmin
}

// Create A New Kafka AdminClient Based On The Kafka Secret In The Specified K8S Namespace
func NewKafkaAdminClient(ctx context.Context, clientId string, namespace string) (AdminClientInterface, error) {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx).Desugar()

	// Get The K8S Client From The Context
	k8sClient := kubeclient.Get(ctx)

	// Get A List Of The Kafka Secrets
	kafkaSecrets, err := adminutil.GetKafkaSecrets(k8sClient, namespace)
	if err != nil {
		logger.Error("Failed To Get Kafka Authentication Secrets", zap.Error(err))
		return nil, err
	}

	// Currently Only Support One Kafka Secret - Invalid AdminClient For All Other Cases!
	var kafkaSecret corev1.Secret
	if len(kafkaSecrets.Items) != 1 {
		logger.Warn(fmt.Sprintf("Expected 1 Kafka Secret But Found %d - Kafka AdminClient Will Not Be Functional!", len(kafkaSecrets.Items)))
		return &KafkaAdminClient{logger: logger, namespace: namespace}, nil
	} else {
		logger.Info("Found 1 Kafka Secret", zap.String("Secret", kafkaSecrets.Items[0].Name))
		kafkaSecret = kafkaSecrets.Items[0]
	}

	// Validate Secret Data
	if !validateKafkaSecret(logger, &kafkaSecret) {
		err = errors.New("invalid Kafka Secret found")
		return nil, err
	}

	// Extract The Relevant Data From The Kafka Secret
	brokers := strings.Split(string(kafkaSecret.Data[constants.KafkaSecretKeyBrokers]), ",")
	username := string(kafkaSecret.Data[constants.KafkaSecretKeyUsername])
	password := string(kafkaSecret.Data[constants.KafkaSecretKeyPassword])

	// Create The Sarama ClusterAdmin Configuration
	config := getConfig(clientId, username, password)

	// Create A New Sarama ClusterAdmin
	clusterAdmin, err := NewClusterAdminWrapper(brokers, config)
	if err != nil {
		logger.Error("Failed To Create New ClusterAdmin", zap.Any("Config", config), zap.Error(err))
		return nil, err
	}

	// Create The KafkaAdminClient
	kafkaAdminClient := &KafkaAdminClient{
		logger:       logger,
		namespace:    namespace,
		kafkaSecret:  kafkaSecret.Name,
		clientId:     clientId,
		clusterAdmin: clusterAdmin,
	}

	// Return The KafkaAdminClient - Success
	return kafkaAdminClient, nil
}

// Sarama NewClusterAdmin() Wrapper Function Variable To Facilitate Unit Testing
var NewClusterAdminWrapper = func(brokers []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
	return sarama.NewClusterAdmin(brokers, config)
}

// Sarama Pass-Through Function For Creating Topics
func (k KafkaAdminClient) CreateTopic(_ context.Context, topicName string, topicDetail *sarama.TopicDetail) *sarama.TopicError {
	if k.clusterAdmin == nil {
		k.logger.Error("Unable To Create Topic Due To Invalid ClusterAdmin - Check Kafka Authorization Secret")
		return adminutil.NewUnknownTopicError("unable to create topic due to invalid ClusterAdmin - check Kafka authorization secrets")
	} else {
		err := k.clusterAdmin.CreateTopic(topicName, topicDetail, false)
		return adminutil.PromoteErrorToTopicError(err)
	}
}

// Sarama Pass-Through Function For Deleting Topics
func (k KafkaAdminClient) DeleteTopic(_ context.Context, topicName string) *sarama.TopicError {
	if k.clusterAdmin == nil {
		k.logger.Error("Unable To Delete Topic Due To Invalid ClusterAdmin - Check Kafka Authorization Secret")
		return adminutil.NewUnknownTopicError("unable to delete topic due to invalid ClusterAdmin - check Kafka authorization secrets")
	} else {
		err := k.clusterAdmin.DeleteTopic(topicName)
		return adminutil.PromoteErrorToTopicError(err)
	}
}

// Sarama Pass-Through Function For Closing ClusterAdmin
func (k KafkaAdminClient) Close() error {
	if k.clusterAdmin == nil {
		k.logger.Error("Unable To Close Invalid ClusterAdmin - Check Kafka Authorization Secret")
		return fmt.Errorf("unable to close invalid ClusterAdmin - check Kafka authorization secrets")
	} else {
		return k.clusterAdmin.Close()
	}
}

// Get The K8S Secret With Kafka Credentials For The Specified Topic Name
func (k KafkaAdminClient) GetKafkaSecretName(_ string) string {
	return k.kafkaSecret
}

// Get The Default Sarama ClusterAdmin Config
func getConfig(clientId string, username string, password string) *sarama.Config {

	// Create A New Base Sarama Config
	config := util.NewSaramaConfig(clientId, username, password)

	// Increase Default Admin Timeout Of 3 Seconds For Topic Creation/Deletion
	config.Admin.Timeout = constants.ConfigAdminTimeout

	// Return The Sarama Config
	return config
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
