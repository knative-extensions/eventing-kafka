package admin

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
)

// Confluent Client Doesn't Code To Interfaces Or Provide Mocks So We're Wrapping Our Usage Of The AdminClient For Testing
// Also Introduced Additional Functionality To Get The Kafka Secret For A Topic
type AdminClientInterface interface {
	CreateTopic(context.Context, string, *sarama.TopicDetail) *sarama.TopicError
	DeleteTopic(context.Context, string) *sarama.TopicError
	Close() error
	GetKafkaSecretName(topicName string) string
}

// AdminClient Type Enumeration
type AdminClientType int

const (
	Kafka AdminClientType = iota
	EventHub
)

//
// Create A New Kafka AdminClient Of Specified Type - Using Credentials From Kafka Secret(s) In Specified K8S Namespace
//
// The K8S Namespace parameter indicates the Kubernetes Namespace in which the Kafka Credentials secret(s)
// will be found.  The secret(s) must contain the constants.KafkaSecretLabel label indicating it is a "Kafka Secret".
//
// For the normal Kafka use case (Confluent, etc.) there should be only one Secret with the following content...
//
//      data:
//		  brokers: SASL_SSL://<host>.<region>.aws.confluent.cloud:9092
//        username: <username>
//        password: <password>
//
// For the Azure EventHub use case there will be multiple Secrets (one per Azure Namespace) each with the following content...
//
//      data:
//        brokers: <azure-namespace>.servicebus.windows.net:9093
//        username: $ConnectionString
//        password: Endpoint=sb://<azure-namespace>.servicebus.windows.net/;SharedAccessKeyName=<shared-access-key-name>;SharedAccessKey=<shared-access-key-value>
//		  namespace: <azure-namespace>
//
// * If no authorization is required (local dev instance) then specify username and password as the empty string ""
//
func CreateAdminClient(ctx context.Context, adminClientType AdminClientType) (AdminClientInterface, error) {
	switch adminClientType {
	case Kafka:
		return NewKafkaAdminClientWrapper(ctx, constants.KnativeEventingNamespace)
	case EventHub:
		return NewEventHubAdminClientWrapper(ctx, constants.KnativeEventingNamespace)
	default:
		return nil, errors.New(fmt.Sprintf("received unsupported AdminClientType of %d", adminClientType))
	}
}

// New Kafka AdminClient Wrapper To Facilitate Unit Testing
var NewKafkaAdminClientWrapper = func(ctx context.Context, namespace string) (AdminClientInterface, error) {
	return NewKafkaAdminClient(ctx, namespace)
}

// New EventHub AdminClient Wrapper To Facilitate Unit Testing
var NewEventHubAdminClientWrapper = func(ctx context.Context, namespace string) (AdminClientInterface, error) {
	return NewEventHubAdminClient(ctx, namespace)
}
