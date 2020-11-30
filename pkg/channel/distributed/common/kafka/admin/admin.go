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

package admin

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"knative.dev/pkg/system"
)

// Sarama ClusterAdmin Wrapping Interface To Facilitate Other Implementations (e.g. Azure EventHubs)
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
	Custom
	Unknown
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
func CreateAdminClient(ctx context.Context, saramaConfig *sarama.Config, clientId string, adminClientType AdminClientType) (AdminClientInterface, error) {
	switch adminClientType {
	case Kafka:
		return NewKafkaAdminClientWrapper(ctx, saramaConfig, clientId, system.Namespace())
	case EventHub:
		return NewEventHubAdminClientWrapper(ctx, system.Namespace())
	case Custom:
		return NewCustomAdminClientWrapper(ctx, system.Namespace())
	case Unknown:
		return nil, errors.New("received unknown AdminClientType") // Should Never Happen But...
	default:
		return nil, fmt.Errorf("received unsupported AdminClientType of %d", adminClientType)
	}
}

// New Kafka AdminClient Wrapper To Facilitate Unit Testing
var NewKafkaAdminClientWrapper = func(ctx context.Context, saramaConfig *sarama.Config, clientId string, namespace string) (AdminClientInterface, error) {
	return NewKafkaAdminClient(ctx, saramaConfig, clientId, namespace)
}

// New EventHub AdminClient Wrapper To Facilitate Unit Testing
var NewEventHubAdminClientWrapper = func(ctx context.Context, namespace string) (AdminClientInterface, error) {
	return NewEventHubAdminClient(ctx, namespace)
}

// New Custom AdminClient Wrapper To Facilitate Unit Testing
var NewCustomAdminClientWrapper = func(ctx context.Context, namespace string) (AdminClientInterface, error) {
	return NewCustomAdminClient(ctx, namespace)
}
