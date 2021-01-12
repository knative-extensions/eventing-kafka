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

package wrapper

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/custom"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/eventhub"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/kafka"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
)

// Define Function Types For Wrapper Variables (Typesafe Stubbing For Tests)
type NewAdminClientFnType = func(ctx context.Context, brokers []string, config *sarama.Config, adminClientType types.AdminClientType) (types.AdminClientInterface, error)

// Function Variables To Facilitate Mocking Of Sarama Functionality In Unit Tests
var NewAdminClientFn = ProductionNewAdminClientWrapper

// Create A New Kafka AdminClient Of Specified Type - Based On Specified Sarama Config
func ProductionNewAdminClientWrapper(ctx context.Context, brokers []string, config *sarama.Config, adminClientType types.AdminClientType) (types.AdminClientInterface, error) {
	switch adminClientType {
	case types.Kafka:
		return kafka.NewAdminClient(ctx, brokers, config)
	case types.EventHub:
		return eventhub.NewAdminClient(ctx, config) // Config Must Contain EventHub Namespace ConnectionString In Net.SASL.Password Field !
	case types.Custom:
		return custom.NewAdminClient(ctx)
	case types.Unknown:
		return nil, fmt.Errorf("received unknown AdminClientType") // Should Never Happen But...
	default:
		return nil, fmt.Errorf("received unsupported AdminClientType of %d", adminClientType)
	}
}
