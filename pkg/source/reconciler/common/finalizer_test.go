/*
Copyright 2021 The Knative Authors

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

package common

import (
	"testing"

	"github.com/Shopify/sarama"

	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	logtesting "knative.dev/pkg/logging/testing"

	bindingsv1beta1 "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
)

func TestFinalizer(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()

	group := "my-group"
	deleteGroupsResponse := sarama.NewMockDeleteGroupsRequest(t).SetDeletedGroups([]string{group})

	metadataResponse := sarama.NewMockMetadataResponse(t).
		SetController(broker.BrokerID()).
		SetBroker(broker.Addr(), broker.BrokerID())

	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"DeleteGroupsRequest": deleteGroupsResponse,

		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, group, broker),

		"MetadataRequest": metadataResponse,
	})

	ctx := logtesting.TestContextWithLogger(t)
	ctx, _ = fakekubeclient.With(ctx)

	bf := &BoundedFinalizer{FinalizerAttempts: map[string]int{}}
	src := &v1beta1.KafkaSource{
		Spec: v1beta1.KafkaSourceSpec{
			KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
				BootstrapServers: []string{broker.Addr()},
			},
			Topics:        nil,
			ConsumerGroup: group,
		},
	}
	err := FinalizeKind(ctx, fakekubeclient.Get(ctx), bf, src)
	if err != nil {
		t.Error("unexpected error: ", err)
	}

}
