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
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	admintesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/testing"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
)

// Test The CreateAdminClient() Functionality
func TestCreateAdminClient(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	brokers := []string{"TestBroker"}
	config := sarama.NewConfig()
	adminClientType := types.Kafka
	mockAdminClient := admintesting.NewMockAdminClient()

	// Stub NewAdminClientFn() & Restore After Test
	admintesting.StubNewAdminClientFn(admintesting.ValidatingNewAdminClientFn(t, ctx, brokers, config, adminClientType, mockAdminClient))
	defer admintesting.RestoreNewAdminClientFn()

	// Perform The Test
	adminClient, err := CreateAdminClient(ctx, brokers, config, adminClientType)

	// Verify Results
	assert.Equal(t, mockAdminClient, adminClient)
	assert.Nil(t, err)
}
