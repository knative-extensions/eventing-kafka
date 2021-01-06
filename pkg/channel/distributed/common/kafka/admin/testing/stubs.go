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

package testing

import (
	"context"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/wrapper"
)

//
// Test Utilities For Stubbing The NewAdminClientFn()
//

// Replace The NewAdminClientFn With Specified Mock / Test Value
func StubNewAdminClientFn(stubNewAdminClientFn wrapper.NewAdminClientFnType) {
	wrapper.NewAdminClientFn = stubNewAdminClientFn
}

// Restore The NewAdminClientFn To Official Production Value
func RestoreNewAdminClientFn() {
	wrapper.NewAdminClientFn = wrapper.ProductionNewAdminClientWrapper
}

// Non-Validating NewAdminClient Function
func NonValidatingNewAdminClientFn(mockAdminClient types.AdminClientInterface) wrapper.NewAdminClientFnType {
	return func(ctx context.Context, brokers []string, saramaConfig *sarama.Config, adminClientType types.AdminClientType) (types.AdminClientInterface, error) {
		return mockAdminClient, nil
	}
}

// Validating NewAdminClient Function
func ValidatingNewAdminClientFn(t *testing.T,
	expectedCtx context.Context,
	expectedBrokers []string,
	expectedConfig *sarama.Config,
	expectedAdminClientType types.AdminClientType,
	mockAdminClient types.AdminClientInterface) wrapper.NewAdminClientFnType {
	return func(ctx context.Context, brokers []string, config *sarama.Config, adminClientType types.AdminClientType) (types.AdminClientInterface, error) {
		assert.Equal(t, expectedCtx, ctx)
		assert.Equal(t, expectedBrokers, brokers)
		assert.Equal(t, expectedConfig, config)
		assert.Equal(t, expectedAdminClientType, adminClientType)
		return mockAdminClient, nil
	}
}

// Error NewAdminClient Function
func ErrorNewAdminClientFn(err error) wrapper.NewAdminClientFnType {
	return func(ctx context.Context, brokers []string, saramaConfig *sarama.Config, adminClientType types.AdminClientType) (types.AdminClientInterface, error) {
		return nil, err
	}
}
