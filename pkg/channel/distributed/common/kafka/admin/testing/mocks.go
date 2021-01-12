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

	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
)

//
// Mock AdminClient
//

var _ types.AdminClientInterface = &MockAdminClient{}

type MockAdminClient struct {
}

func NewMockAdminClient() types.AdminClientInterface {
	return &MockAdminClient{}
}

func (c MockAdminClient) CreateTopic(context.Context, string, *sarama.TopicDetail) *sarama.TopicError {
	return nil
}

func (c MockAdminClient) DeleteTopic(context.Context, string) *sarama.TopicError {
	return nil
}

func (c MockAdminClient) Close() error {
	return nil
}
