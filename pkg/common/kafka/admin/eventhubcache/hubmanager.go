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

package eventhubcache

import (
	"context"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
)

// Azure EventHub Client Doesn't Code To Interfaces Or Provide Mocks So We're Wrapping Our Usage Of The HubManager For Testing
type HubManagerInterface interface {
	Delete(ctx context.Context, name string) error
	List(ctx context.Context) ([]*eventhub.HubEntity, error)
	Put(ctx context.Context, name string, opts ...eventhub.HubManagementOption) (*eventhub.HubEntity, error)
}

// Azure EventHub HubManager Function Reference Variable To Facilitate Mocking In Unit Tests
var NewHubManagerFromConnectionStringWrapper = func(connectionString string) (HubManagerInterface, error) {
	return eventhub.NewHubManagerFromConnectionString(connectionString)
}
