package eventhubcache

import (
	"context"
	eventhub "github.com/Azure/azure-event-hubs-go"
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
