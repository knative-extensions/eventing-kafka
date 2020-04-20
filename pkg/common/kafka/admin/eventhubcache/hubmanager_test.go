package eventhubcache

import (
	"context"
	eventhub "github.com/Azure/azure-event-hubs-go"
)

// Verify The Mock HubManager Implements The HubManagerInterface
var _ HubManagerInterface = &MockHubManager{}

// Mock HubManager Implementation
type MockHubManager struct {
	PutHubEntity    *eventhub.HubEntity   // What To Return From Put() Requests
	ListHubEntities []*eventhub.HubEntity // What To Return From List() Requests
}

func (m MockHubManager) Delete(ctx context.Context, name string) error {
	return nil
}

func (m MockHubManager) List(ctx context.Context) ([]*eventhub.HubEntity, error) {
	return m.ListHubEntities, nil
}

func (m MockHubManager) Put(ctx context.Context, name string, opts ...eventhub.HubManagementOption) (*eventhub.HubEntity, error) {
	return m.PutHubEntity, nil
}
