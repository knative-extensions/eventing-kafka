package eventhubcache

import (
	"context"
	"testing"

	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/stretchr/testify/assert"
)

//
// Test The NewHubManagerFromConnectionStringWrapper() Constructor
//
// This semi-pointless test is here to pacify the OCD Knative coverage tools,
// which (as of this writing) only consider coverage from a file with the
// same name and "_test" suffix instead of all tests aggregated as the Go
// cmd line tooling does.
//
func TestFoo(t *testing.T) {
	hubManager, err := NewHubManagerFromConnectionStringWrapper("foo")
	assert.NotNil(t, err)
	assert.Nil(t, hubManager)
}

//
// Mock HubManager
//

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
