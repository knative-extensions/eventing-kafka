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

package eventhub

import (
	"context"
	"fmt"
	"testing"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

var _ HubManagerInterface = &MockHubManager{}

type MockHubManager struct {
	mock.Mock
}

func (m *MockHubManager) Delete(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *MockHubManager) List(ctx context.Context) ([]*eventhub.HubEntity, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*eventhub.HubEntity), args.Error(1)
}

func (m *MockHubManager) Put(ctx context.Context, name string, opts ...eventhub.HubManagementOption) (*eventhub.HubEntity, error) {
	args := m.Called(ctx, name, opts)
	response := args.Get(0)
	if response == nil {
		return nil, args.Error(1)
	} else {
		return response.(*eventhub.HubEntity), args.Error(1)
	}
}

type MockHubManagerOption = func(mockHubManager *MockHubManager)

func NewMockHubManager(options ...MockHubManagerOption) *MockHubManager {
	mockHubManager := &MockHubManager{}
	for _, options := range options {
		options(mockHubManager)
	}
	return mockHubManager
}

func WithMockedPut(ctx context.Context, topic string, returnErr bool, errCode int) func(mockHubManager *MockHubManager) {
	return func(mockHubManager *MockHubManager) {
		if returnErr {
			errString := fmt.Errorf("error code: %d, etc", errCode)
			mockHubManager.On("Put", ctx, topic, mock.Anything).Return(nil, errString)
		} else {
			mockHubManager.On("Put", ctx, topic, mock.Anything).Return(nil, nil)
		}
	}
}

func WithMockedDelete(ctx context.Context, topic string, returnErr bool, errCode int) func(mockHubManager *MockHubManager) {
	return func(mockHubManager *MockHubManager) {
		if returnErr {
			errString := fmt.Errorf("error code: %d, etc", errCode)
			mockHubManager.On("Delete", ctx, topic).Return(errString)
		} else {
			mockHubManager.On("Delete", ctx, topic).Return(nil)
		}
	}
}
