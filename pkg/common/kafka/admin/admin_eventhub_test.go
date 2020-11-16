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
	"fmt"
	"strconv"
	"testing"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"knative.dev/eventing-kafka/pkg/common/kafka/admin/eventhubcache"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The NewEventHubAdminClient() Constructor - Success Path
func TestNewEventHubAdminClientSuccess(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	connectionStrings := []string{"TestConnectionString"}

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("Update", context.TODO()).Return(nil)

	// Replace The NewCache Wrapper To Provide Mock Implementation & Defer Reset
	newCacheWrapperPlaceholder := NewCacheWrapper
	NewCacheWrapper = func(ctxArg context.Context, connectionStringsArg ...string) eventhubcache.CacheInterface {
		assert.Equal(t, ctx, ctxArg)
		assert.Equal(t, connectionStrings, connectionStringsArg)
		return mockCache
	}
	defer func() { NewCacheWrapper = newCacheWrapperPlaceholder }()

	// Perform The Test
	adminClient, err := NewEventHubAdminClient(ctx)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
	mockCache.AssertExpectations(t)
}

// Test The NewEventHubAdminClient() Constructor - Error Path
func TestNewEventHubAdminClientError(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	connectionStrings := []string{"TestConnectionString"}

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("Update", context.TODO()).Return(fmt.Errorf("expected test error"))

	// Replace The NewCache Wrapper To Provide Mock Implementation & Defer Reset
	newCacheWrapperPlaceholder := NewCacheWrapper
	NewCacheWrapper = func(ctxArg context.Context, connectionStringsArg ...string) eventhubcache.CacheInterface {
		assert.Equal(t, ctx, ctxArg)
		assert.Equal(t, connectionStrings, connectionStringsArg)
		return mockCache
	}
	defer func() { NewCacheWrapper = newCacheWrapperPlaceholder }()

	// Perform The Test
	adminClient, err := NewEventHubAdminClient(ctx)

	// Verify The Results
	assert.NotNil(t, err)
	assert.Nil(t, adminClient)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopic() Functionality - Success Path
func TestEventHubAdminClientCreateTopicSuccess(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	topicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)

	// Create A Mock HubManager
	mockHubManager := &MockHubManager{}
	mockHubManager.On("Put", ctx, topicName, mock.Anything).Return(nil, nil)

	// Create A Namespace With The Mock HubManager
	namespace := &eventhubcache.Namespace{HubManager: mockHubManager}

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(nil)
	mockCache.On("GetLeastPopulatedNamespace").Return(namespace)
	mockCache.On("AddEventHub", ctx, topicName, namespace).Return()

	// Create The Sarama TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions: topicNumPartitions,
		ConfigEntries: map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrNoError, resultTopicError.Err)
	assert.Equal(t, "successfully created topic", *resultTopicError.ErrMsg)
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopic() Functionality - Invalid Config Entry
func TestEventHubAdminClientCreateTopicInvalidConfig(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicRetentionMillisString := "Invalid RetentionMillis"

	// Create The Sarama TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions: topicNumPartitions,
		ConfigEntries: map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient Without Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger}

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrInvalidConfig, resultTopicError.Err)
	assert.Equal(t, "failed to parse retention millis from TopicDetail", *resultTopicError.ErrMsg)
}

// Test The EventHub AdminClient CreateTopic() Functionality - No Namespace Path
func TestEventHubAdminClientCreateTopicNoNamespace(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	topicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(nil)
	mockCache.On("GetLeastPopulatedNamespace").Return(nil)

	// Create The Sarama TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions: topicNumPartitions,
		ConfigEntries: map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrInvalidConfig, resultTopicError.Err)
	assert.Equal(t, "no azure eventhub namespaces in cache - unable to create EventHub 'TestTopicName'", *resultTopicError.ErrMsg)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopic() Functionality - No HubManager Path
func TestEventHubAdminClientCreateTopicNoHubManager(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	topicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)

	// Create A Namespace With The Mock HubManager
	namespace := &eventhubcache.Namespace{}

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(nil)
	mockCache.On("GetLeastPopulatedNamespace").Return(namespace)

	// Create The Sarama TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions: topicNumPartitions,
		ConfigEntries: map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrInvalidConfig, resultTopicError.Err)
	assert.Equal(t, "azure namespace has invalid HubManager - unable to create EventHub 'TestTopicName'", *resultTopicError.ErrMsg)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopic() Functionality - Already Exists Path
func TestEventHubAdminClientCreateTopicAlreadyExists(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	topicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)

	// Create A Mock HubManager
	mockHubManager := &MockHubManager{}
	mockHubManager.On("Put", ctx, topicName, mock.Anything).
		Return(nil, fmt.Errorf("error code: %d, expected test error", constants.EventHubErrorCodeConflict))

	// Create A Namespace With The Mock HubManager
	namespace := &eventhubcache.Namespace{HubManager: mockHubManager}

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(nil)
	mockCache.On("GetLeastPopulatedNamespace").Return(namespace)

	// Create The Sarama TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions: topicNumPartitions,
		ConfigEntries: map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrTopicAlreadyExists, resultTopicError.Err)
	assert.Equal(t, "mapped from EventHubErrorCodeConflict", *resultTopicError.ErrMsg)
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopic() Functionality - Capacity Limit Path
func TestEventHubAdminClientCreateTopicCapacityLimit(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	topicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)

	// Create A Mock HubManager
	mockHubManager := &MockHubManager{}
	mockHubManager.On("Put", ctx, topicName, mock.Anything).
		Return(nil, fmt.Errorf("error code: %d, expected test error", constants.EventHubErrorCodeCapacityLimit))

	// Create A Namespace With The Mock HubManager
	namespace := &eventhubcache.Namespace{HubManager: mockHubManager}

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(nil)
	mockCache.On("GetLeastPopulatedNamespace").Return(namespace)

	// Create The Sarama TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions: topicNumPartitions,
		ConfigEntries: map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrInvalidTxnState, resultTopicError.Err)
	assert.Equal(t, "mapped from EventHubErrorCodeCapacityLimit", *resultTopicError.ErrMsg)
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopic() Functionality - Error Path
func TestEventHubAdminClientCreateTopicError(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	topicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)

	// Create A Mock HubManager
	mockHubManager := &MockHubManager{}
	mockHubManager.On("Put", ctx, topicName, mock.Anything).
		Return(nil, fmt.Errorf("error code: %d, expected test error", constants.EventHubErrorCodeUnknown))

	// Create A Namespace With The Mock HubManager
	namespace := &eventhubcache.Namespace{HubManager: mockHubManager}

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(nil)
	mockCache.On("GetLeastPopulatedNamespace").Return(namespace)

	// Create The Sarama TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions: topicNumPartitions,
		ConfigEntries: map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrUnknown, resultTopicError.Err)
	assert.Equal(t, "mapped from EventHubErrorCodeUnknown", *resultTopicError.ErrMsg)
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient DeleteTopic() Functionality - Success Path
func TestEventHubAdminClientDeleteTopicSuccess(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"

	// Create A Mock HubManager
	mockHubManager := &MockHubManager{}
	mockHubManager.On("Delete", ctx, topicName).Return(nil)

	// Create A Namespace With The Mock HubManager
	namespace := &eventhubcache.Namespace{HubManager: mockHubManager}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(namespace)
	mockCache.On("RemoveEventHub", ctx, topicName).Return()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	resultTopicError := adminClient.DeleteTopic(ctx, topicName)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrNoError, resultTopicError.Err)
	assert.Equal(t, "successfully deleted topic", *resultTopicError.ErrMsg)
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient DeleteTopic() Functionality - No Namespace Path
func TestEventHubAdminClientDeleteTopicNoNamespace(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(nil)

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	resultTopicError := adminClient.DeleteTopic(ctx, topicName)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrInvalidConfig, resultTopicError.Err)
	assert.Equal(t, "no azure namespace found for EventHub - unable to delete EventHub 'TestTopicName'", *resultTopicError.ErrMsg)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient DeleteTopic() Functionality - No HubManager Path
func TestEventHubAdminClientDeleteTopicNoHubManager(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"

	// Create A Namespace With The Mock HubManager
	namespace := &eventhubcache.Namespace{}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(namespace)

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	resultTopicError := adminClient.DeleteTopic(ctx, topicName)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrInvalidConfig, resultTopicError.Err)
	assert.Equal(t, "azure namespace has invalid HubManager - unable to delete EventHub 'TestTopicName'", *resultTopicError.ErrMsg)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient DeleteTopic() Functionality - Delete Error Path
func TestEventHubAdminClientDeleteTopicError(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"

	// Create A Mock HubManager
	mockHubManager := &MockHubManager{}
	mockHubManager.On("Delete", ctx, topicName).Return(fmt.Errorf("expected test error"))

	// Create A Namespace With The Mock HubManager
	namespace := &eventhubcache.Namespace{HubManager: mockHubManager}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(namespace)

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	resultTopicError := adminClient.DeleteTopic(ctx, topicName)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrUnknown, resultTopicError.Err)
	assert.Equal(t, "expected test error", *resultTopicError.ErrMsg)
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient Close() Functionality
func TestEventHubAdminClientClose(t *testing.T) {

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient To Test
	adminClient := &EventHubAdminClient{logger: logger}

	// Perform The Test
	err := adminClient.Close()

	// Nothing To Verify (No-Op)
	assert.Nil(t, err)
}

//
// Mock EventHub Cache
//

// Ensure The Mock EventHub Cache Implements The Interface
var _ eventhubcache.CacheInterface = &MockCache{}

// The Mock EventHub Cache
type MockCache struct {
	mock.Mock
}

func (m *MockCache) Update(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockCache) AddEventHub(ctx context.Context, eventhub string, namespace *eventhubcache.Namespace) {
	m.Called(ctx, eventhub, namespace)
}

func (m *MockCache) RemoveEventHub(ctx context.Context, eventhub string) {
	m.Called(ctx, eventhub)
}

func (m *MockCache) GetNamespace(eventhub string) *eventhubcache.Namespace {
	args := m.Called(eventhub)
	response := args.Get(0)
	if response == nil {
		return nil
	} else {
		return response.(*eventhubcache.Namespace)
	}
}

func (m *MockCache) GetLeastPopulatedNamespace() *eventhubcache.Namespace {
	args := m.Called()
	response := args.Get(0)
	if response == nil {
		return nil
	} else {
		return response.(*eventhubcache.Namespace)
	}
}

//
// Mock EventHub HubManager
//

// Ensure The Mock HubManager Implements The Interface
var _ eventhubcache.HubManagerInterface = &MockHubManager{}

// The Mock EventHub HubManager
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
