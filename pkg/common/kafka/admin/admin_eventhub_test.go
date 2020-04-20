package admin

import (
	"context"
	"fmt"
	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kyma-incubator/knative-kafka/pkg/common/kafka/admin/eventhubcache"
	"github.com/kyma-incubator/knative-kafka/pkg/common/kafka/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	logtesting "knative.dev/pkg/logging/testing"
	"strconv"
	"testing"
)

// Test The NewEventHubAdminClient() Constructor - Success Path
func TestNewEventHubAdminClientSuccess(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	namespace := "TestNamespace"

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("Update", context.TODO()).Return(nil)

	// Replace The NewCache Wrapper To Provide Mock Implementation & Defer Reset
	newCacheWrapperPlaceholder := NewCacheWrapper
	NewCacheWrapper = func(ctxArg context.Context, namespaceArg string) eventhubcache.CacheInterface {
		assert.Equal(t, ctx, ctxArg)
		assert.Equal(t, namespace, namespaceArg)
		return mockCache
	}
	defer func() { NewCacheWrapper = newCacheWrapperPlaceholder }()

	// Perform The Test
	adminClient, err := NewEventHubAdminClient(ctx, namespace)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
	mockCache.AssertExpectations(t)
}

// Test The NewEventHubAdminClient() Constructor - Error Path
func TestNewEventHubAdminClientError(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	namespace := "TestNamespace"

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("Update", context.TODO()).Return(fmt.Errorf("expected test error"))

	// Replace The NewCache Wrapper To Provide Mock Implementation & Defer Reset
	newCacheWrapperPlaceholder := NewCacheWrapper
	NewCacheWrapper = func(ctxArg context.Context, namespaceArg string) eventhubcache.CacheInterface {
		assert.Equal(t, ctx, ctxArg)
		assert.Equal(t, namespace, namespaceArg)
		return mockCache
	}
	defer func() { NewCacheWrapper = newCacheWrapperPlaceholder }()

	// Perform The Test
	adminClient, err := NewEventHubAdminClient(ctx, namespace)

	// Verify The Results
	assert.NotNil(t, err)
	assert.Nil(t, adminClient)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopics() Functionality - Success Path
func TestEventHubAdminClientCreateTopicsSuccess(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := 4
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)

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

	// Create The Kafka TopicSpecification For The Topic/EventHub To Be Created
	topicSpecifications := []kafka.TopicSpecification{{
		Topic:         topicName,
		NumPartitions: topicNumPartitions,
		Config:        map[string]string{constants.TopicSpecificationConfigRetentionMs: strconv.FormatInt(topicRetentionMillis, 10)},
	}}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	kafkaTopicResults, err := adminClient.CreateTopics(ctx, topicSpecifications)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrNoError, kafkaTopicResults[0].Error.Code())
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopics() Functionality - Invalid Config Path
func TestEventHubAdminClientCreateTopicsInvalidConfig(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := 4

	// Create The Kafka TopicSpecification For The Topic/EventHub To Be Created
	topicSpecifications := []kafka.TopicSpecification{{
		Topic:         topicName,
		NumPartitions: topicNumPartitions,
		Config:        map[string]string{constants.TopicSpecificationConfigRetentionMs: "InvalidConfig"},
	}}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger}

	// Perform The Test
	kafkaTopicResults, err := adminClient.CreateTopics(ctx, topicSpecifications)

	// Verify The Results
	assert.NotNil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrInvalidConfig, kafkaTopicResults[0].Error.Code())
}

// Test The EventHub AdminClient CreateTopics() Functionality - No Namespace Path
func TestEventHubAdminClientCreateTopicsNoNamespace(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := 4
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(nil)
	mockCache.On("GetLeastPopulatedNamespace").Return(nil)

	// Create The Kafka TopicSpecification For The Topic/EventHub To Be Created
	topicSpecifications := []kafka.TopicSpecification{{
		Topic:         topicName,
		NumPartitions: topicNumPartitions,
		Config:        map[string]string{constants.TopicSpecificationConfigRetentionMs: strconv.FormatInt(topicRetentionMillis, 10)},
	}}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	kafkaTopicResults, err := adminClient.CreateTopics(ctx, topicSpecifications)

	// Verify The Results
	assert.NotNil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrInvalidConfig, kafkaTopicResults[0].Error.Code())
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopics() Functionality - No HubManager Path
func TestEventHubAdminClientCreateTopicsNoHubManager(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := 4
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)

	// Create A Namespace With The Mock HubManager
	namespace := &eventhubcache.Namespace{}

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(nil)
	mockCache.On("GetLeastPopulatedNamespace").Return(namespace)

	// Create The Kafka TopicSpecification For The Topic/EventHub To Be Created
	topicSpecifications := []kafka.TopicSpecification{{
		Topic:         topicName,
		NumPartitions: topicNumPartitions,
		Config:        map[string]string{constants.TopicSpecificationConfigRetentionMs: strconv.FormatInt(topicRetentionMillis, 10)},
	}}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	kafkaTopicResults, err := adminClient.CreateTopics(ctx, topicSpecifications)

	// Verify The Results
	assert.NotNil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrInvalidConfig, kafkaTopicResults[0].Error.Code())
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopics() Functionality - Already Exists Path
func TestEventHubAdminClientCreateTopicsAlreadyExists(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := 4
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)

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

	// Create The Kafka TopicSpecification For The Topic/EventHub To Be Created
	topicSpecifications := []kafka.TopicSpecification{{
		Topic:         topicName,
		NumPartitions: topicNumPartitions,
		Config:        map[string]string{constants.TopicSpecificationConfigRetentionMs: strconv.FormatInt(topicRetentionMillis, 10)},
	}}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	kafkaTopicResults, err := adminClient.CreateTopics(ctx, topicSpecifications)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrTopicAlreadyExists, kafkaTopicResults[0].Error.Code())
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopics() Functionality - Capacity Limit Path
func TestEventHubAdminClientCreateTopicsCapacityLimit(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := 4
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)

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

	// Create The Kafka TopicSpecification For The Topic/EventHub To Be Created
	topicSpecifications := []kafka.TopicSpecification{{
		Topic:         topicName,
		NumPartitions: topicNumPartitions,
		Config:        map[string]string{constants.TopicSpecificationConfigRetentionMs: strconv.FormatInt(topicRetentionMillis, 10)},
	}}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	kafkaTopicResults, err := adminClient.CreateTopics(ctx, topicSpecifications)

	// Verify The Results
	assert.NotNil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrTopicException, kafkaTopicResults[0].Error.Code())
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient CreateTopics() Functionality - Error Path
func TestEventHubAdminClientCreateTopicsError(t *testing.T) {

	// Test Data
	ctx := context.TODO()
	topicName := "TestTopicName"
	topicNumPartitions := 4
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)

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

	// Create The Kafka TopicSpecification For The Topic/EventHub To Be Created
	topicSpecifications := []kafka.TopicSpecification{{
		Topic:         topicName,
		NumPartitions: topicNumPartitions,
		Config:        map[string]string{constants.TopicSpecificationConfigRetentionMs: strconv.FormatInt(topicRetentionMillis, 10)},
	}}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	kafkaTopicResults, err := adminClient.CreateTopics(ctx, topicSpecifications)

	// Verify The Results
	assert.NotNil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrUnknown, kafkaTopicResults[0].Error.Code())
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient DeleteTopics() Functionality - Success Path
func TestEventHubAdminClientDeleteTopicsSuccess(t *testing.T) {

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
	kafkaTopicResults, err := adminClient.DeleteTopics(ctx, []string{topicName})

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrNoError, kafkaTopicResults[0].Error.Code())
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient DeleteTopics() Functionality - No Namespace Path
func TestEventHubAdminClientDeleteTopicsNoNamespace(t *testing.T) {

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
	kafkaTopicResults, err := adminClient.DeleteTopics(ctx, []string{topicName})

	// Verify The Results
	assert.NotNil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrInvalidConfig, kafkaTopicResults[0].Error.Code())
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient DeleteTopics() Functionality - No HubManager Path
func TestEventHubAdminClientDeleteTopicsNoHubManager(t *testing.T) {

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
	kafkaTopicResults, err := adminClient.DeleteTopics(ctx, []string{topicName})

	// Verify The Results
	assert.NotNil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrInvalidConfig, kafkaTopicResults[0].Error.Code())
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient DeleteTopics() Functionality - Delete Error Path
func TestEventHubAdminClientDeleteTopicsError(t *testing.T) {

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
	kafkaTopicResults, err := adminClient.DeleteTopics(ctx, []string{topicName})

	// Verify The Results
	assert.NotNil(t, err)
	assert.NotNil(t, kafkaTopicResults)
	assert.Len(t, kafkaTopicResults, 1)
	assert.Equal(t, topicName, kafkaTopicResults[0].Topic)
	assert.Equal(t, kafka.ErrFail, kafkaTopicResults[0].Error.Code())
	mockHubManager.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient GetKafkaSecretName() Functionality
func TestEventHubAdminClientGetKafkaSecretName(t *testing.T) {

	// Test Data
	topicName := "TestTopicName"
	secretName := "TestSecretName"
	namespace := &eventhubcache.Namespace{Secret: secretName}

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A Mock EventHub Cache
	mockCache := &MockCache{}
	mockCache.On("GetNamespace", topicName).Return(namespace)

	// Create A New EventHub AdminClient With Mock Cache To Test
	adminClient := &EventHubAdminClient{logger: logger, cache: mockCache}

	// Perform The Test
	actualSecretName := adminClient.GetKafkaSecretName(topicName)

	// Verify The Results
	assert.Equal(t, secretName, actualSecretName)
	mockCache.AssertExpectations(t)
}

// Test The EventHub AdminClient Close() Functionality
func TestEventHubAdminClientClose(t *testing.T) {

	// Test Data
	namespace := "TestNamespace"

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New EventHub AdminClient To Test
	adminClient := &EventHubAdminClient{logger: logger, namespace: namespace}

	// Perform The Test
	adminClient.Close()

	// Nothing To Verify (No-Op)
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

func (m MockCache) Update(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m MockCache) AddEventHub(ctx context.Context, eventhub string, namespace *eventhubcache.Namespace) {
	m.Called(ctx, eventhub, namespace)
}

func (m MockCache) RemoveEventHub(ctx context.Context, eventhub string) {
	m.Called(ctx, eventhub)
}

func (m MockCache) GetNamespace(eventhub string) *eventhubcache.Namespace {
	args := m.Called(eventhub)
	response := args.Get(0)
	if response == nil {
		return nil
	} else {
		return response.(*eventhubcache.Namespace)
	}
}

func (m MockCache) GetLeastPopulatedNamespace() *eventhubcache.Namespace {
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

func (m MockHubManager) Delete(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m MockHubManager) List(ctx context.Context) ([]*eventhub.HubEntity, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*eventhub.HubEntity), args.Error(1)
}

func (m MockHubManager) Put(ctx context.Context, name string, opts ...eventhub.HubManagementOption) (*eventhub.HubEntity, error) {
	args := m.Called(ctx, name, opts)
	response := args.Get(0)
	if response == nil {
		return nil, args.Error(1)
	} else {
		return response.(*eventhub.HubEntity), args.Error(1)
	}
}
