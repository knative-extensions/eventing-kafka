package admin

import (
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing-kafka/pkg/common/kafka/admin/custom"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// Test The NewCustomAdminClient() Functionality - Success Path
func TestNewCustomAdminClientSuccess(t *testing.T) {

	// Test Data
	namespace := "TestNamespace"
	kafkaSecretName := "TestKafkaSecretName"
	kafkaSecretBrokers := "TestKafkaSecretBrokers"
	kafkaSecretUsername := "TestKafkaSecretUsername"
	kafkaSecretPassword := "TestKafkaSecretPassword"

	// Create Test Kafka Secret
	kafkaSecret := createKafkaSecret(kafkaSecretName, namespace, kafkaSecretBrokers, kafkaSecretUsername, kafkaSecretPassword)

	// Create A Context With Test Logger & K8S Client
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)
	ctx = context.WithValue(ctx, injectionclient.Key{}, fake.NewSimpleClientset(kafkaSecret))

	// Perform The Test
	adminClient, err := NewCustomAdminClient(ctx, namespace)

	// Verify The Result
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)
	customAdminClient, ok := adminClient.(*CustomAdminClient)
	assert.True(t, ok)
	assert.NotNil(t, customAdminClient)
	assert.NotEqual(t, logger, customAdminClient.logger)
	assert.NotEqual(t, namespace, customAdminClient.logger)
	assert.Equal(t, kafkaSecretName, customAdminClient.kafkaSecret)
	assert.NotNil(t, customAdminClient.httpClient)
}

// Test The NewCustomAdminClient() Constructor - No Kafka Secrets
func TestNewCustomAdminClientNoKafkaSecret(t *testing.T) {

	// Test Data
	namespace := "TestNamespace"

	// Create A Context With Test Logger & K8S Client
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t))
	ctx = context.WithValue(ctx, injectionclient.Key{}, fake.NewSimpleClientset())

	// Perform The Test
	adminClient, err := NewCustomAdminClient(ctx, namespace)

	// Verify The Result
	assert.Nil(t, err)
	assert.Nil(t, adminClient)
}

// Test The NewCustomAdminClient() Constructor - Invalid Kafka Secrets
func TestNewCustomAdminClientInvalidKafkaSecret(t *testing.T) {

	// Test Data
	namespace := "TestNamespace"

	// Create Test Kafka Secret With Invalid Data
	kafkaSecret := createKafkaSecret("Name", namespace, "", "", "")

	// Create A Context With Test Logger & K8S Client
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t))
	ctx = context.WithValue(ctx, injectionclient.Key{}, fake.NewSimpleClientset(kafkaSecret))

	// Perform The Test
	adminClient, err := NewCustomAdminClient(ctx, namespace)

	// Verify The Result
	assert.NotNil(t, err)
	assert.Nil(t, adminClient)
}

// Test The Custom AdminClient CreateTopic() Functionality
func TestCustomAdminClientCreateTopic(t *testing.T) {

	// Test Data
	namespace := "TestNamespace"
	topicName := "TestTopicName"
	topicNumPartitions := int32(4)
	topicReplicationFactor := int16(2)
	topicRetentionDays := int32(3)
	topicRetentionMillis := int64(topicRetentionDays * constants.MillisPerDay)
	topicRetentionMillisString := strconv.FormatInt(topicRetentionMillis, 10)

	// Create & Start The Test Sidecar HTTP Server (Success Response) & Defer Close
	mockSidecarServer := NewMockSidecarServer(t, http.StatusOK)
	mockSidecarServer.Start()
	defer mockSidecarServer.Close()

	// Create Test Kafka Secret With Dummy (But Valid) Data
	kafkaSecret := createKafkaSecret("Name", namespace, "Brokers", "Username", "Password")

	// Create A Context With Test Logger & K8S Client
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)
	ctx = context.WithValue(ctx, injectionclient.Key{}, fake.NewSimpleClientset(kafkaSecret))

	// Create The Sarama TopicDetail For The Topic/EventHub To Be Created
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     topicNumPartitions,
		ReplicationFactor: topicReplicationFactor,
		ConfigEntries:     map[string]*string{constants.TopicDetailConfigRetentionMs: &topicRetentionMillisString},
	}

	// Create A New Custom AdminClient
	adminClient, err := NewCustomAdminClient(ctx, namespace)
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)

	// Perform The Test
	resultTopicError := adminClient.CreateTopic(ctx, topicName, topicDetail)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrNoError, resultTopicError.Err)
	assert.Equal(t, "custom sidecar topic 'create' operation succeeded with status code '200' and body ''", *resultTopicError.ErrMsg)
	assert.Equal(t, 1, len(mockSidecarServer.requests))
	for request, body := range mockSidecarServer.requests {
		verifySidecarRequest(t, request, body, topicName, topicDetail)
	}
}

// Test The Custom AdminClient DeleteTopic() Functionality
func TestCustomAdminClientDeleteTopic(t *testing.T) {

	// Test Data
	namespace := "TestNamespace"
	topicName := "TestTopicName"

	// Create & Start The Test Sidecar HTTP Server (Success Response) & Defer Close
	mockSidecarServer := NewMockSidecarServer(t, http.StatusOK)
	mockSidecarServer.Start()
	defer mockSidecarServer.Close()

	// Create Test Kafka Secret With Dummy (But Valid) Data
	kafkaSecret := createKafkaSecret("Name", namespace, "Brokers", "Username", "Password")

	// Create A Context With Test Logger & K8S Client
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.TODO(), logger)
	ctx = context.WithValue(ctx, injectionclient.Key{}, fake.NewSimpleClientset(kafkaSecret))

	// Create A New Custom AdminClient
	adminClient, err := NewCustomAdminClient(ctx, namespace)
	assert.Nil(t, err)
	assert.NotNil(t, adminClient)

	// Perform The Test
	resultTopicError := adminClient.DeleteTopic(ctx, topicName)

	// Verify The Results
	assert.NotNil(t, resultTopicError)
	assert.Equal(t, sarama.ErrNoError, resultTopicError.Err)
	assert.Equal(t, "custom sidecar topic 'delete' operation succeeded with status code '200' and body ''", *resultTopicError.ErrMsg)
	assert.Equal(t, 1, len(mockSidecarServer.requests))
	for request, body := range mockSidecarServer.requests {
		verifySidecarRequest(t, request, body, topicName, nil)
	}
}

// Test The Custom AdminClient Close() Functionality
func TestCustomAdminClientClose(t *testing.T) {

	// Create A New Custom AdminClient To Test
	adminClient := &CustomAdminClient{}

	// Perform The Test
	err := adminClient.Close()

	// Verify The Results
	assert.Nil(t, err)
}

// Test The Custom AdminClient GetKafkaSecretName() Functionality
func TestCustomAdminClientGetKafkaSecretName(t *testing.T) {

	// Test Data
	topicName := "TestTopicName"
	secretName := "TestSecretName"

	// Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Create A New Custom AdminClient To Test
	adminClient := &CustomAdminClient{logger: logger, kafkaSecret: secretName}

	// Perform The Test
	actualSecretName := adminClient.GetKafkaSecretName(topicName)

	// Verify The Results
	assert.Equal(t, secretName, actualSecretName)
}

//
// Test HTTP Server - Pretending To Be The Custom Sidecar
//

// MockSidecarServer Struct
type MockSidecarServer struct {
	t          *testing.T
	statusCode int
	server     *httptest.Server
	requests   map[*http.Request][]byte // Map Of Request Pointers To BodyBytes For Tracking Requests For Subsequent Validation
}

// MockSidecarServer Constructor
func NewMockSidecarServer(t *testing.T, statusCode int) *MockSidecarServer {

	// Create The Mock Sidecar Server
	mockSidecarServer := &MockSidecarServer{
		t:          t,
		statusCode: statusCode,
		requests:   make(map[*http.Request][]byte),
	}

	// Create A Custom TCP Listener On The Expected Sidecar Host:Port
	listener, err := net.Listen("tcp", custom.SidecarHost+":"+custom.SidecarPort)
	assert.Nil(t, err)
	assert.NotNil(t, listener)

	// Create A Test HTTP Server With Handler Function & Custom Listener
	mockSidecarServer.server = httptest.NewUnstartedServer(mockSidecarServer)
	mockSidecarServer.server.Listener = listener

	// Return The Initialized Mock Sidecar Server
	return mockSidecarServer
}

// Start The Mock Http Server
func (s *MockSidecarServer) Start() {
	if s.server != nil {
		s.server.Start()
	}
}

// Close The Mock Http Server
func (s *MockSidecarServer) Close() {
	if s.server != nil {
		s.server.Close()
	}
}

// The HTTP Handler Interface Implementation
func (s *MockSidecarServer) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {

	// Read The Request Body Before Responding (It Will Otherwise Be Closed!)
	bodyBytes, err := ioutil.ReadAll(request.Body)
	assert.Nil(s.t, err)

	// Track The Received HTTP Request & Body For Future Validation
	s.requests[request] = bodyBytes

	// Return The Desired StatusCode
	responseWriter.WriteHeader(s.statusCode)
}

// Utility Function For Verifying The Inbound HTTP Request (What Is Sent To The Sidecar)
func verifySidecarRequest(t *testing.T, request *http.Request, body []byte, topicName string, saramaTopicDetail *sarama.TopicDetail) {

	// Verify Common Request Data
	assert.Equal(t, custom.SidecarHost+":"+custom.SidecarPort, request.Host)

	// Verify Method Specific Request Data
	switch request.Method {

	case http.MethodPost:
		assert.Equal(t, custom.TopicsPath, request.URL.Path)
		assert.Equal(t, topicName, request.Header.Get(custom.TopicNameHeader))
		customTopicDetail := &custom.TopicDetail{}
		err := json.Unmarshal(body, customTopicDetail)
		assert.Nil(t, err)
		assert.NotNil(t, customTopicDetail)
		assert.Equal(t, saramaTopicDetail.NumPartitions, customTopicDetail.NumPartitions)
		assert.Equal(t, saramaTopicDetail.ReplicationFactor, customTopicDetail.ReplicationFactor)
		assert.Equal(t, saramaTopicDetail.ConfigEntries, customTopicDetail.ConfigEntries)
		assert.Equal(t, saramaTopicDetail.ReplicaAssignment, customTopicDetail.ReplicaAssignment)

	case http.MethodDelete:
		assert.Equal(t, custom.TopicsPath+"/"+topicName, request.URL.Path)
		assert.Equal(t, "", request.Header.Get(custom.TopicNameHeader))
		assert.Empty(t, body)

	default:
		assert.Fail(t, "Unexpected Request Method Sent To Sidecar")
	}
}
