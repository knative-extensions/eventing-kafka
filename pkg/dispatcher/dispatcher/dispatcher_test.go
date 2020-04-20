package dispatcher

import (
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go/v1"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/go-cmp/cmp"
	kafkaconsumer "github.com/kyma-incubator/knative-kafka/pkg/common/kafka/consumer"
	"github.com/kyma-incubator/knative-kafka/pkg/dispatcher/client"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	logtesting "knative.dev/pkg/logging/testing"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Test Constants
const (
	testBrokers                 = "TestBrokers"
	testTopic                   = "TestTopic"
	testPartition               = 33
	testOffset                  = "latest"
	testGroupId1                = "TestGroupId1"
	testGroupId2                = "TestGroupId2"
	testGroupId3                = "TestGroupId3"
	testKey                     = "TestKey"
	testMessagesToSend          = 3
	testPollTimeoutMillis       = 500 // Not Used By Mock Consumer ; )
	testOffsetCommitCount       = 2
	testOffsetCommitDuration    = 500 * time.Millisecond // Small Durations For Testing!
	testOffsetCommitDurationMin = 500 * time.Millisecond // Small Durations For Testing!
	testUsername                = "TestUsername"
	testPassword                = "TestPassword"
	testChannelKey              = "TestChannel"
)

// Test Data (Non-Constants)
var (
	topic            = testTopic
	testError        = kafka.NewError(1, "sample error", false)
	testNotification = kafka.RevokedPartitions{}
	testValue        = map[string]string{"test": "value"}
)

// Test All The Dispatcher Functionality
func TestDispatcher(t *testing.T) {

	// Create A Test Logger
	logger := logtesting.TestLogger(t).Desugar()

	// Initialize The Test HTTP Server Instance & URL
	callCount1 := 0
	var httpServer = getHttpServer(t, &callCount1)
	testSubscriberUri1 := httpServer.URL
	defer httpServer.Close()

	callCount2 := 0
	var httpServer2 = getHttpServer(t, &callCount2)
	testSubscriberUri2 := httpServer2.URL
	defer httpServer2.Close()

	callCount3 := 0
	var httpServer3 = getHttpServer(t, &callCount3)
	testSubscriberUri3 := httpServer3.URL
	defer httpServer3.Close()

	// Replace The NewClient Wrapper To Provide Mock Consumer & Defer Reset
	newConsumerWrapperPlaceholder := kafkaconsumer.NewConsumerWrapper
	kafkaconsumer.NewConsumerWrapper = func(configMap *kafka.ConfigMap) (kafkaconsumer.ConsumerInterface, error) {
		verifyConfigMapValue(t, configMap, "bootstrap.servers", testBrokers)
		verifyConfigMapValue(t, configMap, "sasl.username", testUsername)
		verifyConfigMapValue(t, configMap, "sasl.password", testPassword)
		return NewMockConsumer(), nil
	}
	defer func() { kafkaconsumer.NewConsumerWrapper = newConsumerWrapperPlaceholder }()

	cloudEventClient := client.NewRetriableCloudEventClient(logtesting.TestLogger(t).Desugar(), false, 500, 5000)

	// Create A New Dispatcher
	dispatcherConfig := DispatcherConfig{
		Logger:                      logger,
		Brokers:                     testBrokers,
		Topic:                       testTopic,
		Offset:                      testOffset,
		PollTimeoutMillis:           testPollTimeoutMillis,
		OffsetCommitCount:           testOffsetCommitCount,
		OffsetCommitDuration:        testOffsetCommitDuration,
		OffsetCommitDurationMinimum: testOffsetCommitDurationMin,
		Username:                    testUsername,
		Password:                    testPassword,
		Client:                      cloudEventClient,
		ChannelKey:                  testChannelKey,
	}
	testDispatcher := NewDispatcher(dispatcherConfig)

	// Verify Dispatcher Configuration
	verifyDispatcher(t,
		testDispatcher,
		testBrokers,
		testTopic,
		testOffset,
		testOffsetCommitCount,
		testOffsetCommitDuration,
		testUsername,
		testPassword,
		testChannelKey)

	// Start 1 Consumer
	subscriptionResults := testDispatcher.UpdateSubscriptions([]Subscription{
		{
			URI:     testSubscriberUri1,
			GroupId: testGroupId1,
		},
	})

	// Verify Consumers Started
	assert.Len(t, subscriptionResults, 0)
	verifyConsumersCount(t, testDispatcher.consumers, 1)

	// Remove All Consumers
	subscriptionResults = testDispatcher.UpdateSubscriptions(make([]Subscription, 0))

	assert.Len(t, subscriptionResults, 0)
	verifyConsumersCount(t, testDispatcher.consumers, 0)

	// Start 3 Consumers
	subscriptionResults = testDispatcher.UpdateSubscriptions([]Subscription{
		{
			URI:     testSubscriberUri1,
			GroupId: testGroupId1,
		},
		{
			URI:     testSubscriberUri2,
			GroupId: testGroupId2,
		},
		{
			URI:     testSubscriberUri3,
			GroupId: testGroupId3,
		},
	})

	// Verify 3 Consumers Active
	assert.Len(t, subscriptionResults, 0)
	verifyConsumersCount(t, testDispatcher.consumers, 3)

	// Send Errors, Notifications, Messages To Each Consumer
	sendErrorToConsumers(t, testDispatcher.consumers, testError)
	sendNotificationToConsumers(t, testDispatcher.consumers, testNotification)
	sendMessagesToConsumers(t, testDispatcher.consumers, testMessagesToSend)

	// Wait For Consumers To Process Messages
	waitForConsumersToProcessEvents(t, testDispatcher.consumers, testMessagesToSend)

	// Verify The Consumer Offset Commit Messages
	verifyConsumerCommits(t, testDispatcher.consumers, 3)

	// Remove 2 Consumers
	subscriptionResults = testDispatcher.UpdateSubscriptions([]Subscription{
		{
			URI:     testSubscriberUri1,
			GroupId: testGroupId1,
		},
	})
	verifyConsumersCount(t, testDispatcher.consumers, 1)
	assert.Len(t, subscriptionResults, 0)

	// Stop Consumers
	testDispatcher.StopConsumers()

	// Verify Consumers Stopped
	verifyConsumersClosed(t, testDispatcher.consumers)

	// Wait A Bit To fSee The Channel Closing Logs (lame but only for visual verification of non-blocking channel shutdown)
	time.Sleep(2 * time.Second)

	// Verify Mock HTTP Server Is Called The Correct Number Of Times
	if callCount1 != testMessagesToSend {
		t.Errorf("expected %+v HTTP calls but got: %+v", testMessagesToSend, callCount1)
	}

	if callCount2 != testMessagesToSend {
		t.Errorf("expected %+v HTTP calls but got: %+v", testMessagesToSend, callCount2)
	}

	if callCount3 != testMessagesToSend {
		t.Errorf("expected %+v HTTP calls but got: %+v", testMessagesToSend, callCount3)
	}
}

// Verify A Specific ConfigMap Value
func verifyConfigMapValue(t *testing.T, configMap *kafka.ConfigMap, key string, expected string) {
	property, err := configMap.Get(key, nil)
	assert.Nil(t, err)
	assert.Equal(t, expected, property.(string))
}

// Return Test HTTP Server - Always Responds With Success
func getHttpServer(t *testing.T, callCount *int) *httptest.Server {
	httpServer := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case "POST":

			// Update The Number Of Calls (Async Safe)
			*callCount++

			// Verify Request Content-Type Header
			requestContentTypeHeader := request.Header.Get("Content-Type")
			if requestContentTypeHeader != "application/json" {
				t.Errorf("expected HTTP request ContentType Header: application/json got: %+v", requestContentTypeHeader)
			}

			// Verify The Request Body
			bodyBytes, err := ioutil.ReadAll(request.Body)
			if err != nil {
				t.Errorf("expected to be able to read HTTP request Body without error: %+v", err)
			} else {
				bodyMap := make(map[string]string)
				json.Unmarshal(bodyBytes, &bodyMap)
				if !reflect.DeepEqual(bodyMap, testValue) {
					t.Errorf("expected HTTP request Body: %+v got: %+v", testValue, bodyMap)
				}
			}

			// Send Success Response (This is the default, but just to be explicit)
			responseWriter.WriteHeader(http.StatusOK)

		default:
			t.Errorf("expected HTTP request method: POST got: %+v", request.Method)

		}
	}))
	return httpServer
}

// Verify The Dispatcher Is Configured As Specified
func verifyDispatcher(t *testing.T,
	dispatcher *Dispatcher,
	expectedBrokers string,
	expectedTopic string,
	expectedOffset string,
	expectedOffsetCommitCount int64,
	expectedOffsetCommitDuration time.Duration,
	expectedUsername string,
	expectedPassword string,
	expectedChannelName string) {

	assert.NotNil(t, dispatcher)
	assert.Equal(t, expectedBrokers, dispatcher.Brokers)
	assert.Equal(t, expectedTopic, dispatcher.Topic)
	assert.Equal(t, expectedOffset, dispatcher.Offset)
	assert.NotNil(t, dispatcher.consumers)
	assert.Len(t, dispatcher.consumers, 0)
	assert.Equal(t, expectedOffsetCommitCount, dispatcher.OffsetCommitCount)
	assert.Equal(t, expectedOffsetCommitDuration, dispatcher.OffsetCommitDuration)
	assert.Equal(t, expectedUsername, dispatcher.Username)
	assert.Equal(t, expectedPassword, dispatcher.Password)
	assert.Equal(t, expectedChannelName, dispatcher.ChannelKey)
}

// Verify The Appropriate Consumers Were Created
func verifyConsumersCount(t *testing.T, consumers map[Subscription]*ConsumerOffset, expectedCount int) {
	assert.NotNil(t, consumers)
	assert.Len(t, consumers, expectedCount)
	for _, consumer := range consumers {
		mockConsumer, ok := consumer.consumer.(*MockConsumer)
		assert.False(t, mockConsumer.closed)
		assert.True(t, ok)
	}
}

// Verify The Consumers Have All Been Closed
func verifyConsumersClosed(t *testing.T, consumers map[Subscription]*ConsumerOffset) {
	assert.NotNil(t, consumers)
	assert.Len(t, consumers, 0)
	for _, consumer := range consumers {
		mockConsumer, ok := consumer.consumer.(*MockConsumer)
		assert.True(t, mockConsumer.closed)
		assert.True(t, ok)
	}
}

// Verify The Consumers Have The Correct Commit Messages / Offsets
func verifyConsumerCommits(t *testing.T, consumers map[Subscription]*ConsumerOffset, expectedCount int) {
	assert.NotNil(t, consumers)
	assert.Len(t, consumers, expectedCount)
	for _, consumer := range consumers {
		mockConsumer, ok := consumer.consumer.(*MockConsumer)
		assert.True(t, ok)
		for _, offset := range mockConsumer.commits {
			assert.NotNil(t, offset)
			assert.Equal(t, kafka.Offset(testMessagesToSend+1), offset)
		}
	}
}

// Send An Error To The Specified Consumers
func sendErrorToConsumers(t *testing.T, consumers map[Subscription]*ConsumerOffset, err kafka.Error) {
	for _, consumer := range consumers {
		mockConsumer, ok := consumer.consumer.(*MockConsumer)
		assert.True(t, ok)
		mockConsumer.sendMessage(err)
	}
}

// Send A Notification To The Specified Consumers
func sendNotificationToConsumers(t *testing.T, consumers map[Subscription]*ConsumerOffset, notification kafka.Event) {
	for _, consumer := range consumers {
		mockConsumer, ok := consumer.consumer.(*MockConsumer)
		assert.True(t, ok)
		mockConsumer.sendMessage(notification)
	}
}

// Send Some Messages To The Specified Consumers
func sendMessagesToConsumers(t *testing.T, consumers map[Subscription]*ConsumerOffset, count int) {
	for i := 0; i < count; i++ {
		message := createKafkaMessage(kafka.Offset(i+1), cloudevents.VersionV1)

		for _, consumer := range consumers {
			mockConsumer, ok := consumer.consumer.(*MockConsumer)
			assert.True(t, ok)
			mockConsumer.sendMessage(message)
		}
	}
}

// Create A New Kafka Message With Specified Offset
func createKafkaMessage(offset kafka.Offset, cloudEventVersion string) *kafka.Message {

	testCloudEvent := createCloudEvent(cloudEventVersion)
	eventBytes, _ := testCloudEvent.DataBytes()

	return &kafka.Message{
		Key:       []byte(testKey),
		Headers:   createMessageHeaders(testCloudEvent.Context),
		Value:     eventBytes,
		Timestamp: time.Now(),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: testPartition,
			Offset:    offset,
		},
	}
}

func createCloudEvent(cloudEventVersion string) cloudevents.Event {
	testCloudEvent := cloudevents.NewEvent(cloudEventVersion)
	testCloudEvent.SetID("ABC-123")
	testCloudEvent.SetType("com.cloudevents.readme.sent")
	testCloudEvent.SetSource("http://localhost:8080/")
	testCloudEvent.SetDataContentType("application/json")

	data, _ := json.Marshal(testValue)
	testCloudEvent.SetData(data)

	return testCloudEvent
}

// Map CloudEvent Context Headers To Kafka Message Headers
func createMessageHeaders(context cloudevents.EventContext) []kafka.Header {
	headers := []kafka.Header{
		{Key: "ce_specversion", Value: []byte(context.GetSpecVersion())},
		{Key: "ce_type", Value: []byte(context.GetType())},
		{Key: "ce_source", Value: []byte(context.GetSource())},
		{Key: "ce_id", Value: []byte(context.GetID())},
		{Key: "ce_time", Value: []byte(context.GetTime().Format(time.RFC3339))},
	}

	if context.GetDataContentType() != "" {
		headers = append(headers, kafka.Header{Key: "ce_datacontenttype", Value: []byte(context.GetDataContentType())})
	}

	if context.GetSubject() != "" {
		headers = append(headers, kafka.Header{Key: "ce_subject", Value: []byte(context.GetSubject())})
	}

	if context.GetDataSchema() != "" {
		headers = append(headers, kafka.Header{Key: "ce_dataschema", Value: []byte(context.GetDataSchema())})
	}

	// Only Setting String Extensions
	for k, v := range context.GetExtensions() {
		if vs, ok := v.(string); ok {
			headers = append(headers, kafka.Header{Key: "ce_" + k, Value: []byte(vs)})
		}
	}

	return headers
}

// Wait For The Consumers To Process All The Events
func waitForConsumersToProcessEvents(t *testing.T, consumers map[Subscription]*ConsumerOffset, expectedOffset kafka.Offset) {

	// Track Wait Start Time
	startTime := time.Now()

	// Loop Over All The Specified Consumers
	for _, consumer := range consumers {

		// Verified Cast Of Consumer As MockConsumer
		mockConsumer, ok := consumer.consumer.(*MockConsumer)
		assert.True(t, ok)

		// Infinite Loop - Broken By Internal Timeout Failure
		for {

			// Get The Partition's Committed Offset (Minimal Synchronization To Avoid Deadlocking The Commit Updates!)
			mockConsumer.offsetsMutex.Lock()
			commitOffset := mockConsumer.getCommits()[testPartition]
			mockConsumer.offsetsMutex.Unlock()

			// Process Committed Offset - Either Stop Looking, Wait, Or Timeout
			if commitOffset >= expectedOffset+1 {
				break
			} else if time.Now().Sub(startTime) > (5 * time.Second) {
				assert.FailNow(t, "Timed-out Waiting For Consumers To Process Events")
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

//
// Mock ConsumerInterface Implementation
//

var _ kafkaconsumer.ConsumerInterface = &MockConsumer{}

func NewMockConsumer() *MockConsumer {
	return &MockConsumer{
		events:       make(chan kafka.Event),
		commits:      make(map[int32]kafka.Offset),
		offsets:      make(map[int32]kafka.Offset),
		offsetsMutex: &sync.Mutex{},
		closed:       false,
	}
}

type MockConsumer struct {
	events             chan kafka.Event
	commits            map[int32]kafka.Offset
	offsets            map[int32]kafka.Offset
	offsetsMutex       *sync.Mutex
	eventsChanEnable   bool
	readerTermChan     chan bool
	rebalanceCb        kafka.RebalanceCb
	appReassigned      bool
	appRebalanceEnable bool // config setting
	closed             bool
}

func (mc *MockConsumer) OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error) {
	return nil, nil
}

func (mc *MockConsumer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	return nil, nil
}

func (mc *MockConsumer) CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (mc *MockConsumer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	return 0, 0, nil
}

func (mc *MockConsumer) Poll(timeout int) kafka.Event {
	// Non-Blocking Event Forwarding (Timeout Ignored)
	select {
	case event := <-mc.events:
		return event
	default:
		return nil
	}
}

func (mc *MockConsumer) CommitMessage(message *kafka.Message) ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (mc *MockConsumer) Close() error {
	mc.closed = true
	close(mc.events)
	return nil
}

func (mc *MockConsumer) Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error {
	return nil
}

func (mc *MockConsumer) sendMessage(message kafka.Event) {
	mc.events <- message
}

func (mc *MockConsumer) getCommits() map[int32]kafka.Offset {
	return mc.commits
}

func (mc *MockConsumer) Assignment() (partitions []kafka.TopicPartition, err error) {
	return nil, nil
}

func (mc *MockConsumer) Commit() ([]kafka.TopicPartition, error) {
	mc.offsetsMutex.Lock()
	for partition, offset := range mc.offsets {
		mc.commits[partition] = offset
	}
	mc.offsetsMutex.Unlock()
	return nil, nil
}

func (mc *MockConsumer) StoreOffsets(offsets []kafka.TopicPartition) (storedOffsets []kafka.TopicPartition, err error) {
	mc.offsetsMutex.Lock()
	for _, partition := range offsets {
		mc.offsets[partition.Partition] = partition.Offset
	}
	mc.offsetsMutex.Unlock()
	return nil, nil
}

func Test_convertToCloudEvent(t *testing.T) {

	// Valid v0.3 Message
	validMessageV03 := createKafkaMessage(1, cloudevents.VersionV03)
	expectedCloudEventV03 := createCloudEvent(cloudevents.VersionV03)

	// Valid v1 Message
	validMessageV1 := createKafkaMessage(2, cloudevents.VersionV1)
	expectedCloudEventV1 := createCloudEvent(cloudevents.VersionV1)

	// Invalid Since Has No Spec Version
	invalidMessage := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    1,
		},
		Value: []byte("foobar"),
		Key:   []byte("foobar"),
		Headers: []kafka.Header{
			{Key: "ce_datacontenttype", Value: []byte("application/json")},
		},
	}

	type args struct {
		message *kafka.Message
	}

	tests := []struct {
		name    string
		args    args
		want    *cloudevents.Event
		wantErr bool
	}{
		{
			name: "valid 0.3 cloudevent",
			args: args{
				message: validMessageV03,
			},
			want:    &expectedCloudEventV03,
			wantErr: false,
		},
		{
			name: "valid 1.0 cloudevent",
			args: args{
				message: validMessageV1,
			},
			want:    &expectedCloudEventV1,
			wantErr: false,
		},
		{
			name: "invalid cloudevent",
			args: args{
				message: &invalidMessage,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertToCloudEvent(tt.args.message)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToCloudEvent() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("Did not receive correct results:\n %s", cmp.Diff(got, tt.want))
			}
		})
	}
}
