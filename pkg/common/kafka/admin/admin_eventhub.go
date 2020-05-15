package admin

import (
	"context"
	"errors"
	"fmt"
	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/common/kafka/admin/eventhubcache"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	"knative.dev/pkg/logging"
	"math"
	"regexp"
	"strconv"
)

//
// This is an implementation of the AdminClient interface backed by Azure EventHubs API.  The Azure EventHub Kafka
// offering doesn't yet expose/support the Kafka AdminClient so we have to interact with it via their golang API
// or their REST service.  Inherently this wrapping will entail the "mapping" of Kafka structs/functions/etc in
// both directions which will be imprecise by nature.  Furthermore, Azure introduces the concept of "Namespaces"
// with a limited number of Topics/EventHubs per "Namespace", so this implementation also handles the hiding of
// the Namespace layer.
//

// Ensure The EventHubAdminClient Struct Implements The AdminClientInterface
var _ AdminClientInterface = &EventHubAdminClient{}

// EventHub AdminClient Definition
type EventHubAdminClient struct {
	logger    *zap.Logger
	namespace string
	cache     eventhubcache.CacheInterface
}

// EventHub ErrorCode RegExp - For Extracting Azure ErrorCodes From Error Messages
var eventHubErrorCodeRegexp = *regexp.MustCompile(`^.*error code: (\d+),.*$`)

// EventHub NewCache Wrapper To Facilitate Unit Testing
var NewCacheWrapper = func(ctx context.Context, k8sNamespace string) eventhubcache.CacheInterface {
	return eventhubcache.NewCache(ctx, k8sNamespace)
}

// Create A New Azure EventHub AdminClient Based On Kafka Secrets In The Specified K8S Namespace
func NewEventHubAdminClient(ctx context.Context, namespace string) (AdminClientInterface, error) {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx).Desugar()

	// Create A New Cache Via the Wrapper
	cache := NewCacheWrapper(ctx, namespace)

	// Initialize The EventHub Namespace Cache
	err := cache.Update(context.TODO())
	if err != nil {
		logger.Error("Failed To Initialize EventHub Cache", zap.Error(err))
		return nil, err
	}

	// Create And Return A New EventHub AdminClient With Namespace Cache
	return &EventHubAdminClient{
		logger:    logger,
		namespace: namespace,
		cache:     cache,
	}, nil
}

// Kafka AdminClient CreateTopics Implementation Using Azure EventHub API
func (c *EventHubAdminClient) CreateTopics(ctx context.Context, topicSpecifications []kafka.TopicSpecification, createTopicsAdminOptions ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {

	// Create The TopicResults Array For Collecting Results
	topicResults := make([]kafka.TopicResult, len(topicSpecifications))

	// Loop Over All TopicSpecifications
	var topicErr error
	for index, topicSpecification := range topicSpecifications {

		// Create The Specified Topic (EventHub) & Track Results
		topicResult, err := c.createTopic(ctx, topicSpecification)
		topicResults[index] = topicResult
		if err != nil {
			topicErr = err // Will Just Use Last Error - Similar To What Kafka Does
		}
	}

	// Return The Topic Results
	return topicResults, topicErr
}

// Create A Single Topic (EventHub) From The Specified TopicSpecification
func (c *EventHubAdminClient) createTopic(ctx context.Context, topicSpecification kafka.TopicSpecification) (kafka.TopicResult, error) {

	// Extract The Kafka TopicSpecification Configuration
	topicName := topicSpecification.Topic
	topicNumPartitions := topicSpecification.NumPartitions
	topicRetentionMillis, err := strconv.ParseInt(topicSpecification.Config[constants.TopicSpecificationConfigRetentionMs], 10, 64)
	if err != nil {
		c.logger.Error("Failed To Parse Retention Millis From TopicSpecification", zap.Error(err))
		return getTopicResult(topicName, kafka.ErrInvalidConfig), err
	}

	// Convert Kafka Retention Millis To Azure EventHub Retention Days
	topicRetentionDays := convertMillisToDays(topicRetentionMillis)

	// Attempt To Get EventHub Namespace Associated With EventHub
	eventHubNamespace := c.cache.GetNamespace(topicName)
	if eventHubNamespace == nil {

		// EventHub Not In Cache - Get The Least Populated Azure EventHub Namespace From The Cache
		eventHubNamespace = c.cache.GetLeastPopulatedNamespace()
		if eventHubNamespace == nil {

			// No EventHub Namespaces Found In Cache - Return Error
			c.logger.Warn("Found No EventHub Namespace In Cache - Skipping Topic Creation", zap.String("Topic", topicName))
			err = errors.New(fmt.Sprintf("no azure eventhub namespaces in cache - unable to create EventHub '%s'", topicName))
			return getTopicResult(topicName, kafka.ErrInvalidConfig), err
		}
	}

	// If The HubManager Is Not Valid Then Return Error
	if eventHubNamespace.HubManager == nil {
		c.logger.Warn("Failed To Find EventHub Namespace With Valid HubManager - Skipping Topic Creation", zap.String("Topic", topicName))
		err = errors.New(fmt.Sprintf("azure namespace has invalid HubManager - unable to create EventHub '%s'", topicName))
		return getTopicResult(topicName, kafka.ErrInvalidConfig), err
	}

	// Create The EventHub (Topic) Via The PUT Rest Endpoint
	_, err = eventHubNamespace.HubManager.Put(ctx, topicName,
		eventhub.HubWithPartitionCount(int32(topicNumPartitions)),
		eventhub.HubWithMessageRetentionInDays(topicRetentionDays))
	if err != nil {

		// Handle Specific EventHub Error Codes (To Emulate Kafka Admin Behavior)
		errorCode := getEventHubErrorCode(err)
		if errorCode == constants.EventHubErrorCodeConflict {
			return getTopicResult(topicName, kafka.ErrTopicAlreadyExists), nil
		} else if errorCode == constants.EventHubErrorCodeCapacityLimit {
			c.logger.Warn("Failed To Create EventHub - Reached Capacity Limit", zap.Error(err))
			return getTopicResult(topicName, kafka.ErrTopicException), err
		} else if errorCode == constants.EventHubErrorCodeUnknown {
			c.logger.Error("Failed To Create EventHub - Missing Error Code", zap.Error(err))
			return getTopicResult(topicName, kafka.ErrUnknown), err
		} else if errorCode == constants.EventHubErrorCodeParseFailure {
			c.logger.Error("Failed To Create EventHub - Failed To Parse Error Code", zap.Error(err))
			return getTopicResult(topicName, kafka.ErrFail), err
		} else {
			c.logger.Error("Failed To Create EventHub - Received Unmapped Error Code", zap.Error(err))
			return getTopicResult(topicName, kafka.ErrFail), err
		}
	}

	// Add The New EventHub To The Cache
	c.cache.AddEventHub(ctx, topicName, eventHubNamespace)

	// Return Success!
	return getTopicResult(topicName, kafka.ErrNoError), nil
}

// Kafka AdminClient DeleteTopics Implementation Using Azure EventHub API
func (c *EventHubAdminClient) DeleteTopics(ctx context.Context, topicNames []string, deleteOptions ...kafka.DeleteTopicsAdminOption) ([]kafka.TopicResult, error) {

	// Create The TopicResults Array For Collecting Results
	topicResults := make([]kafka.TopicResult, len(topicNames))

	// Loop Over All Topic Names
	var topicErr error
	for index, topicName := range topicNames {

		// Delete The Specified Topic (EventHub) & Track Results
		topicResult, err := c.deleteTopic(ctx, topicName)
		topicResults[index] = topicResult
		if err != nil {
			topicErr = err // Will Just Use Last Error - Similar To What Kafka Does
		}
	}

	// Return The Topic Results
	return topicResults, topicErr
}

// Delete A Single Topic (EventHub) From The Specified TopicSpecification
func (c *EventHubAdminClient) deleteTopic(ctx context.Context, topicName string, deleteOptions ...kafka.DeleteTopicsAdminOption) (kafka.TopicResult, error) {

	// Azure EventHub Delete API Does NOT Accept Any Parameters So The Kafka DeleteTopicsAdminOptions Are Ignored

	// Get The Azure EventHub Namespace Associated With This Topic (Should Be One ; )
	eventHubNamespace := c.cache.GetNamespace(topicName)

	// If No Namespace Was Found For The EventHub (Topic) Then Return Error
	if eventHubNamespace == nil {
		c.logger.Warn("Failed To Find EventHub Namespace For Topic - Skipping Topic Deletion", zap.String("Topic", topicName))
		err := errors.New(fmt.Sprintf("no azure namespace found for EventHub - unable to delete EventHub '%s'", topicName))
		return getTopicResult(topicName, kafka.ErrInvalidConfig), err
	}

	// If The HubManager Is Not Valid Then Return Error
	if eventHubNamespace.HubManager == nil {
		c.logger.Warn("Failed To Find EventHub Namespace With Valid HubManager - Skipping Topic Deletion", zap.String("Topic", topicName))
		err := errors.New(fmt.Sprintf("azure namespace has invalid HubManager - unable to delete EventHub '%s'", topicName))
		return getTopicResult(topicName, kafka.ErrInvalidConfig), err
	}

	// Delete The Specified Topic (EventHub)
	err := eventHubNamespace.HubManager.Delete(ctx, topicName)
	if err != nil {

		// Delete API Returns Success For Non-Existent Topics - Nothing To Map (yet?!)
		return getTopicResult(topicName, kafka.ErrFail), err
	}

	// Remove The EventHub From The Cache
	c.cache.RemoveEventHub(ctx, topicName)

	// Return Success!
	return getTopicResult(topicName, kafka.ErrNoError), nil
}

// Get The K8S Secret With Kafka Credentials For The Specified Topic (EventHub)
func (c *EventHubAdminClient) GetKafkaSecretName(topicName string) string {

	// The Default Kafka Secret Name
	kafkaSecretName := ""

	// Attempt To Load The Actual Namespace & It's Kafka Secret Name
	namespace := c.cache.GetNamespace(topicName)
	if namespace != nil {
		return namespace.Secret
	}

	// Return Results
	return kafkaSecretName
}

// Kafka AdminClient Close Implementation Using Azure EventHub API
func (c *EventHubAdminClient) Close() {

	// Nothing to "close" in the HubManager (just a REST client) so this is just a compatibility no-op.
	return
}

// Utility Function For Converting Millis To Days (Rounded Up To Larger Day Value)
func convertMillisToDays(millis int64) int32 {
	return int32(math.Ceil(float64(millis) / float64(constants.MillisPerDay)))
}

// Utility Function For Creating A Kafka TopicResult With Optional ErrorCode (ErrNoError == Success)
func getTopicResult(topicName string, err kafka.ErrorCode) kafka.TopicResult {

	// If A Kafka ErrorCode Was Specified Then Create The Corresponding KafkaError
	var kafkaError kafka.Error
	if err != kafka.ErrNoError {
		kafkaError = kafka.NewError(err, topicName, false)
	}

	// Return The Specified TopicResult With Optional Kafka Error
	return kafka.TopicResult{Topic: topicName, Error: kafkaError}
}

//
// Utility Function For Extracting Error Code From EventHub Errors
//
// EventHub error strings are formatted as...
//
//   "error code: 409, Details: SubCode=40900. Conflict. TrackingId:4d43ef4d-461f-4164-af55-3e710a561c74_G8, SystemTracker:event-hub.servicebus.windows.net:TestTopic, Timestamp:2019-08-13T13:39:56"
//
func getEventHubErrorCode(err error) int {

	// Default Error Code (No Error)
	errorCode := constants.EventHubErrorCodeUnknown

	// Validate The Err Argument
	if err != nil {

		// Get The Matches / SubMatches
		matches := eventHubErrorCodeRegexp.FindStringSubmatch(err.Error())

		// If The SubMatch (Regexp Group) Was Found
		if len(matches) > 0 {

			// Then Get The SubMatch (Regexp Group) & Convert To Int
			errorCode, err = strconv.Atoi(matches[1])
			if err != nil {
				errorCode = constants.EventHubErrorCodeParseFailure
			}
		}
	}

	// Return The Error Code
	return errorCode
}
