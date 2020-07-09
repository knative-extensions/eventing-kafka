package admin

import (
	"context"
	"fmt"
	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/common/kafka/admin/eventhubcache"
	adminutil "knative.dev/eventing-kafka/pkg/common/kafka/admin/util"
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
func (c *EventHubAdminClient) CreateTopic(ctx context.Context, topicName string, topicDetail *sarama.TopicDetail) *sarama.TopicError {

	// Extract The Kafka TopicSpecification Configuration
	topicNumPartitions := topicDetail.NumPartitions
	topicRetentionMillis, err := strconv.ParseInt(*topicDetail.ConfigEntries[constants.TopicDetailConfigRetentionMs], 10, 64)
	if err != nil {
		c.logger.Error("Failed To Parse Retention Millis From TopicDetail", zap.Error(err))
		return adminutil.NewTopicError(sarama.ErrInvalidConfig, "failed to parse retention millis from TopicDetail")
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
			return adminutil.NewTopicError(sarama.ErrInvalidConfig, fmt.Sprintf("no azure eventhub namespaces in cache - unable to create EventHub '%s'", topicName))
		}
	}

	// If The HubManager Is Not Valid Then Return Error
	if eventHubNamespace.HubManager == nil {
		c.logger.Warn("Failed To Find EventHub Namespace With Valid HubManager - Skipping Topic Creation", zap.String("Topic", topicName))
		return adminutil.NewTopicError(sarama.ErrInvalidConfig, fmt.Sprintf("azure namespace has invalid HubManager - unable to create EventHub '%s'", topicName))
	}

	// Create The EventHub (Topic) Via The PUT Rest Endpoint
	_, err = eventHubNamespace.HubManager.Put(ctx, topicName,
		eventhub.HubWithPartitionCount(topicNumPartitions),
		eventhub.HubWithMessageRetentionInDays(topicRetentionDays))
	if err != nil {

		// Handle Specific EventHub Error Codes (To Emulate Kafka Admin Behavior)
		errorCode := getEventHubErrorCode(err)
		if errorCode == constants.EventHubErrorCodeConflict {
			return adminutil.NewTopicError(sarama.ErrTopicAlreadyExists, "mapped from EventHubErrorCodeConflict")
		} else if errorCode == constants.EventHubErrorCodeCapacityLimit {
			c.logger.Warn("Failed To Create EventHub - Reached Capacity Limit", zap.Error(err))
			return adminutil.NewTopicError(sarama.ErrInvalidTxnState, "mapped from EventHubErrorCodeCapacityLimit") // TODO - or maybe ErrNotEnoughReplicas ??? or just unknown ???
		} else if errorCode == constants.EventHubErrorCodeUnknown {
			c.logger.Error("Failed To Create EventHub - Missing Error Code", zap.Error(err))
			return adminutil.NewUnknownTopicError("mapped from EventHubErrorCodeUnknown")
		} else if errorCode == constants.EventHubErrorCodeParseFailure {
			c.logger.Error("Failed To Create EventHub - Failed To Parse Error Code", zap.Error(err))
			return adminutil.NewUnknownTopicError("mapped from EventHubErrorCodeParseFailure")
		} else {
			c.logger.Error("Failed To Create EventHub - Received Unmapped Error Code", zap.Error(err))
			return adminutil.NewUnknownTopicError(fmt.Sprintf("received unmapped eventhub error code '%d'", errorCode))
		}
	}

	// Add The New EventHub To The Cache
	c.cache.AddEventHub(ctx, topicName, eventHubNamespace)

	// Return Success!
	return adminutil.NewTopicError(sarama.ErrNoError, "successfully created topic")
}

// Delete A Single Topic (EventHub) Via The Azure EventHub API
func (c *EventHubAdminClient) DeleteTopic(ctx context.Context, topicName string) *sarama.TopicError {

	// Azure EventHub Delete API Does NOT Accept Any Parameters So The Kafka DeleteTopicsAdminOptions Are Ignored

	// Get The Azure EventHub Namespace Associated With This Topic (Should Be One ; )
	eventHubNamespace := c.cache.GetNamespace(topicName)

	// If No Namespace Was Found For The EventHub (Topic) Then Return Error
	if eventHubNamespace == nil {
		c.logger.Warn("Failed To Find EventHub Namespace For Topic - Skipping Topic Deletion", zap.String("Topic", topicName))
		return adminutil.NewTopicError(sarama.ErrInvalidConfig, fmt.Sprintf("no azure namespace found for EventHub - unable to delete EventHub '%s'", topicName))
	}

	// If The HubManager Is Not Valid Then Return Error
	if eventHubNamespace.HubManager == nil {
		c.logger.Warn("Failed To Find EventHub Namespace With Valid HubManager - Skipping Topic Deletion", zap.String("Topic", topicName))
		return adminutil.NewTopicError(sarama.ErrInvalidConfig, fmt.Sprintf("azure namespace has invalid HubManager - unable to delete EventHub '%s'", topicName))
	}

	// Delete The Specified Topic (EventHub)
	err := eventHubNamespace.HubManager.Delete(ctx, topicName)
	if err != nil {

		// Delete API Returns Success For Non-Existent Topics - Nothing To Map - Just Return Error
		c.logger.Error("Failed To Delete EventHub", zap.String("TopicName", topicName), zap.Error(err))
		return adminutil.PromoteErrorToTopicError(err)
	}

	// Remove The EventHub From The Cache
	c.cache.RemoveEventHub(ctx, topicName)

	// Return Success!
	return adminutil.NewTopicError(sarama.ErrNoError, "successfully deleted topic")
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
func (c *EventHubAdminClient) Close() error {
	return nil // Nothing to "close" in the HubManager (just a REST client) so this is just a compatibility no-op.
}

// Utility Function For Converting Millis To Days (Rounded Up To Larger Day Value)
func convertMillisToDays(millis int64) int32 {
	return int32(math.Ceil(float64(millis) / float64(constants.MillisPerDay)))
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
