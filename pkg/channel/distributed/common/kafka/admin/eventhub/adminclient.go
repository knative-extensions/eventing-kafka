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
	"math"
	"regexp"
	"strconv"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/types"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/admin/util"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/pkg/logging"
)

//
// This is an implementation of the AdminClient interface backed by Azure EventHubs API.  The Azure EventHub Kafka
// offering doesn't yet expose/support the Kafka AdminClient so we have to interact with it via their golang API
// or their REST service.  Inherently this wrapping will entail the "mapping" of Kafka structs/functions/etc in
// both directions which will be imprecise by nature.  Furthermore, Azure introduces the concept of "Namespaces"
// with a limited number of Topics/EventHubs per "Namespace", and this implementation only currently handles a
// single EventHub Namespace (limit 10 EventHubs / Topics).
//

// Ensure The EventHubAdminClient Struct Implements The AdminClientInterface
var _ types.AdminClientInterface = &EventHubAdminClient{}

// EventHub AdminClient Definition
type EventHubAdminClient struct {
	logger     *zap.Logger
	hubManager HubManagerInterface
}

// EventHub ErrorCode RegExp - For Extracting Azure ErrorCodes From Error Messages
var eventHubErrorCodeRegexp = *regexp.MustCompile(`^.*error code: (-?)(\d+),.*$`)

// Create A New Azure EventHub AdminClient Based On Kafka Secrets In The Specified K8S Namespace
func NewAdminClient(ctx context.Context, config *sarama.Config) (types.AdminClientInterface, error) {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx).Desugar()

	// Validate The Sarama Config & Extract The ConnectionString From Sarama Config Net.SASL.Password
	if config == nil {
		return nil, fmt.Errorf("received nil Sarama Config - unable to create EventHub AdminClient")
	}
	connectionString := config.Net.SASL.Password
	if len(connectionString) <= 0 {
		return nil, fmt.Errorf("received Sarama Config without EventHub Namespace ConnectionString in Net.SASL.Password field - unable to create EventHub AdminClient")
	}

	// Create A New Azure EventHub HubManager From ConnectionString
	hubManager, err := NewHubManagerFromConnectionStringWrapper(connectionString)
	if err != nil {
		logger.Error("Failed To Create HubManager From EventHub Namespace ConnectionString - Unable To Create EventHub AdminClient", zap.Error(err))
		return nil, err
	} else if hubManager == nil {
		logger.Error("Created Nil HubManager From EventHub Namespace ConnectionString - Unable To Create EventHub AdminClient")
		return nil, fmt.Errorf("created nil HubManager from EventHub Namespace ConnectionString")
	}

	// Create And Return A New EventHub AdminClient With EventHub Namespace
	logger.Debug("Successfully Created New EventHub AdminClient")
	return &EventHubAdminClient{
		logger:     logger,
		hubManager: hubManager,
	}, nil
}

// Kafka AdminClient CreateTopics Implementation Using Azure EventHub API
func (c *EventHubAdminClient) CreateTopic(ctx context.Context, topicName string, topicDetail *sarama.TopicDetail) *sarama.TopicError {

	// Extract The Kafka TopicSpecification Configuration
	topicNumPartitions := topicDetail.NumPartitions
	topicRetentionMillis, err := strconv.ParseInt(*topicDetail.ConfigEntries[constants.TopicDetailConfigRetentionMs], 10, 64)
	if err != nil {
		c.logger.Error("Failed To Parse Retention Millis From TopicDetail", zap.Error(err))
		return util.NewTopicError(sarama.ErrInvalidConfig, "failed to parse retention millis from TopicDetail")
	}

	// Convert Kafka Retention Millis To Azure EventHub Retention Days
	topicRetentionDays := convertMillisToDays(topicRetentionMillis)

	// If The HubManager Is Not Valid Then Return Error
	if c.hubManager == nil {
		c.logger.Warn("Failed To Find EventHub Namespace With Valid HubManager - Skipping Topic Creation", zap.String("Topic", topicName))
		return util.NewTopicError(sarama.ErrInvalidConfig, fmt.Sprintf("azure namespace has invalid HubManager - unable to create EventHub '%s'", topicName))
	}

	// Create The EventHub (Topic) Via The PUT Rest Endpoint
	_, err = c.hubManager.Put(ctx, topicName,
		eventhub.HubWithPartitionCount(topicNumPartitions),
		eventhub.HubWithMessageRetentionInDays(topicRetentionDays))
	if err != nil {

		// Handle Specific EventHub Error Codes (To Emulate Kafka Admin Behavior)
		errorCode := getEventHubErrorCode(err)
		if errorCode == constants.EventHubErrorCodeConflict {
			return util.NewTopicError(sarama.ErrTopicAlreadyExists, "mapped from EventHubErrorCodeConflict")
		} else if errorCode == constants.EventHubErrorCodeCapacityLimit {
			c.logger.Warn("Failed To Create EventHub - Reached Capacity Limit", zap.Error(err))
			return util.NewTopicError(sarama.ErrInvalidTxnState, "mapped from EventHubErrorCodeCapacityLimit")
		} else if errorCode == constants.EventHubErrorCodeUnknown {
			c.logger.Error("Failed To Create EventHub - Missing Error Code", zap.Error(err))
			return util.NewUnknownTopicError("mapped from EventHubErrorCodeUnknown")
		} else if errorCode == constants.EventHubErrorCodeParseFailure {
			c.logger.Error("Failed To Create EventHub - Failed To Parse Error Code", zap.Error(err))
			return util.NewUnknownTopicError("mapped from EventHubErrorCodeParseFailure")
		} else {
			c.logger.Error("Failed To Create EventHub - Received Unmapped Error Code", zap.Error(err))
			return util.NewUnknownTopicError(fmt.Sprintf("received unmapped eventhub error code '%d'", errorCode))
		}
	}

	// Increment Count & Return Success!
	return util.NewTopicError(sarama.ErrNoError, "successfully created topic")
}

// Delete A Single Topic (EventHub) Via The Azure EventHub API
func (c *EventHubAdminClient) DeleteTopic(ctx context.Context, topicName string) *sarama.TopicError {

	// Azure EventHub Delete API Does NOT Accept Any Parameters So The Kafka DeleteTopicsAdminOptions Are Ignored

	// If The HubManager Is Not Valid Then Return Error
	if c.hubManager == nil {
		c.logger.Warn("Failed To Find EventHub Namespace With Valid HubManager - Skipping Topic Deletion", zap.String("Topic", topicName))
		return util.NewTopicError(sarama.ErrInvalidConfig, fmt.Sprintf("azure namespace has invalid HubManager - unable to delete EventHub '%s'", topicName))
	}

	// Delete The Specified Topic (EventHub)
	err := c.hubManager.Delete(ctx, topicName)
	if err != nil {

		// Delete API Returns Success For Non-Existent Topics - Nothing To Map - Just Return Error
		c.logger.Error("Failed To Delete EventHub", zap.String("TopicName", topicName), zap.Error(err))
		return util.NewTopicError(sarama.ErrUnknown, err.Error())
	}

	// Return Success!
	return util.NewTopicError(sarama.ErrNoError, "successfully deleted topic")
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
			errorCode, err = strconv.Atoi(matches[2])
			if err != nil {
				errorCode = constants.EventHubErrorCodeParseFailure
			}

			// Apply Negation If Present
			if matches[1] == "-" {
				errorCode = errorCode * -1
			}
		}
	}

	// Return The Error Code
	return errorCode
}
