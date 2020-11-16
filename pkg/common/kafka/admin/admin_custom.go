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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/common/kafka/admin/custom"
	adminutil "knative.dev/eventing-kafka/pkg/common/kafka/admin/util"
	"knative.dev/pkg/logging"
)

//
// Custom Kafka AdminClient Implementation (REST Sidecar)
//
// In order to provide the ability for third-parties to implement their own
// custom logic for the Creation / Deletion of Kafka topics, we are including
// this option.  It is a basic REST pass-through to well-defined endpoints on
// a sidecar container running in the eventing-kafka Controller Deployment.
//
// See the .../common/kafka/README.md for full details.
//

// Ensure The KafkaAdminClient Struct Implements The AdminClientInterface
var _ AdminClientInterface = &CustomAdminClient{}

// Custom AdminClient Definition
type CustomAdminClient struct {
	logger     *zap.Logger
	httpClient *http.Client
}

// Create A New Custom Kafka AdminClient Based On The Kafka Secret In The Specified K8S Namespace
func NewCustomAdminClient(ctx context.Context) (AdminClientInterface, error) {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx).Desugar()

	// Create A Custom HTTP Client With Custom Timeout
	httpClient := &http.Client{Timeout: custom.SidecarTimeout}

	// Create A Custom AdminClient (REST Sidecar Endpoint Pass-Through)
	customAdminClient := &CustomAdminClient{
		logger: logger,
		// TODO
		//namespace:   namespace,
		//kafkaSecret: kafkaSecret.Name,
		httpClient: httpClient,
	}

	// Return The Custom AdminClient
	logger.Debug("Successfully Created New Custom (Sidecar) AdminClient")
	return customAdminClient, nil
}

// Custom REST Pass-Through Function For Creating Topics
func (c *CustomAdminClient) CreateTopic(_ context.Context, topicName string, topicDetail *sarama.TopicDetail) *sarama.TopicError {

	// Create An Updated Logger With TopicName
	logger := c.logger.With(zap.String("TopicName", topicName))

	// Validate Topic
	if len(topicName) <= 0 || topicDetail == nil {
		logger.Warn("Received Empty/Nil Topic Configuration", zap.Any("TopicDetail", topicDetail))
		return adminutil.NewTopicError(sarama.ErrInvalidRequest, "received empty/nil topic name and / or detail")
	}

	// Convert The Sarama TopicDetail Into A Custom TopicDetail & Parse Into Request Body
	customTopicDetail := &custom.TopicDetail{}
	customTopicDetail.FromSaramaTopicDetail(topicDetail)

	// Create The Request Body From The Custom TopicDetail
	requestBody, err := json.Marshal(customTopicDetail)
	if err != nil {
		logger.Error("Failed To Marshall Create Topics Request Body", zap.Any("TopicDetail", topicDetail), zap.Error(err))
		return adminutil.NewTopicError(sarama.ErrInvalidConfig, fmt.Sprintf("failed to marshal request body for creation of topic '%s'", topicName))
	}

	// Create Topics URL For Sidecar Endpoint (No TopicName In POST URL!)
	url := c.sidecarTopicsUrl("")

	// Create The HTTP POST Request
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(requestBody))
	if err != nil {
		logger.Error("Failed To Create New HTTP POST Request", zap.String("URL", url), zap.Error(err))
		return adminutil.NewTopicError(sarama.ErrUnknown, fmt.Sprintf("failed to create new http request for creation of topic '%s'", topicName))
	}

	// Populate Required Headers
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(custom.TopicNameHeader, topicName)

	// Make The HTTP Request
	response, err := c.httpClient.Do(request)
	defer c.safeCloseHTTPResponseBody(response)
	if err != nil {
		logger.Error("HTTP POST Request To Create Topic Failed", zap.Error(err))
		return adminutil.NewTopicError(sarama.ErrNetworkException, fmt.Sprintf("failed to make http request for creation of topic '%s'", topicName))
	}

	// Map The HTTP Response Into A Sarama TopicError & Return
	return c.mapHttpResponse("create", response)
}

// Custom REST Pass-Through Function For Deleting Topics
func (c *CustomAdminClient) DeleteTopic(_ context.Context, topicName string) *sarama.TopicError {

	// Create An Updated Logger With TopicName
	logger := c.logger.With(zap.String("TopicName", topicName))

	// Validate The Topic
	if len(topicName) <= 0 {
		logger.Warn("Received Empty/Nil Topic Configuration")
		return adminutil.NewTopicError(sarama.ErrInvalidRequest, "received empty/nil topic name")
	}

	// Create Topics URL For Sidecar Endpoint (TopicName In DELETE URL!)
	url := c.sidecarTopicsUrl(topicName)

	// Create The HTTP POST Request
	request, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		logger.Error("Failed To Create New HTTP POST Request", zap.String("URL", url), zap.Error(err))
		return adminutil.NewTopicError(sarama.ErrUnknown, fmt.Sprintf("failed to create new http request for creation of topic '%s'", topicName))
	}

	// Make The HTTP Request
	response, err := c.httpClient.Do(request)
	defer c.safeCloseHTTPResponseBody(response)
	if err != nil {
		logger.Error("HTTP DELETE Request To Delete Topic Failed", zap.Error(err))
		return adminutil.NewTopicError(sarama.ErrNetworkException, fmt.Sprintf("failed to make http request for deletion of topic '%s'", topicName))
	}

	// Map The HTTP Response Into A Sarama TopicError & Return
	return c.mapHttpResponse("delete", response)
}

// Custom REST Pass-Through Function For Closing The Admin Client
func (c *CustomAdminClient) Close() error {
	return nil // Nothing to "close" in the Custom implementation (just a REST client) so this is just a compatibility no-op.
}

//// Get The K8S Secret With Kafka Credentials For The Specified Topic Name
//func (c *CustomAdminClient) GetKafkaSecretName(_ string) string {
//	return c.kafkaSecret // Only supports 1 topic so just return Kafka Secret name ; )
//}

// Safely Close The Specified HTTP Response Body
func (c *CustomAdminClient) safeCloseHTTPResponseBody(response *http.Response) {
	if response != nil && response.Body != nil {
		err := response.Body.Close()
		if err != nil {
			c.logger.Error("Failed To Close HTTP Response Body", zap.Error(err))
		}
	}
}

// Get The Expected Topics URL For The Custom Sidecar Implementation
func (c *CustomAdminClient) sidecarTopicsUrl(topicName string) string {
	topicsUrl := "http://" + custom.SidecarHost + ":" + custom.SidecarPort + custom.TopicsPath
	if len(topicName) > 0 {
		topicsUrl = topicsUrl + "/" + topicName
	}
	return topicsUrl
}

//
// Utility Function For Mapping Response Codes To Sarama TopicError Struct
//
// This is by definition an imperfect mapping of the custom sidecar's
// HTTP Response into a Kafka Server Response Code.  There are inherently
// different types of failures in each use case.  The important thing
// is that the controllers reconciliation of these errors are handled
// correctly and that the error is traceable to the unique response code.
//
func (c *CustomAdminClient) mapHttpResponse(operation string, response *http.Response) *sarama.TopicError {

	// Verify There Is A Response
	if response != nil {

		// Get The Response's Status Code
		statusCode := response.StatusCode

		// Read The Response Body & Convert To String
		responseBodyBytes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			c.logger.Warn("Failed To Parse Response Body", zap.Error(err))
		}
		responseBodyString := string(responseBodyBytes)

		// Separate Success & Error Response Codes
		switch {
		case statusCode >= 200 && statusCode <= 299:
			return adminutil.NewTopicError(sarama.ErrNoError, fmt.Sprintf("custom sidecar topic '%s' operation succeeded with status code '%d' and body '%s'", operation, statusCode, responseBodyString))
		case statusCode == 404 && operation == "delete": // 404 Not Found Indicates Topic Does Not Exist In Delete Operation
			return adminutil.NewTopicError(sarama.ErrUnknownTopicOrPartition, fmt.Sprintf("custom sidecar topic '%s' operation returned status code '%d' and body '%s'", operation, statusCode, responseBodyString))
		case statusCode == 409 && operation == "create": // 409 Conflict Indicates Topic Already Exists In Create Operation
			return adminutil.NewTopicError(sarama.ErrTopicAlreadyExists, fmt.Sprintf("custom sidecar topic '%s' operation returned status code '%d' and body '%s'", operation, statusCode, responseBodyString))
		default:
			return adminutil.NewTopicError(sarama.ErrInvalidRequest, fmt.Sprintf("custom sidecar topic '%s' operation failed with status code '%d' and body '%s'", operation, statusCode, responseBodyString))
		}

	} else {

		// No Response - Return Error
		return adminutil.NewTopicError(sarama.ErrUnknown, "received nil http response")
	}
}
