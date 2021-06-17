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

package kafkachannel

import (
	"context"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/controller"

	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	controllertesting "knative.dev/eventing-kafka/pkg/channel/distributed/controller/testing"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
)

// Define The Topic TestCase Type
type TopicTestCase struct {
	Name            string
	Channel         *kafkav1beta1.KafkaChannel
	WantTopicDetail *sarama.TopicDetail
	MockErrorCode   sarama.KError
	WantError       string
	WantCreate      bool
	WantDelete      bool
}

//
// Test The Kafka Topic Reconciliation
//
// Ideally the Knative Eventing test runner implementation would have provided a hook for additional
// channel-type-specific (ie Kafka, NATS, etc) validation, but unfortunately it is solely focused
// on the K8S objects existing/not.  Therefore we're left to test the actual Topic handling separately.
//
func TestReconcileTopic(t *testing.T) {

	// Define & Initialize The TopicTestCases
	topicTestCases := []TopicTestCase{
		{
			Name: "Skip Deleted Topic",
			Channel: controllertesting.NewKafkaChannel(
				controllertesting.WithFinalizer,
				controllertesting.WithDeletionTimestamp,
				controllertesting.WithAddress,
				controllertesting.WithInitializedConditions,
				controllertesting.WithKafkaChannelServiceReady,
				controllertesting.WithReceiverServiceReady,
				controllertesting.WithReceiverDeploymentReady,
				controllertesting.WithDispatcherDeploymentReady,
			),
			WantCreate: false,
			WantDelete: false,
		},
		{
			Name: "Create New Topic",
			Channel: controllertesting.NewKafkaChannel(
				controllertesting.WithFinalizer,
				controllertesting.WithAddress,
				controllertesting.WithInitializedConditions,
				controllertesting.WithKafkaChannelServiceReady,
				controllertesting.WithReceiverServiceReady,
				controllertesting.WithReceiverDeploymentReady,
				controllertesting.WithDispatcherDeploymentReady,
			),
			WantCreate: true,
			WantDelete: false,
			WantTopicDetail: &sarama.TopicDetail{
				NumPartitions:     controllertesting.NumPartitions,
				ReplicationFactor: controllertesting.ReplicationFactor,
				ConfigEntries:     map[string]*string{commonconstants.KafkaTopicConfigRetentionMs: &controllertesting.RetentionMillisString},
			},
		},
		{
			Name: "Create Preexisting Topic",
			Channel: controllertesting.NewKafkaChannel(
				controllertesting.WithFinalizer,
				controllertesting.WithAddress,
				controllertesting.WithInitializedConditions,
				controllertesting.WithKafkaChannelServiceReady,
				controllertesting.WithReceiverServiceReady,
				controllertesting.WithReceiverDeploymentReady,
				controllertesting.WithDispatcherDeploymentReady,
			),
			WantCreate: true,
			WantDelete: false,
			WantTopicDetail: &sarama.TopicDetail{
				NumPartitions:     controllertesting.NumPartitions,
				ReplicationFactor: controllertesting.ReplicationFactor,
				ConfigEntries:     map[string]*string{commonconstants.KafkaTopicConfigRetentionMs: &controllertesting.RetentionMillisString},
			},
			MockErrorCode: sarama.ErrTopicAlreadyExists,
		},
		{
			Name: "Error Creating Topic",
			Channel: controllertesting.NewKafkaChannel(
				controllertesting.WithFinalizer,
				controllertesting.WithAddress,
				controllertesting.WithInitializedConditions,
				controllertesting.WithKafkaChannelServiceReady,
				controllertesting.WithReceiverServiceReady,
				controllertesting.WithReceiverDeploymentReady,
				controllertesting.WithDispatcherDeploymentReady,
			),
			WantCreate: true,
			WantDelete: false,
			WantTopicDetail: &sarama.TopicDetail{
				NumPartitions:     controllertesting.NumPartitions,
				ReplicationFactor: controllertesting.ReplicationFactor,
				ConfigEntries:     map[string]*string{commonconstants.KafkaTopicConfigRetentionMs: &controllertesting.RetentionMillisString},
			},
			MockErrorCode: sarama.ErrBrokerNotAvailable,
			WantError:     sarama.ErrBrokerNotAvailable.Error() + " - " + controllertesting.ErrorString,
		},
		{
			Name: "Delete Existing Topic",
			Channel: controllertesting.NewKafkaChannel(
				controllertesting.WithFinalizer,
				controllertesting.WithAddress,
				controllertesting.WithInitializedConditions,
				controllertesting.WithKafkaChannelServiceReady,
				controllertesting.WithReceiverServiceReady,
				controllertesting.WithReceiverDeploymentReady,
				controllertesting.WithDispatcherDeploymentReady,
			),
			WantCreate: false,
			WantDelete: true,
		},
		{
			Name: "Delete Nonexistent Topic",
			Channel: controllertesting.NewKafkaChannel(
				controllertesting.WithFinalizer,
				controllertesting.WithAddress,
				controllertesting.WithInitializedConditions,
				controllertesting.WithKafkaChannelServiceReady,
				controllertesting.WithReceiverServiceReady,
				controllertesting.WithReceiverDeploymentReady,
				controllertesting.WithDispatcherDeploymentReady,
			),
			WantCreate:    false,
			WantDelete:    true,
			MockErrorCode: sarama.ErrUnknownTopicOrPartition,
		},
		{
			Name: "Error Deleting Topic",
			Channel: controllertesting.NewKafkaChannel(
				controllertesting.WithFinalizer,
				controllertesting.WithAddress,
				controllertesting.WithInitializedConditions,
				controllertesting.WithKafkaChannelServiceReady,
				controllertesting.WithReceiverServiceReady,
				controllertesting.WithReceiverDeploymentReady,
				controllertesting.WithDispatcherDeploymentReady,
			),
			WantCreate:    false,
			WantDelete:    true,
			MockErrorCode: sarama.ErrBrokerNotAvailable,
			WantError:     sarama.ErrBrokerNotAvailable.Error() + " - " + controllertesting.ErrorString,
		},
	}

	// Run All The TopicTestCases
	for _, tc := range topicTestCases {
		t.Run(tc.Name, topicTestCaseFactory(tc))
	}
}

// Factory For Creating A Go Test Function For The Specified TopicTestCase
func topicTestCaseFactory(tc TopicTestCase) func(t *testing.T) {
	return func(t *testing.T) {

		// Setup Context With New Recorder For Testing
		recorder := record.NewBroadcaster().NewRecorder(scheme.Scheme, corev1.EventSource{Component: "TestEventSource"})
		ctx := controller.WithEventRecorder(context.TODO(), recorder)

		// Create A Mock Kafka AdminClient For Current TopicTestCase
		mockAdminClient := createMockAdminClientForTestCase(t, tc)

		// Initialize The Reconciler For The Current TopicTestCase
		r := &Reconciler{
			adminClient: mockAdminClient,
			config:      controllertesting.NewConfig(),
		}

		// Track Any Error Responses
		var err error

		// Perform The Test (Create) - Normal Topic Reconciliation Called Indirectly From ReconcileKind()
		if tc.WantCreate {
			err = r.reconcileKafkaTopic(ctx, tc.Channel)
			if !mockAdminClient.CreateTopicsCalled() {
				t.Errorf("expected CreateTopics() called to be %t", tc.WantCreate)
			}
		}

		// Perform The Test (Delete) - Called By Knative FinalizeKind() Directly
		if tc.WantDelete {
			err = r.finalizeKafkaTopic(ctx, tc.Channel)
			if !mockAdminClient.DeleteTopicsCalled() {
				t.Errorf("expected DeleteTopics() called to be %t", tc.WantCreate)
			}
		}

		// Validate TestCase Expected Error State
		var errorString string
		if err != nil {
			errorString = err.Error()
		}
		if diff := cmp.Diff(tc.WantError, errorString); diff != "" {
			t.Errorf("unexpected error (-want, +got) = %v", diff)
		}
	}
}

// Create A Mock Kafka AdminClient For The Specified TopicTestCase
func createMockAdminClientForTestCase(t *testing.T, tc TopicTestCase) *controllertesting.MockAdminClient {

	// Setup Desired Mock ClusterAdmin Behavior From TopicTestCase
	return &controllertesting.MockAdminClient{

		// Mock CreateTopic Behavior - Validate Parameters & Return MockError
		MockCreateTopicFunc: func(ctx context.Context, topicName string, topicDetail *sarama.TopicDetail) *sarama.TopicError {
			if !tc.WantCreate {
				t.Error("Unexpected CreateTopics() Call")
			}
			if ctx == nil {
				t.Error("expected non nil context")
			}
			if topicName != controllertesting.TopicName {
				t.Errorf("unexpected topic name '%s'", topicName)
			}
			if diff := cmp.Diff(tc.WantTopicDetail, topicDetail); diff != "" {
				t.Errorf("expected TopicDetail: %+v", diff)
			}
			errMsg := controllertesting.SuccessString
			if tc.MockErrorCode != sarama.ErrNoError {
				errMsg = controllertesting.ErrorString
			}
			topicError := &sarama.TopicError{
				Err:    tc.MockErrorCode,
				ErrMsg: &errMsg,
			}
			return topicError
		},

		// Mock DeleteTopic Behavior - Validate Parameters & Return MockError
		MockDeleteTopicFunc: func(ctx context.Context, topicName string) *sarama.TopicError {
			if !tc.WantDelete {
				t.Error("Unexpected DeleteTopics() Call")
			}
			if ctx == nil {
				t.Error("expected non nil context")
			}
			if topicName != controllertesting.TopicName {
				t.Errorf("unexpected topic name '%s'", topicName)
			}
			errMsg := controllertesting.SuccessString
			if tc.MockErrorCode != sarama.ErrNoError {
				errMsg = controllertesting.ErrorString
			}
			topicError := &sarama.TopicError{
				Err:    tc.MockErrorCode,
				ErrMsg: &errMsg,
			}
			return topicError
		},
	}
}
