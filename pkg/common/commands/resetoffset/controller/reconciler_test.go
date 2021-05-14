/*
Copyright 2021 The Knative Authors

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

package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	fakekafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	resetoffsetreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/kafka/v1alpha1/resetoffset"
	controllertesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/controller/testing"
	refmapperstesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers/testing"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

// Test The Reconcile Functionality
func TestReconcile(t *testing.T) {

	// Test Data
	logger := logtesting.TestLogger(t)

	kafkaBrokers := []string{controllertesting.Brokers}
	saramaConfig := sarama.NewConfig()
	topicName := controllertesting.TopicName
	groupId := controllertesting.GroupId

	partition := int32(0)
	oldOffset := int64(100)
	newOffset := oldOffset - 50

	offsetTime := sarama.OffsetOldest
	metadata := formatOffsetMetaData(offsetTime)
	invalidOffsetTime := "foo"

	offsetMappings := []kafkav1alpha1.OffsetMapping{
		{Partition: 0, OldOffset: oldOffset, NewOffset: newOffset},
	}

	testErr := fmt.Errorf("test-error")

	// Define The ResetOffset Reconciler Test Cases
	commontesting.SetTestEnvironment(t)
	tableTest := TableTest{

		//
		// Key Tests
		//

		{
			Name: "Bad Key",
			Key:  "too/many/parts",
		},
		{
			Name: "Key Not Found",
			Key:  "foo/not-found",
		},

		//
		// Success Tests
		//

		{
			Name:    "Full Reconciliation Success",
			Key:     controllertesting.ResetOffsetKey,
			Objects: []runtime.Object{controllertesting.NewResetOffset(controllertesting.WithFinalizer)},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewResetOffset(
						controllertesting.WithFinalizer,
						controllertesting.WithStatusTopic(topicName),
						controllertesting.WithStatusGroup(groupId),
						controllertesting.WithStatusPartitions(offsetMappings),
						controllertesting.WithStatusRefMapped(true),
						controllertesting.WithStatusResetInitiated(true),
						controllertesting.WithStatusConsumerGroupsStopped(true),
						controllertesting.WithStatusOffsetsUpdated(true),
						controllertesting.WithStatusConsumerGroupsStarted(true)),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, ResetOffsetMappedRef.String(), "Successfully mapped 'ref' to Kafka Topic and Group"),
				Eventf(corev1.EventTypeNormal, ResetOffsetParsedTime.String(), "Successfully parsed Sarama offset time from Spec"),
				Eventf(corev1.EventTypeNormal, ResetOffsetUpdatedOffsets.String(), "Successfully updated offsets of all partitions"),
				Eventf(corev1.EventTypeNormal, ResetOffsetReconciled.String(), "Reconciled successfully"),
			},
		},

		//
		// "Skipping" Tests
		//

		{
			Name: "Skipping Previously Initiated",
			Key:  controllertesting.ResetOffsetKey,
			Objects: []runtime.Object{
				controllertesting.NewResetOffset(controllertesting.WithFinalizer,
					controllertesting.WithStatusResetInitiated(true)),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewResetOffset(
						controllertesting.WithFinalizer,
						controllertesting.WithStatusInitialized,
						controllertesting.WithStatusResetInitiated(true)),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, ResetOffsetSkipped.String(), "Skipped previously executed ResetOffset"),
			},
		},
		{
			Name: "Skipping Previously Completed",
			Key:  controllertesting.ResetOffsetKey,
			Objects: []runtime.Object{
				controllertesting.NewResetOffset(
					controllertesting.WithFinalizer,
					controllertesting.WithStatusRefMapped(true),
					controllertesting.WithStatusResetInitiated(true),
					controllertesting.WithStatusConsumerGroupsStopped(true),
					controllertesting.WithStatusOffsetsUpdated(true),
					controllertesting.WithStatusConsumerGroupsStarted(true)),
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, ResetOffsetSkipped.String(), "Skipped previously executed ResetOffset"),
			},
		},
		{
			Name: "Skipping Invalid",
			Key:  controllertesting.ResetOffsetKey,
			Objects: []runtime.Object{
				controllertesting.NewResetOffset(
					controllertesting.WithFinalizer,
					controllertesting.WithSpecOffsetTime(invalidOffsetTime)),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewResetOffset(
						controllertesting.WithFinalizer,
						controllertesting.WithSpecOffsetTime(invalidOffsetTime),
						controllertesting.WithStatusInitialized),
				},
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, ResetOffsetSkipped.String(), "Skipping invalid ResetOffset"),
				Eventf(corev1.EventTypeWarning, "UpdateFailed", "Failed to update status for \"resetoffset-name\": invalid value: foo: spec.offset"),
			},
		},

		//
		// Error Tests
		//

		{
			Name:          "MapRef Error",
			Key:           controllertesting.ResetOffsetKey,
			Objects:       []runtime.Object{controllertesting.NewResetOffset(controllertesting.WithFinalizer)},
			OtherTestData: map[string]interface{}{"MapRefErr": testErr},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewResetOffset(
						controllertesting.WithFinalizer,
						controllertesting.WithStatusInitialized,
						controllertesting.WithStatusRefMapped(false, "FailedToMapRef", "Failed to map 'ref' to Kafka Topic and Group: test-error")),
				},
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, ResetOffsetMappedRef.String(), "Failed to map 'ref' to Kafka Topic and Group"),
				Eventf(corev1.EventTypeWarning, "InternalError", testErr.Error()),
			},
		},
		{
			Name:          "Reconcile Offsets Error",
			Key:           controllertesting.ResetOffsetKey,
			Objects:       []runtime.Object{controllertesting.NewResetOffset(controllertesting.WithFinalizer)},
			OtherTestData: map[string]interface{}{"SaramaNewClientFnErr": testErr},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewResetOffset(
						controllertesting.WithFinalizer,
						controllertesting.WithStatusInitialized,
						controllertesting.WithStatusTopic(topicName),
						controllertesting.WithStatusGroup(groupId),
						controllertesting.WithStatusRefMapped(true),
						controllertesting.WithStatusResetInitiated(true),
						controllertesting.WithStatusConsumerGroupsStopped(true),
						controllertesting.WithStatusOffsetsUpdated(false, "FailedToUpdateOffsets", "Failed to update offsets of one or more partitions: test-error")),
				},
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, ResetOffsetMappedRef.String(), "Successfully mapped 'ref' to Kafka Topic and Group"),
				Eventf(corev1.EventTypeNormal, ResetOffsetParsedTime.String(), "Successfully parsed Sarama offset time from Spec"),
				Eventf(corev1.EventTypeWarning, ResetOffsetUpdatedOffsets.String(), "Failed to update offsets of one or more partitions"),
				Eventf(corev1.EventTypeWarning, "InternalError", testErr.Error()),
			},
		},
	}

	// Restore Sarama Client / OffsetManager Stubs After Test Completion
	defer restoreSaramaNewClientFn()
	defer restoreSaramaNewOffsetManagerFromClientFn()

	// Run The TableTest Using The ResetOffset Reconciler Provided By The Factory
	tableTest.Test(t, controllertesting.MakeFactory(func(ctx context.Context, listers *controllertesting.Listers, cmw configmap.Watcher, options map[string]interface{}) controller.Reconciler {

		// Check MapRefErr Option
		var mapRefErr error
		if options != nil {
			mapRefErrOption := options["MapRefErr"]
			if err, ok := mapRefErrOption.(error); ok {
				mapRefErr = err
			}
		}

		// Create A Mock ResetOffset RefMapper
		mockResetOffsetRefMapper := &refmapperstesting.MockResetOffsetRefMapper{}
		mockResetOffsetRefMapper.On("MapRef", mock.Anything).Return(controllertesting.TopicName, controllertesting.GroupId, mapRefErr)

		// Check SaramaNewClientFnErr Option
		var saramaNewClientFnErr error
		if options != nil {
			saramaNewClientFnErrOption := options["SaramaNewClientFnErr"]
			if err, ok := saramaNewClientFnErrOption.(error); ok {
				saramaNewClientFnErr = err
			}
		}

		// Mock & Stub "success" Sarama Client / OffsetManager
		mockClient := newSuccessSaramaClient(topicName, partition, offsetTime, newOffset)
		stubSaramaNewClientFn(t, kafkaBrokers, saramaConfig, mockClient, saramaNewClientFnErr)
		mockOffsetManager := newSuccessSaramaOffsetManager(topicName, partition, oldOffset, newOffset, metadata)
		stubSaramaNewOffsetManagerFromClientFn(t, groupId, mockClient, mockOffsetManager, nil)

		// Create The ResetOffset Reconciler Struct
		r := &Reconciler{
			kafkaBrokers:      kafkaBrokers,
			saramaConfig:      saramaConfig,
			resetoffsetLister: listers.GetResetOffsetLister(),
			refMapper:         mockResetOffsetRefMapper,
		}

		// Create / Return The Full Reconciler
		return resetoffsetreconciler.NewReconciler(ctx, logger, fakekafkaclient.Get(ctx), listers.GetResetOffsetLister(), controller.GetEventRecorder(ctx), r)

	}, logger.Desugar()))
}

func TestReconciler_updateKafkaConfig(t *testing.T) {
	// TODO - Implement Test Once Common ConfigMap Updating Is In Place ; )
}

//
// Sarama Mock Utilities
//

// newSuccessSaramaClient returns a "success" mock Sarama Client for the specified values.
func newSuccessSaramaClient(topicName string, partition int32, offsetTime int64, newOffset int64) sarama.Client {
	return controllertesting.NewMockClient(
		controllertesting.WithClientMockPartitions(topicName, []int32{partition}, nil),
		controllertesting.WithClientMockGetOffset(topicName, partition, offsetTime, newOffset, nil),
		controllertesting.WithClientMockClosed(false),
		controllertesting.WithClientMockClose(nil))
}

// newSuccessSaramaOffsetManager returns a "success" mock Sarama OffsetManager for the specified values.
func newSuccessSaramaOffsetManager(topicName string, partition int32, oldOffset int64, newOffset int64, metadata string, ) sarama.OffsetManager {

	mockPartitionOffsetManager := controllertesting.NewMockPartitionOffsetManager(
		controllertesting.WithPartitionOffsetManagerMockNextOffset(oldOffset, ""),
		controllertesting.WithPartitionOffsetManagerMockResetOffset(newOffset, metadata),
		controllertesting.WithPartitionOffsetManagerMockErrors(),
		controllertesting.WithPartitionOffsetManagerMockClose(nil))

	return controllertesting.NewMockOffsetManager(
		controllertesting.WithOffsetManagerMockManagePartition(topicName, partition, mockPartitionOffsetManager, nil),
		controllertesting.WithOffsetManagerMockCommit(),
		controllertesting.WithOffsetManagerMockClose(nil))
}
