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
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
	ctrl "knative.dev/control-protocol/pkg"
	ctrlmessage "knative.dev/control-protocol/pkg/message"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/system"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	fakekafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	resetoffsetreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/kafka/v1alpha1/resetoffset"
	controllertesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/controller/testing"
	refmapperstesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers/testing"
	"knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
	controlprotocoltesting "knative.dev/eventing-kafka/pkg/common/controlprotocol/testing"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

// Test The Reconcile Functionality
func TestReconcile(t *testing.T) {

	// Test Data
	logger := logtesting.TestLogger(t)

	reconcilerUID := types.UID(uuid.NewString())

	kafkaBrokers := []string{controllertesting.Brokers}
	saramaConfig := sarama.NewConfig()
	topicName := controllertesting.TopicName
	groupId := controllertesting.GroupId

	partition := int32(0)
	oldOffset := int64(100)
	newOffset := oldOffset - 50

	offsetTime := sarama.OffsetOldest
	metadata := formatOffsetMetaData(offsetTime)

	offsetMappings := []kafkav1alpha1.OffsetMapping{
		{Partition: 0, OldOffset: oldOffset, NewOffset: newOffset},
	}

	podIp := "1.2.3.4"
	pods := []*corev1.Pod{{Status: corev1.PodStatus{PodIP: podIp}}}
	podIpPort := fmt.Sprintf("%s:%d", podIp, controlprotocol.ServerPort)
	podIpPorts := []string{podIpPort}

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
						controllertesting.WithStatusAcquireDataPlaneServices(true),
						controllertesting.WithStatusConsumerGroupsStopped(true),
						controllertesting.WithStatusOffsetsUpdated(true),
						controllertesting.WithStatusConsumerGroupsStarted(true)),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, ResetOffsetReconciled.String(), "Reconciled successfully"),
			},
		},

		//
		// "Skipping" Tests
		//

		{
			Name: "Skipping Previously Succeeded",
			Key:  controllertesting.ResetOffsetKey,
			Objects: []runtime.Object{
				controllertesting.NewResetOffset(
					controllertesting.WithFinalizer,
					controllertesting.WithStatusRefMapped(true),
					controllertesting.WithStatusAcquireDataPlaneServices(true),
					controllertesting.WithStatusConsumerGroupsStopped(true),
					controllertesting.WithStatusOffsetsUpdated(true),
					controllertesting.WithStatusConsumerGroupsStarted(true)),
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, ResetOffsetSkipped.String(), "Skipped previously successful ResetOffset"),
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
				Eventf(corev1.EventTypeWarning, "InternalError", fmt.Sprintf("failed to map 'ref' to Kafka Topic and Group: %v", testErr.Error())),
			},
		},
		{
			Name:          "Reconcile DataPlane Services Error",
			Key:           controllertesting.ResetOffsetKey,
			Objects:       []runtime.Object{controllertesting.NewResetOffset(controllertesting.WithFinalizer)},
			OtherTestData: map[string]interface{}{"ReconcileConnectionsErr": testErr},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewResetOffset(
						controllertesting.WithFinalizer,
						controllertesting.WithStatusInitialized,
						controllertesting.WithStatusTopic(topicName),
						controllertesting.WithStatusGroup(groupId),
						controllertesting.WithStatusRefMapped(true),
						controllertesting.WithStatusAcquireDataPlaneServices(false, "FailedToAcquireDataPlaneServices", "Failed to reconciler DataPlane Services from ConnectionPool: test-error")),
				},
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", fmt.Sprintf("failed to reconcile DataPlane Services from ConnectionPool: %v", testErr.Error())),
			},
		},
		{
			Name:          "Stop ConsumerGroups Error",
			Key:           controllertesting.ResetOffsetKey,
			Objects:       []runtime.Object{controllertesting.NewResetOffset(controllertesting.WithFinalizer)},
			OtherTestData: map[string]interface{}{"StopConsumerGroupsErr": testErr},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewResetOffset(
						controllertesting.WithFinalizer,
						controllertesting.WithStatusInitialized,
						controllertesting.WithStatusTopic(topicName),
						controllertesting.WithStatusGroup(groupId),
						controllertesting.WithStatusRefMapped(true),
						controllertesting.WithStatusAcquireDataPlaneServices(true),
						controllertesting.WithStatusConsumerGroupsStopped(false, "FailedToStopConsumerGroups", fmt.Sprintf("Failed to stop one or more ConsumerGroups: failed to send ConsumerGroup AsyncCommand '3866008807': %v", testErr.Error()))),
				},
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", fmt.Sprintf("failed to stop one or more ConsumerGroups: failed to send ConsumerGroup AsyncCommand '3866008807': %v", testErr.Error())),
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
						controllertesting.WithStatusAcquireDataPlaneServices(true),
						controllertesting.WithStatusConsumerGroupsStopped(true),
						controllertesting.WithStatusOffsetsUpdated(false, "FailedToUpdateOffsets", "Failed to update Offsets of ConsumerGroup Partitions: test-error")),
				},
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", fmt.Sprintf("failed to update Offsets of ConsumerGroup Partitions: %v", testErr.Error())),
			},
		},
		{
			Name:          "Start ConsumerGroups Error",
			Key:           controllertesting.ResetOffsetKey,
			Objects:       []runtime.Object{controllertesting.NewResetOffset(controllertesting.WithFinalizer)},
			OtherTestData: map[string]interface{}{"StartConsumerGroupsErr": testErr},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: controllertesting.NewResetOffset(
						controllertesting.WithFinalizer,
						controllertesting.WithStatusInitialized,
						controllertesting.WithStatusTopic(topicName),
						controllertesting.WithStatusGroup(groupId),
						controllertesting.WithStatusPartitions(offsetMappings),
						controllertesting.WithStatusRefMapped(true),
						controllertesting.WithStatusAcquireDataPlaneServices(true),
						controllertesting.WithStatusConsumerGroupsStopped(true),
						controllertesting.WithStatusOffsetsUpdated(true),
						controllertesting.WithStatusConsumerGroupsStarted(false, "FailedToStartConsumerGroups", fmt.Sprintf("Failed to restart one or more ConsumerGroups: failed to send ConsumerGroup AsyncCommand '3899564045': %v", testErr))),
				},
			},
			WantErr: true,
			WantEvents: []string{
				Eventf(corev1.EventTypeWarning, "InternalError", fmt.Sprintf("failed to restart one or more ConsumerGroups: failed to send ConsumerGroup AsyncCommand '3899564045': %v", testErr.Error())),
			},
		},

		//
		// Finalize Tests
		//

		{
			Name: "Full Finalization Success",
			Key:  controllertesting.ResetOffsetKey,
			Objects: []runtime.Object{controllertesting.NewResetOffset(
				controllertesting.WithFinalizer,
				controllertesting.WithDeletionTimestamp,
				controllertesting.WithStatusTopic(topicName),
				controllertesting.WithStatusGroup(groupId),
				controllertesting.WithStatusPartitions(offsetMappings),
				controllertesting.WithStatusRefMapped(true),
				controllertesting.WithStatusAcquireDataPlaneServices(true),
				controllertesting.WithStatusConsumerGroupsStopped(true),
				controllertesting.WithStatusOffsetsUpdated(true),
				controllertesting.WithStatusConsumerGroupsStarted(true))},
			WantPatches: []clientgotesting.PatchActionImpl{
				{
					ActionImpl: clientgotesting.ActionImpl{
						Namespace:   controllertesting.ResetOffsetNamespace,
						Verb:        "patch",
						Resource:    schema.GroupVersionResource{Group: kafkav1alpha1.SchemeGroupVersion.Group, Version: kafkav1alpha1.SchemeGroupVersion.Version, Resource: "resetoffset"},
						Subresource: "",
					},
					Name:      controllertesting.ResetOffsetName,
					PatchType: "application/merge-patch+json",
					Patch:     []byte(`{"metadata":{"finalizers":[],"resourceVersion":""}}`),
				},
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, "FinalizerUpdate", "Updated \"resetoffset-name\" finalizers"),
				Eventf(corev1.EventTypeNormal, ResetOffsetFinalized.String(), "Finalized successfully"),
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

		// Check ReconcileConnectionsErr Option
		var reconcileConnectionsErr error
		if options != nil {
			mapRefErrOption := options["ReconcileConnectionsErr"]
			if err, ok := mapRefErrOption.(error); ok {
				reconcileConnectionsErr = err
			}
		}

		// Check StopConsumerGroupsErr Option
		var stopConsumerGroupsErr error
		if options != nil {
			mapRefErrOption := options["StopConsumerGroupsErr"]
			if err, ok := mapRefErrOption.(error); ok {
				stopConsumerGroupsErr = err
			}
		}

		// Check StartConsumerGroupsErr Option
		var startConsumerGroupsErr error
		if options != nil {
			mapRefErrOption := options["StartConsumerGroupsErr"]
			if err, ok := mapRefErrOption.(error); ok {
				startConsumerGroupsErr = err
			}
		}

		// Create A RefInfo For Testing
		refInfo := refmapperstesting.NewRefInfo()

		// Create A Mock PodLister
		mockPodNamespaceLister := &controllertesting.MockPodNamespaceLister{}
		mockPodNamespaceLister.On("List", labels.Set(refInfo.DataPlaneLabels).AsSelector()).Return(pods, nil)
		mockPodLister := &controllertesting.MockPodLister{}
		mockPodLister.On("Pods", refInfo.DataPlaneNamespace).Return(mockPodNamespaceLister)

		// Create Test ConsumerGroupAsyncCommands
		lockToken := GenerateLockToken(reconcilerUID, "") // TableTest Resources Don't Have A UID And Setting Them Above Doesn't Help
		stopCommandLock := commands.NewCommandLock(lockToken, asyncCommandLockTimeout, true, false)
		startCommandLock := commands.NewCommandLock(lockToken, 0, false, true)
		stopCommandId, err := GenerateCommandId(controllertesting.NewResetOffset(), podIp, commands.StopConsumerGroupOpCode)
		assert.Nil(t, err)
		startCommandId, err := GenerateCommandId(controllertesting.NewResetOffset(), podIp, commands.StartConsumerGroupOpCode)
		assert.Nil(t, err)
		stopConsumerGroupAsyncCommand := &commands.ConsumerGroupAsyncCommand{Version: 1, CommandId: stopCommandId, TopicName: refInfo.TopicName, GroupId: refInfo.GroupId, Lock: stopCommandLock}
		startConsumerGroupAsyncCommand := &commands.ConsumerGroupAsyncCommand{Version: 1, CommandId: startCommandId, TopicName: refInfo.TopicName, GroupId: refInfo.GroupId, Lock: startCommandLock}

		// Create The Mock Service To Test Against
		mockDataPlaneService := &controlprotocoltesting.MockService{}
		mockDataPlaneService.On("SendAndWaitForAck", commands.StopConsumerGroupOpCode, stopConsumerGroupAsyncCommand).Return(stopConsumerGroupsErr)
		mockDataPlaneService.On("SendAndWaitForAck", commands.StartConsumerGroupOpCode, startConsumerGroupAsyncCommand).Return(startConsumerGroupsErr)
		services := map[string]ctrl.Service{podIp: mockDataPlaneService}

		// Create A Mock Control-Protocol ConnectionPool
		mockConnectionPool := &controlprotocoltesting.MockConnectionPool{}
		mockConnectionPool.On("ReconcileConnections",
			mock.Anything,
			refInfo.ConnectionPoolKey,
			podIpPorts,
			mock.AnythingOfType("func(string, control.Service)"),
			mock.AnythingOfType("func(string)")).
			Return(services, reconcileConnectionsErr)

		// Create A Mock ResetOffset RefMapper
		mockResetOffsetRefMapper := &refmapperstesting.MockResetOffsetRefMapper{}
		mockResetOffsetRefMapper.On("MapRef", mock.Anything).Return(refInfo, mapRefErr)

		// Create A Mock Control-Protocol AsyncCommandNotificationStore & Assign To Reconciler
		resetOffsetNamespacedName := controllertesting.NewResetOffsetNamespacedName()
		successResult := &ctrlmessage.AsyncCommandResult{}
		mockAsyncCommandNotificationStore := &controlprotocoltesting.MockAsyncCommandNotificationStore{}
		mockAsyncCommandNotificationStore.On("GetCommandResult", resetOffsetNamespacedName, podIp, stopConsumerGroupAsyncCommand).Return(successResult)
		mockAsyncCommandNotificationStore.On("GetCommandResult", resetOffsetNamespacedName, podIp, startConsumerGroupAsyncCommand).Return(successResult)
		mockAsyncCommandNotificationStore.On("CleanPodsNotifications", types.NamespacedName{
			Namespace: controllertesting.ResetOffsetNamespace,
			Name:      controllertesting.ResetOffsetName,
		}).Return()

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
			uid:                           reconcilerUID,
			kafkaBrokers:                  kafkaBrokers,
			saramaConfig:                  saramaConfig,
			podLister:                     mockPodLister,
			resetoffsetLister:             listers.GetResetOffsetLister(),
			refMapper:                     mockResetOffsetRefMapper,
			connectionPool:                mockConnectionPool,
			asyncCommandNotificationStore: mockAsyncCommandNotificationStore,
		}

		// Create / Return The Full Reconciler
		return resetoffsetreconciler.NewReconciler(ctx, logger, fakekafkaclient.Get(ctx), listers.GetResetOffsetLister(), controller.GetEventRecorder(ctx), r)

	}, logger.Desugar()))
}

func TestReconciler_updateKafkaConfig(t *testing.T) {

	// Define EKConfig String For Use In Test (Note - Preserve Indentation!)
	ekConfigString := strings.Replace(commontesting.TestEKConfig, "kafka:", `kafka:
  authSecretName: `+commontesting.SecretName+`
  authSecretNamespace: `+system.Namespace(), 1)

	// Define Brokers
	defaultKafkaBrokers := []string{commontesting.BrokerString}

	// Create A Sarama Config To Match commontesting.OldSaramaConfig
	oldSaramaConfig := sarama.NewConfig()
	oldSaramaConfig.ClientID = Component
	oldSaramaConfig.Net.TLS.Enable = true
	oldSaramaConfig.Net.SASL.Version = 1
	oldSaramaConfig.Net.SASL.Enable = true
	oldSaramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	oldSaramaConfig.Net.SASL.User = commontesting.OldAuthUsername
	oldSaramaConfig.Net.SASL.Password = commontesting.OldAuthPassword
	oldSaramaConfig.Producer.Return.Successes = true
	oldSaramaConfig.Consumer.Return.Errors = true
	oldSaramaConfig.Consumer.Offsets.AutoCommit.Enable = false
	oldSaramaConfig.Consumer.Offsets.Initial = 0
	oldSaramaConfig.Metadata.RefreshFrequency = 300000000000

	// Define The Test Cases
	tests := []struct {
		name                 string
		configMap            *corev1.ConfigMap
		initialKafkaBrokers  []string
		expectedKafkaBrokers []string
		initialSaramaConfig  *sarama.Config
		expectedSaramaConfig *sarama.Config
	}{
		{
			name:                 "Nil ConfigMap",
			configMap:            nil,
			initialKafkaBrokers:  defaultKafkaBrokers,
			expectedKafkaBrokers: defaultKafkaBrokers,
			initialSaramaConfig:  oldSaramaConfig,
			expectedSaramaConfig: oldSaramaConfig,
		},
		{
			name:                 "Nil ConfigMap Data",
			configMap:            &corev1.ConfigMap{},
			initialKafkaBrokers:  defaultKafkaBrokers,
			expectedKafkaBrokers: defaultKafkaBrokers,
			initialSaramaConfig:  oldSaramaConfig,
			expectedSaramaConfig: oldSaramaConfig,
		},
		{
			name:                 "Load Settings Error",
			configMap:            &corev1.ConfigMap{Data: map[string]string{constants.EventingKafkaSettingsConfigKey: "\tInvalid"}},
			initialKafkaBrokers:  defaultKafkaBrokers,
			expectedKafkaBrokers: defaultKafkaBrokers,
			initialSaramaConfig:  oldSaramaConfig,
			expectedSaramaConfig: oldSaramaConfig,
		},

		{
			name: "Success",
			configMap: &corev1.ConfigMap{Data: map[string]string{
				constants.VersionConfigKey:               constants.CurrentConfigVersion,
				constants.EventingKafkaSettingsConfigKey: ekConfigString,
				constants.SaramaSettingsConfigKey:        commontesting.OldSaramaConfig}},
			initialKafkaBrokers:  nil,
			expectedKafkaBrokers: defaultKafkaBrokers,
			initialSaramaConfig:  nil,
			expectedSaramaConfig: oldSaramaConfig,
		},
	}

	// Create A Context With Fake K8S Client To Return Kafka Secret
	secret := commontesting.GetTestSaramaSecret(
		commontesting.SecretName,
		commontesting.OldAuthUsername,
		commontesting.OldAuthPassword,
		commontesting.OldAuthNamespace,
		commontesting.OldAuthSaslType)
	fakeK8sClient := fake.NewSimpleClientset(secret)
	ctx := context.WithValue(context.TODO(), kubeclient.Key{}, fakeK8sClient)

	// Execute The Test Cases
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// Create The Reconciler To Test
			r := &Reconciler{
				kafkaBrokers: test.initialKafkaBrokers,
				saramaConfig: test.initialSaramaConfig,
			}

			// Perform The Test
			r.updateKafkaConfig(ctx, test.configMap)

			// Verify Reconciler Updated As Expected (Can't Compare Full Sarama.Config Instances Directly Due To Functions Fields)
			assert.Equal(t, test.expectedKafkaBrokers, r.kafkaBrokers)
			assert.Equal(t, Component, test.expectedSaramaConfig.ClientID)
			assert.Equal(t, test.expectedSaramaConfig.Net, r.saramaConfig.Net)
			assert.Equal(t, test.expectedSaramaConfig.Consumer, r.saramaConfig.Consumer)

			// Verify Fixed Configuration
			if r.saramaConfig != nil {
				assert.True(t, r.saramaConfig.Consumer.Return.Errors)
				assert.False(t, r.saramaConfig.Consumer.Offsets.AutoCommit.Enable)
			}
		})
	}

	// Verify that a nil reconciler doesn't panic
	var nilReconciler *Reconciler
	//goland:noinspection GoNilness
	nilReconciler.updateKafkaConfig(context.TODO(), nil)
	assert.Nil(t, nilReconciler)
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
func newSuccessSaramaOffsetManager(topicName string, partition int32, oldOffset int64, newOffset int64, metadata string) sarama.OffsetManager {

	mockPartitionOffsetManager := controllertesting.NewMockPartitionOffsetManager(
		controllertesting.WithPartitionOffsetManagerMockNextOffset(oldOffset, ""),
		controllertesting.WithPartitionOffsetManagerMockResetOffset(newOffset, metadata),
		controllertesting.WithPartitionOffsetManagerMockErrors(),
		controllertesting.WithPartitionOffsetManagerMockAsyncClose())

	return controllertesting.NewMockOffsetManager(
		controllertesting.WithOffsetManagerMockManagePartition(topicName, partition, mockPartitionOffsetManager, nil),
		controllertesting.WithOffsetManagerMockCommit(),
		controllertesting.WithOffsetManagerMockClose(nil))
}
