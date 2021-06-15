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
	"hash/fnv"
	"k8s.io/apimachinery/pkg/util/wait"
	"sync"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "knative.dev/control-protocol/pkg"
	ctrlmessage "knative.dev/control-protocol/pkg/message"
	ctrlreconciler "knative.dev/control-protocol/pkg/reconciler"
	"knative.dev/pkg/logging"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol/commands"
)

const (
	asyncCommandResultPollDuration   = 1 * time.Second // AsyncCommand Result Polling Sleep Duration
	asyncCommandResultPollTimeout    = 10 * time.Second
	asyncCommandResultPollIterations = 5 // AsyncCommand Result Polling Number Of Iterations
)

// reconcileDataPlaneServices updates the Reconciler ConnectionPool Services associated with the specified RefInfo.
func (r *Reconciler) reconcileDataPlaneServices(ctx context.Context, resetOffset *kafkav1alpha1.ResetOffset, refInfo *refmappers.RefInfo) (map[string]ctrl.Service, error) {

	// Get The Logger From Context
	logger := logging.FromContext(ctx).Desugar().With(zap.Any("RefInfo", refInfo))

	// Create A Control-Protocol PodIpGetter & Get The Pod IPs
	podIpGetter := ctrlreconciler.PodIpGetter{Lister: r.podLister}
	podIPs, err := podIpGetter.GetAllPodsIp(refInfo.DataPlaneNamespace, labels.Set(refInfo.DataPlaneLabels).AsSelector())
	if err != nil {
		logger.Error("Failed to getting data-plane Pod IPs", zap.Error(err))
		return nil, err
	}

	// Define Service Callback Functions To Manage The Reconciler AsyncCommandNotificationStore
	resetOffsetNamespacedName := types.NamespacedName{
		Namespace: resetOffset.GetNamespace(),
		Name:      resetOffset.GetName(),
	}
	newServiceCallbackFn := func(newHost string, service ctrl.Service) {
		logger.Debug("New Control-Protocol Service Callback", zap.String("Host", newHost))
		service.MessageHandler(r.asyncCommandNotificationStore.MessageHandler(resetOffsetNamespacedName, newHost))
	}
	oldServiceCallbackFn := func(oldHost string) {
		logger.Debug("Old Control-Protocol Service Callback", zap.String("Host", oldHost))
		r.asyncCommandNotificationStore.CleanPodNotification(resetOffsetNamespacedName, oldHost)
	}

	// Reconcile The Services/Connections For Specified Key / Pods
	services, err := r.connectionPool.ReconcileConnections(ctx, refInfo.ConnectionPoolKey, podIPs, newServiceCallbackFn, oldServiceCallbackFn)
	if err != nil {
		logger.Error("Failed to reconcile connections", zap.Error(err))
		return nil, err
	}

	// Return Success
	return services, nil
}

//
func (r *Reconciler) startConsumerGroups(ctx context.Context, services map[string]ctrl.Service, efInfo *refmappers.RefInfo) error {
	return nil // TODO replicate stopConsumerGroups() logic
}

//
func (r *Reconciler) startConsumerGroup(ctx context.Context, services map[string]ctrl.Service, efInfo *refmappers.RefInfo) error {
	return nil // TODO replicate stopConsumerGroup() logic
}

// stopConsumerGroups sends Stop messages to the specified DataPlane services for a Topic / ConsumerGroup and
// waits for the async responses.  A multi-error is returned if any ConsumerGroup was not stopped successfully.
func (r *Reconciler) stopConsumerGroups(ctx context.Context, resetOffset *kafkav1alpha1.ResetOffset, services map[string]ctrl.Service, refInfo *refmappers.RefInfo) error {

	// Stop All The ConsumerGroups In Parallel
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(services))
	errChan := make(chan error, len(services))
	for podIP, service := range services {
		go func(podIP string, service ctrl.Service) {
			defer waitGroup.Done()
			err := r.stopConsumerGroup(ctx, resetOffset, podIP, service, refInfo)
			if err != nil {
				errChan <- err
			}
		}(podIP, service)
	}

	// Wait For All The AsyncCommand Results
	waitGroup.Wait()

	// Close & Drain The Error Channel
	close(errChan)
	var multiErr error
	for err := range errChan {
		if err != nil {
			multiErr = multierr.Append(multiErr, err)
		}
	}

	// Return Any Errors
	return multiErr
}

// stopConsumerGroup sends a Stop message to the specified DataPlane service for a Topic / ConsumerGroup and
// waits for the async response.  An error is returned if the ConsumerGroup was not stopped successfully.
func (r *Reconciler) stopConsumerGroup(ctx context.Context, resetOffset *kafkav1alpha1.ResetOffset, podIP string, service ctrl.Service, refInfo *refmappers.RefInfo) error {

	// Get The Logger From Context
	logger := logging.FromContext(ctx).Desugar()

	// Generate A CommandID For The ResetOffset
	commandId, err := r.generateCommandId(resetOffset, podIP, commands.StopConsumerGroupOpCode)
	if err != nil {
		logger.Error("Failed to generate Command ID for ResetOffset", zap.Error(err))
		return fmt.Errorf("failed to generate Command ID for ResetOffset: %v", err)
	}

	// Create And Send A Stop ConsumerGroupAsyncCommand to The Control-Protocol Service (Dispatcher)
	consumerGroupAsyncCommand := commands.NewConsumerGroupAsyncCommand(commandId, refInfo.TopicName, refInfo.GroupId)
	err = service.SendAndWaitForAck(commands.StopConsumerGroupOpCode, consumerGroupAsyncCommand)
	if err != nil {
		logger.Error("Failed to send Stop ConsumerGroup AsyncCommand", zap.Int64("CommandID", commandId), zap.Error(err))
		return fmt.Errorf("failed to send Stop ConsumerGroup AsyncCommand '%d' : %v", commandId, err)
	}

	// Wait For And Return The AsyncCommand Result
	return r.waitForAsyncCommandResult(resetOffset, podIP, consumerGroupAsyncCommand)
}

// generateCommandId returns an int64 hash based on the specified ResetOffset.
func (r *Reconciler) generateCommandId(resetOffset *kafkav1alpha1.ResetOffset, podIP string, opCode ctrl.OpCode) (int64, error) {
	hash := fnv.New32a()
	_, err := hash.Write([]byte(fmt.Sprintf("%s-%d-%s-%d", string(resetOffset.UID), resetOffset.Generation, podIP, opCode)))
	if err != nil {
		return -1, err
	}
	return int64(hash.Sum32()), nil
}

// waitForAsyncCommandResult polls the Reconciler AsyncCommandNotificationStore waiting for the
// AsyncCommandResult corresponding to the specified AsyncCommand.  The ConsumerGroupAsyncCommands
// are inherently asynchronous so that they can be used in other scenarios (Pause/Resume), but the
// ResetOffset implementation treats them as Synchronous to facilitate single-pass reconciliation.
func (r *Reconciler) waitForAsyncCommandResult(resetOffset *kafkav1alpha1.ResetOffset, podIP string, asyncCommand ctrlmessage.AsyncCommand) error {

	// Create A NamespacedName For The ResetOffset
	resetOffsetNamespacedName := types.NamespacedName{
		Namespace: resetOffset.GetNamespace(),
		Name:      resetOffset.GetName(),
	}

	// Poll The AsyncCommandNotificationStore For AsyncCommandResult
	for iteration := 0; iteration < asyncCommandResultPollIterations; iteration++ {

		// Check The AsyncCommandNotificationStore For Specified AsyncCommandResult
		time.Sleep(asyncCommandResultPollDuration)
		asyncCommandResult := r.asyncCommandNotificationStore.GetCommandResult(resetOffsetNamespacedName, podIP, asyncCommand)

		// If Result Received - Stop Polling And Return
		if asyncCommandResult != nil {
			if asyncCommandResult.IsFailed() {
				return fmt.Errorf("AsyncCommand ID '%x' resulted in error: %s", asyncCommand.SerializedId(), asyncCommandResult.Error)
			} else {
				return nil // Return Success
			}
		}
	}

	// Finished Polling - No Result - Return Error
	return fmt.Errorf("AsyncCommand ID '%x' timed out waiting for response", asyncCommand.SerializedId())
}

// TODO - Hmmm.... uses wait library but results in less useful error messages
func (r *Reconciler) waitForAsyncCommandResult_NEW(resetOffset *kafkav1alpha1.ResetOffset, podIP string, asyncCommand ctrlmessage.AsyncCommand) error {

	// Create A NamespacedName For The ResetOffset
	resetOffsetNamespacedName := types.NamespacedName{
		Namespace: resetOffset.GetNamespace(),
		Name:      resetOffset.GetName(),
	}

	// Poll The AsyncCommandNotificationStore For AsyncCommandResult
	err := wait.Poll(asyncCommandResultPollDuration, asyncCommandResultPollTimeout, func() (done bool, err error) {
		asyncCommandResult := r.asyncCommandNotificationStore.GetCommandResult(resetOffsetNamespacedName, podIP, asyncCommand)
		if asyncCommandResult != nil {
			if asyncCommandResult.IsFailed() {
				return true, fmt.Errorf("AsyncCommand ID '%x' resulted in error: %s", asyncCommand.SerializedId(), asyncCommandResult.Error)
			} else {
				return true, nil // Return Success
			}
		} else {
			return false, nil // Not Found - Try Again
		}
	})

	// Return The Result
	return err
}
