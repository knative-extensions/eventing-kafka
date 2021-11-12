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

package reconciler

import (
	"bytes"

	"k8s.io/apimachinery/pkg/types"

	control "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/message"
)

// AsyncCommandNotificationStore defines a specialized NotificationStore capable of handling message.AsyncCommandResults
type AsyncCommandNotificationStore interface {
	GetCommandResult(srcName types.NamespacedName, pod string, command message.AsyncCommand) *message.AsyncCommandResult
	CleanPodsNotifications(srcName types.NamespacedName)
	CleanPodNotification(srcName types.NamespacedName, pod string)
	MessageHandler(srcName types.NamespacedName, pod string) control.MessageHandler
}

var _ AsyncCommandNotificationStore = (*asyncCommandNotificationStoreImpl)(nil)

// asyncCommandNotificationStoreImpl is a specialized NotificationStore that is capable to handle message.AsyncCommandResult
type asyncCommandNotificationStoreImpl struct {
	ns *NotificationStore
}

// NewAsyncCommandNotificationStore creates an AsyncCommandNotificationStore
func NewAsyncCommandNotificationStore(enqueueKey func(name types.NamespacedName)) AsyncCommandNotificationStore {
	return &asyncCommandNotificationStoreImpl{
		ns: &NotificationStore{
			enqueueKey:        enqueueKey,
			payloadParser:     message.ParseAsyncCommandResult,
			notificationStore: make(map[types.NamespacedName]map[string]interface{}),
		},
	}
}

// GetCommandResult returns the message.AsyncCommandResult when the notification store contains the command result matching srcName, pod and generation
func (ns *asyncCommandNotificationStoreImpl) GetCommandResult(srcName types.NamespacedName, pod string, command message.AsyncCommand) *message.AsyncCommandResult {
	val, ok := ns.ns.GetPodNotification(srcName, pod)
	if !ok {
		return nil
	}

	res := val.(message.AsyncCommandResult)

	if !bytes.Equal(res.CommandId, command.SerializedId()) {
		return nil
	}

	return &res
}

// CleanPodsNotifications is like NotificationStore.CleanPodsNotifications
func (ns *asyncCommandNotificationStoreImpl) CleanPodsNotifications(srcName types.NamespacedName) {
	ns.ns.CleanPodsNotifications(srcName)
}

// CleanPodNotification is like NotificationStore.CleanPodNotification
func (ns *asyncCommandNotificationStoreImpl) CleanPodNotification(srcName types.NamespacedName, pod string) {
	ns.ns.CleanPodNotification(srcName, pod)
}

// MessageHandler is like NotificationStore.MessageHandler
func (ns *asyncCommandNotificationStoreImpl) MessageHandler(srcName types.NamespacedName, pod string) control.MessageHandler {
	return ns.ns.MessageHandler(srcName, pod, PassNewValue)
}
