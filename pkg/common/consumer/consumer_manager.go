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

package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/eventing-kafka/pkg/common/controlprotocol"
)

// ErrStoppedConsumerGroup is the error returned when a consumer group has been explicitly "stopped" by the manager
var ErrStoppedConsumerGroup = errors.New("kafka: tried to use a consumer group that was stopped")

// NewConsumerGroupFnType Is A Function Definition Types For A Wrapper Variables (Typesafe Stubbing For Tests)
type NewConsumerGroupFnType = func(brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error)

// The opcodes here shouldn't interfere with those in pkg/source/control/message.go
// but out of caution we start with 3
const (
	// StopConsumerGroupsOpCode instructs the manager to stop all of its managed consumer groups
	StopConsumerGroupsOpCode ctrl.OpCode = 3

	// StartConsumerGroupsOpCode instructs the manager to start all of its managed consumer groups
	StartConsumerGroupsOpCode ctrl.OpCode = 4
)

// KafkaConsumerGroupManager keeps track of Sarama consumer groups and handles messages from control-protocol clients
type KafkaConsumerGroupManager interface {
	CreateConsumerGroup(createFn NewConsumerGroupFnType, brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error)
	CloseConsumerGroup(groupId string) error
	IsValid(groupId string) bool
	IsStopped(groupId string) bool
	WaitForStart(groupId string, timeout time.Duration) error
	GetErrors(groupId string) (<-chan error, error)
	Consume(groupId string, ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error
	// Close ?   If the dispatcher tears down the CG, this needs to know about it
}

// managedGroup contains information about a Sarama ConsumerGroup that is required to start (i.e. re-create) it
type managedGroup struct {
	Create  NewConsumerGroupFnType
	Brokers []string
	GroupId string
	Config  *sarama.Config
	Group   sarama.ConsumerGroup
	stopped bool
}

func (m *managedGroup) IsStopped() bool {
	return m.stopped
}

// Stop closes a consumer group and marks the managedGroup as "stopped"
// Note that this must be a pointer receiver or setting m.stopped will only set it on the copy of the struct
func (m *managedGroup) Stop() error {
	if !m.stopped {
		m.stopped = true // TODO:  Thread-safety
		fmt.Printf("EDV: Stop->m.Group.Close ('%s'), m.stopped=%v\n", m.GroupId, m.stopped)
		err := m.Group.Close()
		fmt.Printf("EDV: ~Stop->m.Group.Close ('%s'), m.stopped=%v\n", m.GroupId, m.stopped)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *managedGroup) Start() error {
	consumerGroup, err := m.Create(m.Brokers, m.GroupId, m.Config)
	if err != nil {
		return err
	}
	m.Group = consumerGroup
	m.stopped = false
	return nil
}

type groupMap map[string]*managedGroup

// kafkaConsumerGroupManagerImpl is the primary implementation of a KafkaConsumerGroupManager
type kafkaConsumerGroupManagerImpl struct {
	server controlprotocol.ServerHandler
	groups groupMap
}

func NewConsumerGroupManager(serverHandler controlprotocol.ServerHandler) KafkaConsumerGroupManager {
	fmt.Printf("EDV: NewConsumerGroupManager()\n")
	manager := kafkaConsumerGroupManagerImpl{
		server: serverHandler,
		groups: make(groupMap),
	}
	//serverHandler.AddAsyncHandler(StopConsumerGroupsOpCode, func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage) {
	//	fmt.Printf("ConsumerGroupManager received StopConsumerGroupsOpCode (Async)\n")
	//	err := manager.pauseConsumerGroups()
	//	if err != nil {
	//		commandMessage.NotifyFailed(err)
	//	} else {
	//		commandMessage.NotifySuccess()
	//	}
	//})
	//serverHandler.AddAsyncHandler(StartConsumerGroupsOpCode, func(ctx context.Context, commandMessage ctrlservice.AsyncCommandMessage) {
	//	fmt.Printf("ConsumerGroupManager received StartConsumerGroupsOpCode (Async)\n")
	//	err := manager.resumeConsumerGroups()
	//	if err != nil {
	//		commandMessage.NotifyFailed(err)
	//	} else {
	//		commandMessage.NotifySuccess()
	//	}
	//})
	serverHandler.AddSyncHandler(StopConsumerGroupsOpCode, func(ctx context.Context, message ctrl.ServiceMessage) {
		fmt.Printf("ConsumerGroupManager received StopConsumerGroupsOpCode (Sync)\n")
		err := manager.pauseConsumerGroups()
		if err != nil {
			fmt.Printf("pauseConsumerGroups error: %v\n", err)
		}
		message.Ack()
	})
	serverHandler.AddSyncHandler(StartConsumerGroupsOpCode, func(ctx context.Context, message ctrl.ServiceMessage) {
		fmt.Printf("ConsumerGroupManager received StartConsumerGroupsOpCode (Sync)\n")
		err := manager.resumeConsumerGroups()
		if err != nil {
			fmt.Printf("resumeConsumerGroups error: %v\n", err)
		}
		message.Ack()
	})
	return manager
}

// Consume calls the Consume method of a managed consumer group and returns a custom error
// if it was ended due to being stopped by the manager ("paused")
func (m kafkaConsumerGroupManagerImpl) Consume(groupId string, ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	fmt.Printf("EDV: manager.Consume('%s')\n", groupId)
	groupInfo, ok := m.groups[groupId]
	if !ok {
		return fmt.Errorf("consume called on nonexistent groupId '%s'", groupId)
	}
	err := groupInfo.Group.Consume(ctx, topics, handler)
	fmt.Printf("EDV: ~manager.Consume('%s')\n", groupId)
	if !groupInfo.IsStopped() {
		fmt.Printf("EDV: group '%s' was NOT STOPPED\n", groupId)
		// This ConsumerGroup wasn't stopped by the manager, so pass the error along to the caller
		return err
	}
	return ErrStoppedConsumerGroup
}

func (m kafkaConsumerGroupManagerImpl) WaitForStart(groupId string, timeout time.Duration) error {
	// TODO:  Should wait for a proper signal/channel, not poll the IsStopped function
	return wait.Poll(10*time.Millisecond, timeout, func() (bool, error) {
		return !m.IsStopped(groupId), nil
	})
}

func (m kafkaConsumerGroupManagerImpl) GetErrors(groupId string) (<-chan error, error) {
	groupInfo, ok := m.groups[groupId]
	if !ok {
		return nil, nil
	}
	if groupInfo.IsStopped() {
		return nil, ErrStoppedConsumerGroup
	}
	return groupInfo.Group.Errors(), nil
}

func (m kafkaConsumerGroupManagerImpl) IsValid(groupId string) bool {
	if _, ok := m.groups[groupId]; ok {
		return true
	}
	return false
}

func (m kafkaConsumerGroupManagerImpl) IsStopped(groupId string) bool {
	if _, ok := m.groups[groupId]; ok {
		return m.groups[groupId].IsStopped()
	}
	return false
}

func (m kafkaConsumerGroupManagerImpl) pauseConsumerGroups() error {
	fmt.Printf("EDV: pauseConsumerGroups()\n")
	for id := range m.groups {
		groupInfo := m.groups[id]
		fmt.Printf("EDV: Calling Stop(): groups[%s].stopped=%v\n", id, m.groups[id].stopped)
		err := groupInfo.Stop()
		fmt.Printf("EDV: Called Stop(): groups[%s].stopped=%v\n", id, m.groups[id].stopped)
		if err != nil {
			fmt.Printf("EDV: Error pausing group ID %s: %s (TODO: Add to multi-err?)\n", id, err.Error())
		}
	}
	return nil
}

func (m kafkaConsumerGroupManagerImpl) resumeConsumerGroups() error {
	fmt.Printf("EDV: resumeConsumerGroups()\n")
	for id := range m.groups {
		err := m.groups[id].Start()
		if err != nil {
			fmt.Printf("EDV: Error resuming group ID %s: %s (TODO: Add to multi-err?)\n", id, err.Error())
		}
	}
	return nil
}

func (m kafkaConsumerGroupManagerImpl) CreateConsumerGroup(createFn NewConsumerGroupFnType, brokers []string, groupId string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	group, err := createFn(brokers, groupId, config)
	if err != nil {
		return group, err
	}
	m.groups[groupId] = &managedGroup{
		Create:  createFn,
		Brokers: brokers,
		GroupId: groupId,
		Config:  config,
		Group:   group,
	}
	return group, nil
}

func (m kafkaConsumerGroupManagerImpl) CloseConsumerGroup(groupId string) error {
	groupInfo, ok := m.groups[groupId]
	if !ok {
		return fmt.Errorf("could not close consumer group with id '%s' - group is not present in the managed list", groupId)
	}
	err := groupInfo.Group.Close()
	if err != nil {
		return err
	}
	delete(m.groups, groupId)
	return nil
}

var _ KafkaConsumerGroupManager = (*kafkaConsumerGroupManagerImpl)(nil)
