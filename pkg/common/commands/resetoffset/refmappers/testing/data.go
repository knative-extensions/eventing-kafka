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

package testing

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kafkav1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/util"
	controllertesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/controller/testing"
	"knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers"
)

const (
	ConnectionPoolKey    = "TestConnectionPoolKey"
	DataPlaneNamespace   = "TestDataPlaneNamespace"
	ChannelName          = "TestChannelName"
	ChannelNamespace     = "TestChannelNamespace"
	DataPlaneLabelKey1   = "TestDataPlaneLabelKey1"
	DataPlaneLabelValue1 = "TestDataPlaneLabelValue1"
	DataPlaneLabelKey2   = "TestDataPlaneLabelKey2"
	DataPlaneLabelValue2 = "TestDataPlaneLabelValue2"
)

//
// RefInfo Resources
//

// RefInfoOption allow for customizing a RefInfo
type RefInfoOption func(refInfo *refmappers.RefInfo)

// NewRefInfo creates a custom NewRefInfo
func NewRefInfo(options ...RefInfoOption) *refmappers.RefInfo {

	// Create The Default Test RefInfo
	refInfo := &refmappers.RefInfo{
		TopicName:          controllertesting.TopicName,
		GroupId:            controllertesting.GroupId,
		ConnectionPoolKey:  ConnectionPoolKey,
		DataPlaneNamespace: DataPlaneNamespace,
		DataPlaneLabels:    map[string]string{DataPlaneLabelKey1: DataPlaneLabelValue1, DataPlaneLabelKey2: DataPlaneLabelValue2},
	}

	channel := &kafkav1beta1.KafkaChannel{ObjectMeta: metav1.ObjectMeta{Name: ChannelName, Namespace: ChannelNamespace}}
	refInfo.DataPlaneLabels[constants.AppLabel] = util.DispatcherDnsSafeName(channel)

	// Apply The Specified Customizations
	for _, option := range options {
		option(refInfo)
	}

	// Return The Custom RefInfo
	return refInfo
}
