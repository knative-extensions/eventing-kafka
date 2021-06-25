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

package refmappers

import (
	"context"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
)

// ResetOffsetRefMapperFactory defines the interface for creating ResetOffsetRefMapper
// instances based on the provided Context which comes from SharedMain and includes the
// injected Informers.  It is necessary for delayed initialization against that Context.
type ResetOffsetRefMapperFactory interface {
	Create(context.Context) ResetOffsetRefMapper
}

// ResetOffsetRefMapper defines the interface for the capability to map ResetOffset.Spec.Ref
// to Kafka Topic, Group and ConnectionPool Key. This abstraction allows for future
// extensibility to support additional KRef types, such as Triggers, instead of just
// Subscriptions.
type ResetOffsetRefMapper interface {
	MapRef(*kafkav1alpha1.ResetOffset) (*RefInfo, error)
}

// RefInfo contains the data necessary for ResetOffset reconciliation which is specific
// to a particular use-case, as provided by a customized ResetOffsetRefMapper implementation.
// This allows implementations of Kafka Channels/Brokers/etc to differ from one another
// and still make use of the shared ResetOffset Controller.
type RefInfo struct {
	TopicName          string
	GroupId            string
	ConnectionPoolKey  string
	DataPlaneNamespace string
	DataPlaneLabels    map[string]string
}
