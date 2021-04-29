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

package v1alpha1

import (
	"encoding/json"
)

const (
	ResetOffsetAnnotationTopic      = "topic"
	ResetOffsetAnnotationGroup      = "group"
	ResetOffsetAnnotationPartitions = "partitions"
)

// OffsetMapping is a simple struct representing a single Kafka
// Topic/Partition Offset value before and after repositioning
type OffsetMapping struct {
	Partition int32 `json:"partition"`
	OldOffset int64 `json:"oldOffset"`
	NewOffset int64 `json:"newOffset"`
}

// GetTopicAnnotation sets the specified Kafka Topic name annotation if populated or "" otherwise.
func (ros *ResetOffsetStatus) GetTopicAnnotation() string {
	ros.initializeStatusAnnotations()
	return ros.Annotations[ResetOffsetAnnotationTopic]
}

// SetTopicAnnotation sets the specified Kafka Topic name annotation.
func (ros *ResetOffsetStatus) SetTopicAnnotation(topic string) {
	ros.initializeStatusAnnotations()
	ros.Annotations[ResetOffsetAnnotationTopic] = topic
}

// GetGroupAnnotation sets the specified Kafka ConsumerGroup name annotation if populated or "" otherwise.
func (ros *ResetOffsetStatus) GetGroupAnnotation() string {
	ros.initializeStatusAnnotations()
	return ros.Annotations[ResetOffsetAnnotationGroup]
}

// SetGroupAnnotation sets the specified Kafka ConsumerGroup name annotation.
func (ros *ResetOffsetStatus) SetGroupAnnotation(group string) {
	ros.initializeStatusAnnotations()
	ros.Annotations[ResetOffsetAnnotationGroup] = group
}

// GetPartitionsAnnotation returns the specified OffsetMappings annotation if populated or nil otherwise.
func (ros *ResetOffsetStatus) GetPartitionsAnnotation() ([]OffsetMapping, error) {
	var offsetMappings []OffsetMapping
	var err error
	ros.initializeStatusAnnotations()
	offsetsString := ros.Annotations[ResetOffsetAnnotationPartitions]
	if len(offsetsString) > 0 {
		err = json.Unmarshal([]byte(offsetsString), &offsetMappings)
	}
	return offsetMappings, err
}

// SetPartitionsAnnotation sets the specified OffsetMappings annotation.
func (ros *ResetOffsetStatus) SetPartitionsAnnotation(offsetMappings []OffsetMapping) error {
	ros.initializeStatusAnnotations()
	jsonBytes, err := json.Marshal(offsetMappings)
	if err == nil {
		ros.Annotations[ResetOffsetAnnotationPartitions] = string(jsonBytes)
	}
	return err
}

func (ros *ResetOffsetStatus) initializeStatusAnnotations() {
	if ros.Annotations == nil {
		ros.Annotations = make(map[string]string)
	}
}
