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

package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rickb777/date/period"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
)

// KafkaChannelUpdater is a struct capable of updating the Spec.RetentionDuration of all KafkaChannels based on current Kafka Topic configuration.
type KafkaChannelUpdater struct {
	logger       *zap.Logger
	clientset    kafkaclientset.Interface
	clusterAdmin sarama.ClusterAdmin
}

// NewKafkaChannelUpdater is a convenience constructor of KafkaChannelUpdater structs.
func NewKafkaChannelUpdater(logger *zap.Logger, clientset kafkaclientset.Interface, clusterAdmin sarama.ClusterAdmin) *KafkaChannelUpdater {
	return &KafkaChannelUpdater{
		logger:       logger,
		clientset:    clientset,
		clusterAdmin: clusterAdmin,
	}
}

// Update performs the update of all KafkaChannels based on current Kafka Topic configuration.
func (u KafkaChannelUpdater) Update(ctx context.Context) error {

	// List All The Kafka Topics
	topicDetailMap, err := u.clusterAdmin.ListTopics()
	if err != nil {
		u.logger.Error("Failed To List Kafka Topics", zap.Error(err))
		return err
	}

	// Loop Over The Kafka TopicDetails (Track & Continue On Error To Get As Many Possible!)
	var multiErr error
	for topicName, topicDetail := range topicDetailMap {

		// Log The Kafka Topic
		logger := u.logger.With(zap.String("Topic", topicName))
		logger.Info("<===  Processing Kafka Topic  ===>", zap.Any("TopicDetail.ConfigEntries", topicDetail.ConfigEntries))

		// Get The KafkaChannel's NamespacedName From Topic Name & Track In Logger
		kcNamespacedName, err := getKafkaChannelNamespacedName(topicName)
		if err != nil {
			logger.Error("Failed To Determine KafkaChannel Namespace/Name From Topic Name", zap.Error(err))
			multierr.AppendInto(&multiErr, fmt.Errorf("skipping Topic '%s': failed to determine KafkaChannel Namespace/Name", topicName))
			continue
		} else if kcNamespacedName == nil {
			logger.Info("Skipping Non-Knative Kafka Topic")
			continue
		}
		logger = logger.With(zap.String("KafkaChannel", fmt.Sprintf("%s/%s", kcNamespacedName.Namespace, kcNamespacedName.Name)))

		// Get The KafkaChannel By Name
		kafkaChannel, err := u.clientset.MessagingV1beta1().KafkaChannels(kcNamespacedName.Namespace).Get(ctx, kcNamespacedName.Name, v1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				logger.Info("Skipping Update Of Missing KafkaChannel") // Ignore Orphaned Knative Topics
				continue
			} else {
				logger.Error("Failed To Get KafkaChannel From Topic Name", zap.Error(err))
				multierr.AppendInto(&multiErr, fmt.Errorf("skipping Topic '%s': failed to get KafkaChannel", topicName))
				continue
			}
		}

		// Get The Expected RetentionDuration ISO-8601 String From The TopicDetail
		expectedRetentionDurationString, err := getExpectedRetentionDurationString(topicDetail)
		if err != nil {
			logger.Error("Failed To Determine ISO-8601 RetentionDuration String From TopicDetail", zap.Error(err))
			multierr.AppendInto(&multiErr, fmt.Errorf("skipping Topic '%s': failed to determine ISO-8601 string from TopicDetail", topicName))
			continue
		}
		logger = logger.With(zap.String("RetentionDuration", expectedRetentionDurationString))

		// Set/Update The KafkaChannel's RetentionDuration
		kafkaChannel = kafkaChannel.DeepCopy()
		kafkaChannel.Spec.RetentionDuration = expectedRetentionDurationString
		_, err = u.clientset.MessagingV1beta1().KafkaChannels(kafkaChannel.Namespace).Update(ctx, kafkaChannel, v1.UpdateOptions{})
		if err != nil {
			logger.Error("Failed To Update KafkaChannel With RetentionDuration", zap.Error(err))
			multierr.AppendInto(&multiErr, fmt.Errorf("skipping Topic '%s': failed to update KafkaChannel with RetentionDuration value '%s'", topicName, expectedRetentionDurationString))
			continue
		} else {
			logger.Info("Successfully Updated KafkaChannel With RetentionDuration")
		}
	}

	// Return Results
	return multiErr
}

// getKafkaChannelNamespacedName returns the KafkaChannel's NamespacedName corresponding to the specified Topic name.
//
// Note - This implementation attempts to match only Knative KafkaChannel Topic naming conventions and will return
//        errors if not matched.  This is in attempt to prevent processing "other" Topics in a Kafka cluster.  If
//        someone happens to be using the same <namespace>.<name> convention for non-Knative Topics, it won't
//        cause a problem as we simply won't find that KafkaChannel and will move on.
func getKafkaChannelNamespacedName(topicName string) (*types.NamespacedName, error) {

	// Validate The Topic Name Is Of Expected Minimal Format To Be A Knative Topic
	if strings.HasPrefix(topicName, "__") || len(topicName) < 3 || !strings.Contains(topicName, ".") {
		return nil, nil
	}

	// Strip The Topic Name Prefix Used By Consolidated KafkaChannel
	if strings.HasPrefix(topicName, "knative-messaging-kafka.") {
		topicName = strings.TrimPrefix(topicName, "knative-messaging-kafka.")
	}

	// Split The KafkaChannel Namespace/Name From The Topic Name
	topicNameComponents := strings.Split(topicName, ".")

	// Validate We Only Have The Expected Components
	if len(topicNameComponents) > 2 {
		return nil, fmt.Errorf("received Topic name with multiple components '%s'", topicName)
	}

	// Bundle & Return As A NamespacedName
	return &types.NamespacedName{
		Namespace: topicNameComponents[0],
		Name:      topicNameComponents[1],
	}, nil
}

// getExpectedRetentionDurationString returns an ISO-8601 string representing the retention.ms from the specified TopicDetail.ConfigEntries.
//
// Note - This is somewhat lossy or imprecise as discussed in the period.NewOf() docs, but is expected to
//        be close enough for the intended purpose while still providing a readable ISO-8601 value. See
//        the associated UnitTest for examples.
func getExpectedRetentionDurationString(topicDetail sarama.TopicDetail) (string, error) {

	// Validate The TopicDetail
	if topicDetail.ConfigEntries == nil {
		return "", fmt.Errorf("received nil ConfigEntries: %+v", topicDetail)
	}

	// Get & Validate The Retention Millis From The TopicDetail
	retentionMs := topicDetail.ConfigEntries[commonconstants.KafkaTopicConfigRetentionMs]
	if retentionMs == nil || *retentionMs == "" {
		return "", fmt.Errorf("received ConfigEntries with invalid retention.ms: '%v'", retentionMs)
	}

	// Parse The RetentionMs Into An Int64
	retentionMillis, err := strconv.ParseInt(*retentionMs, 10, 64)
	if err != nil {
		return "", fmt.Errorf("received ConfigEntries with non-numeric retention.ms: '%v'", retentionMs)
	}

	// Turn The Retention Millis Into A time.Duration
	retentionDuration := time.Duration(retentionMillis) * time.Millisecond

	// Turn The Retention Duration Into A Period (Ignoring Precision)
	retentionPeriod, _ := period.NewOf(retentionDuration)

	// Get The ISO-8601 String Representation Of The RetentionDuration
	retentionDurationISO8601String := retentionPeriod.String()

	// Return The Results
	return retentionDurationISO8601String, nil
}
