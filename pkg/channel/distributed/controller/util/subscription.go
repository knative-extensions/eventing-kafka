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

package util

import (
	"fmt"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
)

// Get A Logger With Subscription Info
func SubscriptionLogger(logger *zap.Logger, subscription *messagingv1.Subscription) *zap.Logger {
	return logger.With(zap.String("Subscription", fmt.Sprintf("%s/%s", subscription.Namespace, subscription.Name)))
}

// Create A New ControllerReference Model For The Specified Subscription
func NewSubscriptionControllerRef(subscription *messagingv1.Subscription) metav1.OwnerReference {
	return *metav1.NewControllerRef(subscription, schema.GroupVersionKind{
		Group:   messagingv1.SchemeGroupVersion.Group,
		Version: messagingv1.SchemeGroupVersion.Version,
		Kind:    constants.KnativeSubscriptionKind,
	})
}
