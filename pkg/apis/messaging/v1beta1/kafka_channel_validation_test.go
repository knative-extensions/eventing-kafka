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

package v1beta1

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/webhook/resourcesemantics"

	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/pkg/apis"
)

func TestKafkaChannelValidation(t *testing.T) {

	validRetentionDuration := "P14D"
	invalidRetentionDuration := "NotAValidISO8601String"
	negativeRetentionDuration := "-P1D"

	testCases := map[string]struct {
		cr   resourcesemantics.GenericCRD
		want *apis.FieldError
	}{
		"valid spec": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: &validRetentionDuration,
				},
			},
		},
		"empty spec": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{},
			},
			want: func() *apis.FieldError {
				var errs *apis.FieldError
				fe := apis.ErrInvalidValue(0, "spec.numPartitions")
				errs = errs.Also(fe)
				fe = apis.ErrInvalidValue(0, "spec.replicationFactor")
				errs = errs.Also(fe)
				return errs
			}(),
		},
		"negative numPartitions": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     -10,
					ReplicationFactor: 1,
				},
			},
			want: func() *apis.FieldError {
				fe := apis.ErrInvalidValue(-10, "spec.numPartitions")
				return fe
			}(),
		},
		"negative replicationFactor": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: -10,
				},
			},
			want: func() *apis.FieldError {
				fe := apis.ErrInvalidValue(-10, "spec.replicationFactor")
				return fe
			}(),
		},
		"negative retentionDuration": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: &negativeRetentionDuration,
				},
			},
			want: func() *apis.FieldError {
				fe := apis.ErrInvalidValue(negativeRetentionDuration, "spec.retentionDuration")
				return fe
			}(),
		},
		"invalid retentionDuration": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: &invalidRetentionDuration,
				},
			},
			want: func() *apis.FieldError {
				fe := apis.ErrInvalidValue(invalidRetentionDuration, "spec.retentionDuration")
				return fe
			}(),
		},
		"valid subscribers array": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					ChannelableSpec: eventingduck.ChannelableSpec{
						SubscribableSpec: eventingduck.SubscribableSpec{
							Subscribers: []eventingduck.SubscriberSpec{{
								SubscriberURI: apis.HTTP("subscriberendpoint"),
								ReplyURI:      apis.HTTP("resultendpoint"),
							}},
						}},
				},
			},
			want: nil,
		},
		"empty subscriber at index 1": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					ChannelableSpec: eventingduck.ChannelableSpec{
						SubscribableSpec: eventingduck.SubscribableSpec{
							Subscribers: []eventingduck.SubscriberSpec{{
								SubscriberURI: apis.HTTP("subscriberendpoint"),
								ReplyURI:      apis.HTTP("replyendpoint"),
							}, {}},
						}},
				},
			},
			want: func() *apis.FieldError {
				fe := apis.ErrMissingField("spec.subscribable.subscriber[1].replyURI", "spec.subscribable.subscriber[1].subscriberURI")
				fe.Details = "expected at least one of, got none"
				return fe
			}(),
		},
		"two empty subscribers": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					ChannelableSpec: eventingduck.ChannelableSpec{
						SubscribableSpec: eventingduck.SubscribableSpec{
							Subscribers: []eventingduck.SubscriberSpec{{}, {}},
						},
					},
				},
			},
			want: func() *apis.FieldError {
				var errs *apis.FieldError
				fe := apis.ErrMissingField("spec.subscribable.subscriber[0].replyURI", "spec.subscribable.subscriber[0].subscriberURI")
				fe.Details = "expected at least one of, got none"
				errs = errs.Also(fe)
				fe = apis.ErrMissingField("spec.subscribable.subscriber[1].replyURI", "spec.subscribable.subscriber[1].subscriberURI")
				fe.Details = "expected at least one of, got none"
				errs = errs.Also(fe)
				return errs
			}(),
		},
		"invalid scope annotation": {
			cr: &KafkaChannel{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						eventing.ScopeAnnotationKey: "notvalid",
					},
				},
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
				},
			},
			want: func() *apis.FieldError {
				fe := apis.ErrInvalidValue("notvalid", "metadata.annotations.[eventing.knative.dev/scope]")
				fe.Details = "expected either 'cluster' or 'namespace'"
				return fe
			}(),
		},
	}

	for n, test := range testCases {
		t.Run(n, func(t *testing.T) {
			got := test.cr.Validate(context.Background())
			if diff := cmp.Diff(test.want.Error(), got.Error()); diff != "" {
				t.Errorf("%s: validate (-want, +got) = %v", n, diff)
			}
		})
	}
}
