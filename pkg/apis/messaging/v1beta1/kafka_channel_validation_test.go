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
	"k8s.io/apimachinery/pkg/types"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/webhook/resourcesemantics"

	"knative.dev/eventing-kafka/pkg/common/constants"
)

func TestKafkaChannelValidation(t *testing.T) {

	testCases := map[string]struct {
		cr   resourcesemantics.GenericCRD
		want *apis.FieldError
	}{
		"valid spec": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
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
				fe = apis.ErrInvalidValue("", "spec.retentionDuration")
				errs = errs.Also(fe)
				return errs
			}(),
		},
		"negative numPartitions": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     -10,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
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
					RetentionDuration: "P1D",
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
					RetentionDuration: "-P1D",
				},
			},
			want: func() *apis.FieldError {
				fe := apis.ErrInvalidValue("-P1D", "spec.retentionDuration")
				return fe
			}(),
		},
		"invalid retentionDuration": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "NotAValidISO8601String",
				},
			},
			want: func() *apis.FieldError {
				fe := apis.ErrInvalidValue("NotAValidISO8601String", "spec.retentionDuration")
				return fe
			}(),
		},
		"valid subscribers array": {
			cr: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
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
					RetentionDuration: "P1D",
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
					RetentionDuration: "P1D",
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
					RetentionDuration: "P1D",
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

func TestKafkaChannelImmutability(t *testing.T) {

	retry := int32(4)
	backoffPolicy := eventingduck.BackoffPolicyExponential
	backoffDelay := "PT0.5S"
	uid := types.UID("123")
	url, _ := apis.ParseURL("http://foo.bar.svc.cluster.local")

	testCases := map[string]struct {
		original resourcesemantics.GenericCRD
		updated  resourcesemantics.GenericCRD
		want     *apis.FieldError
	}{
		"updating mutable delivery": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
					ChannelableSpec: eventingduck.ChannelableSpec{
						Delivery: &eventingduck.DeliverySpec{
							Retry:         &retry,
							BackoffPolicy: &backoffPolicy,
							BackoffDelay:  &backoffDelay,
						},
					},
				},
			},
		},
		"updating mutable subscribers": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
					ChannelableSpec: eventingduck.ChannelableSpec{
						SubscribableSpec: eventingduck.SubscribableSpec{Subscribers: []eventingduck.SubscriberSpec{{UID: uid, SubscriberURI: url}}},
					},
				},
			},
		},
		"updating immutable numPartitions": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     2,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
				},
			},
			want: func() *apis.FieldError {
				return &apis.FieldError{
					Message: "Immutable fields changed (-old +new)",
					Paths:   []string{"spec"},
					Details: "{v1beta1.KafkaChannelSpec}.NumPartitions:\n\t-: \"1\"\n\t+: \"2\"\n",
				}
			}(),
		},
		"updating immutable replicationFactor": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 2,
					RetentionDuration: "P1D",
				},
			},
			want: func() *apis.FieldError {
				return &apis.FieldError{
					Message: "Immutable fields changed (-old +new)",
					Paths:   []string{"spec"},
					Details: "{v1beta1.KafkaChannelSpec}.ReplicationFactor:\n\t-: \"1\"\n\t+: \"2\"\n",
				}
			}(),
		},
		"updating immutable retentionDuration": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P1D",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P2D",
				},
			},
			want: func() *apis.FieldError {
				return &apis.FieldError{
					Message: "Immutable fields changed (-old +new)",
					Paths:   []string{"spec"},
					Details: "{v1beta1.KafkaChannelSpec}.RetentionDuration:\n\t-: \"P1D\"\n\t+: \"P2D\"\n",
				}
			}(),
		},
		"updating immutable retentionDuration (empty to default, immutability exception)": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: constants.DefaultRetentionISO8601Duration,
				},
			},
		},
		"updating immutable retentionDuration (empty to canonical zero P0D, immutability exception)": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "P0D",
				},
			},
		},
		"updating immutable retentionDuration (non-empty to default)": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "PT1H",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: constants.DefaultRetentionISO8601Duration,
				},
			},
			want: func() *apis.FieldError {
				return &apis.FieldError{
					Message: "Immutable fields changed (-old +new)",
					Paths:   []string{"spec"},
					Details: "{v1beta1.KafkaChannelSpec}.RetentionDuration:\n\t-: \"PT1H\"\n\t+: \"PT168H\"\n",
				}
			}(),
		},
		"updating immutable retentionDuration (empty to non-default, immutability exception)": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "PT100H",
				},
			},
		},
		"updating immutable retentionDuration (immutability exception) and numPartitions": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     2,
					ReplicationFactor: 1,
					RetentionDuration: "PT100H",
				},
			},
			want: func() *apis.FieldError {
				return &apis.FieldError{
					Message: "Immutable fields changed (-old +new)",
					Paths:   []string{"spec"},
					Details: "{v1beta1.KafkaChannelSpec}.NumPartitions:\n\t-: \"1\"\n\t+: \"2\"\n",
				}
			}(),
		},
		"updating immutable retentionDuration (immutability exception) and numPartitions and replicationFactor": {
			original: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     1,
					ReplicationFactor: 1,
					RetentionDuration: "",
				},
			},
			updated: &KafkaChannel{
				Spec: KafkaChannelSpec{
					NumPartitions:     2,
					ReplicationFactor: 3,
					RetentionDuration: "PT100H",
				},
			},
			want: func() *apis.FieldError {
				return &apis.FieldError{
					Message: "Immutable fields changed (-old +new)",
					Paths:   []string{"spec"},
					Details: "{v1beta1.KafkaChannelSpec}.NumPartitions:\n\t-: \"1\"\n\t+: \"2\"\n{v1beta1.KafkaChannelSpec}.ReplicationFactor:\n\t-: \"1\"\n\t+: \"3\"\n",
				}
			}(),
		},
	}

	for n, test := range testCases {
		t.Run(n, func(t *testing.T) {
			ctx := apis.WithinUpdate(context.Background(), test.original)
			got := test.updated.Validate(ctx)
			if diff := cmp.Diff(test.want.Error(), got.Error()); diff != "" {
				t.Errorf("%s: validate (-want, +got) = %v", n, diff)
			}
		})
	}
}
