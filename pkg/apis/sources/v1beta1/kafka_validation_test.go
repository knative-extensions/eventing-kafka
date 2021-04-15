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

	bindingsv1beta1 "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

var (
	fullSpec = KafkaSourceSpec{
		KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
			BootstrapServers: []string{"servers"},
			Net: bindingsv1beta1.KafkaNetSpec{
				SASL: bindingsv1beta1.KafkaSASLSpec{
					Enable: false,
				},
				TLS: bindingsv1beta1.KafkaTLSSpec{
					Enable: false,
				},
			},
		},
		Topics:        []string{"topics"},
		ConsumerGroup: "group",
		SourceSpec: duckv1.SourceSpec{
			Sink: duckv1.Destination{
				Ref: &duckv1.KReference{
					APIVersion: "foo",
					Kind:       "bar",
					Name:       "qux",
				},
			},
		},
	}
)

func TestKafkaSourceCheckRequiredFields(t *testing.T) {
	testCases := map[string]struct {
		orig    *KafkaSourceSpec
		allowed bool
	}{
		"nil original": {
			orig:    &KafkaSourceSpec{},
			allowed: false,
		},
		"nil topic": {
			allowed: false,
			orig: &KafkaSourceSpec{
				KafkaAuthSpec: fullSpec.KafkaAuthSpec,
				SourceSpec:    fullSpec.SourceSpec,
			},
		},
		"nil bootstrapServer": {
			orig: &KafkaSourceSpec{
				Topics:     fullSpec.Topics,
				SourceSpec: fullSpec.SourceSpec,
			},
			allowed: false,
		},
		"nil sink": {
			orig: &KafkaSourceSpec{
				KafkaAuthSpec: fullSpec.KafkaAuthSpec,
				Topics:        fullSpec.Topics,
			},
			allowed: false,
		},
		"min required fields": {
			orig: &KafkaSourceSpec{
				KafkaAuthSpec: fullSpec.KafkaAuthSpec,
				Topics:        fullSpec.Topics,
				SourceSpec:    fullSpec.SourceSpec,
			},
			allowed: true,
		},
		"full source": {
			orig:    &fullSpec,
			allowed: true,
		},
	}
	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			ctx := context.TODO()
			orig := &KafkaSource{
				Spec: *tc.orig,
			}
			ctx = apis.WithinCreate(ctx)
			err := orig.Validate(ctx)
			if tc.allowed != (err == nil) {
				t.Fatalf("Required Field Not Included: %v", err)
			}
		})
	}
}

func TestKafkaSourceCheckImmutableFields(t *testing.T) {
	testCases := map[string]struct {
		orig    *KafkaSourceSpec
		updated KafkaSourceSpec
		allowed bool
	}{
		"nil orig": {
			updated: fullSpec,
			allowed: true,
		},
		"Topic changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				KafkaAuthSpec: fullSpec.KafkaAuthSpec,
				Topics:        []string{"some-other-topic"},
				ConsumerGroup: fullSpec.ConsumerGroup,
				SourceSpec:    fullSpec.SourceSpec,
			},
			allowed: true,
		},
		"Bootstrap servers changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
					BootstrapServers: []string{"server1,server2"},
					Net:              fullSpec.Net,
				},
				Topics:        []string{"some-other-topic"},
				ConsumerGroup: fullSpec.ConsumerGroup,
				SourceSpec:    fullSpec.SourceSpec,
			},
			allowed: true,
		},
		"Sink.APIVersion changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				KafkaAuthSpec: fullSpec.KafkaAuthSpec,
				Topics:        fullSpec.Topics,
				ConsumerGroup: fullSpec.ConsumerGroup,
				SourceSpec: duckv1.SourceSpec{
					Sink: duckv1.Destination{
						Ref: &duckv1.KReference{
							APIVersion: "some-other-api-version",
							Kind:       fullSpec.Sink.Ref.APIVersion,
							Namespace:  fullSpec.Sink.Ref.Namespace,
							Name:       fullSpec.Sink.Ref.Name,
						},
					},
				},
			},
			allowed: true,
		},
		"Sink.Kind changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				KafkaAuthSpec: fullSpec.KafkaAuthSpec,
				Topics:        fullSpec.Topics,
				ConsumerGroup: fullSpec.ConsumerGroup,
				SourceSpec: duckv1.SourceSpec{
					Sink: duckv1.Destination{
						Ref: &duckv1.KReference{
							APIVersion: fullSpec.Sink.Ref.APIVersion,
							Kind:       "some-other-kind",
							Namespace:  fullSpec.Sink.Ref.Namespace,
							Name:       fullSpec.Sink.Ref.Name,
						},
					},
				},
			},
			allowed: true,
		},
		"Sink.Namespace changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{

				KafkaAuthSpec: fullSpec.KafkaAuthSpec,
				Topics:        fullSpec.Topics,
				ConsumerGroup: fullSpec.ConsumerGroup,
				SourceSpec: duckv1.SourceSpec{
					Sink: duckv1.Destination{
						Ref: &duckv1.KReference{
							APIVersion: fullSpec.Sink.Ref.APIVersion,
							Kind:       fullSpec.Sink.Ref.Kind,
							Namespace:  "some-other-namespace",
							Name:       fullSpec.Sink.Ref.Name,
						},
					},
				},
			},
			allowed: true,
		},
		"Sink.Name changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{

				KafkaAuthSpec: fullSpec.KafkaAuthSpec,
				Topics:        fullSpec.Topics,
				ConsumerGroup: fullSpec.ConsumerGroup,
				SourceSpec: duckv1.SourceSpec{
					Sink: duckv1.Destination{
						Ref: &duckv1.KReference{
							APIVersion: fullSpec.Sink.Ref.APIVersion,
							Kind:       fullSpec.Sink.Ref.Kind,
							Namespace:  fullSpec.Sink.Ref.Namespace,
							Name:       "some-other-name",
						},
					},
				},
			},
			allowed: true,
		},
		"no change": {
			orig:    &fullSpec,
			updated: fullSpec,
			allowed: true,
		},
		"consumerGroup changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				KafkaAuthSpec: fullSpec.KafkaAuthSpec,
				Topics:        fullSpec.Topics,
				ConsumerGroup: "no-way",
				SourceSpec:    fullSpec.SourceSpec,
			},
			allowed: true,
		},
		"Kafka TLS Spec changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				ConsumerGroup: fullSpec.ConsumerGroup,
				SourceSpec:    fullSpec.SourceSpec,
				Topics:        fullSpec.Topics,
				KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
					BootstrapServers: []string{"servers"},
					Net: bindingsv1beta1.KafkaNetSpec{
						TLS: bindingsv1beta1.KafkaTLSSpec{
							Enable: true,
						},
					},
				},
			},
			allowed: true,
		},
		"Kafka SASL Spec changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				ConsumerGroup: fullSpec.ConsumerGroup,
				SourceSpec:    fullSpec.SourceSpec,
				Topics:        fullSpec.Topics,
				KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
					BootstrapServers: []string{"servers"},
					Net: bindingsv1beta1.KafkaNetSpec{
						SASL: bindingsv1beta1.KafkaSASLSpec{
							Enable: true,
						},
					},
				},
			},
			allowed: true,
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			ctx := context.TODO()
			if tc.orig != nil {
				orig := &KafkaSource{
					Spec: *tc.orig,
				}
				ctx = apis.WithinUpdate(ctx, orig)
			}
			updated := &KafkaSource{
				Spec: tc.updated,
			}

			err := updated.Validate(ctx)
			if tc.allowed != (err == nil) {
				t.Fatalf("Unexpected immutable field check. Expected %v. Actual %v", tc.allowed, err)
			}
		})
	}
}
