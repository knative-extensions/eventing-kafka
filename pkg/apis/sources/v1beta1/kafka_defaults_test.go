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
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"k8s.io/utils/pointer"
	"knative.dev/eventing-kafka/pkg/apis/sources/config"
)

type assertFnType func(t *testing.T, ks KafkaSource, expected interface{})

type defaultKafkaTestArgs struct {
	Name        string
	Defaults    config.KafkaSourceDefaults
	Initial     KafkaSource
	Expected    interface{}
	AssertFuncs []assertFnType
}

func TestSetDefaults(t *testing.T) {
	assertUUID := func(t *testing.T, ks KafkaSource, expected interface{}) {
		consumerGroup := strings.Split(ks.Spec.ConsumerGroup, uuidPrefix)
		_, err := uuid.Parse(consumerGroup[len(consumerGroup)-1])
		if err != nil {
			t.Fatalf("Error Parsing UUID value: %s", err)
		}
	}
	assertGivenGroup := func(t *testing.T, ks KafkaSource, expected interface{}) {
		if diff := cmp.Diff(ks.Spec.ConsumerGroup, expected); diff != "" {
			t.Fatalf("Unexpected consumerGroup Set (-want, +got): %s", diff)
		}
	}
	assertConsumers := func(t *testing.T, ks KafkaSource, expected interface{}) {
		i, _ := strconv.Atoi(expected.(string))
		i32 := int32(i)
		if diff := cmp.Diff(ks.Spec.Consumers, &i32); diff != "" {
			t.Fatalf("Unexpected consumers (-want, +got): %s", diff)
		}
	}

	assertNoAnnotations := func(t *testing.T, ks KafkaSource, expected interface{}) {
		if len(ks.Annotations) != 0 {
			t.Fatalf("Unexpected annotations: %v", ks.Annotations)
		}
	}
	assertAnnotations := func(t *testing.T, ks KafkaSource, expected interface{}) {
		if diff := cmp.Diff(ks.Annotations, expected); diff != "" {
			t.Fatalf("Unexpected annotations (-want, +got): %s", diff)
		}
	}
	testCases := []defaultKafkaTestArgs{
		{
			Name:        "nil spec",
			Initial:     KafkaSource{},
			AssertFuncs: []assertFnType{assertUUID, assertNoAnnotations},
		}, {
			Name: "Set consumerGroup",
			Initial: KafkaSource{
				Spec: KafkaSourceSpec{
					ConsumerGroup: "foo",
				},
			},

			Expected:    "foo",
			AssertFuncs: []assertFnType{assertGivenGroup, assertNoAnnotations},
		},
		{
			Name:        "consumers not set",
			Initial:     KafkaSource{},
			Expected:    "1",
			AssertFuncs: []assertFnType{assertConsumers, assertNoAnnotations},
		}, {
			Name:        "consumers set",
			Initial:     KafkaSource{Spec: KafkaSourceSpec{Consumers: pointer.Int32Ptr(4)}},
			Expected:    "4",
			AssertFuncs: []assertFnType{assertConsumers, assertNoAnnotations},
		}, {
			Name: "autoscaling config",
			Defaults: config.KafkaSourceDefaults{
				AutoscalingClass:  "keda.autoscaling.knative.dev",
				MinScale:          40,
				MaxScale:          60,
				PollingInterval:   500,
				CooldownPeriod:    4000,
				KafkaLagThreshold: 100,
			},
			Initial: KafkaSource{},
			Expected: map[string]string{
				classAnnotation:             "keda.autoscaling.knative.dev",
				minScaleAnnotation:          "40",
				maxScaleAnnotation:          "60",
				pollingIntervalAnnotation:   "500",
				cooldownPeriodAnnotation:    "4000",
				kafkaLagThresholdAnnotation: "100",
			},
			AssertFuncs: []assertFnType{assertAnnotations},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := config.ToContext(context.Background(), &config.Config{KafkaSourceDefaults: &tc.Defaults})
			tc.Initial.SetDefaults(ctx)
			for _, assertFunc := range tc.AssertFuncs {
				assertFunc(t, tc.Initial, tc.Expected)
			}
		})
	}
}
