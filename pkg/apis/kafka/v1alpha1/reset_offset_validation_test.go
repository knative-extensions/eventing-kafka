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
	"context"
	"testing"
	
	"github.com/google/go-cmp/cmp"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/webhook/resourcesemantics"
)

func TestResetOffset_Validate(t *testing.T) {

	refAPIVersion := "messaging.knative.dev/v1beta1"
	refKind := "Subscription"
	refNamespace := "ref-namespace"
	refName := "ref-name"

	reference := duckv1.KReference{
		APIVersion: refAPIVersion,
		Kind:       refKind,
		Namespace:  refNamespace,
		Name:       refName,
	}

	tests := []struct {
		name string
		cr   resourcesemantics.GenericCRD
		want *apis.FieldError
	}{
		{
			name: "valid offset earliest",
			cr: &ResetOffset{
				Spec: ResetOffsetSpec{Offset: OffsetEarliest, Ref: reference},
			},
		},
		{
			name: "valid offset latest",
			cr: &ResetOffset{
				Spec: ResetOffsetSpec{Offset: OffsetLatest, Ref: reference},
			},
		},
		{
			name: "valid offset time",
			cr: &ResetOffset{
				Spec: ResetOffsetSpec{Offset: "2021-05-04T05:04:01Z", Ref: reference}, // RFC3339
			},
		},
		{
			name: "invalid offset",
			cr: &ResetOffset{
				Spec: ResetOffsetSpec{Offset: "foo", Ref: reference},
			},
			want: func() *apis.FieldError {
				var errs *apis.FieldError
				fe := apis.ErrInvalidValue("foo", "spec.offset")
				errs = errs.Also(fe)
				return errs
			}(),
		},
		{
			name: "invalid ref nil",
			cr: &ResetOffset{
				Spec: ResetOffsetSpec{Offset: OffsetEarliest},
			},
			want: func() *apis.FieldError {
				var errs *apis.FieldError
				fe := apis.ErrMissingField("spec.apiVersion", "spec.kind", "spec.name")
				errs = errs.Also(fe)
				return errs
			}(),
		},
		{
			name: "invalid ref missing APIVersion",
			cr: &ResetOffset{
				Spec: ResetOffsetSpec{
					Offset: OffsetEarliest,
					Ref: duckv1.KReference{
						Kind:      refKind,
						Namespace: refNamespace,
						Name:      refName,
					},
				},
			},
			want: func() *apis.FieldError {
				var errs *apis.FieldError
				fe := apis.ErrMissingField("spec.apiVersion")
				errs = errs.Also(fe)
				return errs
			}(),
		},
		{
			name: "invalid ref missing Kind",
			cr: &ResetOffset{
				Spec: ResetOffsetSpec{
					Offset: OffsetEarliest,
					Ref: duckv1.KReference{
						APIVersion: refAPIVersion,
						Namespace:  refNamespace,
						Name:       refName,
					},
				},
			},
			want: func() *apis.FieldError {
				var errs *apis.FieldError
				fe := apis.ErrMissingField("spec.kind")
				errs = errs.Also(fe)
				return errs
			}(),
		},
		{
			name: "invalid ref missing Name",
			cr: &ResetOffset{
				Spec: ResetOffsetSpec{
					Offset: OffsetEarliest,
					Ref: duckv1.KReference{
						APIVersion: refAPIVersion,
						Kind:       refKind,
						Namespace:  refNamespace,
					},
				},
			},
			want: func() *apis.FieldError {
				var errs *apis.FieldError
				fe := apis.ErrMissingField("spec.name")
				errs = errs.Also(fe)
				return errs
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.cr.Validate(context.Background())
			if test.want == nil {
				if diff := cmp.Diff(test.want, got); diff != "" {
					t.Errorf("validate (-want, +got) = %v", diff)
				}
			} else if diff := cmp.Diff(test.want.Error(), got.Error()); diff != "" {
				t.Errorf("validate (-want, +got) = %v", diff)
			}
		})
	}
}
