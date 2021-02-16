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

	"github.com/google/go-cmp/cmp/cmpopts"

	"knative.dev/pkg/apis"
	"knative.dev/pkg/kmp"
)

// Validate ensures KafkaSource is properly configured.
func (ks *KafkaSource) Validate(ctx context.Context) *apis.FieldError {
	errs := ks.Spec.Validate(ctx).ViaField("spec")
	if apis.IsInUpdate(ctx) {
		original := apis.GetBaseline(ctx).(*KafkaSource)
		errs = errs.Also(ks.CheckImmutableFields(ctx, original))
	}
	return errs
}

func (kss *KafkaSourceSpec) Validate(ctx context.Context) *apis.FieldError {
	var errs *apis.FieldError

	// Validate sink
	errs = errs.Also(kss.Sink.Validate(ctx).ViaField("sink"))

	// We want to ensure we have a bootstrapServer & Topics for creation
	if apis.IsInCreate(ctx) {
		if len(kss.Topics) <= 0 {
			errs = errs.Also(apis.ErrMissingField("Topics"))
		}
		if len(kss.BootstrapServers) <= 0 {
			errs = errs.Also(apis.ErrMissingField("bootstrapServer"))
		}
	}

	return errs
}

func (ks *KafkaSource) CheckImmutableFields(ctx context.Context, original *KafkaSource) *apis.FieldError {
	if original == nil {
		return nil
	}
	var errs *apis.FieldError

	// Ignore diffs in bootstrapServers and topics
	ignoreArgs := cmpopts.IgnoreFields(KafkaSourceSpec{}, "Topics", "BootstrapServers", "Sink", "Net", "CloudEventOverrides")
	if diff, err := kmp.ShortDiff(original.Spec, ks.Spec, ignoreArgs); err != nil {
		errs = errs.Also(&apis.FieldError{
			Message: "Failed to diff KafkaSource",
			Paths:   []string{"spec"},
			Details: err.Error(),
		})
		return errs
	} else if diff != "" {
		errs = errs.Also(
			&apis.FieldError{
				Message: "Immutable fields changed (-old +new)",
				Paths:   []string{"spec"},
				Details: diff,
			})
		return errs
	}

	return errs
}
