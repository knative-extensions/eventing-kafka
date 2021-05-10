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

package continual

import (
	"knative.dev/eventing/test/upgrade/prober/sut"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

// SourceTest tests source operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func SourceTest(opts *TestOptions) pkgupgrade.BackgroundOperation {
	return continualVerification(
		"SourceContinualTest",
		opts,
		sourceSut(),
	)
}

func sourceSut() sut.SystemUnderTest {
	return &kafkaSourceSut{}
}

type kafkaSourceSut struct{}

func (k kafkaSourceSut) Deploy(ctx sut.Context, destination duckv1.Destination) *apis.URL {
	panic("implement me")
}

func (k kafkaSourceSut) Teardown(ctx sut.Context) {
	panic("implement me")
}
