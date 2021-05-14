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

package testing

import (
	"context"

	"github.com/stretchr/testify/mock"

	kafkav1alpha1 "knative.dev/eventing-kafka/pkg/apis/kafka/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers"
)

//
// Mock ResetOffsetRefMapperFactory
//

var _ refmappers.ResetOffsetRefMapperFactory = &MockResetOffsetRefMapperFactory{}

type MockResetOffsetRefMapperFactory struct {
	mock.Mock
}

func (f *MockResetOffsetRefMapperFactory) Create(ctx context.Context) refmappers.ResetOffsetRefMapper {
	args := f.Called(ctx)
	return args.Get(0).(refmappers.ResetOffsetRefMapper)
}

//
// Mock ResetOffsetRefMapper
//

var _ refmappers.ResetOffsetRefMapper = &MockResetOffsetRefMapper{}

type MockResetOffsetRefMapper struct {
	mock.Mock
}

func (m *MockResetOffsetRefMapper) MapRef(resetOffset *kafkav1alpha1.ResetOffset) (string, string, error) {
	args := m.Called(resetOffset)
	return args.String(0), args.String(1), args.Error(2)
}
