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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"knative.dev/eventing-kafka/pkg/apis/kafka"
)

func TestKind(t *testing.T) {
	kind := Kind("test-kind")
	assert.Equal(t, schema.GroupKind{Group: "kafka.eventing.knative.dev", Kind: "test-kind"}, kind)
}

func TestResource(t *testing.T) {
	resource := Resource("test-resource")
	assert.Equal(t, schema.GroupResource{Group: kafka.GroupName, Resource: "test-resource"}, resource)
}

func TestAddKnownTypes(t *testing.T) {
	scheme := runtime.NewScheme()
	err := addKnownTypes(scheme)
	assert.Nil(t, err)
	types := scheme.KnownTypes(schema.GroupVersion{Group: kafka.GroupName, Version: "v1alpha1"})
	assert.NotNil(t, types)
	roType := types["ResetOffset"]
	assert.NotNil(t, roType)
	roListType := types["ResetOffsetList"]
	assert.NotNil(t, roListType)
}
