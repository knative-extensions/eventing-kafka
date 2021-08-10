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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test The JoinStringMaps() Functionality
func TestJoinStringMaps(t *testing.T) {
	map1 := map[string]string{"key1": "value1a", "key2": "value2", "key3": "value3"}
	map2 := map[string]string{"key1": "value1b", "key4": "value4"}
	emptyMap := map[string]string{}

	resultMap := JoinStringMaps(map1, nil)
	assert.Equal(t, map1, resultMap)

	resultMap = JoinStringMaps(map1, emptyMap)
	assert.Equal(t, map1, resultMap)

	resultMap = JoinStringMaps(nil, map2)
	assert.Equal(t, map2, resultMap)

	resultMap = JoinStringMaps(emptyMap, map2)
	assert.Equal(t, map2, resultMap)

	resultMap = JoinStringMaps(map1, map2)
	assert.Equal(t, map[string]string{"key1": "value1a", "key2": "value2", "key3": "value3", "key4": "value4"}, resultMap)
	assert.Equal(t, map1, map[string]string{"key1": "value1a", "key2": "value2", "key3": "value3"})
	assert.Equal(t, map2, map[string]string{"key1": "value1b", "key4": "value4"})
}
