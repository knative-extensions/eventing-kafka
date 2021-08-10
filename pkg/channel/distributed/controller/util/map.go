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

// JoinStringMaps returns a new map containing the contents of both argument maps, preferring the contents of map1 on conflict.
func JoinStringMaps(map1 map[string]string, map2 map[string]string) map[string]string {
	resultMap := make(map[string]string, len(map1))
	for map1Key, map1Value := range map1 {
		resultMap[map1Key] = map1Value
	}
	for map2Key, map2Value := range map2 {
		if _, ok := map1[map2Key]; !ok {
			resultMap[map2Key] = map2Value
		}
	}
	return resultMap
}
