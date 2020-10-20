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

package util

import (
	"crypto/md5"
	"fmt"
)

// Generate An MD5 Hash Of A String And Return Desired Number Of Characters
func GenerateHash(stringToHash string, length int) string {
	// Create an MD5 hash and return however many characters the caller wants (note that the max is 32 for MD5)
	return fmt.Sprintf(fmt.Sprintf("%%.%ds", length), fmt.Sprintf("%x", md5.Sum([]byte(stringToHash))))
}
