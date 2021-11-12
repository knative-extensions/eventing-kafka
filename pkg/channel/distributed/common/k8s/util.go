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

package k8s

import (
	"regexp"
)

const (
	K8sLabelValueMaxLength = 63
)

// Truncate The Specified K8S Label Value To The Max
func TruncateLabelValue(str string) string {
	strCopy := str
	if len(str) > K8sLabelValueMaxLength {
		strCopy = str[0:K8sLabelValueMaxLength]
	}
	// Strip any and all trailing '-' characters from the end of the string
	strCopy = regexp.MustCompile(`^(.*[^-])-*`).ReplaceAllString(strCopy, "${1}")
	return strCopy
}
