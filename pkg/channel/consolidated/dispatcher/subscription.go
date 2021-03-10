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

package dispatcher

import (
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing/pkg/channel/fanout"
)

type Subscription struct {
	UID types.UID
	fanout.Subscription
}

func (sub Subscription) String() string {
	var s strings.Builder
	s.WriteString("UID: " + string(sub.UID))
	s.WriteRune('\n')
	if sub.Subscriber != nil {
		s.WriteString("Subscriber: " + sub.Subscriber.String())
		s.WriteRune('\n')
	}
	if sub.Reply != nil {
		s.WriteString("Reply: " + sub.Reply.String())
		s.WriteRune('\n')
	}
	if sub.DeadLetter != nil {
		s.WriteString("DeadLetter: " + sub.DeadLetter.String())
		s.WriteRune('\n')
	}
	return s.String()
}
