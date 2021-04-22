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

package upgrade

import (
	"knative.dev/eventing-kafka/test/upgrade/continual"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

// ChannelContinualTest tests channel operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func ChannelContinualTest(opts continual.TestOptions) pkgupgrade.BackgroundOperation {
	return continual.ChannelTest(fillInDefaults(opts))
}

// SourceContinualTest tests source operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func SourceContinualTest(opts continual.TestOptions) pkgupgrade.BackgroundOperation {
	return continual.SourceTest(fillInDefaults(opts))
}

func fillInDefaults(opts continual.TestOptions) continual.TestOptions {
	if opts.ChannelTypeMeta == nil {
		opts.ChannelTypeMeta = &defaultChannelType
	}
	return opts
}
