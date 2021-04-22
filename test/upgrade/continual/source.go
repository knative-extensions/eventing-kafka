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

import pkgupgrade "knative.dev/pkg/test/upgrade"

// SourceTest tests source operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func SourceTest(_ TestOptions) pkgupgrade.BackgroundOperation {
	setup := func(c pkgupgrade.Context) {
		// TODO: not yet implemented
		c.Log.Warn("TODO: not yet implemented")
	}
	verify := func(c pkgupgrade.Context) {
		// TODO: not yet implemented
		c.Log.Warn("TODO: not yet implemented")
	}
	return pkgupgrade.NewBackgroundVerification(
		"SourceContinualTest", setup, verify)
}
