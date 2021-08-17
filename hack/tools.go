//go:build tools
// +build tools

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

package tools

import (
	_ "knative.dev/hack"
	_ "knative.dev/pkg/hack"

	// Test images from eventing
	_ "knative.dev/eventing/test/test_images/event-sender"
	_ "knative.dev/eventing/test/test_images/heartbeats"
	_ "knative.dev/eventing/test/test_images/performance"
	_ "knative.dev/eventing/test/test_images/print"
	_ "knative.dev/eventing/test/test_images/recordevents"
	_ "knative.dev/eventing/test/test_images/wathola-fetcher"
	_ "knative.dev/eventing/test/test_images/wathola-forwarder"
	_ "knative.dev/eventing/test/test_images/wathola-receiver"
	_ "knative.dev/eventing/test/test_images/wathola-sender"
	_ "knative.dev/reconciler-test/cmd/eventshub"

	// For migration
	_ "knative.dev/pkg/apiextensions/storageversion/cmd/migrate"
)
