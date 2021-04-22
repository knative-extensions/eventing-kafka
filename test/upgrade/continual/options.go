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

import (
	"time"

	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/test/lib/resources"
	"knative.dev/eventing/test/upgrade/prober"
	pkgTest "knative.dev/pkg/test"
)

var (
	defaultChannelCustomizeConfig = func(config *prober.Config) {
		config.FailOnErrors = true
		config.Interval = 2 * time.Millisecond
		config.BrokerOpts = append(config.BrokerOpts,
			resources.WithDeliveryForBroker(&eventingduckv1.DeliverySpec{
				Retry:         &retryCount,
				BackoffPolicy: &backoffPolicy,
				BackoffDelay:  &backoffDelay,
			}))
	}
	defaultSourceCustomizeConfig = func(config *prober.Config) {
		defaultChannelCustomizeConfig(config)
		config.Wathola.ContainerImageResolver = func(component string) string {
			if component == "wathola-sender" {
				component = "wathola-kafka-sender"
			}
			return pkgTest.ImagePath(component)
		}
	}
	retryCount    = int32(12)
	backoffPolicy = eventingduckv1.BackoffPolicyExponential
	backoffDelay  = "PT1S"
)
