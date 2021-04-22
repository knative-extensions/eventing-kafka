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
	"context"

	"github.com/kelseyhightower/envconfig"
	"github.com/stretchr/testify/assert"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/upgrade/prober"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

// ChannelTest tests channel operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func ChannelTest(opts TestOptions) pkgupgrade.BackgroundOperation {
	ctx := context.Background()
	var client *testlib.Client
	var probe prober.Prober
	setup := func(c pkgupgrade.Context) {
		// setup
		opts = fillInDefaults(c, opts)
		client = testlib.Setup(c.T, false)
		opts.SetKafkaChannelAsDefault(KafkaChannelAsDefaultOptions{
			UpgradeCtx:         c,
			Ctx:                ctx,
			Client:             client,
			ChannelTypeMeta:    opts.ChannelTypeMeta,
			ReplicationOptions: *opts.ReplicationOptions,
			RetryOptions:       *opts.RetryOptions,
		})
		config := prober.NewConfig(client.Namespace)
		// TODO: knative/eventing#5176 - this is cumbersome
		config.ConfigTemplate = "../../../../../../test/upgrade/config.toml"
		opts.ConfigCustomize(config)
		// envconfig.Process invocation is repeated from within prober.NewConfig to
		// make sure every knob is configurable, but using defaults from Eventing
		// Kafka instead of Core. The prefix is also changed.
		err := envconfig.Process("eventing_kafka_source_upgrade_tests", config)
		assert.NoError(c.T, err)

		probe = prober.RunEventProber(ctx, c.Log, client, config)
	}
	verify := func(c pkgupgrade.Context) {
		// verify
		if client != nil {
			defer testlib.TearDown(client)
		}
		if probe != nil {
			prober.AssertEventProber(ctx, c.T, probe)
		}
	}
	return pkgupgrade.NewBackgroundVerification(
		"ChannelTest", setup, verify)
}
