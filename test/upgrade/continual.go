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
	"context"
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/resources"
	"knative.dev/eventing/test/upgrade/prober"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

var (
	defaultCustomizeConfig = func(config *prober.Config) {
		config.FailOnErrors = true
		config.Interval = 2 * time.Millisecond
		config.BrokerOpts = append(config.BrokerOpts,
			resources.WithDeliveryForBroker(&eventingduckv1.DeliverySpec{
				Retry:         &retryCount,
				BackoffPolicy: &backoffPolicy,
				BackoffDelay:  &backoffDelay,
			}))
	}
	retryCount    = int32(12)
	backoffPolicy = eventingduckv1.BackoffPolicyExponential
	backoffDelay  = "PT1S"
)

// ChannelContinualTest tests channel operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func ChannelContinualTest(opts ContinualTestOptions) pkgupgrade.BackgroundOperation {
	opts = fillInDefaults(opts)
	ctx := context.Background()
	var client *testlib.Client
	var probe prober.Prober
	setup := func(c pkgupgrade.Context) {
		// setup
		client = testlib.Setup(c.T, false)
		opts.SetKafkaChannelAsDefault(KafkaChannelAsDefaultOptions{
			UpgradeCtx:         c,
			Ctx:                ctx,
			Client:             client,
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
		"ChannelContinualTest", setup, verify)
}

// SourceContinualTest tests source operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func SourceContinualTest(_ ContinualTestOptions) pkgupgrade.BackgroundOperation {
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

// ReplicationOptions hold options for replication.
type ReplicationOptions struct {
	NumPartitions     int
	ReplicationFactor int
}

// RetryOptions holds options for retries.
type RetryOptions struct {
	RetryCount    int
	BackoffPolicy eventingduckv1.BackoffPolicyType
	BackoffDelay  string
}

// ContinualTestOptions holds options for EventingKafka continual tests.
type ContinualTestOptions struct {
	ConfigCustomize          func(config *prober.Config)
	SetKafkaChannelAsDefault func(ctx KafkaChannelAsDefaultOptions)
	*ReplicationOptions
	*RetryOptions
}

// KafkaChannelAsDefaultOptions holds a options to run SetKafkaChannelAsDefault func.
type KafkaChannelAsDefaultOptions struct {
	UpgradeCtx pkgupgrade.Context
	Ctx        context.Context
	*testlib.Client
	ReplicationOptions
	RetryOptions
}

func fillInDefaults(opts ContinualTestOptions) ContinualTestOptions {
	o := opts
	if opts.ConfigCustomize == nil {
		o.ConfigCustomize = defaultCustomizeConfig
	}
	if opts.SetKafkaChannelAsDefault == nil {
		o.SetKafkaChannelAsDefault = configureKafkaChannelAsDefault
	}
	if opts.RetryOptions == nil {
		o.RetryOptions = &RetryOptions{
			RetryCount:    int(retryCount),
			BackoffPolicy: backoffPolicy,
			BackoffDelay:  backoffDelay,
		}
	}
	if opts.ReplicationOptions == nil {
		o.ReplicationOptions = &ReplicationOptions{
			NumPartitions:     6,
			ReplicationFactor: 3,
		}
	}
	return o
}

func configureKafkaChannelAsDefault(opts KafkaChannelAsDefaultOptions) {
	c := opts.UpgradeCtx
	systemNs := "knative-eventing"
	configmaps := opts.Client.Kube.CoreV1().ConfigMaps(systemNs)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: systemNs,
			Name:      "config-br-default-channel",
		},
		Data: map[string]string{
			"channelTemplateSpec": fmt.Sprintf(`apiVersion: %s
kind: %s
spec:
  numPartitions: %d
  replicationFactor: %d
  delivery:
    retry: %d
    backoffPolicy: %s
    backoffDelay: %s`,
				defaultChannelType.APIVersion, defaultChannelType.Kind,
				opts.NumPartitions, opts.ReplicationFactor,
				opts.RetryCount, opts.BackoffPolicy, opts.BackoffDelay,
			),
		},
	}
	cm, err := configmaps.Update(opts.Ctx, cm, metav1.UpdateOptions{})

	if !assert.NoError(c.T, err) {
		c.T.FailNow()
	}

	c.Log.Info("Updated config-br-default-channel in ns knative-eventing"+
		" to eq: ", cm.Data)
}
