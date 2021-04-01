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
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/upgrade/prober"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

// ChannelContinualTest tests channel operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func ChannelContinualTest() pkgupgrade.BackgroundOperation {
	ctx := context.Background()
	var client *testlib.Client
	var probe prober.Prober
	setup := func(c pkgupgrade.Context) {
		// setup
		client = testlib.Setup(c.T, false)
		configureKafkaChannelAsDefault(c, ctx, client)
		config := prober.NewConfig(client.Namespace)
		// TODO: knative/eventing#5176 - this is cumbersome
		config.ConfigTemplate = "../../../../../../test/upgrade/config.toml"
		config.FailOnErrors = true
		config.Interval = 2 * time.Millisecond
		// envconfig.Process invocation is repeated from within prober.NewConfig to
		// make sure every knob is configurable, but using defaults from Eventing
		// Kafka instead of Core.
		err := envconfig.Process("e2e_upgrade_tests", config)
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

func configureKafkaChannelAsDefault(
	c pkgupgrade.Context,
	ctx context.Context,
	client *testlib.Client,
) {
	systemNs := "knative-eventing"
	configmaps := client.Kube.CoreV1().ConfigMaps(systemNs)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: systemNs,
			Name:      "config-br-default-channel",
		},
		Data: map[string]string{
			"channelTemplateSpec": fmt.Sprintf("apiVersion: %s\nkind: %s\n",
				defaultChannelType.APIVersion, defaultChannelType.Kind,
			),
		},
	}
	cm, err := configmaps.Update(ctx, cm, metav1.UpdateOptions{})

	if !assert.NoError(c.T, err) {
		c.T.FailNow()
	}

	c.Log.Info("Updated config-br-default-channel in ns knative-eventing"+
		" to eq: ", cm.Data)
}
