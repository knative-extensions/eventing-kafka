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

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
		config.OnDuplicate = prober.Error
		config.FailOnErrors = true
		config.Interval = 2 * time.Millisecond

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
	configmaps := client.Kube.CoreV1().ConfigMaps("knative-eventing")
	data := []byte(fmt.Sprintf(`{
		"channelTemplateSpec": "apiVersion: %s\nkind: %s\n"
  }`, defaultChannelType.APIVersion, defaultChannelType.Kind))
	cm, err := configmaps.Patch(ctx, "config-br-default-channel",
		types.MergePatchType, data, metav1.PatchOptions{})

	if !assert.NoError(c.T, err) {
		c.T.FailNow()
	}

	c.Log.Info("Updated config-br-default-channel in ns knative-eventing"+
			" to eq: ", cm.Data)
}
