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
	"fmt"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/upgrade/prober"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

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

// TestOptions holds options for EventingKafka continual tests.
type TestOptions struct {
	ConfigCustomize          func(config *prober.Config)
	SetKafkaChannelAsDefault func(ctx KafkaChannelAsDefaultOptions)
	ChannelTypeMeta          *metav1.TypeMeta
	*ReplicationOptions
	*RetryOptions
}

// KafkaChannelAsDefaultOptions holds a options to run SetKafkaChannelAsDefault func.
type KafkaChannelAsDefaultOptions struct {
	UpgradeCtx      pkgupgrade.Context
	Ctx             context.Context
	ChannelTypeMeta *metav1.TypeMeta
	*testlib.Client
	ReplicationOptions
	RetryOptions
}
type testTarget int

const (
	targetChannel testTarget = iota
	targetSource
)

func fillInDefaults(c pkgupgrade.Context, tt testTarget, opts TestOptions) TestOptions {
	o := opts
	if opts.ConfigCustomize == nil {
		switch tt {
		case targetChannel:
			o.ConfigCustomize = defaultChannelCustomizeConfig
		case targetSource:
			o.ConfigCustomize = defaultSourceCustomizeConfig
		default:
			c.T.Fatal("unsupported test target: ", tt)
		}
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
	if opts.ChannelTypeMeta == nil {
		c.T.Fatal("option ChannelTypeMeta was't set on TestOptions struct")
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
				opts.ChannelTypeMeta.APIVersion, opts.ChannelTypeMeta.Kind,
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
