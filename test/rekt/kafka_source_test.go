// +build e2e

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

package rekt

import (
	"testing"
	"time"

	"knative.dev/eventing-kafka/test/rekt/features/kafkasource"
	"knative.dev/eventing/pkg/utils"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
)

const (
	kafkaSASLSecret = "strimzi-sasl-secret"
	kafkaTLSSecret  = "strimzi-tls-secret"
)

func TestKafkaSource(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(2*time.Second, 20*time.Second),
		environment.Managed(t),
	)

	kc := kubeclient.Get(ctx)
	_, err := utils.CopySecret(kc.CoreV1(), system.Namespace(), kafkaTLSSecret, env.Namespace(), "default")
	if err != nil {
		t.Fatalf("could not copy secret(%s): %v", kafkaTLSSecret, err)
	}

	_, err = utils.CopySecret(kc.CoreV1(), system.Namespace(), kafkaSASLSecret, env.Namespace(), "default")
	if err != nil {
		t.Fatalf("could not copy secret(%s): %v", kafkaSASLSecret, err)
	}

	for _, f := range kafkasource.DataPlaneDelivery() {
		env.Test(ctx, t, f)
	}
}
