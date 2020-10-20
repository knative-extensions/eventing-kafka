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

package kafkasecretinformer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/kubernetes/fake"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
	_ "knative.dev/pkg/system/testing"
)

// Test The Get() Functionality
func TestGet(t *testing.T) {

	// Create A Context With Test Logger & K8S Client
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t))
	ctx = context.WithValue(ctx, injectionclient.Key{}, fake.NewSimpleClientset())

	// Verify The KafkaSecretInformer Was Added To Knative Injection
	informers := injection.Default.GetInformers()
	assert.NotNil(t, informers)
	assert.Len(t, informers, 1)

	// Add The KafkaSecretInformer To The Test Context
	ctx, controllerInformer := withInformer(ctx)
	assert.NotNil(t, ctx)
	assert.NotNil(t, controllerInformer)

	// Perform The Test & Verify Results
	kafkaSecretInformer := Get(ctx)
	assert.NotNil(t, kafkaSecretInformer)
}
