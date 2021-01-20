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

package kafkasecret

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/rest"
	commonenv "knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	controllerenv "knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	_ "knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkasecretinformer/fake" // Knative Fake Informer Injection
	controllertesting "knative.dev/eventing-kafka/pkg/channel/distributed/controller/testing"
	fakeKafkaClient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	_ "knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel/fake" // Knative Fake Informer Injection
	"knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment/fake" // Knative Fake Informer Injection
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/service/fake"    // Knative Fake Informer Injection
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
)

// Test The NewController() Functionality
func TestNewController(t *testing.T) {

	// Populate Environment Variables For Testing
	populateEnvironmentVariables(t)

	// Create A Context With Test Logger
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.Background(), logger)

	// Register Fake Informers (See Injection "_" Imports Above!)
	ctx, fakeInformers := injection.Fake.SetupInformers(ctx, &rest.Config{})
	assert.NotNil(t, fakeInformers)

	// Add The Fake K8S Clientset To The Context (Populated With ConfigMap)
	configMap := commontesting.GetTestSaramaConfigMap(controllertesting.SaramaConfigYaml, controllertesting.ControllerConfigYaml)
	ctx, fakeClientset := fake.With(ctx, configMap)
	assert.NotNil(t, fakeClientset)

	// Add The Fake Kafka Clientset To The Context (Empty)
	ctx, fakeKafkaClientset := fakeKafkaClient.With(ctx)
	assert.NotNil(t, fakeKafkaClientset)

	// Perform The Test (Create The KafkaChannel Controller)
	environment, err := controllerenv.GetEnvironment(logger.Desugar())
	assert.Nil(t, err)
	ctx = context.WithValue(ctx, controllerenv.Key{}, environment)
	controller := NewController(ctx, nil)

	// Verify The Results
	assert.NotNil(t, controller)
	assert.True(t, len(controller.Name) > 0)
	assert.NotNil(t, controller.Reconciler)
}

// Test The Shutdown() Functionality - No-op Test Just For Coverage ; )
func TestShutdown(t *testing.T) {
	Shutdown()
}

// Utility Function For Populating Required Environment Variables For Testing
func populateEnvironmentVariables(t *testing.T) {
	commontesting.SetTestEnvironment(t)
	assert.Nil(t, os.Setenv(commonenv.ServiceAccountEnvVarKey, controllertesting.ServiceAccount))
	assert.Nil(t, os.Setenv(commonenv.MetricsDomainEnvVarKey, controllertesting.MetricsDomain))
	assert.Nil(t, os.Setenv(commonenv.MetricsPortEnvVarKey, strconv.Itoa(controllertesting.MetricsPort)))
	assert.Nil(t, os.Setenv(controllerenv.DispatcherImageEnvVarKey, controllertesting.DispatcherImage))
	assert.Nil(t, os.Setenv(controllerenv.ReceiverImageEnvVarKey, controllertesting.ReceiverImage))
	assert.Nil(t, os.Setenv(commonenv.ResyncPeriodMinutesEnvVarKey, strconv.Itoa(int(controllertesting.ResyncPeriod/time.Minute))))
}
