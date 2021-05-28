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

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/rest"
	"knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"

	fakeKafkaClient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	_ "knative.dev/eventing-kafka/pkg/client/injection/informers/kafka/v1alpha1/resetoffset/fake" // Force Fake Informer Injection
	refmapperstesting "knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers/testing"
	configtesting "knative.dev/eventing-kafka/pkg/common/config/testing"
	"knative.dev/eventing-kafka/pkg/common/configmaploader"
	fakeConfigmapLoader "knative.dev/eventing-kafka/pkg/common/configmaploader/fake"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	commontesting "knative.dev/eventing-kafka/pkg/common/testing"
)

// Test The NewControllerFactory() Functionality
func TestNewControllerFactory(t *testing.T) {

	// Create A Context With Test Logger
	logger := logtesting.TestLogger(t)
	ctx := logging.WithLogger(context.Background(), logger)

	// Register Fake Informers (See Injection "_" Imports Above!)
	ctx, fakeInformers := injection.Fake.SetupInformers(ctx, &rest.Config{})
	assert.NotNil(t, fakeInformers)

	// Add The Fake K8S Clientset To The Context (Populated With ConfigMap)
	configMap := commontesting.GetTestSaramaConfigMap(commonconstants.CurrentConfigVersion, commontesting.OldSaramaConfig, commontesting.TestEKConfig)
	secret := configtesting.NewKafkaSecret()
	ctx, fakeClientset := fake.With(ctx, configMap, secret)
	assert.NotNil(t, fakeClientset)

	// Add The Fake ConfigMap Loader To The Context
	configmapLoader := fakeConfigmapLoader.NewFakeConfigmapLoader()
	configmapLoader.Register(commonconstants.SettingsConfigMapMountPath, configMap.Data)
	ctx = context.WithValue(ctx, configmaploader.Key{}, configmapLoader.Load)

	// Add The Fake Kafka Clientset To The Context (Empty)
	ctx, fakeKafkaClientset := fakeKafkaClient.With(ctx)
	assert.NotNil(t, fakeKafkaClientset)

	// Create A Watcher On The Configuration Settings ConfigMap & Dynamically Update Configuration
	cmw := sharedmain.SetupConfigMapWatchOrDie(ctx, logger)

	// Create Mock ResetOffset Ref Mapper For Testing
	mockResetOffsetRefMapper := &refmapperstesting.MockResetOffsetRefMapper{}
	mockResetOffsetRefMapperFactory := &refmapperstesting.MockResetOffsetRefMapperFactory{}
	mockResetOffsetRefMapperFactory.On("Create", ctx).Return(mockResetOffsetRefMapper)

	// Verify The ResetOffset ControllerFactory Creates A ControllerConstructor
	controllerConstructor := NewControllerFactory(mockResetOffsetRefMapperFactory)
	assert.NotNil(t, controllerConstructor)

	// Verify The ResetOffset ControllerConstructor
	controllerImpl := controllerConstructor(ctx, cmw)
	assert.NotNil(t, controllerImpl)
	assert.True(t, len(controllerImpl.Name) > 0)
	assert.NotNil(t, controllerImpl.Reconciler)
	mockResetOffsetRefMapperFactory.AssertExpectations(t)
	mockResetOffsetRefMapper.AssertExpectations(t)
}

// Test The Shutdown() Functionality
func TestShutdown(t *testing.T) {
	Shutdown() // Currently nothing to test
}
