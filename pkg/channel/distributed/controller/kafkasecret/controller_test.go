package kafkasecret

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/rest"
	fakeKafkaClient "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/client/fake"
	_ "knative.dev/eventing-contrib/kafka/channel/pkg/client/injection/informers/messaging/v1beta1/kafkachannel/fake" // Knative Fake Informer Injection
	commonconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/constants"
	commonenv "knative.dev/eventing-kafka/pkg/channel/distributed/common/env"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	controllerenv "knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	_ "knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkasecretinformer/fake" // Knative Fake Informer Injection
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/test"
	"knative.dev/eventing/pkg/logging"
	"knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment/fake" // Knative Fake Informer Injection
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/service/fake"    // Knative Fake Informer Injection
	"knative.dev/pkg/injection"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/system"
)

// Test The NewController() Functionality
func TestNewController(t *testing.T) {

	// Populate Environment Variables For Testing
	populateEnvironmentVariables(t)

	// Create A Context With Test Logger
	ctx := logging.WithLogger(context.Background(), logtesting.TestLogger(t).Desugar())

	// Register Fake Informers (See Injection "_" Imports Above!)
	ctx, fakeInformers := injection.Fake.SetupInformers(ctx, &rest.Config{})
	assert.NotNil(t, fakeInformers)

	// Add The Fake K8S Clientset To The Context (Populated With ConfigMap)
	configMap := commontesting.GetTestSaramaConfigMap(test.SaramaConfigYaml, test.ControllerConfigYaml)
	ctx, fakeClientset := fake.With(ctx, configMap)
	assert.NotNil(t, fakeClientset)

	// Add The Fake Kafka Clientset To The Context (Empty)
	ctx, fakeKafkaClientset := fakeKafkaClient.With(ctx)
	assert.NotNil(t, fakeKafkaClientset)

	// Perform The Test (Create The KafkaChannel Controller)
	controller := NewController(ctx, nil)

	// Verify The Results
	assert.NotNil(t, controller)
	assert.Equal(t, "knative.dev-eventing-kafka-pkg-channel-distributed-controller-kafkasecret.Reconciler", controller.Name)
	assert.NotNil(t, controller.Reconciler)
}

// Test The Shutdown() Functionality - No-op Test Just For Coverage ; )
func TestShutdown(t *testing.T) {
	Shutdown()
}

// Utility Function For Populating Required Environment Variables For Testing
func populateEnvironmentVariables(t *testing.T) {
	assert.Nil(t, os.Setenv(system.NamespaceEnvKey, commonconstants.KnativeEventingNamespace))
	assert.Nil(t, os.Setenv(commonenv.ServiceAccountEnvVarKey, test.ServiceAccount))
	assert.Nil(t, os.Setenv(commonenv.MetricsDomainEnvVarKey, test.MetricsDomain))
	assert.Nil(t, os.Setenv(commonenv.MetricsPortEnvVarKey, strconv.Itoa(test.MetricsPort)))
	assert.Nil(t, os.Setenv(controllerenv.DispatcherImageEnvVarKey, test.MetricsDomain))
	assert.Nil(t, os.Setenv(controllerenv.ChannelImageEnvVarKey, test.MetricsDomain))
}
