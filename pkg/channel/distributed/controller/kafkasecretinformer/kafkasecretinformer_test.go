package kafkasecretinformer

import (
	"context"
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/kubernetes/fake"
	"knative.dev/eventing/pkg/logging"
	injectionclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/injection"
	logtesting "knative.dev/pkg/logging/testing"
	_ "knative.dev/pkg/system/testing"
	"testing"
)

// Test The Get() Functionality
func TestGet(t *testing.T) {

	// Create A Context With Test Logger & K8S Client
	ctx := logging.WithLogger(context.TODO(), logtesting.TestLogger(t).Desugar())
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
