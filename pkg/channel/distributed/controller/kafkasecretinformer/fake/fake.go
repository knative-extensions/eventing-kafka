package fake

import (
	"context"
	kafkaSecretInformer "knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkasecretinformer"
	fakeSecretInformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake" // Knative Fake Informer Injection
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
)

var Get = kafkaSecretInformer.Get

func init() {
	injection.Fake.RegisterInformer(withInformer)
}

func withInformer(ctx context.Context) (context.Context, controller.Informer) {
	inf := fakeSecretInformer.Get(ctx) // Using Standard Fake Secret Informer
	return context.WithValue(ctx, kafkaSecretInformer.Key{}, inf), inf.Informer()
}
