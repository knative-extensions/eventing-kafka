package fake

import (
	"context"

	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkasecretinformer"
	"knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake" // Knative Fake Informer Injection
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
)

var Get = kafkasecretinformer.Get

func init() {
	injection.Fake.RegisterInformer(withInformer)
}

func withInformer(ctx context.Context) (context.Context, controller.Informer) {
	inf := fake.Get(ctx) // Using Standard Fake Secret Informer
	return context.WithValue(ctx, kafkasecretinformer.Key{}, inf), inf.Informer()
}
