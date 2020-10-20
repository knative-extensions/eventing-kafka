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
