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
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka/test/e2e/helpers"
	contribtestlib "knative.dev/eventing-kafka/test/lib"
	contribresources "knative.dev/eventing-kafka/test/lib/resources"
	"knative.dev/eventing/test/upgrade/prober"
	"knative.dev/eventing/test/upgrade/prober/sut"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	pkgTest "knative.dev/pkg/test"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

const (
	kafkaBootstrapUrlPlain   = "my-cluster-kafka-bootstrap.kafka.svc:9092"
	kafkaClusterName         = "my-cluster"
	kafkaClusterNamespace    = "kafka"
	sourceConfigTemplatePath = "test/upgrade/continual/source-config.toml"
)

// SourceTest tests source operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func SourceTest(opts *TestOptions) pkgupgrade.BackgroundOperation {
	return continualVerification(
		"SourceContinualTest",
		ensureKafkaSender(opts),
		&kafkaSourceSut{},
		sourceConfigTemplatePath,
	)
}

func ensureKafkaSender(opts *TestOptions) *TestOptions {
	opts.Configurators = append([]prober.Configurator{
		func(config *prober.Config) error {
			config.Wathola.ContainerImageResolver = kafkaSourceSenderImageResolver
			return nil
		},
	}, opts.Configurators...)
	return opts
}

func kafkaSourceSenderImageResolver(component string) string {
	if component == "wathola-sender" {
		// replacing the original image with modified one from this repo
		component = "wathola-kafka-sender"
	}
	return pkgTest.ImagePath(component)
}

type kafkaSourceSut struct{}

func (k kafkaSourceSut) Deploy(ctx sut.Context, destination duckv1.Destination) interface{} {
	topicName := uuid.NewString()
	helpers.MustCreateTopic(ctx.Client, kafkaClusterName, kafkaClusterNamespace,
		topicName, 6)
	contribtestlib.CreateKafkaSourceV1Beta1OrFail(ctx.Client, contribresources.KafkaSourceV1Beta1(
		kafkaBootstrapUrlPlain,
		topicName,
		toObjectReference(destination),
	))
	return kafkaTopicEndpoint{
		BootstrapServers: kafkaBootstrapUrlPlain,
		TopicName:        topicName,
	}
}

func toObjectReference(destination duckv1.Destination) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		APIVersion: destination.Ref.APIVersion,
		Kind:       destination.Ref.Kind,
		Namespace:  destination.Ref.Namespace,
		Name:       destination.Ref.Name,
	}
}
