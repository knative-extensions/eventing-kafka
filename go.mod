module knative.dev/eventing-kafka

go 1.14

require (
	github.com/Azure/azure-event-hubs-go v1.3.1
	github.com/Shopify/sarama v1.27.0
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.2.0
	github.com/cloudevents/sdk-go/v2 v2.2.0
	github.com/ghodss/yaml v1.0.0
	github.com/google/go-cmp v0.5.1
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	github.com/slok/goresilience v0.2.0
	github.com/stretchr/testify v1.6.0
	go.opencensus.io v0.22.5-0.20200716030834-3456e1d174b2
	go.uber.org/zap v1.15.0
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	k8s.io/api v0.18.8
	k8s.io/apimachinery v0.18.8
	k8s.io/client-go v11.0.1-0.20190805182717-6502b5e7b1b5+incompatible
	k8s.io/utils v0.0.0-20200603063816-c1c6865ac451
	knative.dev/eventing v0.17.1-0.20200917130042-886594db5d01
	knative.dev/eventing-contrib v0.17.1-0.20200918150945-976d3638b6f2
	knative.dev/pkg v0.0.0-20200916171541-6e0430fd94db
	knative.dev/test-infra v0.0.0-20200924055440-7e083bb00cf9
	pack.ag/amqp v0.12.4 // indirect
)

replace (
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v12.3.0+incompatible
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 => github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.0.1-0.20200608152019-2ab697c8fc0b
	k8s.io/api => k8s.io/api v0.18.8
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.18.8
	k8s.io/apimachinery => k8s.io/apimachinery v0.18.8
	k8s.io/client-go => k8s.io/client-go v0.18.8
	k8s.io/code-generator => k8s.io/code-generator v0.18.8
)
