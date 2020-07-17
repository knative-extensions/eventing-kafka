module knative.dev/eventing-kafka

go 1.14

require (
	github.com/Azure/azure-event-hubs-go v1.3.1
	github.com/Azure/go-autorest/autorest/azure/auth v0.4.2 // indirect
	github.com/Shopify/sarama v1.26.4
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.0.0-00010101000000-000000000000
	github.com/cloudevents/sdk-go/v2 v2.0.0
	github.com/google/go-cmp v0.4.1
	github.com/grpc-ecosystem/grpc-gateway v1.13.0 // indirect
	github.com/prometheus/client_golang v1.5.0
	github.com/prometheus/client_model v0.2.0
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	github.com/slok/goresilience v0.2.0
	github.com/stretchr/testify v1.5.1
	go.uber.org/zap v1.14.1
	google.golang.org/protobuf v1.22.0 // indirect
	k8s.io/api v0.18.2
	k8s.io/apimachinery v0.18.2
	k8s.io/client-go v12.0.0+incompatible
	knative.dev/eventing v0.14.1-0.20200515003800-b3a3da6ce0a6
	knative.dev/eventing-contrib v0.14.1-0.20200515032200-98e505fd8247
	knative.dev/pkg v0.0.0-20200515002500-16d7b963416f
	knative.dev/test-infra v0.0.0-20200514223200-ef4fd3ad398f
	pack.ag/amqp v0.12.4 // indirect
)

replace (
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v12.3.0+incompatible
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 => github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.0.1-0.20200608152019-2ab697c8fc0b
	github.com/cloudevents/sdk-go/v2 => github.com/cloudevents/sdk-go/v2 v2.0.1-0.20200608152019-2ab697c8fc0b
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v0.9.2
	k8s.io/api => k8s.io/api v0.17.6
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.17.6
	k8s.io/apimachinery => k8s.io/apimachinery v0.17.6
	k8s.io/client-go => k8s.io/client-go v0.17.6
	k8s.io/code-generator => k8s.io/code-generator v0.17.6
)
