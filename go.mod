module knative.dev/eventing-kafka

go 1.14

require (
	github.com/Azure/azure-event-hubs-go v1.3.1
	github.com/Azure/go-autorest/autorest/azure/auth v0.4.2 // indirect
	github.com/cloudevents/sdk-go/v2 v2.0.0-RC4
	github.com/confluentinc/confluent-kafka-go v1.4.2
	github.com/google/go-cmp v0.4.0
	github.com/grpc-ecosystem/grpc-gateway v1.13.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.5.0
	github.com/prometheus/client_model v0.2.0
	github.com/slok/goresilience v0.2.0
	github.com/stretchr/testify v1.5.1
	go.uber.org/zap v1.14.1
	k8s.io/api v0.17.4
	k8s.io/apimachinery v0.17.4
	k8s.io/client-go v12.0.0+incompatible
	knative.dev/eventing v0.14.1-0.20200515003800-b3a3da6ce0a6
	knative.dev/eventing-contrib v0.14.1-0.20200515032200-98e505fd8247
	knative.dev/pkg v0.0.0-20200515002500-16d7b963416f
	knative.dev/test-infra v0.0.0-20200514223200-ef4fd3ad398f
	pack.ag/amqp v0.12.4 // indirect
)

replace (
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v12.3.0+incompatible
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v0.9.2
	k8s.io/api => k8s.io/api v0.16.4
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.16.4
	k8s.io/apimachinery => k8s.io/apimachinery v0.16.4
	k8s.io/client-go => k8s.io/client-go v0.16.4
	k8s.io/code-generator => k8s.io/code-generator v0.16.4
)
