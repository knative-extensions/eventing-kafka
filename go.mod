module knative.dev/eventing-kafka

go 1.14

require (
	github.com/Azure/azure-event-hubs-go v1.3.1
	github.com/Azure/azure-sdk-for-go v37.1.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/azure/auth v0.4.2 // indirect
	github.com/Azure/go-autorest/autorest/to v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.2.0 // indirect
	github.com/cloudevents/sdk-go/v2 v2.0.0-RC2
	github.com/confluentinc/confluent-kafka-go v1.0.0
	github.com/google/go-cmp v0.4.0
	github.com/grpc-ecosystem/grpc-gateway v1.13.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/client_model v0.2.0
	github.com/slok/goresilience v0.2.0
	github.com/stretchr/testify v1.5.1
	go.uber.org/atomic v1.5.1 // indirect
	go.uber.org/multierr v1.4.0 // indirect
	go.uber.org/zap v1.10.0
	k8s.io/api v0.16.4
	k8s.io/apimachinery v0.16.5-beta.1
	k8s.io/client-go v11.0.1-0.20190805182717-6502b5e7b1b5+incompatible
	k8s.io/utils v0.0.0-20191218082557-f07c713de883 // indirect
	knative.dev/eventing v0.14.1-0.20200429150142-6fd15c83fba5
	knative.dev/eventing-contrib v0.14.0
	knative.dev/pkg v0.0.0-20200428194351-90fc61bae7f7
	knative.dev/test-infra v0.0.0-20200428221142-e594f444fabd
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
