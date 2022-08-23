module knative.dev/eventing-kafka

go 1.16

require (
	github.com/Azure/azure-event-hubs-go/v3 v3.3.2
	github.com/Azure/azure-sdk-for-go v47.1.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.0 // indirect
	github.com/Shopify/sarama v1.30.1
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.4.1
	github.com/cloudevents/sdk-go/v2 v2.10.1
	github.com/davecgh/go-spew v1.1.1
	github.com/google/go-cmp v0.5.6
	github.com/google/gofuzz v1.2.0
	github.com/google/uuid v1.3.0
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/rickb777/date v1.13.0
	github.com/slinkydeveloper/loadastic v0.0.0-20201218203601-5c69eea3b7d8
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/xdg-go/scram v1.0.2
	go.opencensus.io v0.23.0
	go.uber.org/multierr v1.8.0
	go.uber.org/zap v1.21.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/protobuf v1.27.1
	k8s.io/api v0.23.9
	k8s.io/apimachinery v0.23.9
	k8s.io/client-go v0.23.9
	k8s.io/utils v0.0.0-20220210201930-3a6ce19ff2f9
	knative.dev/control-protocol v0.0.0-20220818153549-f18dbde7d9bd
	knative.dev/eventing v0.34.0
	knative.dev/hack v0.0.0-20220823140917-8d1e4ccf9dc3
	knative.dev/pkg v0.0.0-20220818004048-4a03844c0b15
	knative.dev/reconciler-test v0.0.0-20220818122349-177f8264c28c
	sigs.k8s.io/yaml v1.3.0
)

replace (
	github.com/dgrijalva/jwt-go => github.com/form3tech-oss/jwt-go v0.0.0-20210511163231-5b2d2b5f6c34
	github.com/miekg/dns v1.0.14 => github.com/miekg/dns v1.1.25
)
