module knative.dev/eventing-kafka

go 1.16

require (
	github.com/Azure/azure-event-hubs-go/v3 v3.3.2
	github.com/Azure/azure-sdk-for-go v47.1.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.0 // indirect
	github.com/Shopify/sarama v1.30.1
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.4.1
	github.com/cloudevents/sdk-go/v2 v2.8.0
	github.com/davecgh/go-spew v1.1.1
	github.com/google/go-cmp v0.5.6
	github.com/google/gofuzz v1.2.0
	github.com/google/uuid v1.3.0
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/mitchellh/mapstructure v1.3.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/rickb777/date v1.13.0
	github.com/slinkydeveloper/loadastic v0.0.0-20201218203601-5c69eea3b7d8
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/xdg-go/scram v1.0.2
	go.opencensus.io v0.23.0
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.19.1
	golang.org/x/crypto v0.0.0-20211202192323-5770296d904e // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/protobuf v1.27.1
	k8s.io/api v0.22.5
	k8s.io/apimachinery v0.22.5
	k8s.io/client-go v0.22.5
	k8s.io/utils v0.0.0-20211208161948-7d6a63dca704
	knative.dev/control-protocol v0.0.0-20220201152531-79a0b1ad34b9
	knative.dev/eventing v0.29.1-0.20220214155047-0326f92dd5b7
	knative.dev/hack v0.0.0-20220215185059-b9cb1983b600
	knative.dev/pkg v0.0.0-20220215153400-3c00bb0157b9
	knative.dev/reconciler-test v0.0.0-20220215020657-3f8092fb8739
	sigs.k8s.io/yaml v1.3.0
)

replace (
	github.com/dgrijalva/jwt-go => github.com/form3tech-oss/jwt-go v0.0.0-20210511163231-5b2d2b5f6c34
	github.com/miekg/dns v1.0.14 => github.com/miekg/dns v1.1.25
)
