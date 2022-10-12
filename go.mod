module knative.dev/eventing-kafka

go 1.16

require (
	github.com/Azure/azure-event-hubs-go/v3 v3.3.2
	github.com/Azure/azure-sdk-for-go v47.1.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.0 // indirect
	github.com/Shopify/sarama v1.30.1
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.4.1
	github.com/cloudevents/sdk-go/sql/v2 v2.8.0 // indirect
	github.com/cloudevents/sdk-go/v2 v2.12.0
	github.com/davecgh/go-spew v1.1.1
	github.com/google/go-cmp v0.5.8
	github.com/google/gofuzz v1.2.0
	github.com/google/uuid v1.3.0
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pelletier/go-toml v1.9.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/rickb777/date v1.13.0
	github.com/slinkydeveloper/loadastic v0.0.0-20201218203601-5c69eea3b7d8
	github.com/stretchr/testify v1.8.0
	github.com/xdg-go/scram v1.0.2
	go.opencensus.io v0.23.0
	go.uber.org/multierr v1.8.0
	go.uber.org/zap v1.21.0
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4
	golang.org/x/time v0.0.0-20220210224613-90d013bbcef8
	google.golang.org/protobuf v1.28.0
	k8s.io/api v0.25.2
	k8s.io/apimachinery v0.25.2
	k8s.io/client-go v0.25.2
	k8s.io/utils v0.0.0-20220728103510-ee6ede2d64ed
	knative.dev/control-protocol v0.0.0-20221012042851-b3d2d775d60c
	knative.dev/eventing v0.34.1-0.20221011150150-2899fcef641f
	knative.dev/hack v0.0.0-20221010154335-3fdc50b9c24a
	knative.dev/pkg v0.0.0-20221011175852-714b7630a836
	knative.dev/reconciler-test v0.0.0-20221006134333-0a1020d0acdc
	sigs.k8s.io/yaml v1.3.0
)

replace (
	github.com/dgrijalva/jwt-go => github.com/form3tech-oss/jwt-go v0.0.0-20210511163231-5b2d2b5f6c34
	github.com/miekg/dns v1.0.14 => github.com/miekg/dns v1.1.25
)
