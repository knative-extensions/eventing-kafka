package main

import (
	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	"knative.dev/eventing-kafka/pkg/controller/constants"
	"knative.dev/eventing-kafka/pkg/controller/kafkachannel"
	"knative.dev/eventing-kafka/pkg/controller/kafkasecret"
	"knative.dev/pkg/injection/sharedmain"
)

// Eventing-Kafka Controller Main
func main() {

	// Shutdown / Cleanup Hook For Controllers
	defer kafkachannel.Shutdown()
	defer kafkasecret.Shutdown()

	// UnComment To Enable Sarama Logging For Local Debug
	// kafkautil.EnableSaramaLogging()

	// Create The SharedMain Instance With The Various Controllers
	sharedmain.Main(constants.ControllerComponentName, kafkachannel.NewController, kafkasecret.NewController)
}
