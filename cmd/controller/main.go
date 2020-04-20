package main

import (
	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	"github.com/kyma-incubator/knative-kafka/pkg/controller/kafkachannel"
	"github.com/kyma-incubator/knative-kafka/pkg/controller/kafkasecret"
	"knative.dev/pkg/injection/sharedmain"
)

// Knative-Kafka Controller Main
func main() {

	// Shutdown / Cleanup Hook For Controllers
	defer kafkachannel.Shutdown()
	defer kafkasecret.Shutdown()

	// Create The SharedMain Instance With The Various Controllers
	sharedmain.Main("knativekafkachannel_controller", kafkachannel.NewController, kafkasecret.NewController)
}
