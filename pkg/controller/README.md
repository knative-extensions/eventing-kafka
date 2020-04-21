# Kafka-Channel Controller

The Controller component implements the KafkaChannel CRD (api, client, reconciler,
etc.) based on the latest knative-eventing SharedMain reconciler framework and
utilities.

The controller is based against the KafkaChannel CRD type, from the Knative
eventing-contrib/kafka implementation, and reconciles all such instances in the
K8S Cluster.  It actually consists of two reconcilers, one for watching
"Kafka" Secrets (those in knative-eventing labelled
`knativekafka.kyma-project.io/kafka-secret: "true"`) which provisions the Kafka
Topic and creates the Channel / Producer Deployment & Service, and another which
is watching `KafkaChannel` resources and creates the Dispatcher / Consumer
Deployment & Service.

**Note** - Deleting a KafkaChannel CRD instance is destructive in that it will
Remove the Kafka Topic resulting in the loss of all events therein.  While the
Dispatcher and Producer will perform semi-graceful shutdown there is no attempt
to "drain" the topic or complete incoming CloudEvents.
