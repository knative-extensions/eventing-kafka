# Kafka-Channel Controller

The Controller component implements the KafkaChannel CRD (api, client,
reconciler, etc.) based on the latest knative-eventing SharedMain reconciler
framework and utilities.

The controller is based against the KafkaChannel CRD and reconciles all such
instances in the K8S Cluster, watching `KafkaChannel` resources and performing
the following functions:
- Provision the Kafka Topic
- Create the single Receiver (Kafka Producer) Deployment and Service, if it
  does not already exist
- Create a uniquely-named (per KafkaChannel) Dispatcher (Kafka Consumer)
  Deployment and Service

**Note** - Deleting a KafkaChannel CRD instance is destructive in that it will
Remove the Kafka Topic resulting in the loss of all events therein. While the
Dispatcher and Receiver will perform semi-graceful shutdown there is no attempt
to "drain" the topic or complete incoming CloudEvents.

## Kafka AdminClient

The current implementation supports the following mechanisms for handling Topic
creation / deletion as specified by the kafka.adminType field in the
[ConfigMap](../../../../config/channel/distributed/300-eventing-kafka-configmap.yaml)

- **"kafka"** - The default value if not specified is to use the standard Sarama
  Kafka ClusterAdmin implementation.
- **"azure"** - If you wish to use the implementation with Azure EventHubs
  you will need to specify this as a custom client/api must be used for such.
- **"custom"** - If you need to implement your own custom AdminClient you will
  use this value (see the [common/kafka/README.md](../common/kafka/README.md)).
