# E2E Cluster Configuration

The YAML content in this directory is used to configure the K8S / Knative
cluster for E2E and Conformance testing of the eventing-kafka KafkaChannel
implementation.

## Kafka ([Strimzi](https://github.com/strimzi/strimzi-kafka-operator))

The strimzi-cluster-operator and kafka-ephemeral-triple YAML files in
test/config are pulled directly from Strimzi without modification (other than
filename) and will be copied / modified slightly by the
[e2e-tests.sh](../e2e-tests.sh) script to customize the installation namespace.
