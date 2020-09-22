
# E2E Cluster Configuration

The YAML content in this directory is used to configure the K8S / Knative cluster
for E2E and Conformance testing of the eventing-kafka KafkaChannel implementation.

## Kafka ([Strimzi](https://github.com/strimzi/strimzi-kafka-operator))

The following files are pulled directly from Strimzi without modification (other than filename) and
will be copied / modified slightly by the [e2e-tests.sh](../e2e-tests.sh) script to customize the
installation namespace.

- **100-strimzi-cluster-operator-0.18.0.yaml:**  Version 0.18.0 of the Strimzi operator installation yaml.
- **200-kafka-ephemeral-single-0.18.0.yaml:**  Version 0.18.0 of the Strimzi Kafka CRD instance for creating an ephemeral, single-replica Kafka Cluster (version 2.5.0).
