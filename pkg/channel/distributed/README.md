# Eventing-Kafka Distributed Channel

This is the Kafka Channel implementation, originally contributed by
[SAP's Kyma project](https://github.com/kyma-project).

> See <https://github.com/knative/eventing-contrib/issues/1070> for discussion
> of the donation process.

This repo falls under the
[Knative Code of Conduct](https://github.com/knative/community/blob/master/CODE-OF-CONDUCT.md)

This project is a Knative Eventing implementation of a Kafka backed channel
which provides a more granular architecture as an alternative to what the
original "[consolidated](../consolidated)" implementation offers. Specifically
it deploys a single/separate Receiver, and one Dispatcher per KafkaChannel.

## Rationale

The Knative "consolidated" KafkaChannel already provides a Kafka backed Channel
implementation, so why invest the time in building another one? At the time this
project was begun, and still today, the reference Kafka implementation does not
provide the scaling characteristics required by a large and varied use case with
many Topics and Consumers. That implementation is based on a single choke point
that could allow one Topic's traffic to impact the throughput of another Topic.

We also had the need to support a variety of Kafka providers, including Azure
EventHubs in Kafka compatibility mode as well as exposing the ability to
customize the Kafka Topic management. Finally, the ability to expose Kafka
configuration was very limited, and we needed the ability to customize certain
aspects of the Kafka Topics / Producers / Consumers.

## Status

Significant work has recently gone into aligning the two implementations from a
CRD, configuration, authorization, and code-sharing perspective, in order to
standardize the user experience as well as maximize code reuse. While the
runtime architectures will always be different
(the "raison d'etre" for having multiple implementations), the goal is to
continue this sharing. The eventual goal might be to have a single KafkaChannel
implementation that can deploy either runtime architecture as desired.

## Architecture

As mentioned in the "Rationale" section above, the desire was to implement
different levels of granularity to achieve improved segregation and scaling
characteristics. Our original implementation was extremely granular in that
there was a separate Channel/Producer Deployment for every `KafkaChannel` (Kafka
Topic), and a separate Dispatcher/Consumer Deployment for every Knative
Subscription. This allowed the highest level of segregation and the ability to
tweak K8S resources at the finest level.

The downside of this approach, however, is the large resource consumption
related to the sheer number of Deployments in the K8S cluster, as well as the
inherent inefficiencies of low traffic rate Channels / Subscriptions being
underutilized. Adding in a service-mesh (such as Istio) further exacerbates the
problem by adding side-cars to every Deployment. Therefore, we've taken a step
back and aggregated the Channels/Producers together into a single Deployment per
Kafka authorization, and the Dispatchers/Consumers into a single Deployment per
`KafkaChannel` (Topic). The implementations of each are horizontally scalable
which provides a reasonable compromise between resource consumption and
segregation / scaling.

### Project Structure

The "distributed" KafkaChannel consists of three distinct runtime K8S
deployments as follows...

- [controller](controller/README.md) - This component implements the
  `KafkaChannel` Controller. It is using the current knative-eventing "Shared
  Main" approach based directly on K8S informers / listers. The controller is
  using the shared `KafkaChannel` CRD, [apis/](../../../pkg/apis), and
  [client](../../../pkg/client) implementations in this repository.

- [dispatcher](dispatcher/README.md) - This component runs the Kafka
  ConsumerGroups responsible for processing messages from the corresponding
  Kafka Topic. This is the "Consumer" from the Kafka perspective. A separate
  dispatcher Deployment will be created for each unique `KafkaChannel` (Kafka
  Topic), and will contain a distinct Kafka Consumer Group for each Subscription
  to the `KafkaChannel`.

- [receiver](receiver/README.md) - The event receiver to which all inbound
  messages are sent. An HTTP server which accepts messages that conform to the
  CloudEvent specification, and then writes those messages to the corresponding
  Kafka Topic. This is the "Producer" from the Kafka perspective. A single
  receiver Deployment is created to service all KafkaChannels in the cluster.

- [config](../../../config/channel/distributed/README.md) - Eventing-kafka
  **ko** installable YAML files for installation.

### Control Plane

The control plane for the Kafka Channels is managed by the
[eventing-kafka-controller](controller/README.md) which is installed in the
knative-eventing namespace. `KafkaChannel` Custom Resource instances can be
created in any user namespace. The eventing-kafka-controller will guarantee that
the Data Plane is configured to support the flow of events as defined by
[Subscriptions](https://knative.dev/docs/reference/eventing/#messaging.knative.dev/v1alpha1.Subscription)
to a KafkaChannel. The underlying Kafka infrastructure to be used is defined in
a specially labeled
[K8S Secret](../../../config/channel/distributed/README.md#Credentials) in the
knative-eventing namespace. Eventing-kafka supports several Kafka (and
Kafka-like)
[infrastructures](../../../config/channel/distributed/README.md#Kafka%20Providers)
.

### Data Plane

The data plane for all `KafkaChannels` runs in the knative-eventing namespace.
There is a single deployment for the receiver side of all channels which accepts
CloudEvents and writes them to Kafka Topics. Each `KafkaChannel` uses one Kafka
Topic. This deployment supports horizontal scaling with linearly increasing
performance characteristics through specifying the number of replicas.

Each `KafkaChannel` has one deployment for the dispatcher side which reads from
the Kafka Topic and sends to subscribers. Each subscriber has its own Kafka
consumer group. This deployment can be scaled up to a replica count equalling
the number of partitions in the Kafka Topic.

### Messaging Guarantees

An event sent to a `KafkaChannel` is guaranteed to be persisted and processed if
a 202 response is received by the sender.

The CloudEvent is partitioned based on the
[CloudEvent partitioning extension](https://github.com/cloudevents/spec/blob/master/extensions/partitioning.md)
field called `partitionkey`. If the `partitionkey` is not present, then the
`subject` field will be used. Finally, if neither is available, it will
fall-back to random partitioning.

Events in each partition are processed in order, with an **at-least-once**
guarantee. If a full cycle of retries for a given subscription fails, the event
is ignored, or sent to the DLQ according to the Subscription's `DeliverySpec`
and processing continues with the next event.

## Installation

For installation and configuration instructions please see the config files
[README](../../../config/channel/distributed/README.md).
