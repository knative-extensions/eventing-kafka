# Contributed Kafka Channel implementation

This is a Kafka Channel implementation, contributed by
[SAP's Kyma project](https://knative.dev/eventing-kafka).

See https://github.com/knative/eventing-contrib/issues/1070 for discussion of
the donation process.

# Knative Kafka

This project is a Knative Eventing implementation of a Kafka backed channel 
which provides advanced functionality and production grade qualities as an alternative
to what the [eventing-contrib/kafka](https://github.com/knative/eventing-contrib/tree/master/kafka) 
implementation offers.

The goal is to merge the eventing-contrib implementation with this one to provide
a consolidated location where all kafka based knative development efforts can take occur.

## Rationale

The Knative eventing-contrib repository already contains a Kafka implementation, 
so why invest the time in building another one?  At the time this project was 
begun, and still today, the reference Kafka implementation does not provide the
scaling characteristics required by a large and varied use case with many 
different Topics and Consumers.  That implementation is based on a single 
choke point that could easily allow one Topic's traffic to impact the 
throughput of another Topic.  It is also not horizontally scalable as it only 
supports a single instance of the dispatcher/consumer.  Further, no ordering 
guarantees on the consumption side are provided which is required in certain 
use cases.  The reference implementation was also based on the 
[Sarama Go Client](https://github.com/Shopify/sarama) which historically had limitations 
compared to the [Confluent Go Client](https://github.com/confluentinc/confluent-kafka-go), 
and the [librdkafka C library](https://github.com/edenhill/librdkafka) on which 
it is based.  We also had the need to support a variety of Kafka providers,
including Azure EventHubs in Kafka compatibility mode.  Finally, the ability 
to expose Kafka configuration was very limited, and we needed the ability to 
customize certain aspects of the Kafka Topics / Producers / Consumers.



## Background / Status

The Knative-kafka project originated as an internal SAP implementation and was 
based on very early knative-eventing implementations.  At the time this meant 
kube-builder and the controller-runtime library were used for the foundation of
the controller.  This also predated any of the more recent duck-typing and 
higher level abstractions (brokers, triggers, etc) which have since been added
to knative-eventing.

The internal SAP project for which this was intended also underwent several 
variations in it's requirements and approach, which sometimes meant the 
development of this project languished behind the fast moving and ever 
changing knative-eventing implementation.  Further, internal corporate 
CI/CD constraints imposed some of the structure of the current project.   

Recently, however, the commitment to this effort was renewed, and the 
implementation is now current with knative-eventing **/master**.  Work is 
in progress to align with implementation structure in eventing-contrib in
the hopes that this project can merge with the default "kafka" implementation.  
The project had been moved to the open source [kyma-incubator](https://github.com/kyma-incubator/) 
repository and the focus was on bringing it into alignment with the other 
eventing-contrib implementations.

Now, with the contribution to Knative and the move to the knative-sandbox,
we are one step closer to a combined solution.

## Architecture 

As mentioned in the "Rationale" section above, the desire was to implement 
different levels of granularity to achieve improved segregation and scaling
characteristics.  Our original implementation was extremely granular in that 
there was a separate Channel/Producer Deployment for every `KafkaChannel` 
(Kafka Topic), and a separate Dispatcher/Consumer Deployment for every Knative 
Subscription.  This allowed the highest level of segregation and the ability to 
tweak K8S resources at the finest level.

The downside of this approach, however, is the large resource consumption 
related to the sheer number of Deployments in the K8S cluster, as well as the
inherent inefficiencies of low traffic rate Channels / Subscriptions being 
underutilized. Adding in a service-mesh (such as Istio) further exacerbates the
problem by adding side-cars to every Deployment.  Therefore, we've taken a step
back and aggregated the Channels/Producers together into a single Deployment per 
Kafka authorization, and the Dispatchers/Consumers into a single Deployment per 
`KafkaChannel` (Topic). The implementations of each are horizontally scalable 
which provides a reasonable compromise between resource consumption and 
segregation / scaling.



### Project Structure

**Eventing-kafka** is comprised of three distinct runtime K8S deployments 
as follows...

- [channel](./pkg/channel/README.md) - The event receiver of the Channel 
to which inbound messages are sent.  An http server which accepts messages that
conform to the CloudEvent specification, and then writes those messages to the 
corresponding Kafka Topic. This is the "Producer" from the Kafka perspective.
A separate Channel Deployment is created for each Kafka Secret detected in the
knative-eventing namespace.
    
- [controller](./pkg/controller/README.md) - This component implements 
the `KafkaChannel` Controller. It is using the current knative-eventing 
"Shared Main" approach based directly on K8S informers / listers.  The 
controller is using the `KafkaChannel` CRD, api, and client from the current 
Knative eventing-contrib/kafka implementation in order to align as closely as 
possible with the existing implementation. 

- [dispatcher](./pkg/dispatcher/README.md) - This component runs the 
Kafka ConsumerGroups responsible for processing messages from the corresponding 
Kafka Topic.  This is the "Consumer" from the Kafka perspective.  A separate 
dispatcher Deployment will be created for each unique `KafkaChannel` (Kafka 
Topic), and will contain a distinct Kafka Consumer Group for each 
Subscription to the `KafkaChannel`.

...and the controller can be deployed via the Helm Chart found in the
resources/ directory...  

- [resources](./resources/README.md) - Eventing-kafka Helm Chart used to install 
the `KafkaChannel` CRD and controller.  The use of **ko** has been explored but 
is currently not feasible due to limitations with its ability to handle our 
build / packaging of the external librdkafka C library.


### Control Plane

The control plane for the Kafka Channels is managed by the 
[eventing-kafka-controller](./pkg/controller/README.md) which is installed
in the knative-eventing namespace. `KafkaChannel` Custom Resource instances can 
be created in any user namespace. The eventing-kafka-controller will guarantee 
that the Data Plane is configured to support the flow of events as defined by 
[Subscriptions](https://knative.dev/docs/reference/eventing/#messaging.knative.dev/v1alpha1.Subscription) 
to a KafkaChannel.  The underlying Kafka infrastructure to be used is defined in 
a specially labeled [K8S Secret](./resources/README.md#Credentials) in the 
knative-eventing namespace.  Eventing-kafka supports several different Kafka 
(and Kafka-like) [infrastructures](./resources/README.md#Kafka%20Providers).


### Data Plane

The data plane for all `KafkaChannels` runs in the knative-eventing namespace.  
There is a single deployment for the receiver side of all channels which accepts 
CloudEvents and sends them to Kafka.  Each `KafkaChannel` uses one Kafka topic.
This deployment supports horizontal scaling with linearly increasing performance 
characteristics through specifying the number of replicas.

Each `KafkaChannel` has one deployment for the dispatcher side which reads from 
the Kafka topic and sends to subscribers.  Each subscriber has its own Kafka 
consumer group. This deployment can be scaled up to a replica count equalling the
number of partitions in the Kafka topic.


### Messaging Guarantees

An event sent to a `KafkaChannel` is guaranteed to be persisted and processed 
if a 202 response is received by the sender.  

The CloudEvent is partitioned based on the [CloudEvent partitioning extension](https://github.com/cloudevents/spec/blob/master/extensions/partitioning.mdhttps://github.com/cloudevents/spec/blob/master/extensions/partitioning.md) 
field called `partitionkey`.  If the `partitionkey` is not present, then the 
`subject` field will be used.  Finally if neither is available, it will fall-back
to random partitioning.

Events in each partition are processed in order, with an **at-least-once** guarantee. 
If a full cycle of retries for a given subscription fails, the event is ignored 
and processing continues with the next event.


## Installation

For installation and configuration instructions please see the Helm Chart 
[README](./resources/README.md).


## Support Tooling

The project uses the Confluent Go Client which relies on the following...
```
brew install pkg-config
brew install librdkafka
```
...alternatively, if the current/latest version of the librdkafka C library is 
not compatible with eventing-kafka, then you can manually install an older 
version via the following.... 
```
curl -LO https://github.com/edenhill/librdkafka/archive/v1.0.1.tar.gz \
  && tar -xvf v1.0.1.tar.gz \
  && cd librdkafka-1.0.1 \
  && ./configure \
  && make \
  && sudo make install
```
