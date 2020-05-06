# Eventing-kafka Dispatcher

The "Dispatcher" implementation is a set of Kafka ConsumerGroups which are
responsible for reading Kafka Messages from a Kafka Topic and converting
them back to CloudEvents before sending them on to the Knative Subscription
endpoint.

A unique "Dispatcher" Deployment is created for every KafkaChannel (Topic)
and contains a Kafka Consumer for every Subscription to that KafkaChannel.
The single Deployment is horizontally scalable as necessary (via controller
environment variables.)  Each such instance will contain a Kafka Consumer in
the same ConsumerGroup for the Subscription.  The maximum number of such instances
should never be larger than your configured partitioning, as no further
performance will be gained beyond that.  This allows for an efficient use of
cluster resources while still supporting high volume use cases in a segregated
manner.

A Service is also created for the Dispatcher but this is simply to facilitate
Prometheus monitoring endpoints.

The Dispatcher is watching KafkaChannel resources to perform reconciliation of
changes to the Subscribable (Channelable) list of subscribers.  It also updates
the KafkaChannel's SubscriberStatus when the Kafka ConsumerGroups are deemed to
be operational.

The implementation makes use of the
[Conluent Go Client](https://github.com/confluentinc/confluent-kafka-go)
library, along with the underlying C/C++
[librdkafka](https://github.com/edenhill/librdkafka) library.
The Kafka brokers and credentials are obtained from mounted Secret data
from the aforementiond Kafka Secret.


