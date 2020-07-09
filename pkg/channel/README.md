# Eventing-Kafka Channel

The "Channel" implementation is a Kafka Producer which is responsible for
receiving CloudEvents, converting them to Kafka Messages, and writing them
to the appropriate Kafka Topic.

A unique Channel Deployment / Service is created for every Kafka Secret (in
the knative-eventing namespace and labelled with
`eventing-kafka.knative.dev/kafka-secret: "true"`).  The single Deployment
is horizontally scalable as necessary (via controller environment variables.) This
allows for an efficient use of cluster resources while still supporting high
volume and multi-tenant use cases.

An additional Service for each KafkaChannel is created in the user namespace
where the KafkaChannel exists.  This is the actual endpoint of the `KafkaChannel`
This service forwards requests to the service mentioned above in the
knative-eventing namespace.  This is all necessary so that the Channel
implementation can use the hostname of the request to map incoming CloudEvents
to the appropriate Kafka Topic.

The Kafka brokers and credentials are obtained from mounted Secret data
from the aforementiond Kafka Secret.
