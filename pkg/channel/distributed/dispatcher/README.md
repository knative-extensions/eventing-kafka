# Eventing-kafka Dispatcher

The "Dispatcher" implementation is a set of Kafka ConsumerGroups which are
responsible for reading Kafka Messages from a Kafka Topic and converting them
back to CloudEvents before sending them on to the Knative Subscription endpoint.

A unique "Dispatcher" Deployment is created for every KafkaChannel (Topic) and
contains a Kafka Consumer for every Subscription to that KafkaChannel. The
single Deployment is horizontally scalable as necessary (via controller
environment variables.) Each such instance will contain a Kafka Consumer in the
same ConsumerGroup for the Subscription. The maximum number of such instances
should never be larger than your configured partitioning, as no further
performance will be gained beyond that. This allows for an efficient use of
cluster resources while still supporting high volume use cases in a segregated
manner.

A Service is also created for the Dispatcher but this is simply to facilitate
Prometheus monitoring endpoints.

The Dispatcher is watching KafkaChannel resources to perform reconciliation of
changes to the Subscribable (Channelable) list of subscribers. It also updates
the KafkaChannel's SubscriberStatus when the Kafka ConsumerGroups are deemed to
be operational.

The Kafka brokers and credentials are obtained from mounted Secret data from the
aforementioned Kafka Secret.

## Tracing, Profiling, and Metrics

The Dispatcher makes use of the infrastructure surrounding the config-tracing
and config-observability configmaps (see
[Accessing CloudEvent traces](https://knative.dev/docs/eventing/accessing-traces)
and
[Installing logging, metrics, and traces](https://knative.dev/docs/serving/installing-logging-metrics-traces)
for more information on the basic Knative-Eventing concepts behind these
features. The default behavior for tracing and profiling is provided such that
accessing a tracing server (such as Zipkin), and the debug profiling
information, should work as described in those links. For example, you might
access your zipkin server via
`http://localhost:8001/api/v1/namespaces/knative-eventing/services/zipkin:9411/proxy/zipkin`
after running a "kubectl proxy" command or your profiling server via
`http://localhost:8008/debug/pprof` after executing "kubectl -n knative-eventing
port-forward my-dispatcher-pod-name 8008:8008"

Eventing-Kafka does provide some of its own custom metrics that use the
Prometheus server provided by the Knative-Eventing framework. When a dispatcher
deployment starts, you can test the custom metrics with curl as in the following
example, which assumes you have created a dispatcher named
"kafka-channel-dispatcher" and exposed the metrics endpoint of 8081 in the
service. These commands presume you are running from inside the cluster in a pod
that has curl available:

Send a cloud event to your eventing-kafka installation:

```
echo -e "{\n  \"id\": \"12345\",\n  \"originationTime\": \"$(date -u +'%Y-%m-%dT%H:%M:%S.000000000Z')\",\n  \"content\": \"Test Message\"\n}" | curl -X POST -H "Content-Type: application/json" -H "cache-control: no-cache" -H "ce-id: 123" -H "ce-source: /testsource" -H "ce-specversion: 1.0" -H "ce-type: test.type.v1" --data @- -v http://my-kafkachannel-service.mynamespace.svc.cluster.local/
< HTTP/1.1 202 Accepted
```

Check the dispatcher metrics, noting that the eventing_kafka_consumed_msg_count
has increased by 1 on one of the partitions

```
curl -v http://kafka-channel-dispatcher.knative-eventing.svc.cluster.local:8081/metrics | grep consumed_msg_count | sort
# HELP eventing_kafka_consumed_msg_count Consumed Message Count
# TYPE eventing_kafka_consumed_msg_count gauge
eventing_kafka_consumed_msg_count{consumer="rdkafka#consumer-1",partition="0",topic="mynamespace.my-kafkachannel-service"} 0
eventing_kafka_consumed_msg_count{consumer="rdkafka#consumer-1",partition="1",topic="mynamespace.my-kafkachannel-service"} 0
eventing_kafka_consumed_msg_count{consumer="rdkafka#consumer-1",partition="2",topic="mynamespace.my-kafkachannel-service"} 0
eventing_kafka_consumed_msg_count{consumer="rdkafka#consumer-1",partition="3",topic="mynamespace.my-kafkachannel-service"} 0
eventing_kafka_consumed_msg_count{consumer="rdkafka#consumer-2",partition="0",topic="mynamespace.my-kafkachannel-service"} 0
eventing_kafka_consumed_msg_count{consumer="rdkafka#consumer-2",partition="1",topic="mynamespace.my-kafkachannel-service"} 0
eventing_kafka_consumed_msg_count{consumer="rdkafka#consumer-2",partition="2",topic="mynamespace.my-kafkachannel-service"} 1
eventing_kafka_consumed_msg_count{consumer="rdkafka#consumer-2",partition="3",topic="mynamespace.my-kafkachannel-service"} 0
```
