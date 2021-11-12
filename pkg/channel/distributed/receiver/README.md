# Eventing-Kafka Receiver

The Receiver implementation is a Kafka Producer which is responsible for
receiving CloudEvents, converting them to Kafka Messages, and writing them to
the appropriate Kafka Topic.

## Runtime Deployments

A unique Deployment / Service is created for every installation. The single
Deployment is horizontally scalable as necessary; the replicas will initially be
configured by the eventing-kafka.receiver.replicas field in the
[ConfigMap](../../../../config/channel/distributed/300-eventing-kafka-configmap.yaml)
and can be modified directly after that, for example through the use of a
Horizontal Pod Autoscaler). This allows for an efficient use of cluster
resources while still supporting high volume and multi-tenant use cases.

An additional Service for each KafkaChannel is created in the user namespace
where the KafkaChannel exists. This is the actual endpoint of the `KafkaChannel`
This service forwards requests to the service mentioned above in the
knative-eventing namespace. This is all necessary so that the Receiver
implementation can use the hostname of the request to map incoming CloudEvents
to the appropriate Kafka Topic.

The Kafka brokers and credentials are obtained from mounted data in the Kafka
Secret (by default named `kafka-cluster` in the knative-eventing namespace,
configurable by altering the kafka.authSecretName and/or the
kafka.authSecretNamespace values in the
[ConfigMap](../../../../config/channel/distributed/300-eventing-kafka-configmap.yaml))
.

## CPU Requirements

Providing CPU guidance is a difficult endeavor as there are so many variables
(event size, event rate, acceptable latency, kafka provider, use of an event
mesh, etc...) that can affect performance. The following is a single data point
to help you get started, and you should analyze your particular use case to fine
tune.

Sending ~2000 events/second on a single replica of the receiver, without an
event-mesh, where each event is approximately 20Kb in size, required **~1000m**
(a full CPU core) where the CPU was a GCP
[N1-Standard-2](https://cloud.google.com/compute/docs/cpu-platforms) 2 Core
Intel Skylake processor with a base clock of 2.0GHz

## Memory Requirements

The receiver simply verifies the destination KafkaChannel (Topic) and produces
the event. It does very little with the actual CloudEvent and thus the memory
usage is rather low as the events are transitory and garbage collected.
Preliminary performance testing has shown that the receiver will require less
than **~50Mi** to process ~2000 events/second where each event is approximately
20Kb in size. These are rough guidelines, and you should perform your own
analysis, but hopefully this provides a useful starting point.

## Tracing, Profiling, and Metrics

The Receiver makes use of the infrastructure surrounding the config-tracing and
config-observability configmaps (see
[Accessing CloudEvent traces](https://knative.dev/docs/eventing/accessing-traces)
and
[Installing logging, metrics, and traces](https://knative.dev/docs/serving/installing-logging-metrics-traces)
for more information on the basic Knative-Eventing concepts behind these
features). The default behavior for tracing and profiling is provided such that
accessing a tracing server (such as Zipkin), and the debug profiling
information, should work as described in those links. For example, you might
access your zipkin server via
`http://localhost:8001/api/v1/namespaces/knative-eventing/services/zipkin:9411/proxy/zipkin`
after running a "kubectl proxy" command, or the profiling server via
`http://localhost:8008/debug/pprof` after executing "kubectl -n knative-eventing
port-forward my-channel-pod-name 8008:8008"

Eventing-Kafka does provide some of its own custom metrics that use the
Prometheus server provided by the Knative-Eventing framework. When a channel
deployment starts, you can test the custom metrics with curl as in the following
example, which assumes you have created a channel named "kafka-cluster-channel"
and exposed the metrics endpoint of 8081 in the service. These commands presume
you are running from inside the cluster in a pod that has curl available:

Send a cloud event to your eventing-kafka installation:

```
echo -e "{\n  \"id\": \"12345\",\n  \"originationTime\": \"$(date -u +'%Y-%m-%dT%H:%M:%S.000000000Z')\",\n  \"content\": \"Test Message\"\n}" | curl -X POST -H "Content-Type: application/json" -H "cache-control: no-cache" -H "ce-id: 123" -H "ce-source: /testsource" -H "ce-specversion: 1.0" -H "ce-type: test.type.v1" --data @- -v http://my-kafkachannel-service.mynamespace.svc.cluster.local/
< HTTP/1.1 202 Accepted
```

Check the channel metrics, noting that the eventing_kafka_produced_msg_count has
increased by 1 on one of the partitions

```
curl -v http://kafka-cluster-channel.knative-eventing.svc.cluster.local:8081/metrics | grep produced_msg_count | sort
# HELP eventing_kafka_produced_msg_count Produced Message Count
# TYPE eventing_kafka_produced_msg_count gauge
eventing_kafka_produced_msg_count{partition="0",producer="rdkafka#producer-1",topic="mynamespace.my-kafkachannel-service"} 0
eventing_kafka_produced_msg_count{partition="1",producer="rdkafka#producer-1",topic="mynamespace.my-kafkachannel-service"} 0
eventing_kafka_produced_msg_count{partition="2",producer="rdkafka#producer-1",topic="mynamespace.my-kafkachannel-service"} 1
eventing_kafka_produced_msg_count{partition="3",producer="rdkafka#producer-1",topic="mynamespace.my-kafkachannel-service"} 0
```
