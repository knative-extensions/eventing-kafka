# Stats Reporter

This code provides the ability to register with OpenCensus the Views required to
have the various metrics associated with eventing-kafka exported to whatever
backend is configured (Prometheus by default). The METRICS_DOMAIN and
METRICS_PORT environment variables will be used by Knative to produce unique
metrics names and start the metrics service when InitializeObservability is
called.

Note that this will serve only to expose the metrics on the specified port. The
creation of the K8S Service and any external monitoring is left up to the
individual component to provide.

## Metrics Endpoint

Assuming the use of the default Prometheus backend and port, you may manually
test the exposed /metrics endpoint by running a Kubernetes port forward to the
service.

```
kubectl port-forward svc/<service> -n <namespace> 8081:8081
```

...and point your browser at
[http://localhost:8081/metrics](http://localhost:8081/metrics), or you can use
[telepresence](https://www.telepresence.io/) and curl...

```
telepresence
curl http://<service>.<namespace>.svc.cluster.local:8081/metrics
```

## Prometheus Alerts

The following are sample Prometheus alerts based on the distributed KafkaChannel
metrics and are provided only as a starting point for creating your own alerts.

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  labels:
    prometheus: kube-prometheus
    role: alert-rules
  name: eventing-kafka-channel.rules
spec:
  groups:
  - name: eventing-kafka-channel.rules
    rules:
    - alert: EventingKafkaReceiverBrokerLatencyWarning
      annotations:
        summary: "Receiver Kafka broker request latency is excessively high."
        description: "{{`Kafka broker request latency ({{ $value }}ms) has exceeded 200ms for more than 5 minutes.`}}"
      expr: eventing_kafka_request_latency_in_ms{job="eventing-kafka-channels", percentile="95%"} > 200
      for: 5m
      labels:
        severity: warning
    - alert: EventingKafkaDispatcherBrokerLatencyWarning
      annotations:
        summary: "Dispatcher Kafka broker request latency is excessively high."
        description: "{{`Kafka broker request latency ({{ $value }}ms) has exceeded 400ms for more than 5 minutes.`}}"
      expr: eventing_kafka_request_latency_in_ms{job="eventing-kafka-dispatchers", percentile="95%"} > 400
      for: 5m
      labels:
        severity: warning
```
