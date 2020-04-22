# Prometheus Metrics HTTP Server

This code will provides the ability to start/stop a native Golang HTTP Server backed by the default
[Prometheus](https://github.com/prometheus/client_golang) handler.  The desired HTTP Port & Path are
specified upon server creation.  This should allow the various Eventing-Kafka "components" to quickly add
this capability in a consistent manner without code duplication.

This code only serves to expose the default Prometheus metrics on a specified port/path.  The creation
of the K8S Service and Prometheus ServiceMonitor is left up to the individual component to provide.

## Metrics Endpoint

In order to manually test the exposed /metrics endpoint you can port forward the service

```
kubectl port-forward svc/<service> -n <namespace> 8081:8081
```

...and point your browser at [http://localhost:8081/metrics](http://localhost:8081/metrics), or you can
use [telepresence](https://www.telepresence.io/) and curl...

```
telepresence
curl http://<service>.<namespace>.svc.cluster.local:8081/metrics
```

## Prometheus Console

In order to expose the Prometheus console in the Kyma cluster simply expose the port...

```
kubectl port-forward svc/monitoring-prometheus -n kyma-system 9090:9090
```

...and then point you browser at [http://localhost:9090/graph](http://localhost:9090/graph) or
[http://localhost:9090/targets](http://localhost:9090/targets)
