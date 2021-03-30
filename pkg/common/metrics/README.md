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
