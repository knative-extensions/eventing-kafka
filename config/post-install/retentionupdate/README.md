# RetentionMillis Update - Post Install Job - v0.26.0

The **v0.26.0** release added the new **RetentionDuration** to the Spec of the
KafkaChannel CRD. This post-install job will populate this new field of all
existing KafkaChannels based on the current configuration of the corresponding
Kafka Topic.

## Usage

By default, the post-install job is configured to run in "cluster" scope. It is
deployed into the **knative-eventing** namespace, and expects the
**config-kafka** ConfigMap and associated Kafka auth Secret to be in place.

```shell
kubectl apply -f config/post-install/retentionupdate
```

The consolidated KafkaChannel implementation also supports the "namespace"
scope. In order to use the post-install job in this mode you will need to do the
following...

- Remove the hard-coded **knative-eventing** namespace from these YAML files.
- For each namespace in which you have created KafkaChannels...
    - Ensure the namespace has the appropriate **config-kafka** ConfigMap and
      Kafka auth Secret
    - Apply the yaml, specifying the namespace such as...
      ```shell
      kubectl apply -n <namespace> -f config/post-install/retentionupdate
      ```
    - Use the same per-namespace approach in the _Cleanup_ section as well ;)

## Cleanup

The post-install job will retry (up to 10 times) on failure, and you can monitor
the job via...

```shell
kubectl get jobs -n knative-eventing -w
```

It is suggested that you grab the log prior to uninstalling....

```shell
kubectl logs -n knative-eventing eventing-kafka-v0.26-post-install-retention-update > postinstall-retentionupdate.log
```

...which can be done via...

```shell
kubectl delete -f config/post-install/retentionupdate
```
