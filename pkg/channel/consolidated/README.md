# Apache Kafka Channels

Kafka channels are those backed by [Apache Kafka](http://kafka.apache.org/)
topics.

## Deployment steps

1. Setup
   [Knative Eventing](https://knative.dev/docs/install/any-kubernetes-cluster/#installing-the-eventing-component)
1. Install an Apache Kafka cluster, if you have not done so already.

   For Kubernetes a simple installation is done using the
   [Strimzi Kafka Operator](http://strimzi.io). Its installation
   [guides](http://strimzi.io/quickstarts/) provide content for Kubernetes and
   Openshift.

   > Note: This `KafkaChannel` is not limited to Apache Kafka installations on
   > Kubernetes. It is also possible to use an off-cluster Apache Kafka
   > installation.

1. Now that Apache Kafka is installed, you need to configure the
   `brokers` value in the `config-kafka` ConfigMap, located inside the
   `config/400-kafka-config.yaml` file.

   ```yaml
   apiVersion: v1
   kind: ConfigMap
   metadata:
     name: config-kafka
     namespace: knative-eventing
   # eventing-kafka.kafka.brokers: Replace this with the URLs for your kafka cluster,
   #   which is in the format of my-cluster-kafka-bootstrap.my-kafka-namespace:9092.
   data:
     eventing-kafka: |
      kafka:
        brokers: REPLACE_WITH_CLUSTER_URL
   ```

1. Apply the Kafka config:

   ```sh
   ko apply -f config/channel/consolidated
   ```

1. Create the `KafkaChannel` custom objects:

   ```yaml
   apiVersion: messaging.knative.dev/v1beta1
   kind: KafkaChannel
   metadata:
     name: my-kafka-channel
   spec:
     numPartitions: 1
     replicationFactor: 1
     retentionMillis: 604800000
   ```

   You can configure the number of partitions with `numPartitions`, as well as
   the replication factor with `replicationFactor`. You can set the 
   retention Time of topic (in millisecond) with `retentionMillis`. If not set, 
   all values will default to the values provided in the `eventing-kafka.kafka.topic` section of
   the `config-kafka` ConfigMap.

## Components

The major components are:

- Kafka Channel Controller
- Kafka Channel Dispatcher
- Kafka Webhook
- Kafka Config Map

The Kafka Channel Controller is located in one Pod:

```shell
kubectl get deployment -n knative-eventing kafka-ch-controller
```

The Kafka Channel Dispatcher receives and distributes all events to the
appropriate consumers:

```shell
kubectl get deployment -n knative-eventing kafka-ch-dispatcher
```

The Kafka Webhook is used to validate and set defaults to `KafkaChannel` custom
objects:

```shell
kubectl get deployment -n knative-eventing kafka-webhook
```

The Kafka Config Map is used to configure the `brokers` of your Apache
Kafka installation, as well as other settings.  Note that not all settings are
applicable to the consolidated channel type.  In particular, the `receiver` and
`admintype` fields of the `eventing-kafka.channel` section are only used by the
distributed channel type.

```shell
kubectl get configmap -n knative-eventing config-kafka
```

### Namespace Dispatchers

By default, events are received and dispatched by a single cluster-scoped
dispatcher component. You can also specify whether events should be received
and dispatched by the dispatcher in the same namespace as the channel definition
by adding the `eventing.knative.dev/scope: namespace` annotation.

First, you need to create the configMap `config-kafka` in the same namespace as
the KafkaChannel.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: config-kafka
  namespace: <YOUR_NAMESPACE>
data:
  eventing-kafka: |
    kafka:
      brokers: REPLACE_WITH_CLUSTER_URL
```

> Note: the `brokers` value does not have to be the same as the one
> specified in `knative-eventing/config-kafka`.

Then create a KafkaChannel:

```yaml
apiVersion: messaging.knative.dev/v1beta1
kind: KafkaChannel
metadata:
  name: my-kafka-channel
  namespace: <YOUR_NAMESPACE>
  annotations:
    eventing.knative.dev/scope: namespace
spec:
  numPartitions: 1
  replicationFactor: 1
  retentionMillis: 604800000
```

The dispatcher is created in `<YOUR_NAMESPACE>`:

```sh
kubectl get deployment -n <YOUR_NAMESPACE> kafka-ch-dispatcher
```

Both cluster-scoped and namespace-scoped dispatcher can coexist. However once
the annotation is set (or not set), its value is immutable.

### Configuring Kafka client, Sarama

You can configure the Sarama instance used in the KafkaChannel by defining a
`sarama` field inside the `config-kafka` configmap.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: config-kafka
  namespace: knative-eventing
data:
  bootstrapServers: ...
  ...
  sarama: |
    config: |
      Version: 2.0.0 # Kafka Version Compatibility From Sarama's Supported List (Major.Minor.Patch)
      Admin:
        Timeout: 10000000000  # 10 seconds
      Net:
        KeepAlive: 30000000000  # 30 seconds
      Metadata:
        RefreshFrequency: 300000000000  # 5 minutes
      Consumer:
        Offsets:
          AutoCommit:
            Interval: 5000000000  # 5 seconds
          Retention: 604800000000000  # 1 week
      Producer:
        Idempotent: true  # Must be false for Azure EventHubs
        RequiredAcks: -1  # -1 = WaitForAll, Most stringent option for "at-least-once" delivery.
```

Settings defined here are used as the defaults by the KafkaChannel. The
additional settings defined in the channel CR, such as authentication, are
applied on top of these defaults.

Also, some Sarama settings are required for the channel to work, such as
`Consumer.Return.Errors` and `Producer.Return.Successes`, so the value for these
in the `config-kafka` is ignored.

Value of the `sarama.config` key must be valid YAML string. The string is marshalled
into a
[Sarama config struct](https://github.com/Shopify/sarama/blob/master/config.go),
with a few exceptions (`Version` and certificates).

To specify a certificate, you can use the following format where you should make
sure to use the YAML string syntax of "|-" in order to prevent trailing
linefeed. The indentation of the PEM content is also important and must be
aligned as shown.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: config-kafka
  namespace: knative-eventing
data:
  sarama: |
    config: |
      ...
      Net:
        TLS:
          ...
          Config:
            RootCaPems:
            - |-
              -----BEGIN CERTIFICATE-----
              MIIGBDCCA+ygAwIBAgIJAKi1aEV58cQ1MA0GCSqGSIb3DQEBCwUAMIGOMQswCQYD
              ...
              2wk9rLRZaQnhspt6MhlmU0qkaEZpYND3emR2XZ07m51jXqDUgTjXYCSggImUsARs
              NAehp9bMeco=
              -----END CERTIFICATE-----
```
