# Distributed KafkaChannel Config

The YAML files in this directory represent the [ko](https://github.com/google/ko) development
level configuration use to work on the distributed kafka channel source.  They will install
into the existing Kubernetes cluster which should have Knative Eventing installed.   It is also
expected that the user will provide a viable Kafka cluster and associated configuration.

## Usage

As mentioned above these YAML files are to be installed with `ko`.  Thus, you need to set the
`$KO_DOCKER_REPO` environment variable as desired.  If using `ko.local` then you'll need to
change the `ImagePullPolicy` to `IfNotPresent`.

The **data** values of the [kafka-secret.yaml](300-kafka-secret.yaml) should be correctly
populated for the user provided Kafka cluster.  Similarly the **data** values of the
[eventing-kafka-configmap.yaml](200-eventing-kafka-configmap.yaml) file should be configured
for your particular use case.

Install via `ko apply --strict -f ./config/channel/distributed` from the repository root
directory in order to build and deploy the project.  The `--strict` option is really only
needed when using the `custom` AdminType, but shouldn't hurt in other cases.

## Kafka Admin Types

Eventing-Kafka supports a few options for the administration of Kafka Topics (Create / Delete) in the
user provided Kafka cluster.  The desired mechanism is specified via the `eventing-kafka.kafka.adminType`
field in [eventing-kafka-configmap.yaml](200-eventing-kafka-configmap.yaml) and must be one of `kafka`,
`azure`, or `custom` as follows...

- **kafka:** This is the normal / default use case that most users will want.  It uses the standard Kafka API (via the Sarama ClusterAdmin) for managing Kafka Topics in the cluster.
- **azure:** Users of Azure EventHubs will know that Microsoft does not support the standard Kafka Topic administration and are required instead to use their API.  This option provides for such support via the Microsoft Azure EventHub Go client.
- **custom:** This option provides an external hook for users who need to manage Topics themselves. This could be to support a proprietary Kafka implementation with a custom interface / API.  The user is responsible for implementing a sidecar Container with the expected HTTP endpoints.  They will need to add their sidecar Container to the [deployment.yaml](400-deployment.yaml).  Details for implementing such a solution can be found in the [Kafka README](../../../pkg/channel/distributed/common/kafka/README.md).

> Note: This setting only alters the mechanism by which Kafka Topics are managed (Create & Delete).
> In all cases the same Sarama SyncProducer and ConsumerGroup implementation is used to
> actually produce and consume to/from Kafka.

## Credentials

### Install & Label Kafka Credentials In Knative-Eventing Namespace

The Kafka brokers and associated auth are specified in a Kubernetes Secret in the `knative-eventing`
namespace which has been labelled as `eventing-kafka.knative.dev/kafka-secret="true"`.  For the
`kafka` and `custom` Admin Types (see above) there should be exactly 1 such Secret. For the `azure`
Admin Type (see above) multiple such Secrets are possible, each representing a different EventHub
Namespace.  In that case Topics will be load balanced across all EventHub Namespaces. The
[kakfa-secret.yaml](300-kafka-secret.yaml) is included in the config directory, but must be modified
to hold real values.  The values from this file will override the username/password in the
[eventing-kafka-configmap.yaml](200-eventing-kafka-configmap.yaml).  It is also expected that the
`Config.Net.SASL` and `Config.Net.TLS` are enabled to perform the required authentication with
the Kafka cluster.

Example values for a standard Kafka (must be base64 encoded):

```
  brokers: SASL_SSL://my-cluster.eu-west-1.aws.confluent.cloud:9092
  password: XVLEXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
  username: KIELSXXXXXXXXXXX
```

Example values for Azure Event Hubs (must be base64 encoded):

```
  namespace: my-cluster-name-1
  brokers: my-cluster-name-1.servicebus.windows.net:9093
  password: Endpoint=sb://my-cluster-name-1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX=
  username: $ConnectionString
```

Alternatively, or if you need to specify the broker secret(s) after installation, they may also be created manually:

```
# Example A Creating A Kafka Secret In Knative-Eventing
kubectl create secret -n knative-eventing generic kafka-credentials \
    --from-literal=brokers=<BROKER CONNECTION STRING> \
    --from-literal=username=<USERNAME> \
    --from-literal=password=<PASSWORD> \
    --from-literal=namespace=<AZURE EVENTHUBS NAMESPACE> \
kubectl label secret -n knative-eventing kafka-credentials eventing-kafka.knative.dev/kafka-secret="true"
```

## Configuration

The [eventing-kafka-configmap.yaml](200-eventing-kafka-configmap.yaml) contains configuration for both
the Eventing-Kafka implementation, and the majority of the Sarama Config structure as follows.  These
are presented as inline YAML for convenience so mind the indentation.  Further it is assumed in the
default configuration that you will be performing PLAIN SASL / TLS authentication with your Kafka
cluster.

- **sarama:**  This is a direct exposure of the [Sarama.Config Golang Struct](https://github.com/Shopify/sarama/blob/master/config.go) which allows for significant customization of the Sarama client (ClusterAdmin, Producer, Consumer). There are, however, several caveats as the entire structure is not supported...

  - **Version:** The Sarama.Config.Version field is implemented as a Struct with private storage of the
    version numbers and cannot be easily parsed.  Therefore, we have implemented custom parsing which
    requires you to enter `2.3.0` instead of the Sarama value of `V2_3_0_0`.  Further it should be
    noted that when using with `azure` it should be set to `1.0.0`.
  - **Net.SASL.Enable** Enable (true) / disable (false) according to your authentication needs.
  - **Net.SASL.User:** If you specify the username in the ConfigMap it will be overridden by the
  values from the [kafka-secret.yaml](300-kafka-secret.yaml) file!
  - **Net.SASL.Password:** If you specify the password in the ConfigMap it will be overridden by
  the values from the [kafka-secret.yaml](300-kafka-secret.yaml) file!
  - **Net.TLS.Enable** Enable (true) / disable (false) according to your authentication needs.
  - **Net.TLS.Config:** The Golang [tls.Config struct](https://golang.org/pkg/crypto/tls/#Config) is not
  completely supported.  We have though added a Custom field called `RootPEMs` which can contain an inline
  PEM file to be used for validating RootCerts from the Kafka cluster during TLS authentication.  The
  format and white-spacing is VERY important for this nested YAML string to be parsed correctly.  It
  must look like this...

  ```yaml
    data:
      sarama: |
        Net:
          TLS:
            Config:
              RootPEMs:
              - |-
                -----BEGIN CERTIFICATE-----
                MIIGBDCCA+ygAwIBAgIJAKi1aEV58cQ1MA0GCSqGSIb3DQEBCwUAMIGOMQswCQYD
                ...
                2wk9rLRZaQnhspt6MhlmU0qkaEZpYND3emR2XZ07m51jXqDUgTjXYCSggImUsARs
                NAehp9bMeco=
                -----END CERTIFICATE-----
  ```

    > Remember to change **Net.TLS.Enable** and/or **Net.SASL.Enable** to "true" if you are
    > configuring authentication in the [kafka-secret.yaml](300-kafka-secret.yaml) file.
    > Also be aware that the e2e tests require those values to be false, so if you change the
    > defaults in the [eventing-kafka-configmap.yaml](200-eventing-kafka-configmap.yaml) file
    > itself and commit it to the repository, the tests will fail if you do not also make
    > changes to the [e2e-tests.sh](../../../test/e2e-tests.sh) file to compensate.

  - **Net.MaxOpenRequests:**  While you are free to change this value it is paired with the Idempotent value below to provide in-order guarantees.
  - **Producer.Idempotent:** This value is expected to be `true` in order to help provide the in-order guarantees of eventing-kafka.  The exception is when using `azure`, in which case it must be `false`.
  - **Producer.RequiredAcks:** Same `in-order` concerns as above ; )

- **eventing-kafka:** This section provides customization of runtime behavior of the eventing-kafka implementation as follows...

  - **channel:** Controls the Deployment runtime characteristics of the Channel (one Deployment per Kafka Secret).
  - **dispatcher:** Controls the Deployment runtime characterstics of the Dispatcher (one Deployment per KafkaChannel CR).
  - **kafka.adminType:** As described above this value must be set to one of `kafka`, `azure`, or `custom`.  The default is `kakfa` and will be used by most users.
