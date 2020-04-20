# Knative-Kafka Helm Chart

This helm chart installs the knative-kafka knative eventing implementation.  When installing make sure to provide the appropriate values file for the 
cluster you are deploying to.

1. Remove the existing installation `helm delete --purge --tls knative-kafka`
2. Install the helm chart `helm install --tls -n knative-kafka ./knative-kafka -f <your value overrides>.yaml`

## Kafka Providers

Knative Kafka supports a number of kafka providers to configure a particular provider to be used, set the following
in the values file:

`environment.kafkaProvider: <CHOSEN PROVIDER>`

The current allowed values are:

* `local`: Standard Kafka installation with no special authorization required
* `confluent`: Confluent Cloud 
* `azure`: Azure Event Hubs

The provider chosen effects how authentication as well as admin calls (topic creation, deletion etc) work.

## Credentials

### Install & Label Kafka Credentials In Knative-Eventing Namespace 
Knative-Kafka depends on secrets labeled with `knativekafka.kyma-project.io/kafka-secret="true"`, multiple
secrets are supported for the use of the `azure` integration, representing different EventHubs namespaces.  Some fields
may not apply to your particular Kafka implementation and can be left blank.  If you specify values in your overrides
YAML file during the helm chart installation, the initial broker secret will be created for you.

An example overrides.yaml file for Azure Event Hubs:

```
environment:
  kafkaProvider: azure
  defaultNumPartitions: 1
  defaultReplicationFactor: 3
  defaultKafkaConsumers: 1

kafka:
  namespace: my-cluster-name-1
  brokers: my-cluster-name-1.servicebus.windows.net:9093
  password: Endpoint=sb://my-cluster-name-1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX=
  username: $ConnectionString
  secretName: my-cluster-name-1
```

An example overrides.yaml file for Confluent Cloud:

```
environment:
  kafkaProvider: confluent
  defaultNumPartitions: 1
  defaultReplicationFactor: 3
  defaultKafkaConsumers: 1

kafka:
  brokers: SASL_SSL://my-cluster.eu-west-1.aws.confluent.cloud:9092
  password: XVLEXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
  username: KIELSXXXXXXXXXXX
  secretName: kafka-credentials
```

Alternatively, or if you need to specify the broker secret(s) after installation, they may also be created manually:
   
```
# Example A Creating A Kafka Secret In Knative-Eventing
kubectl create secret -n knative-eventing generic kafka-credentials \
    --from-literal=brokers=<BROKER CONNECTION STRING> \
    --from-literal=username=<USERNAME> \ 
    --from-literal=password=<PASSWORD> \
    --from-literal=namespace=<AZURE EVENTHUBS NAMESPACE> \
kubectl label secret -n knative-eventing kafka-credentials knativekafka.kyma-project.io/kafka-secret="true"
```
