# Creating A Simple Kind Cluster With Kafka

If you want to quickly spin up a local kubernetes cluster with Kafka installed,
then the following instructions might be helpful.

> There are lots of ways to do this (different k8s clusters, and kafka
> implementations) and the following is just **ONE** way. Feel free to try other
> stacks and add to this documentation ; )

## Prerequisites

These instructions assume you have the following installed...

- [Docker](https://www.docker.com/products/docker-desktop)
- [kind](https://kind.sigs.k8s.io/)
- [ko](https://github.com/google/ko)

...and have forked / cloned this repo locally.

## Install Kind Cluster

```
# Specify Kind / Kubernetes Version  (See... https://hub.docker.com/r/kindest/node/tags)
export KUBERNETES_VERSION=v1.18.6

# Create The Kind Cluster
kind create cluster --image kindest/node:${KUBERNETES_VERSION}

# Add Kind Cluster To ~/.kube/config   (Optional based on your local ~/.kube management)
kubectl cluster-info --context kind-kind

# Check Your Current Cluster Is Kind
kubectl config current-context
```

## Install Knative Eventing

```
# Specify Knative Version
export EVENTING_VERSION=v0.22.1   # Choose from https://github.com/knative/eventing/releases

# Install Knative-Eventing CRDs & Core
kubectl apply --filename https://github.com/knative/eventing/releases/download/${EVENTING_VERSION}/eventing-crds.yaml
kubectl apply --filename https://github.com/knative/eventing/releases/download/${EVENTING_VERSION}/eventing-core.yaml
```

## Set Knative Debug Logging (Optional)

To switch knative-eventing and eventing-kafka components to **debug** level
logging edit the logging ConfigMap ...

```
kubectl edit configmap -n knative-eventing config-logging
```

... and set the zap-logger-config `"level"` field ...

```
apiVersion: v1
data:
  zap-logger-config: |
    {
      "level": "debug",
...
```

## Install Strimzi Kafka

```
Specify Strimzi Kafka Version (See... https://github.com/strimzi/strimzi-kafka-operator/releases)
export STRIMZI_VERSION=0.21.1

# Create A "kafka" Namespace
kubectl create namespace kafka

# Install Strimzi Kafka Cluster  (Ephemeral / Non-Persistent Lightweight Test Instance)
curl -L https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml |\
  sed 's/namespace: .*/namespace: kafka/' | \
  kubectl apply -f - -n kafka
kubectl apply -f https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/${STRIMZI_VERSION}/examples/kafka/kafka-ephemeral-single.yaml -n kafka

# Wait For Strimzi Kafka Cluster "my-cluster" To Have Started
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
```

## Install Distributed KafkaChannel

The following example covers deploying the distributed KafkaChannel, but similar
steps could be used for the Consolidated KafkaChannel or Source.

- Configure The Distributed KafkaChannel For Strimzi...

  - Disable TLS/SASL and specify Strimzi Brokers in
    [config/channel/distributed/300-eventing-kafka-configmap.yaml](../config/channel/distributed/300-eventing-kafka-configmap.yaml)

    ```
    data:
      sarama: |
        Net:
          TLS:
            Enable: false
          SASL:
            Enable: false
      eventing-kafka: |
        kafka:
          brokers: my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
    ```

  - Install the distributed KafkaChannel

    ```
    # Set Ko To Use Your Docker Registry (For deploying built images)
    export KO_DOCKER_REPO=<docker-registry-path>

    # Build / Deploy
    ko apply -f config/channel/distributed/
    ```

## Uninstall Distributed KafkaChannel

```
ko delete -f config/channel/distributed/
```

## Uninstall Kind Cluster

When you're done you can remove the Kind cluster as follows...

```
# Double Check Current Cluster
kubectl config current-context

# Delete The Kind Cluster (Updates ~/.kube/config)
kind delete cluster
```
