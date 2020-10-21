# Kafka Connections

This README describes the implementation of this common/kafka package. Primarily
the implementation is focused on providing utilities for creating the basic
connections (i.e. AdminClient, Producer, Consumer) used in a Kafka based
solution. The solution provides support for various Kafka implementations /
deployments both with and without authentication. Further, the implementation
provides support for administering Azure EventHubs (Topics) via the standard
Sarama ClusterAdmin interface so that the users of this logic do not have to
concern themselves with the underlying implementation. Finally, support is
provided for users to implement their own "custom" AdminClient functionality via
a simple sidecar Container.

## AdminClient & K8S Secrets

The AdminClient expects there to be a single K8S Secret (or one Secret per
EventHub Namespace when using Azure) in the `knative-eventing` namespace. The
Secret MUST be labelled with the `eventing-kafka.knative.dev/kafka-secret` label
and contain the following fields...

```
data:
    brokers:   Kafka Brokers String (comma separated)
    password:  SASL Password or Azure Connection String of Azure Namespace
    username:  SASL Username or '$ConnectionString' for Azure Namespace
    namespace: Only required for Azure AdminClient usage - specifies the Azure EventHub Namespace
```

> Note - The username and password fields from the Kubernetes Secret will
> override any similar values provided in the `sarama` section of the
> [ConfigMap](../../../../../config/channel/distributed/200-eventing-kafka-configmap.yaml).

## Producer / Consumer

The Kafka Producer and Consumer are simpler and expect to be provided the
specific Broker string and SASL Username and Password (if used). It is expected
that the utilities exposed by the custom AdminClient in this implementation can
be used to get the name of the Kafka Secret for a specific Topic / EventHub
which can then be used to acquire the needed information.

## EventHubs (Azure)

While Azure EventHubs support the standard Kafka interfaces for Producer /
Consumer based event transmission, it does not yet provide a similar management
layer. Instead, users are required to either use their
[REST API](https://docs.microsoft.com/en-us/rest/api/eventhub/) or the
[go-client](https://github.com/Azure/azure-event-hubs-go/tree/master). The
go-client is easier to use but only provides limited EventHub related
functionality (e.g - No "namespace" management, etc...) This implementation is
currently based on the go-client and expects Azure Namespaces to be manually
allocated and pre-existing. The implementation does, however, abstract away
these Azure Namespaces so that a user's Azure Subscription looks like one big
allocation of possible EventHubs. The creation of new Topics / EventHubs will be
load-balanced across the available Azure EventHub Namespaces as identified by
their K8S Secret (instead of dynamic lookup via the Azure REST API).

## Custom (REST Sidecar)

If the standard Kafka administration of Topics via the Sarama ClusterAdmin is
not sufficient, it is possible for a user to provide their own custom
implementation via a Kubernetes "sidecar" Container. The eventing-kafka
implementation will then proxy all Topic Create/Delete requests to the sidecar
and convert responses for normal processing. The implementation of this sidecar
is expected to explicitly adhere to the following design and implementation
requirements in order for this proxying of requests to work successfully.

1. Eventing-Kafka Configuration

   The following changes to the default config/ YAML will be required in order
   to deploy a custom sidecar as part of the eventing-kafka Controller
   Deployment...

   - **ConfigMap:** The `data.eventing-kafka.kafka.adminType` field in the
     [ConfigMap](../../../../../config/channel/distributed/200-eventing-kafka-configmap.yaml)
     must be set to `custom`. This tells eventing-kafka that it should proxy
     Topic creation and deletion requests to the expected sidecar endpoints as
     defined below.

   - **Deployment:** The sidecar Container will need to be added to the
     [Controller Deployment](../../../../../config/channel/distributed/400-deployment.yaml)
     along with any other supporting infrastructure (VolumeMounts, etc.).

   - **Service Account:** The default
     [ServiceAccount](../../../../../config/channel/distributed/100-controller-serviceaccount.yaml)
     is used by the
     [Controller Deployment](../../../../../config/channel/distributed/400-deployment.yaml)
     to pull images. This ServiceAccount will need to be updated to include any
     `imagePullSecrets` required to pull the custom sidecar Container image.

   - **Miscellaneous:** It is the responsibility of the sidecar implementer to
     include any additional Kubernetes resources that might be needed by their
     sidecar. They have complete control and responsibility for any supporting
     components such as Secrets, ConfigMaps, etc.

1. Sidecar Interface

   These instructions define the expected interface with the custom sidecar
   container and are required for it to function properly. While it is possible
   for the user to implement the sidecar in the language of their choice, Golang
   is the obvious go-to. To aid in that implementation the user can make use of
   the code in the [custom package](admin/custom). Specifically there are
   constants defining the expected `Host`, `Port`, `Path` and `Header` values
   used in creating the sidecar's REST endpoints. Coding directly against these
   constants reduces the implementation effort, and is an easy way to stay
   current with updates to the eventing-kafka implementation. Also included is
   the `TopicDetail` struct which can be used when Unmarshalling the JSON body
   of the POST request.

   Specifically the sidecar is expected to expose the following endpoints to be
   called by the eventing-kafka AdminClient implementation...

   - **Create** ( `POST http://localhost:8888/topics` )
     - Endpoint
       - Protocol: HTTP
       - Method: POST
       - Host: localhost (_SidecarHost Constant_)
       - Port: 8888 (_SidecarPort Constant_)
       - Path: /topics (_TopicsPath Constant_)
       - Param: n/a
     - Request
       - Header: "Slug" (_TopicNameHeader Constant_) will contain the Topic
         Name.
       - Body: application/json TopicDetail (_TopicDetail Struct_)
         - numPartitions: int32
         - replicationFactor: int16
         - replicaAssignment: map[int32][]int32
         - configEntries: map[string]\*string
     - Response
       - 2XX: Treated as success by eventing-kafka and mapped to
         Sarama.ErrNoError.
       - 3XX: Treated as error by eventing-kafka and mapped to
         Sarama.ErrInvalidRequest.
       - 4XX: Treated as error by eventing-kafka and mapped to
         Sarama.ErrInvalidRequest.
       - 409: Treated as "_already exists_" by eventing-kafka and mapped to
         Sarama.ErrTopicAlreadyExists.
       - 5XX: Treated as error by eventing-kafka and mapped to
         Sarama.ErrInvalidRequest.
   - **Delete** ( `DELETE http://localhost:8888/topics/<topic-name>` )
     - Endpoint
       - Protocol: HTTP
       - Method: DELETE
       - Host: localhost (_SidecarHost Constant_)
       - Port: 8888 (_SidecarPort Constant_)
       - Path: **/** (_TopicsPath Constant_)
       - Param: _topic-name_
     - Request
       - Header: n/a
       - Body: n/a
     - Response
       - 2XX: Treated as success by eventing-kafka and mapped to
         Sarama.ErrNoError.
       - 3XX: Treated as error by eventing-kafka and mapped to
         Sarama.ErrInvalidRequest.
       - 4XX: Treated as error by eventing-kafka and mapped to
         Sarama.ErrInvalidRequest.
       - 404: Treated as "_not found_" by eventing-kafka and mapped to
         Sarama.ErrUnknownTopicOrPartition.
       - 5XX: Treated as error by eventing-kafka and mapped to
         Sarama.ErrInvalidRequest.

> Note - The 409 and 404 HTTP StatusCodes, and their corresponding Sarama Types,
> are an expected part of the normal operation of eventing-kafka, and your
> side-car should return them when encountering those scenarios (already exists,
> and already deleted).
>
> Note - There is no authentication / security between the endpoints as all
> communication is entirely intra-Pod.
