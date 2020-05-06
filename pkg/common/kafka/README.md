# Kafka Connections

This readme describes the implementation of this "kafka" common package.  Primarily the implementation is focused on
providing utilities for creating the basic connections (i.e. AdminClient, Producer, Consumer) used in a Kafka based
solution.  The solution provides support for various Kafka implementations / deployments both with and without
authentication.  Further, the implementation provides support for administering Azure EventHubs (Topics) via the
standard Confluent / librdkafka client so that the users of this logic do not have to concern themselves with the
underlying implementation.

## AdminClient & K8S Secrets

The AdminClient expects there to be a K8S Secret (or one Secret per EventHub Namespace when using Azure) in the
K8S namespace specified by the `$RUNTIME_NAMESPACE` environment variable.  The Secret MUST be labelled with the
`eventing-kafka.knative.dev/kafka-secret` label and contain the following fields...

```
data:
    brokers: Kafka Brokers
    namespace: Only required for Azure namespaces - the name of the Azure Namespace
    password: SASL Password or Azure Connection String of Azure Namespace
    username: SASL Username or '$ConnectionString' for Azure Namespace
```

## Producer / Consumer

The producer and consumer are simpler and expect to be provided the specific Broker string and SASL Username and
Password.  It is expected that the utilities exposed by the custom AdminClient in this implementation can be used
to get the name of the Kafka Secret for a specific Topic / EventHub which can then be used to acquire the needed
information.

## EventHubs (Azure)

While Azure EventHubs support the standard Kafka interfaces for Producer / Consumer based event transmission, it
does not yet provide a similar management later.  Instead users are required to either use their
[REST API](https://docs.microsoft.com/en-us/rest/api/eventhub/) or the
[go-client](https://github.com/Azure/azure-event-hubs-go/tree/master).  The go-client is easier to use but only
provides limited EventHub related functionality (e.g - No "namespace" management, etc...)  This implementation is
currently based on the go-client and expects Azure Namespaces to be manually allocated and pre-existing.  The
implementation does, however, abstract away these Azure Namespaces so that a user's Azure Subscription looks like
one big allocation of possible EventHubs.  The creation of new Topics / EventHubs will be load-balanced across
the available Azure EventHub Namespaces as identified by their K8S Secret (instead of dynamic lookup via the
Azure REST API).
