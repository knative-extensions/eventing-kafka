# Kafka Connections

This README describes the implementation of this "kafka" common package.  Primarily the implementation is focused on
providing utilities for creating the basic connections (i.e. AdminClient, Producer, Consumer) used in a Kafka based
solution.  The solution provides support for various Kafka implementations / deployments both with and without
authentication.  Further, the implementation provides support for administering Azure EventHubs (Topics) via the
standard Sarama ClusterAdmin interface so that the users of this logic do not have to concern themselves with the
underlying implementation.

## AdminClient & K8S Secrets

The AdminClient expects there to be a single K8S Secret (or one Secret per EventHub Namespace when using Azure) in 
the knative-eventing namespace.  The Secret MUST be labelled with the `eventing-kafka.knative.dev/kafka-secret` 
label and contain the following fields...

```
data:
    brokers:   Kafka Brokers String (comma separated)
    password:  SASL Password or Azure Connection String of Azure Namespace
    username:  SASL Username or '$ConnectionString' for Azure Namespace    
    namespace: Only required for Azure AdminClient usage - specifies the the Azure EventHub Namespace.
    plugin:    Only required for Plugin AdminClient usage - specifies the path to the custom AdminClient plugin.
```

## Producer / Consumer

The Kafka Producer and Consumer are simpler and expect to be provided the specific Broker string and SASL Username 
and Password.  It is expected that the utilities exposed by the custom AdminClient in this implementation can be used
to get the name of the Kafka Secret for a specific Topic / EventHub which can then be used to acquire the needed
information.

## EventHubs (Azure)

While Azure EventHubs support the standard Kafka interfaces for Producer / Consumer based event transmission, it
does not yet provide a similar management layer.  Instead, users are required to either use their
[REST API](https://docs.microsoft.com/en-us/rest/api/eventhub/) or the
[go-client](https://github.com/Azure/azure-event-hubs-go/tree/master).  The go-client is easier to use but only
provides limited EventHub related functionality (e.g - No "namespace" management, etc...)  This implementation is
currently based on the go-client and expects Azure Namespaces to be manually allocated and pre-existing.  The
implementation does, however, abstract away these Azure Namespaces so that a user's Azure Subscription looks like
one big allocation of possible EventHubs.  The creation of new Topics / EventHubs will be load-balanced across
the available Azure EventHub Namespaces as identified by their K8S Secret (instead of dynamic lookup via the
Azure REST API).

## Plugins (Custom)

In case you need to provide your own custom Kafka AdminClient implementation for a use case similar to the EventHub
scenario discussed above, support is provided for a pluggable implementation to be specified.  Note, that this is
only for the AdminClient and the default Sarama Producer / Consumer implementations will still be used.

To implement your own custom pluggable implementation of the `AdminClientInterface` you will need to create a
[GO Module](https://golang.org/pkg/plugin/) which exposes a function with the following signature...
```
func NewPluginAdminClient(*zap.Logger, *corev1.Secret) (admin.AdminClientInterface, error)
```
...which is provided with the Kafka Secret and must return an implementation of the `AdminClientInterface`. 
You may add whatever custom fields to the Kafka Secret that you might need and are responsible for loading
them from your custom Kafka AdminClient plugin.  

The plugin must be named `kafka-admin-client.so` and be in the `$KO_DATA_PATH` (defaults to **/var/run/ko**) as
described in the [ko documentation](https://github.com/google/ko).  The easiest way to do this is to create
a separate GO project for your custom Kafka AdminClient, from which you use the official eventing-kafka controller
image as the Base Docker Image onto which you can place the kafka-admin-client.so built from your custom source.

> Note - Deployment of the plugin based controller will have to be managed outside the normal eventing-kafka ko
> deployment. 
