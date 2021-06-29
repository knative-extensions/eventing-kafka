# ResetOffset Command

The common ResetOffset "command" provides the ability to reposition the Kafka
ConsumerGroup Offsets for the Topic / ConsumerGroup associated with a particular
Knative resources (Subscription, Trigger, etc.). Any Kafka related
implementation can include this capability by including the reusable
[Controller](./controller/controller.go) and
[Manager](../../consumer/consumer_manager.go) which rely on the associated
[ResetOffset CRD](../../../../config/command/resources/RESET_OFFSET.md).

## Controller

To work around the fact that the Knative `sharedmain` framework does not provide
an easy mechanism to pass arguments to the Controller during instantiation, the
ResetOffset implementation uses Factories and delayed instantiation to expose
the necessary "custom" components that allow it to be re-usable.

Including the ResetOffset Controller into your existing Controller will look
something like this...

```go
package main

import (
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/control-protocol/pkg/reconciler"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/kafkachannel"
	"knative.dev/eventing-kafka/pkg/common/commands/resetoffset/controller"
	"knative.dev/eventing-kafka/pkg/common/commands/resetoffset/refmappers"
)

func main() {

	...

	// Create A Subscription RefMapper Factory With Custom Topic/Group Naming
	subscriptionRefMapperFactory := refmappers.NewSubscriptionRefMapperFactory(
		myTopicNameMapper,
		myGroupIdMapper,
		myConnectionPoolKeyMapper,
		myDataPlaneNamespaceMapper,
		myDataPlaneLabelsMapper,
	)

	// Create A control-protocol ControlPlaneConnectionPool
	connectionPool := reconciler.NewInsecureControlPlaneConnectionPool()
	defer connectionPool.Close(ctx)

	// Create A ResetOffset ControllerConstructor Factory With Custom Subscription Ref Mapping
	resetOffsetControllerConstructor := controller.NewControllerFactory(subscriptionRefMapperFactory, connectionPool)

	// Create The SharedMain Instance With The Various Controllers
	sharedmain.MainWithContext(ctx, ControllerComponentName, kafkachannel.NewController, resetOffsetControllerConstructor)
}
```

## Reconciliation

The general algorithm for reconciling ResetOffset instances is as follows...

```
1 - Map the ResetOffset spec.Ref to the corresponding Kafka Topic and ConsumerGroup.

2 - Update DataPlane communication infrastructure between Controller & Dispatchers.

3 - Stop all related ConsumerGroups in the Dispatcher Replicas.

4 - Reposition the Offsets of all ConsumerGroup Partitions.

5 - Re-Start all related ConsumerGroups in the Dispatcher Replicas.
```

The reconciler will continue to process a ResetOffset until is has `Succeeded`.
It is careful to only ever reposition the ConsumerGroup Offsets a single time to
prevent the back/forth repositioning in certain failure scenarios.

## RefMappers

In order to provide a reusable "common" implementation of the ResetOffset
Controller while still allowing for differences in implementation between
various use cases, it is necessary to expose a custom mechanism for mapping
between various Knative resources (Subscriptions, Triggers, etc) and the Kafka
Topic / ConsumerGroup to be repositioned.

The [refmappers](./refmappers) package contains the Type definitions to allow
for such customization, as well as the initial implementation for Subscriptions
of KafkaChannels. This implementation includes common logic for loading the
Channel associated with the subscription and providing it to the custom mappers
responsible for determining the Kafka Topic and ConsumerGroup names.

Once the Reconciler has the "mapped" `RefInfo` data, it is able to proceed with
the Offset repositioning process.

## DataPlane

In order to Stop / Start the ConsumerGroups the Control-Plane needs to
communicate with the Data-Plane where the ConsumerGroups are running. The
Dispatchers which comprise the Data-Plane are likely to be horizontally scalable
and thus must each be contacted. To facilitate this communication the
[control-protocol](https://github.com/knative-sandbox/control-protocol) is used.

The [ConsumerGroupAsyncCommand](../../controlprotocol/commands/consumergroup.go)
defines the Start / Stop messages between the Control-Plane and Data-Plane. The
commands are inherently asynchronous and are sent to all Data-Plane Replicas in
parallel for efficiency. The responses are aggregated and block to be
synchronous so that the Reconciler can deal with them as a single unit.

While you are free to use the ResetOffset Controller with your own custom
Data-Plane implementation, the intent is that the common
[ConsumerManager](../../consumer/consumer_manager.go) will be used. This
implementation already provides the expected ConsumerGroup lifecycle management
and locking control.






