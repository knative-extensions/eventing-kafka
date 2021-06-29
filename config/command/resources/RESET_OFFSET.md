# ResetOffset Command CRD

The ResetOffset "command" provides the ability to reposition the Kafka
ConsumerGroup Offsets of a particular Knative resources (Subscription, Trigger,
etc.) in any implementation that supports its use. See the documentation of the
individual Kafka Channels to determine support.

The ability to reposition ConsumerGroup Offsets to a specific timestamp (forward
or backward), is intended to aid in failure recovery scenarios where you might
otherwise have to resort to low-level manual Offset manipulation. It is expected
to be an infrequently used tool in your toolbox for when a Subscriber has
experienced unexpected downtime. As such, it requires some special handling
compared with the traditional fire-and-forget nature of most CustomResources.

This "command" CRD differs from your standard Kubernetes CustomResource in that
it expresses the intent to perform a **one-time** repositioning of the Offsets
for a particular ConsumerGroup, and not a desired final state of the cluster.

## Usage

Once you have determined that the Kafka Channel implementation you are using
supports the ResetOffset CRDs, you can create and deploy a simple bit of YAML to
initiate the repositioning:

```yaml
apiVersion: kafka.eventing.knative.dev/v1alpha1
kind: ResetOffset
metadata:
  name: my-reset-offset
  namespace: my-namespace
spec:
  offset:
    time: "2021-06-17T15:04:39Z"
  ref:
    apiVersion: messaging.knative.dev/v1
    kind: Subscription
    namespace: my-namespace
    name: my-subscription
```

It is intended that you create a ResetOffset CRD, monitor its execution status,
and then remove it once complete. Successfully completed ResetOffset CRDs will
not be re-executed, but will be re-evaluated by the Controller and are thus
clutter that reduce efficiency and should be removed. Note, "completed" refers
to the process of repositioning the Offsets only, and does not imply that the
event processing has once again caught up to the head of the Topic.

It is expected that only a single ResetOffset is ever present for a specific
Subscription at any given time. As mentioned above, this resource is intended to
aid in failure recovery and should not be frequently used. If however, multiple
ResetOffset instances do exist for the same Subscription, there are no
guarantees as to their execution order, which could result in unintended
sequences of replayed events. Basic locking is in place to ensure the two
ResetOffsets are handled sequentially and are not interleaved. This expectation
doesn't hold in failure scenarios where one or more of the ResetOffsets is
experiencing lengthy exponential back-offs, allowing the locks to expire and
another instance to begin.

While you can create the ResetOffset instance in any namespace, it makes sense
to use the same namespace as the Subscription "ref" of which you wish to
reposition the Offsets. If no `spec.ref.namespace` value is provided it will
default to the ResetOffset namespace.

The `spec.offset.time` is a string that can be one of **"earliest"**,
**"latest"** or a valid RFC3339 format timestamp. As you might expect, the
**"earliest"** and **"latest"** keywords refer to the boundaries of the Kafka
retention window, while the timestamp is expected to be a valid time in that
window. Specifying times outside the retention window will result in a failure.

The `spec.ref` is a standard Knative Reference which indicates the Subscription
whose ConsumerGroup's Offsets will be repositioned. In the future, other
implementations might choose to support others types (e.g., Brokers / Triggers).

## Algorithm

It will help to have a high-level understanding of the process for repositioning
the Offsets when reviewing the following sections.

```
1 - Map the ResetOffset spec.Ref to the corresponding Kafka Topic and ConsumerGroup.

2 - Update DataPlane communication infrastructure between Controller & Dispatchers.

3 - Stop all related ConsumerGroups in the Dispatcher Replicas.

4 - Reposition the Offsets of all ConsumerGroup Partitions.

5 - Re-Start all related ConsumerGroups in the Dispatcher Replicas.
```

The first two steps are read-only and do not alter the cluster in any way.
Stopping and Starting the ConsumerGroups require the Controller to communicate
with the Dispatcher Replicas, and can possibly fail on one or more instances
resulting in an inconsistent state. The Repositioning of the Offsets is atomic
and will fail or succeed for all Partitions in the ConsumerGroup which greatly
simplifies recovery scenarios as detailed below.

Once the `OffsetsUpdated` step has succeeded the ConsumerGroups will never be
stopped again, nor will the Offsets be repositioned, and the re-reconciliation
will focus on attempting to restart the ConsumerGroups. This is to prevent
repetitive Offset repositioning and restarting of any ConsumerGroups instances
that were able to successfully restart.

## Status

The `Status` subresource of the ResetOffset CRD represents the steps required to
perform a successful repositioning of the Kafka ConsumerGroup Offsets. The
ResetOffset will be re-reconciled perpetually until it has `Succeeded` or is
deleted.

Additionally, meta-data is also provided indicating the Kafka `Topic`, `Group`,
and the old/new partition `Offsets`. The meta-data information is intended to
aid any manual recovery required in failure scenarios as described below.

```yaml
status:
  conditions:
  - lastTransitionTime: "2021-06-28T14:22:47Z"
    status: "True"
    type: AcquireDataPlaneServices
  - lastTransitionTime: "2021-06-28T14:22:47Z"
    status: "True"
    type: ConsumerGroupsStarted
  - lastTransitionTime: "2021-06-28T14:22:47Z"
    status: "True"
    type: ConsumerGroupsStopped
  - lastTransitionTime: "2021-06-28T14:22:47Z"
    status: "True"
    type: OffsetsUpdated
  - lastTransitionTime: "2021-06-28T14:22:47Z"
    status: "True"
    type: RefMapped
  - lastTransitionTime: "2021-06-28T14:22:47Z"
    status: "True"
    type: Succeeded
  group: kafka.114aee7c-9fa6-4315-ac78-c78f2053b69b
  partitions:
  - newOffset: 0
    oldOffset: 2
    partition: 0
  - newOffset: 0
    oldOffset: 2
    partition: 1
  - newOffset: 0
    oldOffset: 2
    partition: 2
  - newOffset: 0
    oldOffset: 2
    partition: 3
  topic: tenant1.sample-kafka-channel-1
```

## Failures

The repositioning of Kafka ConsumerGroup Offsets should generally be a fast,
successful operation. However, the process for repositioning Offsets requires
all existing ConsumerGroup instances to be stopped and restarted, and so
failures can potentially result in disabled Subscriptions. The Controller will
attempt to re-reconcile failed ResetOffsets indefinitely until they are
successful. In the case of unresolvable errors it is important to understand the
state of the ConsumerGroup and the recommended manual recovery procedure. The
following failure states align with the `Status` fields shown above and are
described in the Algorithm section.

- **Reference Mapping Error:** If the Controller is unable to map the `spec.ref`
  field to a valid Subscription of a Kafka Topic / ConsumerGroup it will fail
  the operation and re-queue to try again. At this point NO ConsumerGroups have
  been stopped, and NO changes have been made to any Offsets, and the
  ResetOffset can simply be removed.


- **DataPlane Communication Error:** The Controller responsible for reconciling
  the ResetOffset CRD communicates with the Dispatcher (DataPlane) in order to
  Stop / Start all ConsumerGroup instances. If this communication infrastructure
  could not be initiated an error will occur, and the Controller will re-queue
  and try again. At this point NO ConsumerGroups have been stopped, and NO
  changes have been made to any Offsets, and the ResetOffset can simply be
  removed.


- **ConsumerGroup Stop Error:** If there was a failure while attempting to stop
  the Kafka ConsumerGroups, it is possible that some ConsumerGroup instances
  were stopped while others were not. At this point NO Offsets have been
  modified, and you can simply restart the associated Dispatcher Pods to restart
  all ConsumerGroups from their current position.


- **Offset Repositioning Error:**  If there is a failure while attempting to
  reposition the ConsumerGroup Offsets it can be assumed that they have NOT been
  changed. They are committed atomically as a set and will have been changed for
  all Partitions or not at all. To recover you can simply restart the Dispatcher
  Pods to resume processing events from their prior position.


- **ConsumerGroup Start Error:** If the ConsumerGroups are failing to restart
  then they HAVE been repositioned to the newly desired state and one or more
  ConsumerGroups is failing to re-start. If the ResetOffset is stuck in this
  state then the Offsets have been successfully repositioned, and you can simply
  restart the Dispatcher Pods to start processing events from the new Offsets.
