/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package source

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/eventing-kafka/pkg/common/kafka/offset"
	"knative.dev/eventing-kafka/pkg/source/client"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/pkg/utils"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	ctrlreconciler "knative.dev/control-protocol/pkg/reconciler"
	ctrlservice "knative.dev/control-protocol/pkg/service"

	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	kafkasourcecontrol "knative.dev/eventing-kafka/pkg/source/control"
	"knative.dev/eventing-kafka/pkg/source/reconciler/source/resources"

	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	reconcilerkafkasource "knative.dev/eventing-kafka/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"
	listers "knative.dev/eventing-kafka/pkg/client/listers/sources/v1beta1"
	"knative.dev/eventing-kafka/pkg/source/reconciler/common"
)

const (
	raImageEnvVar                = "KAFKA_RA_IMAGE"
	kafkaSourceDeploymentCreated = "KafkaSourceDeploymentCreated"
	kafkaSourceDeploymentUpdated = "KafkaSourceDeploymentUpdated"
	kafkaSourceDeploymentScaled  = "KafkaSourceDeploymentScaled"
	kafkaSourceDeploymentFailed  = "KafkaSourceDeploymentFailed"
	kafkaSourceDeploymentDeleted = "KafkaSourceDeploymentDeleted"
	component                    = "kafkasource"
)

// newDeploymentCreated makes a new reconciler event with event type Normal, and
// reason KafkaSourceDeploymentCreated.
func newDeploymentCreated(namespace, name string) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeNormal, kafkaSourceDeploymentCreated, "KafkaSource created deployment: \"%s/%s\"", namespace, name)
}

// deploymentUpdated makes a new reconciler event with event type Normal, and
// reason KafkaSourceDeploymentUpdated.
func deploymentUpdated(namespace, name string) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeNormal, kafkaSourceDeploymentUpdated, "KafkaSource updated deployment: \"%s/%s\"", namespace, name)
}

// deploymentScaled makes a new reconciler event with event type Normal, and
// reason KafkaSourceDeploymentScaled
func deploymentScaled(namespace, name string) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeNormal, kafkaSourceDeploymentScaled, "KafkaSource scaled deployment: \"%s/%s\"", namespace, name)
}

// newDeploymentFailed makes a new reconciler event with event type Warning, and
// reason KafkaSourceDeploymentFailed.
func newDeploymentFailed(namespace, name string, err error) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeWarning, kafkaSourceDeploymentFailed, "KafkaSource failed to create deployment: \"%s/%s\", %w", namespace, name, err)
}

type Reconciler struct {
	// KubeClientSet allows us to talk to the k8s for core APIs
	KubeClientSet kubernetes.Interface

	receiveAdapterImage string

	kafkaLister      listers.KafkaSourceLister
	deploymentLister appsv1listers.DeploymentLister

	kafkaClientSet versioned.Interface
	loggingContext context.Context

	sinkResolver *resolver.URIResolver

	configs KafkaSourceConfigAccessor

	podIpGetter             ctrlreconciler.PodIpGetter
	connectionPool          ctrlreconciler.ControlPlaneConnectionPool
	claimsNotificationStore *ctrlreconciler.NotificationStore
}

// Check that our Reconciler implements Interface
var _ reconcilerkafkasource.Interface = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, src *v1beta1.KafkaSource) pkgreconciler.Event {
	src.Status.InitializeConditions()

	if (src.Spec.Sink == duckv1.Destination{}) {
		src.Status.MarkNoSink("SinkMissing", "")
		return fmt.Errorf("spec.sink missing")
	}

	dest := src.Spec.Sink.DeepCopy()
	if dest.Ref != nil {
		// To call URIFromDestination(), dest.Ref must have a Namespace. If there is
		// no Namespace defined in dest.Ref, we will use the Namespace of the source
		// as the Namespace of dest.Ref.
		if dest.Ref.Namespace == "" {
			dest.Ref.Namespace = src.GetNamespace()
		}
	}
	sinkURI, err := r.sinkResolver.URIFromDestinationV1(ctx, *dest, src)
	if err != nil {
		src.Status.MarkNoSink("NotFound", "")
		//delete adapter deployment if sink not found
		if err := r.deleteReceiveAdapter(ctx, src); err != nil && !apierrors.IsNotFound(err) {
			logging.FromContext(ctx).Error("Unable to delete receiver adapter when sink is missing", zap.Error(err))
		}
		return fmt.Errorf("getting sink URI: %v", err)
	}
	src.Status.MarkSink(sinkURI)

	selector, err := resources.GetLabelsAsSelector(src.Name)
	if err != nil {
		return fmt.Errorf("getting labels as selector: %v", err)
	}
	src.Status.Selector = selector.String()

	if val, ok := src.GetLabels()[v1beta1.KafkaKeyTypeLabel]; ok {
		found := false
		for _, allowed := range v1beta1.KafkaKeyTypeAllowed {
			if allowed == val {
				found = true
			}
		}
		if !found {
			src.Status.MarkKeyTypeIncorrect("IncorrectKafkaKeyTypeLabel", "Invalid value for %s: %s. Allowed: %v", v1beta1.KafkaKeyTypeLabel, val, v1beta1.KafkaKeyTypeAllowed)
			logging.FromContext(ctx).Errorf("Invalid value for %s: %s. Allowed: %v", v1beta1.KafkaKeyTypeLabel, val, v1beta1.KafkaKeyTypeAllowed)
			return errors.New("IncorrectKafkaKeyTypeLabel")
		} else {
			src.Status.MarkKeyTypeCorrect()
		}
	}

	// Create or update the receive adapter
	ra, err := r.createReceiveAdapter(ctx, src, sinkURI)
	if err != nil {
		var event *pkgreconciler.ReconcilerEvent
		isReconcilerEvent := pkgreconciler.EventAs(err, &event)
		if isReconcilerEvent && event.EventType != corev1.EventTypeNormal {
			logging.FromContext(ctx).Error("Unable to create the receive adapter. Reconciler error", zap.Error(err))
			return err
		} else if !isReconcilerEvent {
			logging.FromContext(ctx).Error("Unable to create the receive adapter. Generic error", zap.Error(err))
			return err
		}
	}

	// Propagate the receive adapter status
	msg, ready, err := r.receiveAdapterStatus(ra)
	if err != nil {
		logging.FromContext(ctx).Errorw("receive adapter is not ready", zap.Error(err))
		src.Status.MarkNotDeployed("DeploymentError", err.Error())
		return nil
	}

	if !ready {
		logging.FromContext(ctx).Errorw(msg)
		src.Status.MarkNotDeployed("DeploymentNotReady", msg)
		return nil
	}

	src.Status.MarkDeployed(ra)

	logging.FromContext(ctx).Debugf("we have a RA deployment")

	// We need to get all the pods for that ra deployment
	podIPs, err := r.podIpGetter.GetAllPodsIp(src.Namespace, labels.Set(resources.GetLabels(src.Name)).AsSelector())
	if err != nil {
		return fmt.Errorf("error getting receive adapter pods %q: %v", ra.Name, err)
	}

	// Check if all the pods are up as they should be
	if derefReplicas(ra.Spec.Replicas) != int32(len(podIPs)) {
		return fmt.Errorf("returning because the numbers of pods deployed doesn't match the expected: %d", len(podIPs))
	}

	// Reconcile connections
	srcNamespacedName := types.NamespacedName{Name: src.Name, Namespace: src.Namespace}
	_, err = r.connectionPool.ReconcileConnections(
		ctx,
		string(src.UID),
		podIPs,
		func(newHost string, service ctrl.Service) {
			service.MessageHandler(ctrlservice.MessageRouter{
				kafkasourcecontrol.NotifySetupClaimsOpCode: r.claimsNotificationStore.MessageHandler(
					srcNamespacedName,
					newHost,
					kafkasourcecontrol.ClaimsMerger,
				),
				kafkasourcecontrol.NotifyCleanupClaimsOpCode: r.claimsNotificationStore.MessageHandler(
					srcNamespacedName,
					newHost,
					kafkasourcecontrol.ClaimsDifference,
				),
			})
		},
		func(oldHost string) {
			r.claimsNotificationStore.CleanPodNotification(srcNamespacedName, oldHost)
		},
	)
	if err != nil {
		return fmt.Errorf("error while reconciling connections: %w", err)
	}

	logging.FromContext(ctx).Debugf("Control connections reconciled")

	// Update consumer group status
	lastClaimStatus, ok := r.claimsNotificationStore.GetPodsNotifications(srcNamespacedName)
	if ok {
		src.Status.UpdateConsumerGroupStatus(stringifyClaimsStatus(lastClaimStatus))
	}

	// Finally, validate configuration and offsets.
	bs, config, err := client.NewConfigFromSpec(ctx, r.KubeClientSet, src)
	if err != nil {
		logging.FromContext(ctx).Errorw("unable to build Kafka configuration", zap.Error(err))
		src.Status.MarkConnectionNotEstablished("InvalidConfiguration", err.Error())
		return err
	}

	// InitOffsets manually commits offsets if needed (see below)
	config.Consumer.Offsets.AutoCommit.Enable = false

	c, err := sarama.NewClient(bs, config)
	if err != nil {
		logging.FromContext(ctx).Errorw("unable to create a kafka client", zap.Error(err))
		src.Status.MarkConnectionNotEstablished("ClientCreationFailed", err.Error())
		return err
	}
	defer c.Close()
	src.Status.MarkConnectionEstablished()

	kafkaAdminClient, err := sarama.NewClusterAdminFromClient(c)
	if err != nil {
		src.Status.MarkInitialOffsetNotCommitted("OffsetsNotCommitted", "Unable to initialize consumergroup offsets: %v", err)
		return fmt.Errorf("failed to create a Kafka admin client: %w", err)
	}
	defer kafkaAdminClient.Close()

	_, err = offset.InitOffsets(ctx, c, kafkaAdminClient, src.Spec.Topics, src.Spec.ConsumerGroup)
	if err != nil {
		logging.FromContext(ctx).Errorw("unable to initialize consumergroup offsets", zap.Error(err))
		src.Status.MarkInitialOffsetNotCommitted("OffsetsNotCommitted", "Unable to initialize consumergroup offsets: %v", err)
		return err
	}
	src.Status.MarkInitialOffsetCommitted()
	src.Status.CloudEventAttributes = r.createCloudEventAttributes(src)

	return nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, src *v1beta1.KafkaSource) pkgreconciler.Event {
	// Cleanup all the connections in the connection pool associated to src
	r.connectionPool.RemoveAllConnections(ctx, string(src.UID))

	r.claimsNotificationStore.CleanPodsNotifications(types.NamespacedName{
		Namespace: src.Namespace,
		Name:      src.Name,
	})

	if err := r.deleteReceiveAdapter(ctx, src); !apierrors.IsNotFound(err) {
		return err
	}

	return common.FinalizeKind(ctx, r.KubeClientSet, src)
}

func (r *Reconciler) createReceiveAdapter(ctx context.Context, src *v1beta1.KafkaSource, sinkURI *apis.URL) (*appsv1.Deployment, error) {
	raArgs := resources.ReceiveAdapterArgs{
		Image:          r.receiveAdapterImage,
		Source:         src,
		Labels:         resources.GetLabels(src.Name),
		SinkURI:        sinkURI.String(),
		AdditionalEnvs: r.configs.ToEnvVars(),
	}
	expected := resources.MakeReceiveAdapter(&raArgs)

	ra, err := r.KubeClientSet.AppsV1().Deployments(src.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil && apierrors.IsNotFound(err) {
		// Issue eventing#2842: Adater deployment name uses kmeta.ChildName. If a deployment by the previous name pattern is found, it should
		// be deleted. This might cause temporary downtime.
		if deprecatedName := utils.GenerateFixedName(raArgs.Source, fmt.Sprintf("kafkasource-%s", raArgs.Source.Name)); deprecatedName != expected.Name {
			if err := r.KubeClientSet.AppsV1().Deployments(src.Namespace).Delete(ctx, deprecatedName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("error deleting deprecated named deployment: %v", err)
			}
			controller.GetEventRecorder(ctx).Eventf(src, corev1.EventTypeNormal, kafkaSourceDeploymentDeleted, "Deprecated deployment removed: \"%s/%s\"", src.Namespace, deprecatedName)
		}
		ra, err = r.KubeClientSet.AppsV1().Deployments(src.Namespace).Create(ctx, expected, metav1.CreateOptions{})
		if err != nil {
			return nil, newDeploymentFailed(ra.Namespace, ra.Name, err)
		}
		return ra, newDeploymentCreated(ra.Namespace, ra.Name)
	} else if err != nil {
		logging.FromContext(ctx).Error("Unable to get an existing receive adapter", zap.Error(err))
		return nil, err
	} else if !metav1.IsControlledBy(ra, src) {
		return nil, fmt.Errorf("deployment %q is not owned by KafkaSource %q", ra.Name, src.Name)
	} else if podSpecChanged(ra.Spec.Template.Spec, expected.Spec.Template.Spec) {
		ra.Spec.Template.Spec = expected.Spec.Template.Spec
		if ra, err = r.KubeClientSet.AppsV1().Deployments(src.Namespace).Update(ctx, ra, metav1.UpdateOptions{}); err != nil {
			return ra, err
		}
		return ra, deploymentUpdated(ra.Namespace, ra.Name)
	} else if derefReplicas(ra.Spec.Replicas) != derefReplicas(expected.Spec.Replicas) {
		ra.Spec.Replicas = expected.Spec.Replicas
		if ra, err = r.KubeClientSet.AppsV1().Deployments(src.Namespace).Update(ctx, ra, metav1.UpdateOptions{}); err != nil {
			return ra, err
		}
		return ra, deploymentScaled(ra.Namespace, ra.Name)
	} else {
		logging.FromContext(ctx).Debug("Reusing existing receive adapter", zap.Any("receiveAdapter", ra))
	}
	return ra, nil
}

// deleteReceiveAdapter deletes the receiver adapter deployment if any
func (r *Reconciler) deleteReceiveAdapter(ctx context.Context, src *v1beta1.KafkaSource) error {
	name := kmeta.ChildName(fmt.Sprintf("kafkasource-%s-", src.Name), string(src.GetUID()))

	return r.KubeClientSet.AppsV1().Deployments(src.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

// receiveAdapterStatus returns a message describing deployment status, and a bool value indicating if the status is considered done.
func (r *Reconciler) receiveAdapterStatus(deployment *appsv1.Deployment) (string, bool, error) {
	// Copied from https://github.com/kubernetes/kubectl/blob/release-1.22/pkg/polymorphichelpers/rollout_status.go#L75-L91
	if deployment.Generation <= deployment.Status.ObservedGeneration {
		cond := GetDeploymentCondition(deployment.Status, appsv1.DeploymentProgressing)
		if cond != nil && cond.Reason == "ProgressDeadlineExceeded" {
			return "", false, fmt.Errorf("deployment %q exceeded its progress deadline", deployment.Name)
		}
		if deployment.Spec.Replicas != nil && deployment.Status.UpdatedReplicas < *deployment.Spec.Replicas {
			return fmt.Sprintf("Waiting for deployment %q rollout to finish: %d out of %d new replicas have been updated", deployment.Name, deployment.Status.UpdatedReplicas, *deployment.Spec.Replicas), false, nil
		}
		if deployment.Status.Replicas > deployment.Status.UpdatedReplicas {
			return fmt.Sprintf("Waiting for deployment %q rollout to finish: %d old replicas are pending termination", deployment.Name, deployment.Status.Replicas-deployment.Status.UpdatedReplicas), false, nil
		}
		if deployment.Status.AvailableReplicas < deployment.Status.UpdatedReplicas {
			return fmt.Sprintf("Waiting for deployment %q rollout to finish: %d of %d updated replicas are available", deployment.Name, deployment.Status.AvailableReplicas, deployment.Status.UpdatedReplicas), false, nil
		}
		return fmt.Sprintf("deployment %q successfully rolled out", deployment.Name), true, nil
	}
	return "Waiting for deployment spec update to be observed", false, nil
}

func podSpecChanged(oldPodSpec corev1.PodSpec, newPodSpec corev1.PodSpec) bool {
	if !equality.Semantic.DeepDerivative(newPodSpec, oldPodSpec) {
		return true
	}
	if len(oldPodSpec.Containers) != len(newPodSpec.Containers) {
		return true
	}
	for i := range newPodSpec.Containers {
		if !equality.Semantic.DeepEqual(newPodSpec.Containers[i].Env, oldPodSpec.Containers[i].Env) {
			return true
		}
	}
	return false
}

func (r *Reconciler) createCloudEventAttributes(src *v1beta1.KafkaSource) []duckv1.CloudEventAttributes {
	ceAttributes := make([]duckv1.CloudEventAttributes, 0, len(src.Spec.Topics))
	for i := range src.Spec.Topics {
		topics := strings.Split(src.Spec.Topics[i], ",")
		for _, topic := range topics {
			ceAttributes = append(ceAttributes, duckv1.CloudEventAttributes{
				Type:   v1beta1.KafkaEventType,
				Source: v1beta1.KafkaEventSource(src.Namespace, src.Name, topic),
			})
		}
	}
	return ceAttributes
}

func derefReplicas(i *int32) int32 {
	if i == nil {
		return 1
	}
	return *i
}

func stringifyClaimsStatus(status map[string]interface{}) string {
	strs := make([]string, 0, len(status))
	for podIp, claims := range status {
		strs = append(strs, fmt.Sprintf("Pod %s: %v", podIp, claims))
	}
	return strings.Join(strs, "\n")
}

// GetDeploymentCondition returns the condition with the provided type.
func GetDeploymentCondition(status appsv1.DeploymentStatus, condType appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}
