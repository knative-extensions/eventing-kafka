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

package controller

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	rbacv1listers "k8s.io/client-go/listers/rbac/v1"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing-kafka/pkg/common/kafka/offset"
	v1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/apis/eventing"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/apis/duck"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
	pkgreconciler "knative.dev/pkg/reconciler"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/reconciler/controller/resources"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	kafkaScheme "knative.dev/eventing-kafka/pkg/client/clientset/versioned/scheme"
	kafkaChannelReconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	listers "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/client"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
)

const (
	// controllerAgentName is the string used by this controller to identify
	// itself when creating events.
	controllerAgentName = "kafka-ch-controller"

	// Name of the corev1.Events emitted from the reconciliation process.
	dispatcherDeploymentCreated     = "DispatcherDeploymentCreated"
	dispatcherDeploymentUpdated     = "DispatcherDeploymentUpdated"
	dispatcherDeploymentFailed      = "DispatcherDeploymentFailed"
	dispatcherServiceCreated        = "DispatcherServiceCreated"
	dispatcherServiceUpdated        = "DispatcherServiceUpdated"
	dispatcherServiceFailed         = "DispatcherServiceFailed"
	dispatcherServiceAccountCreated = "DispatcherServiceAccountCreated"
	dispatcherRoleBindingCreated    = "DispatcherRoleBindingCreated"

	dispatcherName = "kafka-ch-dispatcher"
)

var (
	scopeNamespace = "namespace"
	scopeCluster   = "cluster"
)

func newReconciledNormal(namespace, name string) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeNormal, "KafkaChannelReconciled", "KafkaChannel reconciled: \"%s/%s\"", namespace, name)
}

func newDeploymentWarn(err error) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeWarning, "DispatcherDeploymentFailed", "Reconciling dispatcher Deployment failed with: %s", err)
}

func newServiceWarn(err error) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeWarning, "DispatcherServiceFailed", "Reconciling dispatcher Service failed with: %s", err)
}

func newDispatcherServiceWarn(err error) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeWarning, "DispatcherServiceFailed", "Reconciling dispatcher Service failed with: %s", err)
}

func newServiceAccountWarn(err error) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeWarning, "DispatcherServiceAccountFailed", "Reconciling dispatcher ServiceAccount failed: %s", err)
}

func newRoleBindingWarn(err error) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeWarning, "DispatcherRoleBindingFailed", "Reconciling dispatcher RoleBinding failed: %s", err)
}

func init() {
	// Add run types to the default Kubernetes Scheme so Events can be
	// logged for run types.
	_ = kafkaScheme.AddToScheme(scheme.Scheme)
}

// Reconciler reconciles Kafka Channels.
type Reconciler struct {
	KubeClientSet kubernetes.Interface

	EventingClientSet eventingclientset.Interface

	systemNamespace          string
	dispatcherImage          string
	dispatcherServiceAccount string

	kafkaConfig        *utils.KafkaConfig
	kafkaConfigMapHash string
	kafkaAuthConfig    *client.KafkaAuthConfig
	kafkaConfigError   error
	kafkaClientSet     kafkaclientset.Interface
	kafkaClient        sarama.Client
	// Using a shared kafkaClusterAdmin does not work currently because of an issue with
	// Shopify/sarama, see https://github.com/Shopify/sarama/issues/1162.
	kafkaClusterAdmin    sarama.ClusterAdmin
	kafkachannelLister   listers.KafkaChannelLister
	kafkachannelInformer cache.SharedIndexInformer
	deploymentLister     appsv1listers.DeploymentLister
	serviceLister        corev1listers.ServiceLister
	endpointsLister      corev1listers.EndpointsLister
	serviceAccountLister corev1listers.ServiceAccountLister
	roleBindingLister    rbacv1listers.RoleBindingLister
	controllerRef        metav1.OwnerReference
}

type envConfig struct {
	Image                    string `envconfig:"DISPATCHER_IMAGE" required:"true"`
	DispatcherServiceAccount string `envconfig:"SERVICE_ACCOUNT" required:"true"`
}

// Check that our Reconciler implements kafka's injection Interface
var _ kafkaChannelReconciler.Interface = (*Reconciler)(nil)
var _ kafkaChannelReconciler.Finalizer = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, kc *v1beta1.KafkaChannel) pkgreconciler.Event {
	kc.Status.InitializeConditions()
	logger := logging.FromContext(ctx)
	// Verify channel is valid.
	kc.SetDefaults(ctx)
	if err := kc.Validate(ctx); err != nil {
		logger.Errorw("Invalid kafka channel", zap.String("channel", kc.Name), zap.Error(err))
		return err
	}
	if r.kafkaConfig == nil {
		if r.kafkaConfigError == nil {
			r.kafkaConfigError = fmt.Errorf("the config map '%s' does not exist", constants.SettingsConfigMapName)
		}
		kc.Status.MarkConfigFailed("MissingConfiguration", "%v", r.kafkaConfigError)
		return r.kafkaConfigError
	}

	kafkaClient, err := r.createKafkaClient()
	if err != nil {
		logger.Errorw("Can't obtain Kafka Client", zap.Any("channel", kc), zap.Error(err))
		kc.Status.MarkConfigFailed("InvalidConfiguration", "Unable to build Kafka client for channel %s: %v", kc.Name, err)
		return err
	}
	defer kafkaClient.Close()

	kafkaClusterAdmin, err := r.createClusterAdmin()
	if err != nil {
		logger.Errorw("Can't obtain Kafka cluster admin", zap.Any("channel", kc), zap.Error(err))
		kc.Status.MarkConfigFailed("InvalidConfiguration", "Unable to build Kafka admin client for channel %s: %v", kc.Name, err)
		return err
	}
	defer kafkaClusterAdmin.Close()

	kc.Status.MarkConfigTrue()

	// We reconcile the status of the Channel by looking at:
	// 1. Kafka topic used by the channel.
	// 2. Dispatcher Deployment for it's readiness.
	// 3. Dispatcher k8s Service for it's existence.
	// 4. Dispatcher endpoints to ensure that there's something backing the Service.
	// 5. K8s service representing the channel that will use ExternalName to point to the Dispatcher k8s service.

	if err := r.reconcileTopic(ctx, kc, kafkaClusterAdmin); err != nil {
		kc.Status.MarkTopicFailed("TopicCreateFailed", "error while creating topic: %s", err)
		return err
	}
	kc.Status.MarkTopicTrue()

	scope, ok := kc.Annotations[eventing.ScopeAnnotationKey]
	if !ok {
		scope = scopeCluster
	}

	dispatcherNamespace := r.systemNamespace
	if scope == scopeNamespace {
		dispatcherNamespace = kc.Namespace
	}

	// Make sure the dispatcher deployment exists and propagate the status to the Channel
	err = r.reconcileDispatcher(ctx, scope, dispatcherNamespace, kc)
	if err != nil {
		return err
	}

	// Make sure the dispatcher service exists and propagate the status to the Channel in case it does not exist.
	// We don't do anything with the service because it's status contains nothing useful, so just do
	// an existence check. Then below we check the endpoints targeting it.
	err = r.reconcileDispatcherService(ctx, dispatcherNamespace, kc)
	if err != nil {
		return err
	}

	// Get the Dispatcher Service Endpoints and propagate the status to the Channel
	// endpoints has the same name as the service, so not a bug.
	e, err := r.endpointsLister.Endpoints(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			kc.Status.MarkEndpointsFailed("DispatcherEndpointsDoesNotExist", "Dispatcher Endpoints does not exist")
		} else {
			logger.Errorw("Unable to get the dispatcher endpoints", zap.Error(err))
			kc.Status.MarkEndpointsFailed("DispatcherEndpointsGetFailed", "Failed to get dispatcher endpoints")
		}
		return err
	}

	if len(e.Subsets) == 0 {
		logger.Errorw("No endpoints found for Dispatcher service", zap.Error(err))
		kc.Status.MarkEndpointsFailed("DispatcherEndpointsNotReady", "There are no endpoints ready for Dispatcher service")
		return fmt.Errorf("there are no endpoints ready for Dispatcher service %s", dispatcherName)
	}
	kc.Status.MarkEndpointsTrue()

	// Reconcile the k8s service representing the actual Channel. It points to the Dispatcher service via ExternalName
	svc, err := r.reconcileChannelService(ctx, dispatcherNamespace, kc)
	if err != nil {
		return err
	}
	kc.Status.MarkChannelServiceTrue()
	kc.Status.SetAddress(&apis.URL{
		Scheme: "http",
		Host:   network.GetServiceHostname(svc.Name, svc.Namespace),
	})
	err = r.reconcileSubscribers(ctx, kc, kafkaClient, kafkaClusterAdmin)
	if err != nil {
		return fmt.Errorf("error reconciling subscribers %v", err)
	}

	// Ok, so now the Dispatcher Deployment & Service have been created, we're golden since the
	// dispatcher watches the Channel and where it needs to dispatch events to.
	return newReconciledNormal(kc.Namespace, kc.Name)
}

func (r *Reconciler) reconcileSubscribers(ctx context.Context, ch *v1beta1.KafkaChannel, kafkaClient sarama.Client, kafkaClusterAdmin sarama.ClusterAdmin) error {
	after := ch.DeepCopy()
	after.Status.Subscribers = make([]v1.SubscriberStatus, 0)
	logger := logging.FromContext(ctx)
	for _, s := range ch.Spec.Subscribers {
		logger.Debugw("Reconciling initial offset for subscription", zap.Any("subscription", s), zap.Any("channel", ch))
		err := r.reconcileInitialOffset(ctx, ch, s, kafkaClient, kafkaClusterAdmin)

		if err != nil {
			logger.Errorw("reconcile failed to initial offset for subscription. Marking the subscription not ready", zap.String("channel", fmt.Sprintf("%s.%s", ch.Namespace, ch.Name)), zap.Any("subscription", s), zap.Error(err))
			after.Status.Subscribers = append(after.Status.Subscribers, v1.SubscriberStatus{
				UID:                s.UID,
				ObservedGeneration: s.Generation,
				Ready:              corev1.ConditionFalse,
				Message:            fmt.Sprintf("Initial offset cannot be committed: %v", err),
			})
		} else {
			logger.Debugw("Reconciled initial offset for subscription. Marking the subscription ready", zap.String("channel", fmt.Sprintf("%s.%s", ch.Namespace, ch.Name)), zap.Any("subscription", s))
			after.Status.Subscribers = append(after.Status.Subscribers, v1.SubscriberStatus{
				UID:                s.UID,
				ObservedGeneration: s.Generation,
				Ready:              corev1.ConditionTrue,
			})
		}
	}

	jsonPatch, err := duck.CreatePatch(ch, after)
	if err != nil {
		return fmt.Errorf("creating JSON patch: %w", err)
	}
	// If there is nothing to patch, we are good, just return.
	// Empty patch is [], hence we check for that.
	if len(jsonPatch) == 0 {
		return nil
	}
	patch, err := jsonPatch.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshaling JSON patch: %w", err)
	}
	patched, err := r.kafkaClientSet.MessagingV1beta1().KafkaChannels(ch.Namespace).Patch(ctx, ch.Name, types.JSONPatchType, patch, metav1.PatchOptions{}, "status")

	if err != nil {
		return fmt.Errorf("Failed patching: %w", err)
	}
	logger.Debugw("Patched resource", zap.Any("patch", patch), zap.Any("patched", patched))
	return nil
}

func (r *Reconciler) reconcileDispatcher(ctx context.Context, scope string, dispatcherNamespace string, kc *v1beta1.KafkaChannel) error {
	logger := logging.FromContext(ctx)
	if scope == scopeNamespace {
		// Configure RBAC in namespace to access the configmaps
		sa, err := r.reconcileServiceAccount(ctx, dispatcherNamespace, kc)
		if err != nil {
			return err
		}

		_, err = r.reconcileRoleBinding(ctx, dispatcherName, dispatcherNamespace, kc, dispatcherName, sa)
		if err != nil {
			return err
		}

		// Reconcile the RoleBinding allowing read access to the shared configmaps.
		// Note this RoleBinding is created in the system namespace and points to a
		// subject in the dispatcher's namespace.
		// TODO: might change when ConfigMapPropagation lands
		roleBindingName := fmt.Sprintf("%s-%s", dispatcherName, dispatcherNamespace)
		_, err = r.reconcileRoleBinding(ctx, roleBindingName, r.systemNamespace, kc, "eventing-config-reader", sa)
		if err != nil {
			return err
		}
	}

	args := resources.DispatcherDeploymentArgs{
		DispatcherScope:       scope,
		DispatcherNamespace:   dispatcherNamespace,
		Image:                 r.dispatcherImage,
		Replicas:              1,
		ServiceAccount:        r.dispatcherServiceAccount,
		ConfigMapHash:         r.kafkaConfigMapHash,
		OwnerRef:              r.controllerRef,
		DeploymentAnnotations: r.kafkaConfig.EventingKafka.Channel.Dispatcher.DeploymentAnnotations,
		DeploymentLabels:      r.kafkaConfig.EventingKafka.Channel.Dispatcher.DeploymentLabels,
		PodAnnotations:        r.kafkaConfig.EventingKafka.Channel.Dispatcher.PodAnnotations,
		PodLabels:             r.kafkaConfig.EventingKafka.Channel.Dispatcher.PodLabels,
	}

	want := resources.NewDispatcherDeploymentBuilder().WithArgs(&args).Build()
	d, err := r.deploymentLister.Deployments(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			d, err := r.KubeClientSet.AppsV1().Deployments(dispatcherNamespace).Create(ctx, want, metav1.CreateOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(kc, corev1.EventTypeNormal, dispatcherDeploymentCreated, "Dispatcher deployment created")
				kc.Status.PropagateDispatcherStatus(&d.Status)
				return err
			} else {
				logger.Errorw("error while creating dispatcher deployment", zap.Error(err), zap.String("namespace", dispatcherNamespace), zap.Any("deployment", want))
				kc.Status.MarkDispatcherFailed(dispatcherDeploymentFailed, "Failed to create the dispatcher deployment: %v", err)
				return newDeploymentWarn(err)
			}
		}
		logger.Errorw("can't get dispatcher deployment", zap.Error(err), zap.String("namespace", dispatcherNamespace),
			zap.String("dispatcher-name", dispatcherName))
		kc.Status.MarkDispatcherUnknown("DispatcherDeploymentFailed", "Failed to get dispatcher deployment: %v", err)
		return err
	} else {
		// scale up the dispatcher to 1, otherwise keep the existing number in case the user has scaled it up.
		if *d.Spec.Replicas == 0 {
			logger.Infof("Dispatcher deployment has 0 replica. Scaling up deployment to 1 replica")
			args.Replicas = 1
		} else {
			args.Replicas = *d.Spec.Replicas
		}
		want = resources.NewDispatcherDeploymentBuilderFromDeployment(d.DeepCopy()).WithArgs(&args).Build()

		if !equality.Semantic.DeepEqual(want.ObjectMeta, d.ObjectMeta) || !equality.Semantic.DeepEqual(want.Spec, d.Spec) {
			logger.Infof("Dispatcher deployment changed; reconciling: ObjectMeta=\n%s, Spec=\n%s", cmp.Diff(want.ObjectMeta, d.ObjectMeta), cmp.Diff(want.Spec, d.Spec))
			if d, err = r.KubeClientSet.AppsV1().Deployments(dispatcherNamespace).Update(ctx, want, metav1.UpdateOptions{}); err != nil {
				logger.Errorw("error while updating dispatcher deployment", zap.Error(err), zap.String("namespace", dispatcherNamespace), zap.Any("deployment", want))
				kc.Status.MarkServiceFailed("DispatcherDeploymentUpdateFailed", "Failed to update the dispatcher deployment: %v", err)
				return newDeploymentWarn(err)
			} else {
				controller.GetEventRecorder(ctx).Event(kc, corev1.EventTypeNormal, dispatcherDeploymentUpdated, "Dispatcher deployment updated")
			}
		}
		kc.Status.PropagateDispatcherStatus(&d.Status)
		return nil
	}
}

func (r *Reconciler) reconcileServiceAccount(ctx context.Context, dispatcherNamespace string, kc *v1beta1.KafkaChannel) (*corev1.ServiceAccount, error) {
	sa, err := r.serviceAccountLister.ServiceAccounts(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			expected := resources.MakeServiceAccount(dispatcherNamespace, dispatcherName)
			sa, err := r.KubeClientSet.CoreV1().ServiceAccounts(dispatcherNamespace).Create(ctx, expected, metav1.CreateOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(kc, corev1.EventTypeNormal, dispatcherServiceAccountCreated, "Dispatcher service account created")
				return sa, nil
			} else {
				kc.Status.MarkDispatcherFailed("DispatcherDeploymentFailed", "Failed to create the dispatcher service account: %v", err)
				return sa, newServiceAccountWarn(err)
			}
		}

		kc.Status.MarkDispatcherUnknown("DispatcherServiceAccountFailed", "Failed to get dispatcher service account: %v", err)
		return nil, newServiceAccountWarn(err)
	}
	return sa, err
}

func (r *Reconciler) reconcileRoleBinding(ctx context.Context, name string, ns string, kc *v1beta1.KafkaChannel, clusterRoleName string, sa *corev1.ServiceAccount) (*rbacv1.RoleBinding, error) {
	rb, err := r.roleBindingLister.RoleBindings(ns).Get(name)
	if err != nil {
		if apierrs.IsNotFound(err) {
			expected := resources.MakeRoleBinding(ns, name, sa, clusterRoleName)
			rb, err := r.KubeClientSet.RbacV1().RoleBindings(ns).Create(ctx, expected, metav1.CreateOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(kc, corev1.EventTypeNormal, dispatcherRoleBindingCreated, "Dispatcher role binding created")
				return rb, nil
			} else {
				kc.Status.MarkDispatcherFailed("DispatcherDeploymentFailed", "Failed to create the dispatcher role binding: %v", err)
				return rb, newRoleBindingWarn(err)
			}
		}
		kc.Status.MarkDispatcherUnknown("DispatcherRoleBindingFailed", "Failed to get dispatcher role binding: %v", err)
		return nil, newRoleBindingWarn(err)
	}
	return rb, err
}

func (r *Reconciler) reconcileDispatcherService(ctx context.Context, dispatcherNamespace string, kc *v1beta1.KafkaChannel) error {

	logger := logging.FromContext(ctx)

	args := resources.DispatcherServiceArgs{
		DispatcherNamespace: dispatcherNamespace,
		ServiceAnnotations:  r.kafkaConfig.EventingKafka.Channel.Dispatcher.ServiceAnnotations,
		ServiceLabels:       r.kafkaConfig.EventingKafka.Channel.Dispatcher.ServiceLabels,
	}

	want := resources.NewDispatcherServiceBuilder().WithArgs(&args).Build()
	svc, err := r.serviceLister.Services(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			_, err := r.KubeClientSet.CoreV1().Services(dispatcherNamespace).Create(ctx, want, metav1.CreateOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(kc, corev1.EventTypeNormal, dispatcherServiceCreated, "Dispatcher service created")
				kc.Status.MarkServiceTrue()
			} else {
				logger.Errorw("Unable to create the dispatcher service", zap.Error(err))
				controller.GetEventRecorder(ctx).Eventf(kc, corev1.EventTypeWarning, dispatcherServiceFailed, "Failed to create the dispatcher service: %v", err)
				kc.Status.MarkServiceFailed("DispatcherServiceFailed", "Failed to create the dispatcher service: %v", err)
				return err
			}
			return err
		}
		logger.Errorw("can't get dispatcher service", zap.Error(err), zap.String("namespace", dispatcherNamespace), zap.String("dispatcher-name", dispatcherName))
		kc.Status.MarkServiceUnknown("DispatcherServiceFailed", "Failed to get dispatcher service: %v", err)
		return newDispatcherServiceWarn(err)
	} else {
		want = resources.NewDispatcherServiceBuilderFromService(svc.DeepCopy()).WithArgs(&args).Build()
		if !equality.Semantic.DeepEqual(want.ObjectMeta, svc.ObjectMeta) || !equality.Semantic.DeepEqual(want.Spec, svc.Spec) {
			logger.Infof("Dispatcher service changed; reconciling: ObjectMeta=\n%s, Spec=\n%s", cmp.Diff(want.ObjectMeta, svc.ObjectMeta), cmp.Diff(want.Spec, svc.Spec))
			if _, err = r.KubeClientSet.CoreV1().Services(dispatcherNamespace).Update(ctx, want, metav1.UpdateOptions{}); err != nil {
				logger.Errorw("error while updating dispatcher service", zap.Error(err), zap.String("namespace", dispatcherNamespace), zap.Any("service", want))
				kc.Status.MarkServiceFailed("DispatcherServiceUpdateFailed", "Failed to update the dispatcher service: %v", err)
				return newServiceWarn(err)
			} else {
				controller.GetEventRecorder(ctx).Event(kc, corev1.EventTypeNormal, dispatcherServiceUpdated, "Dispatcher service updated")
			}
		}
		kc.Status.MarkServiceTrue()
		return nil
	}
}

func (r *Reconciler) reconcileChannelService(ctx context.Context, dispatcherNamespace string, channel *v1beta1.KafkaChannel) (*corev1.Service, error) {
	logger := logging.FromContext(ctx)
	// Get the  Service and propagate the status to the Channel in case it does not exist.
	// We don't do anything with the service because it's status contains nothing useful, so just do
	// an existence check. Then below we check the endpoints targeting it.
	// We may change this name later, so we have to ensure we use proper addressable when resolving these.
	expected, err := resources.MakeK8sService(channel, resources.ExternalService(dispatcherNamespace, dispatcherName))
	if err != nil {
		logger.Errorw("failed to create the channel service object", zap.Error(err))
		channel.Status.MarkChannelServiceFailed("ChannelServiceFailed", fmt.Sprintf("Channel Service failed: %s", err))
		return nil, err
	}

	svc, err := r.serviceLister.Services(channel.Namespace).Get(resources.MakeChannelServiceName(channel.Name))
	if err != nil {
		if apierrs.IsNotFound(err) {
			svc, err = r.KubeClientSet.CoreV1().Services(channel.Namespace).Create(ctx, expected, metav1.CreateOptions{})
			if err != nil {
				logger.Errorw("failed to create the channel service object", zap.Error(err))
				channel.Status.MarkChannelServiceFailed("ChannelServiceFailed", fmt.Sprintf("Channel Service failed: %s", err))
				return nil, err
			}
			return svc, nil
		}
		logger.Errorw("Unable to get the channel service", zap.Error(err))
		return nil, err
	} else if !equality.Semantic.DeepEqual(svc.Spec, expected.Spec) {
		svc = svc.DeepCopy()
		svc.Spec = expected.Spec

		svc, err = r.KubeClientSet.CoreV1().Services(channel.Namespace).Update(ctx, svc, metav1.UpdateOptions{})
		if err != nil {
			logger.Errorw("Failed to update the channel service", zap.Error(err))
			return nil, err
		}
	}
	// Check to make sure that the KafkaChannel owns this service and if not, complain.
	if !metav1.IsControlledBy(svc, channel) {
		err := fmt.Errorf("kafkachannel: %s/%s does not own Service: %q", channel.Namespace, channel.Name, svc.Name)
		channel.Status.MarkChannelServiceFailed("ChannelServiceFailed", "Channel Service failed: %s", err)
		return nil, err
	}
	return svc, nil
}

func (r *Reconciler) createKafkaClient() (sarama.Client, error) {
	kafkaClient := r.kafkaClient
	if kafkaClient == nil {
		var err error

		if r.kafkaConfig.EventingKafka.Sarama.Config == nil {
			return nil, fmt.Errorf("error creating Kafka client: Sarama config is nil")
		}
		kafkaClient, err = sarama.NewClient(r.kafkaConfig.Brokers, r.kafkaConfig.EventingKafka.Sarama.Config)
		if err != nil {
			return nil, err
		}
	}

	return kafkaClient, nil
}

func (r *Reconciler) createClusterAdmin() (sarama.ClusterAdmin, error) {
	// We don't currently initialize r.kafkaClusterAdmin, hence we end up creating the cluster admin client every time.
	// This is because of an issue with Shopify/sarama. See https://github.com/Shopify/sarama/issues/1162.
	// Once the issue is fixed we should use a shared cluster admin client. Also, r.kafkaClusterAdmin is currently
	// used to pass a fake admin client in the tests.
	kafkaClusterAdmin := r.kafkaClusterAdmin
	if kafkaClusterAdmin == nil {
		var err error

		if r.kafkaConfig.EventingKafka.Sarama.Config == nil {
			return nil, fmt.Errorf("error creating admin client: Sarama config is nil")
		}
		kafkaClusterAdmin, err = sarama.NewClusterAdmin(r.kafkaConfig.Brokers, r.kafkaConfig.EventingKafka.Sarama.Config)
		if err != nil {
			return nil, err
		}
	}
	return kafkaClusterAdmin, nil
}

func (r *Reconciler) reconcileTopic(ctx context.Context, channel *v1beta1.KafkaChannel, kafkaClusterAdmin sarama.ClusterAdmin) error {
	logger := logging.FromContext(ctx)

	topicName := utils.TopicName(utils.KafkaChannelSeparator, channel.Namespace, channel.Name)
	logger.Infow("Creating topic on Kafka cluster", zap.String("topic", topicName),
		zap.Int32("partitions", channel.Spec.NumPartitions), zap.Int16("replication", channel.Spec.ReplicationFactor))

	// Parse & Format the RetentionDuration into Sarama retention.ms string
	retentionDuration, err := channel.Spec.ParseRetentionDuration()
	if err != nil {
		// Should never happen with webhook defaulting and validation in place.
		logger.Errorw("Error parsing RetentionDuration, using default instead", zap.String("RetentionDuration", channel.Spec.RetentionDuration), zap.Error(err))
		retentionDuration = constants.DefaultRetentionDuration
	}
	retentionMillisString := strconv.FormatInt(retentionDuration.Milliseconds(), 10)

	err = kafkaClusterAdmin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     channel.Spec.NumPartitions,
		ReplicationFactor: channel.Spec.ReplicationFactor,
		ConfigEntries: map[string]*string{
			constants.KafkaTopicConfigRetentionMs: &retentionMillisString,
		},
	}, false)
	if e, ok := err.(*sarama.TopicError); ok && e.Err == sarama.ErrTopicAlreadyExists {
		return nil
	} else if err != nil {
		logger.Errorw("Error creating topic", zap.String("topic", topicName), zap.Error(err))
	} else {
		logger.Infow("Successfully created topic", zap.String("topic", topicName))
	}
	return err
}

func (r *Reconciler) reconcileInitialOffset(ctx context.Context, channel *v1beta1.KafkaChannel, sub v1.SubscriberSpec, kafkaClient sarama.Client, kafkaClusterAdmin sarama.ClusterAdmin) error {
	subscriptionStatus := findSubscriptionStatus(channel, sub.UID)
	if subscriptionStatus != nil && subscriptionStatus.Ready == corev1.ConditionTrue {
		// subscription is ready, the offsets must have been initialized already
		return nil
	}

	topicName := utils.TopicName(utils.KafkaChannelSeparator, channel.Namespace, channel.Name)
	groupID := fmt.Sprintf("kafka.%s.%s.%s", channel.Namespace, channel.Name, string(sub.UID))
	_, err := offset.InitOffsets(ctx, kafkaClient, kafkaClusterAdmin, []string{topicName}, groupID)
	if err != nil {
		logger := logging.FromContext(ctx)
		logger.Errorw("error reconciling initial offset", zap.String("channel", fmt.Sprintf("%s.%s", channel.Namespace, channel.Name)), zap.Any("subscription", sub), zap.Error(err))
	}
	return err
}

func (r *Reconciler) deleteTopic(ctx context.Context, channel *v1beta1.KafkaChannel, kafkaClusterAdmin sarama.ClusterAdmin) error {
	logger := logging.FromContext(ctx)

	topicName := utils.TopicName(utils.KafkaChannelSeparator, channel.Namespace, channel.Name)
	logger.Infow("Deleting topic on Kafka Cluster", zap.String("topic", topicName))
	err := kafkaClusterAdmin.DeleteTopic(topicName)
	if err == sarama.ErrUnknownTopicOrPartition {
		logger.Debugw("Received an unknown topic or partition response. Ignoring")
		return nil
	} else if err != nil {
		logger.Errorw("Error deleting topic", zap.String("topic", topicName), zap.Error(err))
	} else {
		logger.Infow("Successfully deleted topic", zap.String("topic", topicName))
	}
	return err
}

func (r *Reconciler) updateKafkaConfig(ctx context.Context, configMap *corev1.ConfigMap) {
	logger := logging.FromContext(ctx)

	if r == nil {
		// This typically happens during startup and can be ignored
		return
	}

	if configMap == nil {
		logger.Warn("Nil ConfigMap passed to updateKafkaConfig; ignoring")
		return
	}

	logger.Info("Reloading Kafka configuration")
	kafkaConfig, err := utils.GetKafkaConfig(ctx, controllerAgentName, configMap.Data, kafkasarama.LoadAuthConfig)
	if err != nil {
		logger.Errorw("Error reading Kafka configuration", zap.Error(err))
		return
	}

	// Manually commit the offsets in KafkaChannel controller.
	// That's because we want to make sure we initialize the offsets within the controller
	// before dispatcher actually starts consuming messages.
	kafkaConfig.EventingKafka.Sarama.Config.Consumer.Offsets.AutoCommit.Enable = false

	r.kafkaAuthConfig = kafkaConfig.EventingKafka.Auth
	// For now just override the previous config.
	// Eventually the previous config should be snapshotted to delete Kafka topics
	r.kafkaConfig = kafkaConfig
	r.kafkaConfigError = err
	r.kafkaConfigMapHash = commonconfig.ConfigmapDataCheckSum(configMap.Data)
}

func (r *Reconciler) FinalizeKind(ctx context.Context, kc *v1beta1.KafkaChannel) pkgreconciler.Event {
	// Do not attempt retrying creating the client because it might be a permanent error
	// in which case the finalizer will never get removed.
	logger := logging.FromContext(ctx)
	channel := fmt.Sprintf("%s/%s", kc.GetNamespace(), kc.GetName())
	logger.Debugw("FinalizeKind", zap.String("channel", channel))

	kafkaClusterAdmin, err := r.createClusterAdmin()
	if err != nil || r.kafkaConfig == nil {
		logger.Errorw("cannot obtain Kafka cluster admin", zap.String("channel", channel), zap.Error(err))
		// even in error case, we return `normal`, since we are fine with leaving the
		// topic undeleted e.g. when we lose connection
		return newReconciledNormal(kc.Namespace, kc.Name)
	}
	defer kafkaClusterAdmin.Close()

	logger.Debugw("got client, about to delete topic")
	if err := r.deleteTopic(ctx, kc, kafkaClusterAdmin); err != nil {
		logger.Errorw("error deleting Kafka channel topic", zap.String("channel", channel), zap.Error(err))
		return err
	}
	return newReconciledNormal(kc.Namespace, kc.Name) //ok to remove finalizer
}

func findSubscriptionStatus(kc *v1beta1.KafkaChannel, subUID types.UID) *v1.SubscriberStatus {
	for _, subStatus := range kc.Status.Subscribers {
		if subStatus.UID == subUID {
			return &subStatus
		}
	}
	return nil
}
