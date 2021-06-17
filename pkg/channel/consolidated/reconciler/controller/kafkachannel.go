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
	"k8s.io/utils/pointer"
	v1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/apis/duck"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
	pkgreconciler "knative.dev/pkg/reconciler"

	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/reconciler/controller/resources"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/status"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/utils"
	kafkaclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	kafkaScheme "knative.dev/eventing-kafka/pkg/client/clientset/versioned/scheme"
	kafkaChannelReconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	listers "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/client"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
	commonconstants "knative.dev/eventing-kafka/pkg/common/constants"
	kafkasarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
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
	statusManager        status.Manager
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

	kafkaClusterAdmin, err := r.createClient(ctx)
	if err != nil {
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
	_, err = r.reconcileDispatcherService(ctx, dispatcherNamespace, kc)
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
	err = r.reconcileSubscribers(ctx, kc)
	if err != nil {
		return fmt.Errorf("error reconciling subscribers %v", err)
	}

	// Ok, so now the Dispatcher Deployment & Service have been created, we're golden since the
	// dispatcher watches the Channel and where it needs to dispatch events to.
	return newReconciledNormal(kc.Namespace, kc.Name)
}

func (r *Reconciler) reconcileSubscribers(ctx context.Context, ch *v1beta1.KafkaChannel) error {
	after := ch.DeepCopy()
	after.Status.Subscribers = make([]v1.SubscriberStatus, 0)
	for _, s := range ch.Spec.Subscribers {
		if r, _ := r.statusManager.IsReady(ctx, *ch, s); r {
			logging.FromContext(ctx).Debugw("marking subscription", zap.Any("subscription", s))
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
	logging.FromContext(ctx).Debugw("Patched resource", zap.Any("patch", patch), zap.Any("patched", patched))
	return nil
}

func (r *Reconciler) reconcileDispatcher(ctx context.Context, scope string, dispatcherNamespace string, kc *v1beta1.KafkaChannel) error {
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
	args := resources.DispatcherArgs{
		DispatcherScope:     scope,
		DispatcherNamespace: dispatcherNamespace,
		Image:               r.dispatcherImage,
		Replicas:            1,
		ServiceAccount:      r.dispatcherServiceAccount,
		ConfigMapHash:       r.kafkaConfigMapHash,
	}

	expected := resources.MakeDispatcher(args)
	d, err := r.deploymentLister.Deployments(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			d, err := r.KubeClientSet.AppsV1().Deployments(dispatcherNamespace).Create(ctx, expected, metav1.CreateOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(kc, corev1.EventTypeNormal, dispatcherDeploymentCreated, "Dispatcher deployment created")
				kc.Status.PropagateDispatcherStatus(&d.Status)
				return err
			} else {
				kc.Status.MarkDispatcherFailed(dispatcherDeploymentFailed, "Failed to create the dispatcher deployment: %v", err)
				return newDeploymentWarn(err)
			}
		}

		logging.FromContext(ctx).Errorw("Unable to get the dispatcher deployment", zap.Error(err))
		kc.Status.MarkDispatcherUnknown("DispatcherDeploymentFailed", "Failed to get dispatcher deployment: %v", err)
		return err
	} else {
		existing := utils.FindContainer(d, resources.DispatcherContainerName)
		if existing == nil {
			logging.FromContext(ctx).Errorw("Container %s does not exist in existing dispatcher deployment. Updating the deployment", resources.DispatcherContainerName)
			d, err := r.KubeClientSet.AppsV1().Deployments(dispatcherNamespace).Update(ctx, expected, metav1.UpdateOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(kc, corev1.EventTypeNormal, dispatcherDeploymentUpdated, "Dispatcher deployment updated")
				kc.Status.PropagateDispatcherStatus(&d.Status)
				return nil
			} else {
				kc.Status.MarkServiceFailed("DispatcherDeploymentUpdateFailed", "Failed to update the dispatcher deployment: %v", err)
			}
			return newDeploymentWarn(err)
		}

		expectedContainer := utils.FindContainer(expected, resources.DispatcherContainerName)
		if expectedContainer == nil {
			return fmt.Errorf("container %s does not exist in expected dispatcher deployment. Cannot check if the deployment needs an update", resources.DispatcherContainerName)
		}

		expectedConfigMapHash := r.kafkaConfigMapHash

		needsUpdate := false
		// do not touch the original deployment, deepcopy it
		deploymentCopy := d.DeepCopy()
		existingCopy := utils.FindContainer(deploymentCopy, resources.DispatcherContainerName)

		if existingCopy.Image != expectedContainer.Image {
			logging.FromContext(ctx).Infof("Dispatcher deployment image is not what we expect it to be, updating Deployment Got: %q Expect: %q", expected.Spec.Template.Spec.Containers[0].Image, d.Spec.Template.Spec.Containers[0].Image)
			existingCopy.Image = expectedContainer.Image
			needsUpdate = true
		}

		if *deploymentCopy.Spec.Replicas == 0 {
			logging.FromContext(ctx).Infof("Dispatcher deployment has 0 replica. Scaling up deployment to 1 replica")
			deploymentCopy.Spec.Replicas = pointer.Int32Ptr(1)
			needsUpdate = true
		}

		if deploymentCopy.Spec.Template.Annotations == nil {
			logging.FromContext(ctx).Infof("Configmap hash is not set. Updating the dispatcher deployment.")
			deploymentCopy.Spec.Template.Annotations = map[string]string{
				commonconstants.ConfigMapHashAnnotationKey: expectedConfigMapHash,
			}
			needsUpdate = true
		}

		if deploymentCopy.Spec.Template.Annotations[commonconstants.ConfigMapHashAnnotationKey] != expectedConfigMapHash {
			logging.FromContext(ctx).Infof("Configmap hash is changed. Updating the dispatcher deployment.")
			deploymentCopy.Spec.Template.Annotations[commonconstants.ConfigMapHashAnnotationKey] = expectedConfigMapHash
			needsUpdate = true
		}

		if needsUpdate {
			deploymentCopy, err = r.KubeClientSet.AppsV1().Deployments(dispatcherNamespace).Update(ctx, deploymentCopy, metav1.UpdateOptions{})
			if err == nil {
				controller.GetEventRecorder(ctx).Event(kc, corev1.EventTypeNormal, dispatcherDeploymentUpdated, "Dispatcher deployment updated")
				kc.Status.PropagateDispatcherStatus(&deploymentCopy.Status)
				return nil
			} else {
				kc.Status.MarkServiceFailed("DispatcherDeploymentUpdateFailed", "Failed to update the dispatcher deployment: %v", err)
				return newDeploymentWarn(err)
			}
		} else {
			kc.Status.PropagateDispatcherStatus(&d.Status)
			return nil
		}
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

func (r *Reconciler) reconcileDispatcherService(ctx context.Context, dispatcherNamespace string, kc *v1beta1.KafkaChannel) (*corev1.Service, error) {
	svc, err := r.serviceLister.Services(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			expected := resources.MakeDispatcherService(dispatcherNamespace)
			svc, err := r.KubeClientSet.CoreV1().Services(dispatcherNamespace).Create(ctx, expected, metav1.CreateOptions{})

			if err == nil {
				controller.GetEventRecorder(ctx).Event(kc, corev1.EventTypeNormal, dispatcherServiceCreated, "Dispatcher service created")
				kc.Status.MarkServiceTrue()
			} else {
				logging.FromContext(ctx).Errorw("Unable to create the dispatcher service", zap.Error(err))
				controller.GetEventRecorder(ctx).Eventf(kc, corev1.EventTypeWarning, dispatcherServiceFailed, "Failed to create the dispatcher service: %v", err)
				kc.Status.MarkServiceFailed("DispatcherServiceFailed", "Failed to create the dispatcher service: %v", err)
				return svc, err
			}

			return svc, err
		}

		kc.Status.MarkServiceUnknown("DispatcherServiceFailed", "Failed to get dispatcher service: %v", err)
		return nil, newDispatcherServiceWarn(err)
	}

	kc.Status.MarkServiceTrue()
	return svc, nil
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

func (r *Reconciler) createClient(ctx context.Context) (sarama.ClusterAdmin, error) {
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

	retentionMillisString := strconv.FormatInt(commonconfig.RetentionMillis(channel, r.kafkaConfig.EventingKafka, logger), 10)

	err := kafkaClusterAdmin.CreateTopic(topicName, &sarama.TopicDetail{
		ReplicationFactor: commonconfig.ReplicationFactor(channel, r.kafkaConfig.EventingKafka, logger),
		NumPartitions:     commonconfig.NumPartitions(channel, r.kafkaConfig.EventingKafka, logger),
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
	kafkaClusterAdmin, err := r.createClient(ctx)
	if err != nil || r.kafkaConfig == nil {
		logger.Errorw("Can't obtain Kafka Client", zap.String("channel", channel), zap.Error(err))
	} else {
		defer kafkaClusterAdmin.Close()
		logger.Debugw("Got client, about to delete topic")
		if err := r.deleteTopic(ctx, kc, kafkaClusterAdmin); err != nil {
			logger.Errorw("Error deleting Kafka channel topic", zap.String("channel", channel), zap.Error(err))
			return err
		}
	}
	for _, s := range kc.Spec.Subscribers {
		logger.Debugw("Canceling probing", zap.String("channel", channel), zap.Any("subscription", s))
		r.statusManager.CancelProbing(s)
	}
	return newReconciledNormal(kc.Namespace, kc.Name) //ok to remove finalizer
}
