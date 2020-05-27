package kafkasecret

import (
	"fmt"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	kafkav1alpha1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1alpha1"
	"knative.dev/eventing-kafka/pkg/controller/constants"
	"knative.dev/eventing-kafka/pkg/controller/util"
	"knative.dev/pkg/reconciler"
)

//
// KafkaChannel Status Reconciliation
//

// Reconcile KafkaChannel Status With Specified Channel Service/Deployment State
func (r *Reconciler) reconcileKafkaChannelStatus(secret *corev1.Secret,
	serviceValid bool, serviceReason string, serviceMessage string,
	deploymentValid bool, deploymentReason string, deploymentMessage string) error {

	// Get A Secret Logger (With The Valid Service/Deployment State
	logger := util.SecretLogger(r.logger, secret).With(zap.Bool("Service", serviceValid), zap.Bool("Deployment", deploymentValid))

	// Create Selector With Requirement For KafkaSecret Labels With Value Of Specified Secret Name
	selector := labels.NewSelector()
	requirement, err := labels.NewRequirement(constants.KafkaSecretLabel, selection.Equals, []string{secret.Name})
	if err != nil {
		logger.Error("Failed To Create Selector Requirement For Kafka Secret Label", zap.Error(err)) // Should Never Happen
		return err
	}
	selector.Add(*requirement)

	// List The KafkaChannels Which Match The Selector (All Namespaces)
	kafkaChannels, err := r.kafkachannelLister.List(selector)
	if err != nil {
		logger.Error("Failed To List KafkaChannels For Kafka Secret", zap.Error(err))
		return err
	}

	// Update All The KafkaChannels Status As Specified (Process All Regardless Of Error)
	statusUpdateErrors := false
	for _, kafkaChannel := range kafkaChannels {
		if kafkaChannel != nil {
			err := r.updateKafkaChannelStatus(kafkaChannel, serviceValid, serviceReason, serviceMessage, deploymentValid, deploymentReason, deploymentMessage)
			if err != nil {
				logger.Error("Failed To Update KafkaChannel Status", zap.Error(err))
				statusUpdateErrors = true
			}
		}
	}

	// Return Status Update Error
	if statusUpdateErrors {
		return fmt.Errorf("failed to update Status of one or more KafkaChannels")
	} else {
		return nil
	}
}

// Update A Single KafkaChannel's Status To Reflect The Specified Channel Service/Deployment State
func (r *Reconciler) updateKafkaChannelStatus(originalChannel *kafkav1alpha1.KafkaChannel,
	serviceValid bool, serviceReason string, serviceMessage string,
	deploymentValid bool, deploymentReason string, deploymentMessage string) error {

	// Get A KafkaChannel Logger
	logger := util.ChannelLogger(r.logger, originalChannel)

	// Update The KafkaChannel (Retry On Conflict - KafkaChannel Controller Will Also Be Updating KafkaChannel Status)
	return reconciler.RetryUpdateConflicts(func(attempts int) error {

		var err error

		// After First Attempt - Reload The Original KafkaChannel From K8S
		if attempts > 0 {
			originalChannel, err = r.kafkachannelLister.KafkaChannels(originalChannel.Namespace).Get(originalChannel.Name)
			if err != nil {
				logger.Error("Failed To Reload KafkaChannel For Status Update", zap.Error(err))
				return err
			}
		}

		// Clone The KafkaChannel So As Not To Perturb Informers Copy
		updatedChannel := originalChannel.DeepCopy()

		// Update Service Status Based On Specified State
		if serviceValid {
			updatedChannel.Status.MarkServiceTrue()
		} else {
			updatedChannel.Status.MarkServiceFailed(serviceReason, serviceMessage)
		}

		//
		// Update Deployment Status Based On Specified State
		//
		// TODO - As part of the conversion to the eventing-contrib KafkaChannel CRD and its associated
		//        Status, we've not yet implemented Endpoint tracking.  Until this is done we'll track
		//        the Deployments As Endpoints (since they will result in the Endpoints being up anyway).
		//
		if deploymentValid {
			updatedChannel.Status.MarkEndpointsTrue()
		} else {
			updatedChannel.Status.MarkEndpointsFailed(deploymentReason, deploymentMessage)
		}

		// If The KafkaChannel Status Changed
		if !equality.Semantic.DeepEqual(originalChannel.Status, updatedChannel.Status) {

			// Then Attempt To Update The KafkaChannel Status
			_, err = r.kafkaChannelClient.MessagingV1alpha1().KafkaChannels(updatedChannel.Namespace).UpdateStatus(updatedChannel)
			if err != nil {
				logger.Error("Failed To Update KafkaChannel Status", zap.Error(err))
				return err
			} else {
				logger.Info("Successfully Updated KafkaChannel Status")
				return nil
			}

		} else {

			// Otherwise No Change To Status - Return Success
			logger.Info("Successfully Verified KafkaChannel Status")
			return nil
		}
	})
}
