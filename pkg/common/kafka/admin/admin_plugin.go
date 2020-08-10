package admin

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka/pkg/common/env"
	adminutil "knative.dev/eventing-kafka/pkg/common/kafka/admin/util"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	"os"
	"plugin"
	"strings"
)

// Constants
const (

	// Plugin Must Expose Function By This Name With Signature Of...  func(*zap.Logger, *corev1.Secret) (AdminClientInterface, error)
	PluginAdminClientConstructorFunctionName = "NewPluginAdminClient"
)

// Create A New Plugin Kafka AdminClient Based On The Kafka Secret In The Specified K8S Namespace
func NewPluginAdminClient(ctx context.Context, namespace string) (AdminClientInterface, error) {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx).Desugar()

	// Get The K8S Client From The Context
	k8sClient := kubeclient.Get(ctx)

	// Get A List Of The Kafka Secrets
	kafkaSecrets, err := adminutil.GetKafkaSecrets(k8sClient, namespace)
	if err != nil {
		logger.Error("Failed To Get Kafka Authentication Secrets", zap.Error(err))
		return nil, err
	}

	// Currently Only Support One Kafka Secret - Invalid AdminClient For All Other Cases!
	var kafkaSecret corev1.Secret
	if len(kafkaSecrets.Items) != 1 {
		logger.Warn(fmt.Sprintf("Expected 1 Kafka Secret But Found %d - Kafka AdminClient Will Not Be Functional!", len(kafkaSecrets.Items)))
		return nil, nil
	} else {
		logger.Info("Found 1 Kafka Secret", zap.String("Secret", kafkaSecrets.Items[0].Name))
		kafkaSecret = kafkaSecrets.Items[0]
	}

	// Validate Secret Data
	if !adminutil.ValidateKafkaSecret(logger, &kafkaSecret) {
		err = errors.New("invalid Kafka Secret found")
		return nil, err
	}

	// Get The AdminClient Plugin Path
	adminClientPluginPath := getAdminClientPluginPath()

	// Open The Specified Plugin
	adminClientPlugin, err := plugin.Open(adminClientPluginPath)
	if err != nil {
		logger.Error("Failed To Open Kafka AdminClient Plugin", zap.Error(err))
		return nil, err
	} else if adminClientPlugin == nil {
		logger.Warn("Opened Nil Kafka AdminClient Plugin")
		return nil, errors.New("opened nil plugin")
	}

	// Lookup The Expected Plugin Constructor Function
	adminClientConstructorFunction, err := adminClientPlugin.Lookup(PluginAdminClientConstructorFunctionName)
	if err != nil {
		logger.Error("Failed To Lookup Expected Plugin AdminClient Constructor Function", zap.String("Function", PluginAdminClientConstructorFunctionName), zap.Error(err))
		return nil, err
	} else if adminClientConstructorFunction == nil {
		logger.Warn("Expected AdminClient Constructor Function Not Found In Plugin", zap.String("Name", PluginAdminClientConstructorFunctionName))
		return nil, errors.New(fmt.Sprintf("expected plugin adminclient contrstuctor function '%s' not found in plugin", PluginAdminClientConstructorFunctionName))
	}

	// Type Check The Plugin's AdminClient Constructor Function Signature
	typedAdminClientConstructorFunction, ok := adminClientConstructorFunction.(func(*zap.Logger, *corev1.Secret) (AdminClientInterface, error))
	if !ok {
		logger.Warn("Plugin Contains Kafka AdminClient Constructor Function With Incorrect Signature")
		return nil, errors.New(fmt.Sprintf("plugin kafka adminclient constructor function '%s' has incorrect signature", PluginAdminClientConstructorFunctionName))
	}

	// Call The Type Checked AdminClient Constructor Function
	return typedAdminClientConstructorFunction(logger, &kafkaSecret)
}

// Get The Absolute Kafka AdminClient Plugin Path ($KO_DATA_PATH/kafka-admin-client.so)
func getAdminClientPluginPath() string {
	koDataPath := os.Getenv(env.KoDataPathEnvVarKey)
	if len(koDataPath) <= 0 {
		koDataPath = env.KoDataPathDefaultValue
	}
	koDataPath = strings.TrimSuffix(koDataPath, "/")
	return koDataPath + "/" + constants.PluginAdminClientName
}
