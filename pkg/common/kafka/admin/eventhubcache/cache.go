package eventhubcache

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"knative.dev/eventing-kafka/pkg/common/kafka/admin/util"
	"knative.dev/eventing-kafka/pkg/common/kafka/constants"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
)

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   This package implements a cache of the state of Azure EventHub Namespaces.  This is necessary to facilitate the
   usage of the Azure Namespace grouping of EventHubs (which each have their own Connection String / Credentials).
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

// Define An Interface For The EventHub Cache
type CacheInterface interface {
	Update(ctx context.Context) error
	AddEventHub(ctx context.Context, eventhub string, namespace *Namespace)
	RemoveEventHub(ctx context.Context, eventhub string)
	GetNamespace(eventhub string) *Namespace
	GetLeastPopulatedNamespace() *Namespace
}

// Verify The Cache Struct Implements The Interface
var _ CacheInterface = &Cache{}

// Azure EventHubs Cache Struct
type Cache struct {
	logger       *zap.Logger
	k8sClient    kubernetes.Interface
	k8sNamespace string
	namespaceMap map[string]*Namespace // Map Of The Azure Namespace Name To Namespace Struct
	eventhubMap  map[string]*Namespace // Maps The Azure EventHub Name To It's Namespace Struct
}

// Azure EventHubs Cache Constructor
func NewCache(ctx context.Context, k8sNamespace string) CacheInterface {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx).Desugar()

	// Get The K8S Client From The Context
	k8sClient := kubeclient.Get(ctx)

	// Create & Return A New Cache
	return &Cache{
		logger:       logger,
		k8sClient:    k8sClient,
		k8sNamespace: k8sNamespace,
		namespaceMap: make(map[string]*Namespace),
		eventhubMap:  make(map[string]*Namespace),
	}
}

// Update The Cache From K8S & Azure
func (c *Cache) Update(ctx context.Context) error {

	// Get A List Of The Kafka Secrets From The K8S Namespace
	kafkaSecrets, err := util.GetKafkaSecrets(c.k8sClient, c.k8sNamespace)
	if err != nil {
		c.logger.Error("Failed To Get Kafka Secrets", zap.String("Namespace", c.k8sNamespace), zap.Error(err))
		return err
	}

	// Loop Over The Secrets Populating The Cache
	for _, kafkaSecret := range kafkaSecrets.Items {

		// Validate Secret Data
		if !c.validateKafkaSecret(&kafkaSecret) {
			err = errors.New("invalid Kafka Secret found")
			c.logger.Error("Found Invalid Kafka Secret", zap.Any("Kafka Secret", kafkaSecret), zap.Error(err))
			return err
		}

		// Create A New Namespace From The Secret
		namespace, err := NewNamespaceFromKafkaSecret(c.logger, &kafkaSecret)
		if err != nil {
			c.logger.Error("Failed To Get Namespace From Kafka Secret", zap.Any("Kafka Secret", kafkaSecret), zap.Error(err))
			return err
		}

		// Add The Namespace To The Namespace Map
		c.namespaceMap[namespace.Name] = namespace

		// List The EventHubs For The Namespace
		eventHubs, err := namespace.HubManager.List(ctx)
		if err != nil {
			c.logger.Error("Failed To List EventHubs In Namespace", zap.String("Namespace", namespace.Name), zap.Error(err))
			return err
		}

		// Loop Over The EventHubs For The Namespace
		for _, eventHub := range eventHubs {

			// Add The EventHub To The Namespace & Increment The Namespace EventHub Count
			c.eventhubMap[eventHub.Name] = namespace
			namespace.Count = namespace.Count + 1
		}
	}

	// Log Some Basic Cache Information
	c.logger.Info("Updating EventHub Cache",
		zap.Any("Namespaces", c.getNamespaceNames()),
		zap.Any("EventHubs", c.getEventHubNames()),
	)

	// Return Success
	return nil
}

// Add The Specified EventHub / Namespace To The Cache
func (c *Cache) AddEventHub(ctx context.Context, eventhub string, namespace *Namespace) {
	if namespace != nil {
		namespace.Count = namespace.Count + 1
		c.eventhubMap[eventhub] = namespace
	}
}

// Remove The Specified EventHub / Namespace From The Cache
func (c *Cache) RemoveEventHub(ctx context.Context, eventhub string) {
	namespace := c.GetNamespace(eventhub)
	if namespace != nil && namespace.Count > 0 {
		namespace.Count = namespace.Count - 1
	}
	delete(c.eventhubMap, eventhub)
}

// Get The Namespace Associated With The Specified EventHub (Topic) Name
func (c *Cache) GetNamespace(eventhub string) *Namespace {
	return c.eventhubMap[eventhub]
}

// Get The Namespace With The Least Number Of EventHubs
func (c *Cache) GetLeastPopulatedNamespace() *Namespace {

	// Track The Least Populated Namespace
	var leastPopulatedNamespace *Namespace

	// Loop Over The Namespaces In The Map
	for _, namespace := range c.namespaceMap {

		// Skip Any Invalid Data (Precautionary - Shouldn't Happen)
		if namespace == nil {
			continue
		}

		// Initialize The Least Populated Namespace If Not Set
		if leastPopulatedNamespace == nil {
			leastPopulatedNamespace = namespace
			continue
		}

		// Stop Looking If We Have An Empty Namespace (Can't get any less populated than that ; )
		if leastPopulatedNamespace.Count == 0 {
			break
		}

		// Update The Least Populated Namespace If Current Namespace Has Fewer EventHubs
		if namespace.Count < leastPopulatedNamespace.Count {
			leastPopulatedNamespace = namespace
		}
	}

	// Log The Least Populated Namespace
	if leastPopulatedNamespace != nil {
		c.logger.Info("Least Populated Namespace Lookup",
			zap.String("Namespace", leastPopulatedNamespace.Name),
			zap.Int("Count", leastPopulatedNamespace.Count),
		)
	} else {
		c.logger.Warn("No Azure EventHub Namespaces In Cache!")
	}

	// Return The Least Populated Namespace
	return leastPopulatedNamespace
}

// Utility Function For Validating Kafka Secret
func (c *Cache) validateKafkaSecret(secret *corev1.Secret) bool {

	// Assume Invalid Until Proven Otherwise
	valid := false

	// Validate The Kafka Secret
	if secret != nil {

		// Extract The Relevant Data From The Kafka Secret
		brokers := string(secret.Data[constants.KafkaSecretKeyBrokers])
		username := string(secret.Data[constants.KafkaSecretKeyUsername])
		password := string(secret.Data[constants.KafkaSecretKeyPassword])
		namespace := string(secret.Data[constants.KafkaSecretKeyNamespace])

		// Validate Kafka Secret Data
		if len(brokers) > 0 && len(username) > 0 && len(password) > 0 && len(namespace) > 0 {

			// Mark Kafka Secret As Valid
			valid = true

		} else {

			// Invalid Kafka Secret - Log State
			pwdString := ""
			if len(password) > 0 {
				pwdString = "********"
			}
			c.logger.Error("Kafka Secret Contains Invalid Data",
				zap.String("Name", secret.Name),
				zap.String("Brokers", brokers),
				zap.String("Username", username),
				zap.String("Password", pwdString),
				zap.String("Namespace", namespace))
		}
	}

	// Return Kafka Secret Validity
	return valid
}

// Utility Function For Getting The Namespace "Name (Count)" As A []string
func (c *Cache) getNamespaceNames() []string {
	namespaceNames := make([]string, len(c.namespaceMap))
	for namespaceName, namespace := range c.namespaceMap {
		namespaceNames = append(namespaceNames, fmt.Sprintf("%s (%d)", namespaceName, namespace.Count))
	}
	return namespaceNames
}

// Utility Function For Getting The "EventHub Name -> Namespace Name" As A []string
func (c *Cache) getEventHubNames() []string {
	eventHubNames := make([]string, len(c.eventhubMap))
	for eventHubName, namespace := range c.eventhubMap {
		eventHubNames = append(eventHubNames, fmt.Sprintf("%s -> %s", eventHubName, namespace.Name))
	}
	return eventHubNames
}
