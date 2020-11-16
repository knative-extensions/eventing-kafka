/*
Copyright 2020 The Knative Authors

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

package eventhubcache

import (
	"context"
	"fmt"

	"go.uber.org/zap"
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
	namespaceMap map[string]*Namespace // Map Of The Azure Namespace Name To Namespace Struct
	eventhubMap  map[string]*Namespace // Maps The Azure EventHub Name To It's Namespace Struct
}

// Azure EventHubs Cache Constructor
func NewCache(ctx context.Context, connectionStrings ...string) CacheInterface {

	// Get The Logger From The Context
	logger := logging.FromContext(ctx).Desugar()

	// Populate NamespaceMap
	namespaceMap := make(map[string]*Namespace, len(connectionStrings))
	for _, connectionString := range connectionStrings {
		namespace, err := NewNamespace(logger, connectionString)
		if namespace == nil || err != nil {
			logger.Error("Failed To Determine EventHub Namespace For ConnectionString - Skipping", zap.Error(err))
		} else {
			logger.Info("Tracking EventHub Namespace From ConnectionString", zap.String("Name", namespace.Name))
			namespaceMap[namespace.Name] = namespace
		}
	}

	// Create & Return A New Cache
	return &Cache{
		logger:       logger,
		namespaceMap: namespaceMap,
		eventhubMap:  make(map[string]*Namespace),
	}
}

// Update The Cache From K8S & Azure
func (c *Cache) Update(ctx context.Context) error {

	// Clear The EventHub Map
	c.eventhubMap = make(map[string]*Namespace)

	// Loop Over The Secrets Populating The Cache
	for _, namespace := range c.namespaceMap {

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
func (c *Cache) AddEventHub(_ context.Context, eventhub string, namespace *Namespace) {
	if namespace != nil {
		namespace.Count = namespace.Count + 1
		c.eventhubMap[eventhub] = namespace
	}
}

// Remove The Specified EventHub / Namespace From The Cache
func (c *Cache) RemoveEventHub(_ context.Context, eventhub string) {
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
