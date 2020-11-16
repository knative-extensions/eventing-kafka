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
	"errors"
	"go.uber.org/zap"
	"regexp"
)

// RegExp For Selecting ServiceBus SubString From EventHubs ConnectionString
var ServiceBusRegExp = *regexp.MustCompile(`^Endpoint=sb://(.*)/;.*$`)

// Azure EventHubs Namespace Struct
type Namespace struct {
	Name             string
	ConnectionString string
	HubManager       HubManagerInterface
	Count            int
}

// Namespace Complete Argument Constructor
func NewNamespace(logger *zap.Logger, connectionString string) (*Namespace, error) {

	serviceBus, err := parseServiceBusFromConnectionString(connectionString)
	if err != nil {
		logger.Error("Failed To Parse ServiceBus From EventHub ConnectionString", zap.Error(err))
		return nil, err
	}

	// Create A New HubManager For The Specified ConnectionString
	hubManager, err := NewHubManagerFromConnectionStringWrapper(connectionString)
	if err != nil {
		logger.Error("Failed To Create New HubManager For Azure EventHubs Namespace", zap.Error(err))
		return nil, err
	}

	// Create & Return A New Namespace With Specified Configuration & Initialized HubManager
	return &Namespace{
		Name:             serviceBus, // Using ServiceBus As Name Since Not Secure
		ConnectionString: connectionString,
		HubManager:       hubManager,
		Count:            0,
	}, nil
}

//
// Parse The ServiceBus SubString From The Specified ConnectionString
//
// EventHub ConnectionStrings are expected to look like this...
//     Endpoint=sb://{eventhub-namespace>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<encrypted-shared-access-key-for-namespace>
//
func parseServiceBusFromConnectionString(connectionString string) (string, error) {

	// The ServiceBus SubString To Be Parsed
	serviceBus := ""

	// Validate The Specified ConnectionString
	if len(connectionString) > 0 {

		// Parse The ServiceBus SubString
		subStrings := ServiceBusRegExp.FindStringSubmatch(connectionString)
		if len(subStrings) == 2 {
			serviceBus = subStrings[1] // First Result is regexp match, second is sub-string
		}
	}

	// Return The Parse ServiceBus SubString
	if len(serviceBus) > 0 {
		return serviceBus, nil
	} else {
		return serviceBus, errors.New("no ServiceBus detected in empty or invalid EventHub ConnectionString")
	}
}
