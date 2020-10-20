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
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
)

// Azure EventHubs Namespace Struct
type Namespace struct {
	Name       string
	Username   string
	Password   string
	Secret     string
	HubManager HubManagerInterface
	Count      int
}

// Namespace Complete Argument Constructor
func NewNamespace(logger *zap.Logger, name string, username string, password string, secret string, count int) (*Namespace, error) {

	// Create A New HubManager For The Specified ConnectionString (Password)
	hubManager, err := NewHubManagerFromConnectionStringWrapper(password)
	if err != nil {
		logger.Error("Failed To Create New HubManager For Azure EventHubs Namespace", zap.Error(err))
		return nil, err
	}

	// Create & Return A New Namespace With Specified Configuration & Initialized HubManager
	return &Namespace{
		Name:       name,
		Username:   username,
		Password:   password,
		Secret:     secret,
		HubManager: hubManager,
		Count:      count,
	}, nil
}

// Namespace Secret Constructor
func NewNamespaceFromKafkaSecret(logger *zap.Logger, kafkaSecret *corev1.Secret) (*Namespace, error) {

	// Extract The Relevant Data From The Kafka Secret
	data := kafkaSecret.Data
	username := string(data[constants.KafkaSecretKeyUsername])
	password := string(data[constants.KafkaSecretKeyPassword])
	namespace := string(data[constants.KafkaSecretKeyNamespace])
	secret := kafkaSecret.Name

	// Create A New Namespace From The Secret
	return NewNamespace(logger, namespace, username, password, secret, 0)
}
