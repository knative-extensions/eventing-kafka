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

package kafka

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/Shopify/sarama"
	"knative.dev/pkg/logging"
)

var mutex sync.Mutex

type ClusterAdminFactory func() (sarama.ClusterAdmin, error)

type AdminClient interface {
	// ListConsumerGroups Lists the consumer groups
	ListConsumerGroups() ([]string, error)
}

// AdminClientManager manages a ClusterAdmin connection and recreates one when needed
// it is made to overcome https://github.com/Shopify/sarama/issues/1162
type AdminClientManager struct {
	logger    *zap.SugaredLogger
	caFactory ClusterAdminFactory
	ca        sarama.ClusterAdmin
}

func NewAdminClient(ctx context.Context, caFactory ClusterAdminFactory) (AdminClient, error) {
	logger := logging.FromContext(ctx)
	logger.Info("Creating a new AdminClient")
	kafkaClusterAdmin, err := caFactory()
	if err != nil {
		logger.Error("error while creating ClusterAdmin", err)
		return &AdminClientManager{}, err
	}
	return &AdminClientManager{
		logger:    logger,
		caFactory: caFactory,
		ca:        kafkaClusterAdmin,
	}, nil
}

func (c *AdminClientManager) ListConsumerGroups() ([]string, error) {
	c.logger.Info("Attempting to list consumer group")
	mutex.Lock()
	defer mutex.Unlock()
	r := 0
	// This gives us around ~13min of exponential backoff
	max := 13
	cgsMap, err := c.ca.ListConsumerGroups()
	for err != nil && r <= max {
		// presuming we can reconnect
		t := int(math.Pow(2, float64(r)) * 100)
		d := time.Duration(t) * time.Millisecond
		c.logger.Error("listing consumer group failed. Refreshing the ClusterAdmin and retrying.",
			zap.Error(err),
			zap.Duration("retry after", d),
			zap.Int("Retry attempt", r),
			zap.Int("Max retries", max),
		)
		time.Sleep(d)
		c.ca, err = c.caFactory()
		r += 1
		if err != nil {
			// skip this attempt
			continue
		}
		cgsMap, err = c.ca.ListConsumerGroups()
	}

	if r > max {
		return []string{}, fmt.Errorf("failed to refresh the culster admin and retry: %v", err)
	}

	return keys(cgsMap), nil
}

func keys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
