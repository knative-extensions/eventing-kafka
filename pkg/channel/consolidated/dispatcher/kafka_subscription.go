/*
Copyright 2021 The Knative Authors

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

package dispatcher

import (
	"sync"

	"go.uber.org/zap"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

type KafkaSubscription struct {
	logger *zap.SugaredLogger
	subs   []types.UID
	// readySubscriptionsLock must be used to synchronize access to channelReadySubscriptions
	readySubscriptionsLock    sync.RWMutex
	channelReadySubscriptions map[string]sets.Int32
}

// SetReady will mark the subid in the KafkaSubscription and call any registered callbacks
func (ks *KafkaSubscription) SetReady(subID types.UID, partition int32, ready bool) {
	ks.logger.Debugw("Setting subscription readiness", zap.Any("subscription", subID), zap.Bool("ready", ready))
	ks.readySubscriptionsLock.Lock()
	defer ks.readySubscriptionsLock.Unlock()
	if ready {
		if subs, ok := ks.channelReadySubscriptions[string(subID)]; ok {
			ks.logger.Debugw("Adding ready ready partition to cached subscription", zap.Any("subscription", subID), zap.Int32("partition", partition))
			subs.Insert(partition)
		} else {
			ks.logger.Debugw("Caching ready subscription", zap.Any("subscription", subID), zap.Int32("partition", partition))
			ks.channelReadySubscriptions[string(subID)] = sets.NewInt32(partition)
		}
	} else {
		if subs, ok := ks.channelReadySubscriptions[string(subID)]; ok {
			ks.logger.Debugw("Ejecting cached ready subscription", zap.Any("subscription", subID), zap.Int32("partition", partition))
			subs.Delete(partition)
			if subs.Len() == 0 {
				delete(ks.channelReadySubscriptions, string(subID))
			}
		}
	}
}
