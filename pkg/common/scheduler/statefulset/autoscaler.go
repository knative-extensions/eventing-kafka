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

package statefulset

import (
	"context"
	"math"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *StatefulSetScheduler) autoscale(ctx context.Context) {
	// Regularly compute the average number of replicas per pods.
	// When the average goes above a certain ratio, scale up
	// otherwise scale down.

	for {
		func() {
			s.lock.Lock()
			defer s.lock.Unlock()

			// Refresh state as the scheduler does not receive deletion notifications.
			snapshot, err := s.snapshot(s.logger)
			if err != nil {
				s.logger.Info("error while refreshing scheduler state (will retry)", zap.Error(err))
				return
			}

			s.logger.Infow("checking adapter capacity",
				zap.Int32("free", s.freeCapacity(snapshot)),
				zap.Int32("used", s.usedCapacity(snapshot)),
				zap.Int32("pending", s.pendingVReplicas()),
				zap.Int32("replicas", s.replicas),
				zap.Int32("last ordinal", snapshot.lastOrdinal))

			var ratio float64
			if s.replicas == 0 {
				if s.pendingVReplicas() > 0 {
					ratio = 1.0
				} else {
					ratio = 0.5
				}
			} else {
				ratio = s.avgUsedCapacityPerPod(snapshot) / float64(s.capacity)
			}

			if ratio > 0.7 || ratio < 0.3 {
				// Scale up when capacity is above 80% (TODO: configurable)
				// Scale down when capacity is below 30% (TODO: configurable)
				s.logger.Infow("autoscaling statefulset", zap.Int("avg used capacity per pod", int(ratio*100.0)))

				scale, err := s.statefulSetClient.GetScale(ctx, s.statefulSetName, metav1.GetOptions{})
				if err != nil {
					// skip a beat
					s.logger.Infow("failed to get scale subresource", zap.Error(err))
					return
				}

				// Desired ratio is 0.5 (TODO: configurable)
				new := int32(math.Ceil(float64(s.usedCapacity(snapshot)+s.pendingVReplicas()) / (float64(s.capacity) * 0.5)))

				// Make sure not to scale down past the last pod with placed vpods
				//if new < snapshot.lastOrdinal+1 {
				//	new = snapshot.lastOrdinal + 1
				//}

				if new > scale.Spec.Replicas {
					scale.Spec.Replicas = new
					s.logger.Infow("updating adapter replicas", zap.Int32("replicas", scale.Spec.Replicas))

					_, err = s.statefulSetClient.UpdateScale(ctx, s.statefulSetName, scale, metav1.UpdateOptions{})
					if err != nil {
						s.logger.Errorw("updating scale subresource failed", zap.Error(err))
					}
				}
			}
		}()

		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second): // TODO: configurable refresh period
		}
	}
}

// freeCapacity returns the free capacity across all pods
func (s *StatefulSetScheduler) freeCapacity(snapshot *Snapshot) int32 {
	t := int32(0)
	for i := int32(0); i < s.replicas; i++ {
		t += snapshot.free[podNameFromOrdinal(s.statefulSetName, i)]
	}
	return t
}

// usedCapacity returns the used capacity across all pods
func (s *StatefulSetScheduler) usedCapacity(snapshot *Snapshot) int32 {
	return s.capacity*s.replicas - s.freeCapacity(snapshot)
}

// pendingReplicas returns the total number of virtual replicas
// that haven't been scheduled yet
func (s *StatefulSetScheduler) pendingVReplicas() int32 {
	t := int32(0)
	for _, v := range s.pending {
		t += v
	}
	return t
}

// avgUsedCapacityPerPod returns the average used capacity per replica
func (s *StatefulSetScheduler) avgUsedCapacityPerPod(snapshot *Snapshot) float64 {
	return float64(s.usedCapacity(snapshot)) / float64(s.replicas)
}
