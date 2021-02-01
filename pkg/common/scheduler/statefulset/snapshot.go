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

import "go.uber.org/zap"

// Snapshot provides some information about the current scheduling of all vpods
type Snapshot struct {
	// free tracks the free capacity of each pod.
	free map[string]int32
}

func (s *StatefulSetScheduler) snapshot(logger *zap.SugaredLogger) (*Snapshot, error) {
	vpods, err := s.vpodLister()
	if err != nil {
		return nil, err
	}

	free := make(map[string]int32)
	for i := int32(0); i < s.replicas; i++ {
		podName := podNameFromOrdinal(s.statefulSetName, i)
		free[podName] = s.capacity
	}

	for _, vpod := range vpods {
		ps := vpod.GetPlacements()

		for i := 0; i < len(ps); i++ {
			podName := ps[i].PodName
			vreplicas := ps[i].VReplicas

			free[podName] -= vreplicas

			if free[podName] < 0 {
				// Over committed. The autoscaler will fix it.
				logger.Infow("pod is overcommitted", zap.String("podName", podName))
			}
		}
	}
	return &Snapshot{free: free}, nil
}
