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

package testing

import "knative.dev/eventing-kafka/pkg/common/scheduler"

type VPodClient struct {
	store  *VPodStore
	lister scheduler.VPodLister
}

func NewVPodClient() *VPodClient {
	store := newVPodStore()
	return &VPodClient{
		store:  store,
		lister: newVPodLister(store),
	}
}

func (s *VPodClient) Create(ns, name string, vreplicas int32) scheduler.VPod {
	vpod := newVPod(ns, name, vreplicas)
	s.store.vpods = append(s.store.vpods, vpod)
	return vpod
}

func (s *VPodClient) List() ([]scheduler.VPod, error) {
	return s.lister()
}
