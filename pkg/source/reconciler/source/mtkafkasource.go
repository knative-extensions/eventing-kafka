/*
Copyright 2019 The Knative Authors

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

package source

import (
	"context"

	"k8s.io/apimachinery/pkg/labels"
	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
)

func (r *Reconciler) reconcileMTReceiveAdapter(ctx context.Context, src *v1beta1.KafkaSource) error {
	placements, err := r.scheduler.Schedule(src)
	if err != nil {
		// TODO: consider allowing partial placement

		src.Status.MarkNotScheduled("Unschedulable", err.Error())
		return err // retrying...
	}
	src.Status.Placement = placements
	src.Status.MarkScheduled()

	// TODO: patch envvars
	//return r.KubeClientSet.AppsV1().DaemonSets(system.Namespace()).Get(ctx, mtadapterName, metav1.GetOptions{})

	return nil
}

func (r *Reconciler) schedulableLister() ([]scheduler.Schedulable, error) {
	sources, err := r.kafkaLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	schedulables := make([]scheduler.Schedulable, len(sources))
	for i := 0; i < len(sources); i++ {
		schedulables[i] = sources[i]
	}
	return schedulables, nil
}
