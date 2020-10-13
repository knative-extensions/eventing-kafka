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

	"knative.dev/pkg/system"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"

	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
)

func (r *Reconciler) reconcileMTReceiveAdapter(ctx context.Context, src *v1beta1.KafkaSource, sinkURI *apis.URL) (*appsv1.Deployment, error) {
	// TODO: patch envvars
	return r.KubeClientSet.AppsV1().Deployments(system.Namespace()).Get(ctx, mtadapterName, metav1.GetOptions{})
}
