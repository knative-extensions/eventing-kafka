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

package resources

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
)

type DispatcherServiceArgs struct {
	ServiceAnnotations map[string]string
	ServiceLabels      map[string]string
}

// MakeDispatcherService creates the Kafka Dispatcher Service
func MakeDispatcherService(namespace string, args DispatcherServiceArgs) *corev1.Service {

	// Create A New Dispatcher Service
	service := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      dispatcherName,
			Namespace: namespace,
			Labels:    dispatcherLabels,
		},
		Spec: corev1.ServiceSpec{
			Selector: dispatcherLabels,
			Ports: []corev1.ServicePort{
				{
					Name:       "http-dispatcher",
					Protocol:   corev1.ProtocolTCP,
					Port:       80,
					TargetPort: intstr.IntOrString{IntVal: 8080},
				},
			},
		},
	}

	// Update The Dispatcher Service's Annotations & Labels With Custom Config Values
	service.Annotations = commonconfig.JoinStringMaps(service.Annotations, args.ServiceAnnotations)
	service.Labels = commonconfig.JoinStringMaps(service.Labels, args.ServiceLabels)

	// Return The Dispatcher Service
	return service
}
