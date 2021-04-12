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

package resources

import (
	"fmt"
	"strconv"

	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/kmeta"

	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
)

type ReceiveAdapterArgs struct {
	Image          string
	Source         *v1beta1.KafkaSource
	Labels         map[string]string
	SinkURI        string
	AdditionalEnvs []corev1.EnvVar
}

func MakeReceiveAdapter(args *ReceiveAdapterArgs) *v1.Deployment {
	env := append([]corev1.EnvVar{{
		Name:  "KAFKA_NET_SASL_ENABLE",
		Value: strconv.FormatBool(args.Source.Spec.Net.SASL.Enable),
	}, {
		Name:  "KAFKA_NET_TLS_ENABLE",
		Value: strconv.FormatBool(args.Source.Spec.Net.TLS.Enable),
	}, {
		Name:  "K_SINK",
		Value: args.SinkURI,
	}, {
		Name:  "NAME",
		Value: args.Source.Name,
	}, {
		Name:  "NAMESPACE",
		Value: args.Source.Namespace,
	}}, args.AdditionalEnvs...)

	env = appendEnvFromSecretKeyRef(env, "KAFKA_NET_SASL_USER", args.Source.Spec.Net.SASL.User.SecretKeyRef)
	env = appendEnvFromSecretKeyRef(env, "KAFKA_NET_SASL_PASSWORD", args.Source.Spec.Net.SASL.Password.SecretKeyRef)
	env = appendEnvFromSecretKeyRef(env, "KAFKA_NET_SASL_TYPE", args.Source.Spec.Net.SASL.Type.SecretKeyRef)
	env = appendEnvFromSecretKeyRef(env, "KAFKA_NET_TLS_CERT", args.Source.Spec.Net.TLS.Cert.SecretKeyRef)
	env = appendEnvFromSecretKeyRef(env, "KAFKA_NET_TLS_KEY", args.Source.Spec.Net.TLS.Key.SecretKeyRef)
	env = appendEnvFromSecretKeyRef(env, "KAFKA_NET_TLS_CA_CERT", args.Source.Spec.Net.TLS.CACert.SecretKeyRef)

	return &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kmeta.ChildName(fmt.Sprintf("kafkasource-%s-", args.Source.Name), string(args.Source.GetUID())),
			Namespace: args.Source.Namespace,
			Labels:    args.Labels,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(args.Source),
			},
		},
		Spec: v1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: args.Labels,
			},
			Replicas: args.Source.Spec.Consumers,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"sidecar.istio.io/inject": "true",
					},
					Labels: args.Labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "receive-adapter",
							Image: args.Image,
							Env:   env,
							Ports: []corev1.ContainerPort{
								{Name: "metrics", ContainerPort: 9090},
								{Name: "profiling", ContainerPort: 8008},
								{Name: "control", ContainerPort: 9000},
							},
						},
					},
				},
			},
		},
	}
}

// appendEnvFromSecretKeyRef returns env with an EnvVar appended
// setting key to the secret and key described by ref.
// If ref is nil, env is returned unchanged.
func appendEnvFromSecretKeyRef(env []corev1.EnvVar, key string, ref *corev1.SecretKeySelector) []corev1.EnvVar {
	if ref == nil {
		return env
	}

	env = append(env, corev1.EnvVar{
		Name: key,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: ref,
		},
	})

	return env
}
