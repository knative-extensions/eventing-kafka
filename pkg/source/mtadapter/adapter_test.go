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

package mtadapter

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	pkgtesting "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/source"

	"knative.dev/eventing/pkg/adapter/v2"
	adaptertest "knative.dev/eventing/pkg/adapter/v2/test"
	"knative.dev/eventing/pkg/kncloudevents"

	bindingsv1beta1 "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
)

var (
	runningAdapterChan  = make(chan *sampleAdapter)
	stoppingAdapterChan = make(chan *sampleAdapter)
)

const (
	podName = "sample-podname"
)

func TestUpdateRemoveSources(t *testing.T) {
	ctx, _ := pkgtesting.SetupFakeContext(t)
	ctx, cancelAdapter := context.WithCancel(ctx)

	env := &AdapterConfig{PodName: podName, MemoryLimit: "0"}
	ceClient := adaptertest.NewTestClient()

	adapter := newAdapter(ctx, env, ceClient, newSampleAdapter).(*Adapter)

	adapterStopped := make(chan bool)
	go func() {
		err := adapter.Start(ctx)
		if err != nil {
			t.Error("Unexpected error ", err)
		}
		adapterStopped <- true
	}()

	err := adapter.Update(ctx, &sourcesv1beta1.KafkaSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-ns",
		},
		Spec: sourcesv1beta1.KafkaSourceSpec{},
		Status: sourcesv1beta1.KafkaSourceStatus{
			Placeable: duckv1alpha1.Placeable{
				Placement: []duckv1alpha1.Placement{
					{PodName: podName, VReplicas: int32(1)},
				}},
		},
	})

	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}

	if _, ok := adapter.sources["test-ns/test-name"]; !ok {
		t.Error(`Expected adapter to contain "test-ns/test-name"`)
	}

	err = adapter.Update(ctx, &sourcesv1beta1.KafkaSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name-badBroker",
			Namespace: "test-ns",
		},
		Spec: sourcesv1beta1.KafkaSourceSpec{
			KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
				BootstrapServers: []string{"my-cluster-kafka-bootstrap-badBroker.kafka.svc:9092"},
			},
		},
		Status: sourcesv1beta1.KafkaSourceStatus{
			Placeable: duckv1alpha1.Placeable{
				Placement: []duckv1alpha1.Placement{
					{PodName: podName, VReplicas: int32(1)},
				}},
		},
	})

	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}

	if _, ok := adapter.sources["test-ns/test-name-badBroker"]; !ok {
		t.Error(`Expected adapter to contain "test-ns/test-name-badBroker"`)
	}

	select {
	case a := <-runningAdapterChan:
		if !a.running {
			t.Error("Expected adapter to be running")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("sub-adapter failed to start after 100 ms")
	}

	adapter.Remove(&sourcesv1beta1.KafkaSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-ns",
		},
		Spec:   sourcesv1beta1.KafkaSourceSpec{},
		Status: sourcesv1beta1.KafkaSourceStatus{},
	})

	if _, ok := adapter.sources["test-ns/test-name"]; ok {
		t.Error(`Expected adapter to not contain "test-ns/test-name"`)
	}

	adapter.Remove(&sourcesv1beta1.KafkaSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name-badBroker",
			Namespace: "test-ns",
		},
		Spec:   sourcesv1beta1.KafkaSourceSpec{},
		Status: sourcesv1beta1.KafkaSourceStatus{},
	})

	if _, ok := adapter.sources["test-ns/test-name-badBroker"]; ok {
		t.Error(`Expected adapter to not contain "test-ns/test-name-badBroker"`)
	}

	select {
	case a := <-stoppingAdapterChan:
		if a.running {
			t.Error("Expected adapter to not be running")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("sub-adapter failed to stop after 100 ms")
	}

	// Make sure the adapter is still running
	select {
	case <-ctx.Done():
		t.Error("Expected adapter to be running")
	default:
	}

	cancelAdapter()

	select {
	case <-adapterStopped:
	case <-time.After(2 * time.Second):
		t.Error("adapter failed to stop after 2 seconds")
	}
}

func TestSourceMTAdapter(t *testing.T) {
	testCases := map[string]struct {
		objects []runtime.Object
		wantErr bool
		source  sourcesv1beta1.KafkaSource
	}{
		"with sasl secret": {
			wantErr: false,
			objects: []runtime.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "mysecret",
						Namespace: "test-ns",
					},
					Data: map[string][]byte{
						"user1": []byte("fjwoia"),
					},
				},
			},
			source: sourcesv1beta1.KafkaSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-missing-sasl-secret",
					Namespace: "test-ns",
				},
				Spec: sourcesv1beta1.KafkaSourceSpec{
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
						Net: bindingsv1beta1.KafkaNetSpec{
							SASL: bindingsv1beta1.KafkaSASLSpec{
								User: bindingsv1beta1.SecretValueFromSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "mysecret",
										},
										Key: "user1",
									},
								},
							},
						},
					},
				},
				Status: sourcesv1beta1.KafkaSourceStatus{
					Placeable: duckv1alpha1.Placeable{
						Placement: []duckv1alpha1.Placement{
							{PodName: podName, VReplicas: int32(1)},
						}},
				},
			},
		},
		"missing sasl secret key": {
			wantErr: true,
			objects: []runtime.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "mysecret",
						Namespace: "test-ns",
					},
					Data: map[string][]byte{
						"user": []byte("fjwoia"),
					},
				},
			},
			source: sourcesv1beta1.KafkaSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-missing-sasl-secret",
					Namespace: "test-ns",
				},
				Spec: sourcesv1beta1.KafkaSourceSpec{
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
						Net: bindingsv1beta1.KafkaNetSpec{
							SASL: bindingsv1beta1.KafkaSASLSpec{
								User: bindingsv1beta1.SecretValueFromSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "mysecret",
										},
										Key: "user1",
									},
								},
							},
						},
					},
				},
				Status: sourcesv1beta1.KafkaSourceStatus{
					Placeable: duckv1alpha1.Placeable{
						Placement: []duckv1alpha1.Placement{
							{PodName: podName, VReplicas: int32(1)},
						}},
				},
			},
		},
		"missing sasl secret": {
			wantErr: true,
			source: sourcesv1beta1.KafkaSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-missing-sasl-secret",
					Namespace: "test-ns",
				},
				Spec: sourcesv1beta1.KafkaSourceSpec{
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
						Net: bindingsv1beta1.KafkaNetSpec{
							SASL: bindingsv1beta1.KafkaSASLSpec{
								User: bindingsv1beta1.SecretValueFromSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "bogus",
										},
									},
								},
							},
						},
					},
				},
				Status: sourcesv1beta1.KafkaSourceStatus{
					Placeable: duckv1alpha1.Placeable{
						Placement: []duckv1alpha1.Placement{
							{PodName: podName, VReplicas: int32(1)},
						}},
				},
			},
		},
		"missing tls secret": {
			wantErr: true,
			source: sourcesv1beta1.KafkaSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-missing-tls-secret",
					Namespace: "test-ns",
				},
				Spec: sourcesv1beta1.KafkaSourceSpec{
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
						Net: bindingsv1beta1.KafkaNetSpec{
							TLS: bindingsv1beta1.KafkaTLSSpec{
								Cert: bindingsv1beta1.SecretValueFromSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "bogus",
										},
									},
								},
							},
						},
					},
				},
				Status: sourcesv1beta1.KafkaSourceStatus{
					Placeable: duckv1alpha1.Placeable{
						Placement: []duckv1alpha1.Placement{
							{PodName: podName, VReplicas: int32(1)},
						}},
				},
			},
		},
	}
	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			ctx, _ := pkgtesting.SetupFakeContext(t)

			if tc.objects != nil {
				ctx, _ = fakekubeclient.With(ctx, tc.objects...)
			}

			ctx, cancelAdapter := context.WithCancel(ctx)

			env := &AdapterConfig{PodName: podName, MemoryLimit: "0"}
			ceClient := adaptertest.NewTestClient()

			adapter := newAdapter(ctx, env, ceClient, newSampleAdapter).(*Adapter)

			adapterStopped := make(chan bool)
			go func() {
				err := adapter.Start(ctx)
				if err != nil {
					t.Error("Unexpected error ", err)
				}
				adapterStopped <- true
			}()

			err := adapter.Update(ctx, &tc.source)
			if tc.wantErr && err == nil {
				t.Error("Error expected, got none")
			}

			if !tc.wantErr && err != nil {
				t.Errorf("No error expected, got %v", err)
			}

			cancelAdapter()

			select {
			case <-adapterStopped:
			case <-time.After(2 * time.Second):
				t.Error("adapter failed to stop after 2 seconds")
			}
		})
	}
}

type sampleAdapter struct {
	running bool
}

func newSampleAdapter(ctx context.Context, env adapter.EnvConfigAccessor, adapter *kncloudevents.HTTPMessageSender, reporter source.StatsReporter) adapter.MessageAdapter {
	return &sampleAdapter{}
}

func (d *sampleAdapter) Start(ctx context.Context) error {
	d.running = true
	runningAdapterChan <- d

	<-ctx.Done()

	d.running = false
	stoppingAdapterChan <- d
	return nil
}
