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

package mtsource

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/labels"
	"strings"

	"k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing/pkg/reconciler/source"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka/pkg/client/clientset/versioned"
	reconcilerkafkasource "knative.dev/eventing-kafka/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"
	listers "knative.dev/eventing-kafka/pkg/client/listers/sources/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
)

const (
	component     = "kafkasource"
	mtadapterName = "kafkasource-mt-adapter"
)

type Reconciler struct {
	// KubeClientSet allows us to talk to the k8s for core APIs
	KubeClientSet kubernetes.Interface

	kafkaLister      listers.KafkaSourceLister
	deploymentLister appsv1listers.DeploymentLister

	kafkaClientSet versioned.Interface
	loggingContext context.Context

	sinkResolver *resolver.URIResolver

	configs source.ConfigAccessor

	scheduler scheduler.Scheduler
}

// Check that our Reconciler implements Interface
var _ reconcilerkafkasource.Interface = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, src *v1beta1.KafkaSource) pkgreconciler.Event {
	src.Status.InitializeConditions()

	if (src.Spec.Sink == duckv1.Destination{}) {
		src.Status.MarkNoSink("SinkMissing", "")
		return fmt.Errorf("spec.sink missing")
	}

	dest := src.Spec.Sink.DeepCopy()
	if dest.Ref != nil {
		// To call URIFromDestination(), dest.Ref must have a Namespace. If there is
		// no Namespace defined in dest.Ref, we will use the Namespace of the source
		// as the Namespace of dest.Ref.
		if dest.Ref.Namespace == "" {
			dest.Ref.Namespace = src.GetNamespace()
		}
	}
	sinkURI, err := r.sinkResolver.URIFromDestinationV1(ctx, *dest, src)
	if err != nil {
		src.Status.MarkNoSink("NotFound", "")
		return fmt.Errorf("getting sink URI: %v", err)
	}
	src.Status.MarkSink(sinkURI)

	if val, ok := src.GetLabels()[v1beta1.KafkaKeyTypeLabel]; ok {
		found := false
		for _, allowed := range v1beta1.KafkaKeyTypeAllowed {
			if allowed == val {
				found = true
			}
		}
		if !found {
			src.Status.MarkKeyTypeIncorrect("IncorrectKafkaKeyTypeLabel", "Invalid value for %s: %s. Allowed: %v", v1beta1.KafkaKeyTypeLabel, val, v1beta1.KafkaKeyTypeAllowed)
			logging.FromContext(ctx).Errorf("Invalid value for %s: %s. Allowed: %v", v1beta1.KafkaKeyTypeLabel, val, v1beta1.KafkaKeyTypeAllowed)
			return errors.New("IncorrectKafkaKeyTypeLabel")
		} else {
			src.Status.MarkKeyTypeCorrect()
		}
	}

	if err := r.reconcileMTReceiveAdapter(src); err != nil {
		return err
	}

	src.Status.CloudEventAttributes = r.createCloudEventAttributes(src)

	return nil
}

func (r *Reconciler) reconcileMTReceiveAdapter(src *v1beta1.KafkaSource) error {
	placements, err := r.scheduler.Schedule(src)

	// Update placements, even partial ones.
	if placements != nil {
		src.Status.Placement = placements
	}

	if err != nil {
		src.Status.MarkNotScheduled("Unschedulable", err.Error())
		return err // retrying...
	}
	src.Status.MarkScheduled()

	// TODO: patch envvars
	//return r.KubeClientSet.AppsV1().DaemonSets(system.Namespace()).Get(ctx, mtadapterName, metav1.GetOptions{})

	return nil
}

func (r *Reconciler) vpodLister() ([]scheduler.VPod, error) {
	sources, err := r.kafkaLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	vpods := make([]scheduler.VPod, len(sources))
	for i := 0; i < len(sources); i++ {
		vpods[i] = sources[i]
	}
	return vpods, nil
}

func (r *Reconciler) createCloudEventAttributes(src *v1beta1.KafkaSource) []duckv1.CloudEventAttributes {
	ceAttributes := make([]duckv1.CloudEventAttributes, 0, len(src.Spec.Topics))
	for i := range src.Spec.Topics {
		topics := strings.Split(src.Spec.Topics[i], ",")
		for _, topic := range topics {
			ceAttributes = append(ceAttributes, duckv1.CloudEventAttributes{
				Type:   v1beta1.KafkaEventType,
				Source: v1beta1.KafkaEventSource(src.Namespace, src.Name, topic),
			})
		}
	}
	return ceAttributes
}
