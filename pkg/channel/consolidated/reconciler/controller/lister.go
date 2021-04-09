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

package controller

import (
	"context"
	"fmt"
	"net/url"

	"knative.dev/eventing-kafka/pkg/channel/consolidated/status"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/sets"
	v1 "k8s.io/client-go/listers/core/v1"
	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/pkg/system"
)

type DispatcherPodsLister struct {
	logger         *zap.SugaredLogger
	endpointLister v1.EndpointsLister
}

func (t *DispatcherPodsLister) ListProbeTargets(ctx context.Context, kc v1beta1.KafkaChannel) (*status.ProbeTarget, error) {
	scope, ok := kc.Annotations[eventing.ScopeAnnotationKey]
	if !ok {
		scope = scopeCluster
	}

	dispatcherNamespace := system.Namespace()
	if scope == scopeNamespace {
		dispatcherNamespace = kc.Namespace
	}

	// Get the Dispatcher Service Endpoints and propagate the status to the Channel
	// endpoints has the same name as the service, so not a bug.
	eps, err := t.endpointLister.Endpoints(dispatcherNamespace).Get(dispatcherName)
	if err != nil {
		return nil, fmt.Errorf("failed to get internal service: %w", err)
	}
	var readyIPs []string

	for _, sub := range eps.Subsets {
		for _, address := range sub.Addresses {
			readyIPs = append(readyIPs, address.IP)
		}
	}

	if len(readyIPs) == 0 {
		return nil, fmt.Errorf("no gateway pods available")
	}

	u, _ := url.Parse(fmt.Sprintf("http://%s.%s/%s/%s", dispatcherName, dispatcherNamespace, kc.Namespace, kc.Name))

	return &status.ProbeTarget{
		PodIPs:  sets.NewString(readyIPs...),
		PodPort: "8081",
		URL:     u,
	}, nil
}

func NewProbeTargetLister(logger *zap.SugaredLogger, lister v1.EndpointsLister) status.ProbeTargetLister {
	tl := DispatcherPodsLister{
		logger:         logger,
		endpointLister: lister,
	}
	return &tl
}
