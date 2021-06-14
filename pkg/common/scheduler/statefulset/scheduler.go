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
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	clientappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/integer"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	statefulsetinformer "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	duckv1alpha1 "knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
)

type SchedulerPolicyType string

const (
	// MAXFILLUP policy type adds vreplicas to existing pods to fill them up before adding to new pods
	MAXFILLUP SchedulerPolicyType = "MAXFILLUP"
	// EVENSPREAD policy type spreads vreplicas uniformly across zones to reduce impact of failure
	EVENSPREAD = "EVENSPREAD"
	// EVENSPREAD_BYNODE policy type spreads vreplicas uniformly across nodes to reduce impact of failure
	EVENSPREAD_BYNODE = "EVENSPREAD_BYNODE"
)

const (
	ZoneLabel = "topology.kubernetes.io/zone"
)

// NewScheduler creates a new scheduler with pod autoscaling enabled.
func NewScheduler(ctx context.Context,
	namespace, name string,
	lister scheduler.VPodLister,
	refreshPeriod time.Duration,
	capacity int32,
	schedulerPolicy SchedulerPolicyType,
	nodeLister corev1listers.NodeLister,
	evictor scheduler.Evictor) scheduler.Scheduler {

	stateAccessor := newStateBuilder(ctx, lister, capacity, schedulerPolicy, nodeLister)
	autoscaler := NewAutoscaler(ctx, namespace, name, lister, stateAccessor, evictor, refreshPeriod, capacity)
	podInformer := podinformer.Get(ctx)
	podLister := podInformer.Lister().Pods(namespace)

	go autoscaler.Start(ctx)

	return NewStatefulSetScheduler(ctx, namespace, name, lister, stateAccessor, autoscaler, podLister)
}

// StatefulSetScheduler is a scheduler placing VPod into statefulset-managed set of pods
type StatefulSetScheduler struct {
	logger            *zap.SugaredLogger
	statefulSetName   string
	statefulSetClient clientappsv1.StatefulSetInterface
	podLister         corev1listers.PodNamespaceLister
	vpodLister        scheduler.VPodLister
	lock              sync.Locker
	stateAccessor     stateAccessor
	autoscaler        Autoscaler

	// replicas is the (cached) number of statefulset replicas.
	replicas int32

	// pending tracks the number of virtual replicas that haven't been scheduled yet
	// because there wasn't enough free capacity.
	// The autoscaler uses
	pending map[types.NamespacedName]int32

	// reserved tracks vreplicas that have been placed (ie. scheduled) but haven't been
	// committed yet (ie. not appearing in vpodLister)
	reserved map[types.NamespacedName]map[string]int32
}

func NewStatefulSetScheduler(ctx context.Context,
	namespace, name string,
	lister scheduler.VPodLister,
	stateAccessor stateAccessor,
	autoscaler Autoscaler, podlister corev1listers.PodNamespaceLister) scheduler.Scheduler {

	scheduler := &StatefulSetScheduler{
		logger:            logging.FromContext(ctx),
		statefulSetName:   name,
		statefulSetClient: kubeclient.Get(ctx).AppsV1().StatefulSets(namespace),
		podLister:         podlister,
		vpodLister:        lister,
		pending:           make(map[types.NamespacedName]int32),
		lock:              new(sync.Mutex),
		stateAccessor:     stateAccessor,
		reserved:          make(map[types.NamespacedName]map[string]int32),
		autoscaler:        autoscaler,
	}

	// Monitor our statefulset
	statefulsetInformer := statefulsetinformer.Get(ctx)
	statefulsetInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterWithNameAndNamespace(namespace, name),
		Handler:    controller.HandleAll(scheduler.updateStatefulset),
	})

	return scheduler
}

func (s *StatefulSetScheduler) Schedule(vpod scheduler.VPod) ([]duckv1alpha1.Placement, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	placements, err := s.scheduleVPod(vpod)
	if placements == nil {
		return placements, err
	}

	sort.SliceStable(placements, func(i int, j int) bool {
		return ordinalFromPodName(placements[i].PodName) < ordinalFromPodName(placements[j].PodName)
	})

	// Reserve new placements until they are committed to the vpod.
	s.reservePlacements(vpod, placements)

	return placements, err
}

func (s *StatefulSetScheduler) scheduleVPod(vpod scheduler.VPod) ([]duckv1alpha1.Placement, error) {
	logger := s.logger.With("key", vpod.GetKey())
	logger.Info("scheduling")

	// Get the current placements state
	// Quite an expensive operation but safe and simple.
	state, err := s.stateAccessor.State(s.reserved)
	if err != nil {
		logger.Info("error while refreshing scheduler state (will retry)", zap.Error(err))
		return nil, err
	}

	placements := vpod.GetPlacements()
	var spreadVal, left int32

	// The scheduler when policy type is
	// Policy: MAXFILLUP (SchedulerPolicyType == MAXFILLUP)
	// - allocates as many vreplicas as possible to the same pod(s)
	// - allocates remaining vreplicas to new pods
	// Policy: EVENSPREAD (SchedulerPolicyType == EVENSPREAD)
	// - divides up vreplicas equally between the zones and
	// - allocates as many vreplicas as possible to existing pods while not going over the equal spread value
	// - allocates remaining vreplicas to new pods created in new zones still satisfying equal spread
	// Policy: EVENSPREAD_BYNODE (SchedulerPolicyType == EVENSPREAD_BYNODE)
	// - divides up vreplicas equally between the nodes and
	// - allocates as many vreplicas as possible to existing pods while not going over the equal spread value
	// - allocates remaining vreplicas to new pods created on new nodes and zones still satisfying equal spread

	// Exact number of vreplicas => do nothing
	tr := scheduler.GetTotalVReplicas(placements)
	if tr == vpod.GetVReplicas() {
		logger.Info("scheduling succeeded (already scheduled)")
		delete(s.pending, vpod.GetKey())

		// Fully placed. Nothing to do
		return placements, nil
	}

	// Need less => scale down
	if tr > vpod.GetVReplicas() {
		logger.Infow("scaling down", zap.Int32("vreplicas", tr), zap.Int32("new vreplicas", vpod.GetVReplicas()))
		if state.schedulerPolicy == EVENSPREAD || state.schedulerPolicy == EVENSPREAD_BYNODE {
			//spreadVal is the minimum number of replicas to be left behind in each failure domain for high availability
			if state.schedulerPolicy == EVENSPREAD {
				spreadVal = int32(math.Floor(float64(vpod.GetVReplicas()) / float64(state.numZones)))
			} else if state.schedulerPolicy == EVENSPREAD_BYNODE {
				spreadVal = int32(math.Floor(float64(vpod.GetVReplicas()) / float64(state.numNodes)))
			}
			logger.Infow("number of replicas per domain", zap.Int32("spreadVal", spreadVal))
			placements = s.removeReplicasEvenSpread(state, tr-vpod.GetVReplicas(), placements, spreadVal)
		} else {
			placements = s.removeReplicas(tr-vpod.GetVReplicas(), placements)
		}

		// Do not trigger the autoscaler to avoid unnecessary churn

		return placements, nil
	}

	// Need more => scale up
	logger.Infow("scaling up", zap.Int32("vreplicas", tr), zap.Int32("new vreplicas", vpod.GetVReplicas()))
	if state.schedulerPolicy == EVENSPREAD || state.schedulerPolicy == EVENSPREAD_BYNODE {
		//spreadVal is the maximum number of replicas to be placed in each failure domain for high availability
		if state.schedulerPolicy == EVENSPREAD {
			spreadVal = int32(math.Ceil(float64(vpod.GetVReplicas()) / float64(state.numZones)))
		} else if state.schedulerPolicy == EVENSPREAD_BYNODE {
			spreadVal = int32(math.Ceil(float64(vpod.GetVReplicas()) / float64(state.numNodes)))
		}
		logger.Infow("number of replicas per domain", zap.Int32("spreadVal", spreadVal))
		placements, left = s.addReplicasEvenSpread(state, vpod.GetVReplicas()-tr, placements, spreadVal)
	} else {
		placements, left = s.addReplicas(state, vpod.GetVReplicas()-tr, placements)
	}

	if left > 0 {
		// Give time for the autoscaler to do its job
		logger.Info("scheduling failed (not enough pod replicas)", zap.Any("placement", placements), zap.Int32("left", left))

		s.pending[vpod.GetKey()] = left

		// Trigger the autoscaler
		if s.autoscaler != nil {
			s.autoscaler.Autoscale(s.pendingVReplicas())
		}

		return placements, scheduler.ErrNotEnoughReplicas
	}

	logger.Infow("scheduling successful", zap.Any("placement", placements))
	delete(s.pending, vpod.GetKey())
	return placements, nil
}

func (s *StatefulSetScheduler) removeReplicas(diff int32, placements []duckv1alpha1.Placement) []duckv1alpha1.Placement {
	newPlacements := make([]duckv1alpha1.Placement, 0, len(placements))
	for i := len(placements) - 1; i > -1; i-- {
		if diff >= placements[i].VReplicas {
			// remove the entire placement
			diff -= placements[i].VReplicas
		} else {
			newPlacements = append(newPlacements, duckv1alpha1.Placement{
				PodName:   placements[i].PodName,
				VReplicas: placements[i].VReplicas - diff,
			})
			diff = 0
		}
	}
	return newPlacements
}

func (s *StatefulSetScheduler) removeReplicasEvenSpread(state *state, diff int32, placements []duckv1alpha1.Placement, evenSpread int32) []duckv1alpha1.Placement {
	logger := s.logger.Named("remove replicas")

	newPlacements := s.removeFromExistingReplicas(state, logger, diff, placements, evenSpread)
	return newPlacements
}

func (s *StatefulSetScheduler) removeFromExistingReplicas(state *state, logger *zap.SugaredLogger, diff int32, placements []duckv1alpha1.Placement, evenSpread int32) []duckv1alpha1.Placement {
	var domainNames []string
	var placementsByDomain map[string][]int32
	newPlacements := make([]duckv1alpha1.Placement, 0, len(placements))

	if state.schedulerPolicy == EVENSPREAD_BYNODE {
		placementsByDomain = s.getPlacementsByNodeKey(state, placements)
	} else {
		placementsByDomain = s.getPlacementsByZoneKey(state, placements)
	}
	for domainName := range placementsByDomain {
		domainNames = append(domainNames, domainName)
	}
	sort.Strings(domainNames) //for ordered accessing of map

	for i := 0; i < len(domainNames); i++ { //iterate through each domain
		var totalInDomain int32
		if state.schedulerPolicy == EVENSPREAD_BYNODE {
			totalInDomain = s.getTotalVReplicasInNode(state, placements, domainNames[i])
		} else {
			totalInDomain = s.getTotalVReplicasInZone(state, placements, domainNames[i])
		}
		logger.Info(zap.String("domainName", domainNames[i]), zap.Int32("totalInDomain", totalInDomain))

		placementOrdinals := placementsByDomain[domainNames[i]]
		for j := len(placementOrdinals) - 1; j >= 0; j-- { //iterating through all existing pods belonging to a single domain from larger cardinal to smaller
			ordinal := placementOrdinals[j]
			placement := s.getPlacementFromPodOrdinal(placements, ordinal)

			if diff > 0 && totalInDomain >= evenSpread {
				deallocation := integer.Int32Min(diff, integer.Int32Min(placement.VReplicas, totalInDomain-evenSpread))
				logger.Info(zap.Int32("diff", diff), zap.Int32("ordinal", ordinal), zap.Int32("deallocation", deallocation))

				if deallocation > 0 && deallocation < placement.VReplicas {
					newPlacements = append(newPlacements, duckv1alpha1.Placement{
						PodName:   placement.PodName,
						VReplicas: placement.VReplicas - deallocation,
					})
					diff -= deallocation
					totalInDomain -= deallocation
				} else if deallocation >= placement.VReplicas {
					diff -= placement.VReplicas
					totalInDomain -= placement.VReplicas
				} else {
					newPlacements = append(newPlacements, duckv1alpha1.Placement{
						PodName:   placement.PodName,
						VReplicas: placement.VReplicas,
					})
				}
			} else {
				newPlacements = append(newPlacements, duckv1alpha1.Placement{
					PodName:   placement.PodName,
					VReplicas: placement.VReplicas,
				})

			}
		}
	}
	return newPlacements
}

func (s *StatefulSetScheduler) addReplicas(state *state, diff int32, placements []duckv1alpha1.Placement) ([]duckv1alpha1.Placement, int32) {
	// Pod affinity algorithm: prefer adding replicas to existing pods before considering other replicas
	newPlacements := make([]duckv1alpha1.Placement, 0, len(placements))

	// Add to existing
	for i := 0; i < len(placements); i++ {
		podName := placements[i].PodName
		ordinal := ordinalFromPodName(podName)

		// Is there space in PodName?
		f := state.Free(ordinal)
		if diff >= 0 && f > 0 {
			allocation := integer.Int32Min(f, diff)
			newPlacements = append(newPlacements, duckv1alpha1.Placement{
				PodName:   podName,
				VReplicas: placements[i].VReplicas + allocation,
			})

			diff -= allocation
			state.SetFree(ordinal, f-allocation)
		} else {
			newPlacements = append(newPlacements, placements[i])
		}
	}

	if diff > 0 {
		// Needs to allocate replicas to additional pods
		for ordinal := int32(0); ordinal < s.replicas; ordinal++ {
			f := state.Free(ordinal)
			if f > 0 {
				allocation := integer.Int32Min(f, diff)
				newPlacements = append(newPlacements, duckv1alpha1.Placement{
					PodName:   podNameFromOrdinal(s.statefulSetName, ordinal),
					VReplicas: allocation,
				})

				diff -= allocation
				state.SetFree(ordinal, f-allocation)
			}

			if diff == 0 {
				break
			}
		}
	}

	return newPlacements, diff
}

func (s *StatefulSetScheduler) addReplicasEvenSpread(state *state, diff int32, placements []duckv1alpha1.Placement, evenSpread int32) ([]duckv1alpha1.Placement, int32) {
	// Pod affinity MAXFILLUP algorithm prefer adding replicas to existing pods to fill them up before adding to new pods
	// Pod affinity EVENSPREAD and EVENSPREAD_BYNODE algorithm spread replicas across pods in different regions (zone or node) for HA
	logger := s.logger.Named("add replicas")

	newPlacements, diff := s.addToExistingReplicas(state, logger, diff, placements, evenSpread)

	if diff > 0 {
		newPlacements, diff = s.addToNewReplicas(state, logger, diff, newPlacements, evenSpread)
	}
	return newPlacements, diff
}

func (s *StatefulSetScheduler) addToExistingReplicas(state *state, logger *zap.SugaredLogger, diff int32, placements []duckv1alpha1.Placement, evenSpread int32) ([]duckv1alpha1.Placement, int32) {
	var domainNames []string
	var placementsByDomain map[string][]int32
	newPlacements := make([]duckv1alpha1.Placement, 0, len(placements))

	if state.schedulerPolicy == EVENSPREAD_BYNODE {
		placementsByDomain = s.getPlacementsByNodeKey(state, placements)
	} else {
		placementsByDomain = s.getPlacementsByZoneKey(state, placements)
	}
	for domainName := range placementsByDomain {
		domainNames = append(domainNames, domainName)
	}
	sort.Strings(domainNames) //for ordered accessing of map

	for i := 0; i < len(domainNames); i++ { //iterate through each domain
		var totalInDomain int32
		if state.schedulerPolicy == EVENSPREAD_BYNODE {
			totalInDomain = s.getTotalVReplicasInNode(state, placements, domainNames[i])
		} else {
			totalInDomain = s.getTotalVReplicasInZone(state, placements, domainNames[i])
		}
		logger.Info(zap.String("domainName", domainNames[i]), zap.Int32("totalInDomain", totalInDomain))

		placementOrdinals := placementsByDomain[domainNames[i]]
		for j := 0; j < len(placementOrdinals); j++ { //iterating through all existing pods belonging to a single domain
			ordinal := placementOrdinals[j]
			placement := s.getPlacementFromPodOrdinal(placements, ordinal)

			// Is there space in Pod?
			f := state.Free(ordinal)
			if diff >= 0 && f > 0 && totalInDomain < evenSpread {
				allocation := integer.Int32Min(diff, integer.Int32Min(f, (evenSpread-totalInDomain)))
				logger.Info(zap.Int32("diff", diff), zap.Int32("allocation", allocation))

				newPlacements = append(newPlacements, duckv1alpha1.Placement{
					PodName:   placement.PodName,
					VReplicas: placement.VReplicas + allocation,
				})

				diff -= allocation
				state.SetFree(ordinal, f-allocation)
				totalInDomain += allocation
			} else {
				newPlacements = append(newPlacements, placement)
			}
		}
	}
	return newPlacements, diff
}

func (s *StatefulSetScheduler) addToNewReplicas(state *state, logger *zap.SugaredLogger, diff int32, newPlacements []duckv1alpha1.Placement, evenSpread int32) ([]duckv1alpha1.Placement, int32) {
	for ordinal := int32(0); ordinal < s.replicas; ordinal++ {
		f := state.Free(ordinal)
		if f > 0 { //here it is possible to hit pods that are in existing placements
			podName := podNameFromOrdinal(s.statefulSetName, ordinal)
			zoneName, nodeName, err := s.getPodInfo(state, podName)
			if err != nil {
				logger.Errorw("Error getting zone and node info from pod", zap.Error(err))
				continue //TODO: not continue?
			}

			var totalInDomain int32
			if state.schedulerPolicy == EVENSPREAD_BYNODE {
				totalInDomain = s.getTotalVReplicasInNode(state, newPlacements, nodeName)
			} else {
				totalInDomain = s.getTotalVReplicasInZone(state, newPlacements, zoneName)
			}
			if totalInDomain >= evenSpread {
				continue //since current zone that pod belongs to is already at max spread
			}
			logger.Info("Need to schedule on a new pod", zap.Int32("ordinal", ordinal), zap.Int32("free", f), zap.String("zoneName", zoneName), zap.String("nodeName", nodeName), zap.Int32("totalInDomain", totalInDomain))

			allocation := integer.Int32Min(diff, integer.Int32Min(f, (evenSpread-totalInDomain)))
			logger.Info(zap.Int32("diff", diff), zap.Int32("allocation", allocation))

			newPlacements = append(newPlacements, duckv1alpha1.Placement{
				PodName:   podName,
				VReplicas: allocation, //TODO could there be existing vreplicas already?
			})

			diff -= allocation
			state.SetFree(ordinal, f-allocation)
		}

		if diff == 0 {
			break
		}
	}
	return newPlacements, diff
}
func (s *StatefulSetScheduler) getPodInfo(state *state, podName string) (zoneName string, nodeName string, err error) {
	pod, err := s.podLister.Get(podName)
	if err != nil {
		return zoneName, nodeName, err
	}

	nodeName = pod.Spec.NodeName
	zoneName, ok := state.nodeToZoneMap[nodeName]
	if !ok {
		return zoneName, nodeName, errors.New("could not find zone")
	}
	return zoneName, nodeName, nil
}

func (s *StatefulSetScheduler) getPlacementsByZoneKey(state *state, placements []duckv1alpha1.Placement) (placementsByZone map[string][]int32) {
	placementsByZone = make(map[string][]int32)
	for i := 0; i < len(placements); i++ {
		zoneName, _, _ := s.getPodInfo(state, placements[i].PodName)
		placementsByZone[zoneName] = append(placementsByZone[zoneName], ordinalFromPodName(placements[i].PodName))
	}
	return placementsByZone
}

func (s *StatefulSetScheduler) getPlacementsByNodeKey(state *state, placements []duckv1alpha1.Placement) (placementsByNode map[string][]int32) {
	placementsByNode = make(map[string][]int32)
	for i := 0; i < len(placements); i++ {
		_, nodeName, _ := s.getPodInfo(state, placements[i].PodName)
		placementsByNode[nodeName] = append(placementsByNode[nodeName], ordinalFromPodName(placements[i].PodName))
	}
	return placementsByNode
}

func (s *StatefulSetScheduler) getTotalVReplicasInZone(state *state, placements []duckv1alpha1.Placement, zoneName string) int32 {
	var totalReplicasInZone int32
	for i := 0; i < len(placements); i++ {
		pZone, _, _ := s.getPodInfo(state, placements[i].PodName)
		if pZone == zoneName {
			totalReplicasInZone += placements[i].VReplicas
		}
	}
	return totalReplicasInZone
}

func (s *StatefulSetScheduler) getTotalVReplicasInNode(state *state, placements []duckv1alpha1.Placement, nodeName string) int32 {
	var totalReplicasInNode int32
	for i := 0; i < len(placements); i++ {
		_, pNode, _ := s.getPodInfo(state, placements[i].PodName)
		if pNode == nodeName {
			totalReplicasInNode += placements[i].VReplicas
		}
	}
	return totalReplicasInNode
}

func (s *StatefulSetScheduler) getPlacementFromPodOrdinal(placements []duckv1alpha1.Placement, ordinal int32) (placement duckv1alpha1.Placement) {
	for i := 0; i < len(placements); i++ {
		if placements[i].PodName == podNameFromOrdinal(s.statefulSetName, ordinal) {
			return placements[i]
		}
	}
	return placement
}

// pendingReplicas returns the total number of vreplicas
// that haven't been scheduled yet
func (s *StatefulSetScheduler) pendingVReplicas() int32 {
	t := int32(0)
	for _, v := range s.pending {
		t += v
	}
	return t
}

func (s *StatefulSetScheduler) updateStatefulset(obj interface{}) {
	statefulset, ok := obj.(*appsv1.StatefulSet)
	if !ok {
		s.logger.Fatalw("expected a Statefulset object", zap.Any("object", obj))
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if statefulset.Spec.Replicas == nil {
		s.replicas = 1
	} else if s.replicas != *statefulset.Spec.Replicas {
		s.replicas = *statefulset.Spec.Replicas
		s.logger.Infow("statefulset replicas updated", zap.Int32("replicas", s.replicas))
	}
}

func (s *StatefulSetScheduler) reservePlacements(vpod scheduler.VPod, placements []duckv1alpha1.Placement) {
	existing := vpod.GetPlacements()
	for _, p := range placements {
		placed := int32(0)
		for _, e := range existing {
			if e.PodName == p.PodName {
				placed = e.VReplicas
				break
			}
		}

		// Only record placements exceeding existing ones, since the
		// goal is to prevent pods to be overcommitted.
		if p.VReplicas > placed {
			if _, ok := s.reserved[vpod.GetKey()]; !ok {
				s.reserved[vpod.GetKey()] = make(map[string]int32)
			}

			// note: track all vreplicas, not only the new ones since
			// the next time `state()` is called some vreplicas might
			// have been committed.
			s.reserved[vpod.GetKey()][p.PodName] = p.VReplicas
		}
	}
}
