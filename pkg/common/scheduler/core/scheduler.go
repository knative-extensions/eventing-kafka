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

package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
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
	evictor scheduler.Evictor,
	policy *SchedulerPolicy) scheduler.Scheduler {

	stateAccessor := newStateBuilder(ctx, lister, capacity, schedulerPolicy, nodeLister)
	autoscaler := NewAutoscaler(ctx, namespace, name, lister, stateAccessor, evictor, refreshPeriod, capacity)
	podInformer := podinformer.Get(ctx)
	podLister := podInformer.Lister().Pods(namespace)

	go autoscaler.Start(ctx)

	return NewStatefulSetScheduler(ctx, namespace, name, lister, stateAccessor, autoscaler, podLister, policy)
}

// StatefulSetScheduler is a scheduler placing VPod into statefulset-managed set of pods
type StatefulSetScheduler struct {
	logger            *zap.SugaredLogger
	statefulSetName   string
	statefulSetClient clientappsv1.StatefulSetInterface
	podLister         corev1listers.PodNamespaceLister
	vpodLister        scheduler.VPodLister
	lock              sync.Locker
	stateAccessor     StateAccessor
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

	//Scheduler responsible for initializing and running scheduler plugins
	policy *SchedulerPolicy

	// predicates that will always be configured.
	mandatoryPredicates sets.String

	// predicates and priorities that will be used if either was set to nil in a
	// given v1.Policy configuration.
	defaultPredicates sets.String
	defaultPriorities map[string]int64
}

func ValidatePolicy(policy *SchedulerPolicy) []error {
	var validationErrors []error

	for _, priority := range policy.Priorities {
		if priority.Weight <= 0 || priority.Weight >= MaxTotalScore {
			validationErrors = append(validationErrors, fmt.Errorf("priority %s should have a positive weight applied to it or it has overflown", priority.Name))
		}
	}
	return validationErrors
}

// AppendPredicateConfigs returns predicates configuration that will run as framework plugins.
func (s *StatefulSetScheduler) AppendPredicateConfigs(keys sets.String, plugins SchedulerPlugins) (SchedulerPlugins, error) {
	/* allPredicates := keys.Union(s.mandatoryPredicates)

	// Create the framework plugin configurations, and place them in the order
	// that the corresponding predicates were supposed to run.
	for _, predicateKey := range predicateOrdering {
		if allPredicates.Has(predicateKey) {
			producer, exist := lr.predicateToConfigProducer[predicateKey]
			if !exist {
				return config.Plugins{}, nil, fmt.Errorf("no framework config producer registered for %q", predicateKey)
			}
			producer(*args, &plugins, &pluginConfig)
			allPredicates.Delete(predicateKey)
		}
	}

	// Sort the keys so that it is easier for unit tests to do compare.
	sortedKeys := make([]string, 0, len(allPredicates))
	for k := range allPredicates {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, predicateKey := range sortedKeys {
		producer, exist := predicateToConfigProducer[predicateKey]
		if !exist {
			return config.Plugins{}, nil, fmt.Errorf("no framework config producer registered for %q", predicateKey)
		}
		producer(*args, &plugins)
	} */

	return plugins, nil
}

// AppendPriorityConfigs returns priorities configuration that will run as framework plugins.
func (s *StatefulSetScheduler) AppendPriorityConfigs(keys map[string]int64, plugins SchedulerPlugins) (SchedulerPlugins, error) {
	// Sort the keys so that it is easier for unit tests to do compare.
	/* sortedKeys := make([]string, 0, len(keys))
	for k := range keys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, priority := range sortedKeys {
		weight := keys[priority]
		producer, exist := priorityToConfigProducer[priority]
		if !exist {
			return nil, fmt.Errorf("no config producer registered for %q", priority)
		}
		a := *args
		a.Weight = int32(weight)
		producer(a, &plugins, &pluginConfig)
	} */
	return plugins, nil
}

// RunFilterPlugins runs the set of configured Filter plugins for pod on
// the given node. If any of these plugins doesn't return "Success", the
// given node is not suitable for running pod.
// Meanwhile, the failure message and status are set for the given node.
func (s *StatefulSetScheduler) RunFilterPlugins(ctx context.Context, pod *v1.Pod) PluginToStatus {
	statuses := make(PluginToStatus)
	/* 	for _, pl := range f.filterPlugins {
		pluginStatus := f.runFilterPlugin(ctx, pl, state, pod)
		if !pluginStatus.IsSuccess() {
			if !pluginStatus.IsUnschedulable() {
				// Filter plugins are not supposed to return any status other than
				// Success or Unschedulable.
				errStatus := NewStatus(Error, fmt.Sprintf("running %q filter plugin for pod %q: %v", pl.Name(), pod.Name, pluginStatus.Message()))
				return map[string]*Status{pl.Name(): errStatus}
			}
			statuses[pl.Name()] = pluginStatus
			if !f.runAllFilters {
				// Exit early if we don't need to run all filters.
				return statuses
			}
		}
	} */

	return statuses
}

func (s *StatefulSetScheduler) runFilterPlugin(ctx context.Context, pl FilterPlugin, state StateAccessor, pod *v1.Pod) *Status {
	status := pl.Filter(ctx, state, pod)
	return status
}

// HasFilterPlugins returns true if at least one filter plugin is defined.
func (s *StatefulSetScheduler) HasFilterPlugins() bool {
	return len(s.policy.Priorities) > 0
}

// HasScorePlugins returns true if at least one score plugin is defined.
func (s *StatefulSetScheduler) HasScorePlugins() bool {
	return len(s.policy.Priorities) > 0
}

func NewStatefulSetScheduler(ctx context.Context,
	namespace, name string,
	lister scheduler.VPodLister,
	stateAccessor StateAccessor,
	autoscaler Autoscaler, podlister corev1listers.PodNamespaceLister, policy *SchedulerPolicy) scheduler.Scheduler {

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
		policy:            policy,
		mandatoryPredicates: sets.NewString(
			PodFitsResources,
		),
		defaultPredicates: sets.NewString(
			PodFitsResources,
			NoMaxResourceCount,
		),
		defaultPriorities: map[string]int64{
			EvenPodSpreadPriority:    1,
			AvailabilityNodePriority: 0,
			AvailabilityZonePriority: 1,
			LowestOrdinalPriority:    1,
		},
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

	/* 	if errs := ValidatePolicy(s.policy); errs != nil {
		return nil, errs
	} */

	predicateKeys := sets.NewString()
	if s.policy.Predicates == nil {
		//predicateKeys =  //Default
	} else {
		for _, predicate := range s.policy.Predicates {
			predicateKeys.Insert(predicate.Name)
		}
	}

	priorityKeys := make(map[string]int64)
	if s.policy.Priorities == nil {
		//priorityKeys = //DefaultPriorities
	} else {
		for _, priority := range s.policy.Priorities {
			priorityKeys[priority.Name] = priority.Weight
		}
	}

	var err error
	plugins := SchedulerPlugins{}
	if plugins, err = s.AppendPredicateConfigs(predicateKeys, plugins); err != nil {
		return nil, err
	}
	if plugins, err = s.AppendPriorityConfigs(priorityKeys, plugins); err != nil {
		return nil, err
	}

	placements, err := s.scheduleVPod(vpod)
	if placements == nil {
		return placements, err
	}

	sort.SliceStable(placements, func(i int, j int) bool {
		return OrdinalFromPodName(placements[i].PodName) < OrdinalFromPodName(placements[j].PodName)
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
		if state.SchedulerPolicy == EVENSPREAD || state.SchedulerPolicy == EVENSPREAD_BYNODE {
			//spreadVal is the minimum number of replicas to be left behind in each failure domain for high availability
			if state.SchedulerPolicy == EVENSPREAD {
				spreadVal = int32(math.Floor(float64(vpod.GetVReplicas()) / float64(state.NumZones)))
			} else if state.SchedulerPolicy == EVENSPREAD_BYNODE {
				spreadVal = int32(math.Floor(float64(vpod.GetVReplicas()) / float64(state.NumNodes)))
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
	if state.SchedulerPolicy == EVENSPREAD || state.SchedulerPolicy == EVENSPREAD_BYNODE {
		//spreadVal is the maximum number of replicas to be placed in each failure domain for high availability
		if state.SchedulerPolicy == EVENSPREAD {
			spreadVal = int32(math.Ceil(float64(vpod.GetVReplicas()) / float64(state.NumZones)))
		} else if state.SchedulerPolicy == EVENSPREAD_BYNODE {
			spreadVal = int32(math.Ceil(float64(vpod.GetVReplicas()) / float64(state.NumNodes)))
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

	if state.SchedulerPolicy == EVENSPREAD_BYNODE {
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
		if state.SchedulerPolicy == EVENSPREAD_BYNODE {
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
		ordinal := OrdinalFromPodName(podName)

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
					PodName:   PodNameFromOrdinal(s.statefulSetName, ordinal),
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

	if state.SchedulerPolicy == EVENSPREAD_BYNODE {
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
		if state.SchedulerPolicy == EVENSPREAD_BYNODE {
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
			podName := PodNameFromOrdinal(s.statefulSetName, ordinal)
			zoneName, nodeName, err := s.getPodInfo(state, podName)
			if err != nil {
				logger.Errorw("Error getting zone and node info from pod", zap.Error(err))
				continue //TODO: not continue?
			}

			var totalInDomain int32
			if state.SchedulerPolicy == EVENSPREAD_BYNODE {
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
	zoneName, ok := state.NodeToZoneMap[nodeName]
	if !ok {
		return zoneName, nodeName, errors.New("could not find zone")
	}
	return zoneName, nodeName, nil
}

func (s *StatefulSetScheduler) getPlacementsByZoneKey(state *state, placements []duckv1alpha1.Placement) (placementsByZone map[string][]int32) {
	placementsByZone = make(map[string][]int32)
	for i := 0; i < len(placements); i++ {
		zoneName, _, _ := s.getPodInfo(state, placements[i].PodName)
		placementsByZone[zoneName] = append(placementsByZone[zoneName], OrdinalFromPodName(placements[i].PodName))
	}
	return placementsByZone
}

func (s *StatefulSetScheduler) getPlacementsByNodeKey(state *state, placements []duckv1alpha1.Placement) (placementsByNode map[string][]int32) {
	placementsByNode = make(map[string][]int32)
	for i := 0; i < len(placements); i++ {
		_, nodeName, _ := s.getPodInfo(state, placements[i].PodName)
		placementsByNode[nodeName] = append(placementsByNode[nodeName], OrdinalFromPodName(placements[i].PodName))
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
		if placements[i].PodName == PodNameFromOrdinal(s.statefulSetName, ordinal) {
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
