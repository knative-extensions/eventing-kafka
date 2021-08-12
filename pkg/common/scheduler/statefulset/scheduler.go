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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
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
	"knative.dev/eventing-kafka/pkg/common/scheduler/factory"
	st "knative.dev/eventing-kafka/pkg/common/scheduler/state"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"

	_ "knative.dev/eventing-kafka/pkg/common/scheduler/plugins/core/availabilitynodepriority"
	_ "knative.dev/eventing-kafka/pkg/common/scheduler/plugins/core/availabilityzonepriority"
	_ "knative.dev/eventing-kafka/pkg/common/scheduler/plugins/core/evenpodspread"
	_ "knative.dev/eventing-kafka/pkg/common/scheduler/plugins/core/lowestordinalpriority"
	_ "knative.dev/eventing-kafka/pkg/common/scheduler/plugins/core/podfitsresources"
	_ "knative.dev/eventing-kafka/pkg/common/scheduler/plugins/core/removewithavailabilityzonepriority"
	_ "knative.dev/eventing-kafka/pkg/common/scheduler/plugins/core/removewithevenpodspreadpriority"
	_ "knative.dev/eventing-kafka/pkg/common/scheduler/plugins/core/removewithhighestordinalpriority"
	_ "knative.dev/eventing-kafka/pkg/common/scheduler/plugins/kafka/nomaxresourcecount"
)

// NewScheduler creates a new scheduler with pod autoscaling enabled.
func NewScheduler(ctx context.Context,
	namespace, name string,
	lister scheduler.VPodLister,
	refreshPeriod time.Duration,
	capacity int32,
	schedulerPolicy scheduler.SchedulerPolicyType,
	nodeLister corev1listers.NodeLister,
	evictor scheduler.Evictor,
	schedPolicy *scheduler.SchedulerPolicy,
	deschedPolicy *scheduler.SchedulerPolicy) scheduler.Scheduler {

	podInformer := podinformer.Get(ctx)
	podLister := podInformer.Lister().Pods(namespace)

	stateAccessor := st.NewStateBuilder(ctx, namespace, name, lister, capacity, schedulerPolicy, schedPolicy, deschedPolicy, podLister, nodeLister)
	autoscaler := NewAutoscaler(ctx, namespace, name, lister, stateAccessor, evictor, refreshPeriod, capacity)

	go autoscaler.Start(ctx)

	return NewStatefulSetScheduler(ctx, namespace, name, lister, stateAccessor, autoscaler, podLister)
}

// StatefulSetScheduler is a scheduler placing VPod into statefulset-managed set of pods
type StatefulSetScheduler struct {
	ctx               context.Context
	logger            *zap.SugaredLogger
	statefulSetName   string
	statefulSetClient clientappsv1.StatefulSetInterface
	podLister         corev1listers.PodNamespaceLister
	vpodLister        scheduler.VPodLister
	lock              sync.Locker
	stateAccessor     st.StateAccessor
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

	// predicates that will always be configured.
	mandatoryPredicates sets.String

	// predicates and priorities that will be used if either was set to nil in the config map
	defaultPredicates sets.String
	defaultPriorities map[string]int64
}

func NewStatefulSetScheduler(ctx context.Context,
	namespace, name string,
	lister scheduler.VPodLister,
	stateAccessor st.StateAccessor,
	autoscaler Autoscaler, podlister corev1listers.PodNamespaceLister) scheduler.Scheduler {

	scheduler := &StatefulSetScheduler{
		ctx:               ctx,
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
		mandatoryPredicates: sets.NewString(
			st.PodFitsResources,
		),
		defaultPredicates: sets.NewString(
			st.PodFitsResources,
			st.NoMaxResourceCount,
			st.EvenPodSpread,
		),
		defaultPriorities: map[string]int64{
			st.AvailabilityNodePriority: 1,
			st.AvailabilityZonePriority: 1,
			st.LowestOrdinalPriority:    1,
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

	placements, err := s.scheduleVPod(vpod)
	if placements == nil {
		return placements, err
	}

	sort.SliceStable(placements, func(i int, j int) bool {
		return st.OrdinalFromPodName(placements[i].PodName) < st.OrdinalFromPodName(placements[j].PodName)
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

	if state.SchedulerPolicy != "" {
		// Need less => scale down
		if tr > vpod.GetVReplicas() {
			logger.Infow("scaling down", zap.Int32("vreplicas", tr), zap.Int32("new vreplicas", vpod.GetVReplicas()))
			if state.SchedulerPolicy == scheduler.EVENSPREAD || state.SchedulerPolicy == scheduler.EVENSPREAD_BYNODE {
				//spreadVal is the minimum number of replicas to be left behind in each failure domain for high availability
				if state.SchedulerPolicy == scheduler.EVENSPREAD {
					spreadVal = int32(math.Floor(float64(vpod.GetVReplicas()) / float64(state.NumZones)))
				} else if state.SchedulerPolicy == scheduler.EVENSPREAD_BYNODE {
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
		if state.SchedulerPolicy == scheduler.EVENSPREAD || state.SchedulerPolicy == scheduler.EVENSPREAD_BYNODE {
			//spreadVal is the maximum number of replicas to be placed in each failure domain for high availability
			if state.SchedulerPolicy == scheduler.EVENSPREAD {
				spreadVal = int32(math.Ceil(float64(vpod.GetVReplicas()) / float64(state.NumZones)))
			} else if state.SchedulerPolicy == scheduler.EVENSPREAD_BYNODE {
				spreadVal = int32(math.Ceil(float64(vpod.GetVReplicas()) / float64(state.NumNodes)))
			}
			logger.Infow("number of replicas per domain", zap.Int32("spreadVal", spreadVal))
			placements, left = s.addReplicasEvenSpread(state, vpod.GetVReplicas()-tr, placements, spreadVal)
		} else {
			placements, left = s.addReplicas(state, vpod.GetVReplicas()-tr, placements)
		}

	} else { //Predicates and priorities must be used for scheduling
		// Need less => scale down
		if tr > vpod.GetVReplicas() && state.DeschedPolicy != nil {
			logger.Infow("scaling down", zap.Int32("vreplicas", tr), zap.Int32("new vreplicas", vpod.GetVReplicas()))
			placements = s.removeReplicasWithPolicy(vpod, tr-vpod.GetVReplicas(), placements)

			// Do not trigger the autoscaler to avoid unnecessary churn

			return placements, nil
		}

		if state.SchedPolicy != nil {
			// Need more => scale up
			if s.pending[vpod.GetKey()] > 0 { //rebalancing needed for all vreps most likely since there are pending vreps from previous reconciliation
				logger.Infow("scaling up", zap.Int32("new vreplicas", vpod.GetVReplicas()))
				placements, left = s.rebalanceReplicasWithPolicy(vpod, vpod.GetVReplicas(), placements)
			} else { //scheduling of only new reps
				logger.Infow("scaling up", zap.Int32("vreplicas", tr), zap.Int32("new vreplicas", vpod.GetVReplicas()))
				placements, left = s.addReplicasWithPolicy(vpod, vpod.GetVReplicas()-tr, placements)
			}
		}
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

func (s *StatefulSetScheduler) rebalanceReplicasWithPolicy(vpod scheduler.VPod, diff int32, placements []duckv1alpha1.Placement) ([]duckv1alpha1.Placement, int32) {
	newPlacements := make([]duckv1alpha1.Placement, len(placements))
	for i := 0; i < len(placements); i++ {
		newPlacements[i].PodName = placements[i].PodName
		newPlacements[i].VReplicas = 0
	}
	s.reservePlacements(vpod, newPlacements)

	placements, diff = s.addReplicasWithPolicy(vpod, diff, newPlacements)

	return placements, diff
}

func (s *StatefulSetScheduler) removeReplicasWithPolicy(vpod scheduler.VPod, diff int32, placements []duckv1alpha1.Placement) []duckv1alpha1.Placement {
	logger := s.logger.Named("remove replicas with policy")
	numVreps := diff

	for i := int32(0); i < numVreps; i++ { //deschedule one vreplica at a time
		state, err := s.stateAccessor.State(s.reserved)
		if err != nil {
			logger.Info("error while refreshing scheduler state (will retry)", zap.Error(err))
			return placements
		}

		feasiblePods, err := s.findFeasiblePods(s.ctx, state, vpod, state.DeschedPolicy)
		if err != nil {
			logger.Info("error while filtering pods using predicates", zap.Error(err))
			s.reservePlacements(vpod, placements)
			break
		}

		feasiblePods = s.removePodsNotInPlacement(vpod, feasiblePods)

		if len(feasiblePods) == 1 { //nothing to score, remove vrep from that pod
			placementPodID := feasiblePods[0]
			logger.Infof("Selected pod #%v to remove vreplica #%v from", placementPodID, i)
			placements = s.removeSelectionFromPlacements(placementPodID, placements)
			state.SetFree(placementPodID, state.Free(placementPodID)+1)
			s.reservePlacements(vpod, placements)
			continue
		}

		priorityList, err := s.prioritizePods(s.ctx, state, vpod, feasiblePods, state.DeschedPolicy)
		if err != nil {
			logger.Info("error while scoring pods using priorities", zap.Error(err))
			s.reservePlacements(vpod, placements)
			break
		}

		placementPodID, err := s.selectPod(priorityList)
		if err != nil {
			logger.Info("error while selecting the placement pod", zap.Error(err))
			s.reservePlacements(vpod, placements)
			break
		}

		logger.Infof("Selected pod #%v to remove vreplica #%v from", placementPodID, i)
		placements = s.removeSelectionFromPlacements(placementPodID, placements)
		state.SetFree(placementPodID, state.Free(placementPodID)+1)
		s.reservePlacements(vpod, placements)
	}
	return placements
}

func (s *StatefulSetScheduler) removeSelectionFromPlacements(placementPodID int32, placements []duckv1alpha1.Placement) []duckv1alpha1.Placement {
	newPlacements := make([]duckv1alpha1.Placement, 0, len(placements))

	for i := 0; i < len(placements); i++ {
		ordinal := st.OrdinalFromPodName(placements[i].PodName)
		if placementPodID == ordinal {
			if placements[i].VReplicas == 1 {
				// remove the entire placement
			} else {
				newPlacements = append(newPlacements, duckv1alpha1.Placement{
					PodName:   placements[i].PodName,
					VReplicas: placements[i].VReplicas - 1,
				})
			}
		} else {
			newPlacements = append(newPlacements, duckv1alpha1.Placement{
				PodName:   placements[i].PodName,
				VReplicas: placements[i].VReplicas,
			})
		}
	}
	return newPlacements
}

func (s *StatefulSetScheduler) addReplicasWithPolicy(vpod scheduler.VPod, diff int32, placements []duckv1alpha1.Placement) ([]duckv1alpha1.Placement, int32) {
	logger := s.logger.Named("add replicas with policy")

	numVreps := diff
	for i := int32(0); i < numVreps; i++ { //schedule one vreplica at a time (find most suitable pod placement satisying predicates with high score)
		// Get the current placements state
		state, err := s.stateAccessor.State(s.reserved)
		if err != nil {
			logger.Info("error while refreshing scheduler state (will retry)", zap.Error(err))
			return placements, diff
		}

		if s.replicas == 0 { //no pods to filter
			logger.Infow("no pods available in statefulset")
			s.reservePlacements(vpod, placements)
			diff = numVreps - i //for autoscaling up
			break               //end the iteration for all vreps since there are not pods
		}

		feasiblePods, err := s.findFeasiblePods(s.ctx, state, vpod, state.SchedPolicy)
		if err != nil {
			logger.Info("error while filtering pods using predicates", zap.Error(err))
			s.reservePlacements(vpod, placements)
			diff = numVreps - i //for autoscaling up and possible rebalancing
			break
		}

		if len(feasiblePods) == 0 { //no pods available to schedule this vreplica
			logger.Info("no feasible pods available to schedule this vreplica")
			s.reservePlacements(vpod, placements)
			diff = numVreps - i //for autoscaling up and possible rebalancing
			break
		}

		if len(feasiblePods) == 1 { //nothing to score, place vrep on that pod
			placementPodID := feasiblePods[0]
			logger.Infof("Selected pod #%v for vreplica #%v ", placementPodID, i)
			placements = s.addSelectionToPlacements(placementPodID, placements)
			state.SetFree(placementPodID, state.Free(placementPodID)-1)
			s.reservePlacements(vpod, placements)
			diff--
			continue
		}

		priorityList, err := s.prioritizePods(s.ctx, state, vpod, feasiblePods, state.SchedPolicy)
		if err != nil {
			logger.Info("error while scoring pods using priorities", zap.Error(err))
			s.reservePlacements(vpod, placements)
			diff = numVreps - i //for autoscaling up and possible rebalancing
			break
		}

		placementPodID, err := s.selectPod(priorityList)
		if err != nil {
			logger.Info("error while selecting the placement pod", zap.Error(err))
			s.reservePlacements(vpod, placements)
			diff = numVreps - i //for autoscaling up and possible rebalancing
			break
		}

		logger.Infof("Selected pod #%v for vreplica #%v", placementPodID, i)
		placements = s.addSelectionToPlacements(placementPodID, placements)
		state.SetFree(placementPodID, state.Free(placementPodID)-1)
		s.reservePlacements(vpod, placements)
		diff--
	}
	return placements, diff
}

func (s *StatefulSetScheduler) addSelectionToPlacements(placementPodID int32, placements []duckv1alpha1.Placement) []duckv1alpha1.Placement {
	seen := false

	for i := 0; i < len(placements); i++ {
		ordinal := st.OrdinalFromPodName(placements[i].PodName)
		if placementPodID == ordinal {
			seen = true
			placements[i].VReplicas = placements[i].VReplicas + 1
		}
	}
	if !seen {
		placements = append(placements, duckv1alpha1.Placement{
			PodName:   st.PodNameFromOrdinal(s.statefulSetName, placementPodID),
			VReplicas: 1,
		})
	}
	return placements
}

// findFeasiblePods finds the pods that fit the filter plugins
func (s *StatefulSetScheduler) findFeasiblePods(ctx context.Context, state *st.State, vpod scheduler.VPod, policy *scheduler.SchedulerPolicy) ([]int32, error) {
	logger := s.logger.Named("find feasible pods")

	feasiblePods := make([]int32, 0)
	for podId := int32(0); podId < s.replicas; podId++ {
		statusMap := s.RunFilterPlugins(ctx, state, vpod, podId, policy)
		status := statusMap.Merge()
		if status.IsSuccess() {
			logger.Infof("SUCCESS! Adding Pod #%v to feasible list", podId)
			feasiblePods = append(feasiblePods, podId)
		}
	}

	return feasiblePods, nil
}

// removePodsNotInPlacement removes pods that do not have vreplicas placed
func (s *StatefulSetScheduler) removePodsNotInPlacement(vpod scheduler.VPod, feasiblePods []int32) []int32 {
	newFeasiblePods := make([]int32, 0)
	for _, e := range vpod.GetPlacements() {
		for _, podID := range feasiblePods {
			if podID == st.OrdinalFromPodName(e.PodName) { //if pod is in current placement list
				newFeasiblePods = append(newFeasiblePods, podID)
			}
		}
	}

	return newFeasiblePods
}

// prioritizePods prioritizes the pods by running the score plugins, which return a score for each pod.
// The scores from each plugin are added together to make the score for that pod.
func (s *StatefulSetScheduler) prioritizePods(ctx context.Context, states *st.State, vpod scheduler.VPod, feasiblePods []int32, policy *scheduler.SchedulerPolicy) (st.PodScoreList, error) {
	logger := s.logger.Named("prioritize all feasible pods")

	// If no priority configs are provided, then all pods will have a score of one
	result := make(st.PodScoreList, 0, len(feasiblePods))
	if !s.HasScorePlugins(states, policy) {
		for _, podID := range feasiblePods {
			result = append(result, st.PodScore{
				ID:    podID,
				Score: 1,
			})
		}
		return result, nil
	}

	scoresMap, scoreStatus := s.RunScorePlugins(ctx, states, vpod, feasiblePods, policy)
	if !scoreStatus.IsSuccess() {
		logger.Infof("FAILURE! Cannot score feasible pods due to plugin errors %v", scoreStatus.AsError())
		return nil, scoreStatus.AsError()
	}

	// Summarize all scores.
	for i := range feasiblePods {
		result = append(result, st.PodScore{ID: feasiblePods[i], Score: 0})
		for j := range scoresMap {
			result[i].Score += scoresMap[j][i].Score
		}
	}

	return result, nil
}

// selectPod takes a prioritized list of pods and then picks one
func (s *StatefulSetScheduler) selectPod(podScoreList st.PodScoreList) (int32, error) {
	if len(podScoreList) == 0 {
		return -1, fmt.Errorf("empty priority list") //no selected pod
	}

	maxScore := podScoreList[0].Score
	selected := podScoreList[0].ID
	cntOfMaxScore := 1
	for _, ps := range podScoreList[1:] {
		if ps.Score > maxScore {
			maxScore = ps.Score
			selected = ps.ID
			cntOfMaxScore = 1
		} else if ps.Score == maxScore { //if equal scores, randomly picks one
			cntOfMaxScore++
			if rand.Intn(cntOfMaxScore) == 0 {
				selected = ps.ID
			}
		}
	}
	return selected, nil
}

// RunFilterPlugins runs the set of configured Filter plugins for a vrep on the given pod.
// If any of these plugins doesn't return "Success", the pod is not suitable for placing the vrep.
// Meanwhile, the failure message and status are set for the given pod.
func (s *StatefulSetScheduler) RunFilterPlugins(ctx context.Context, states *st.State, vpod scheduler.VPod, podID int32, policy *scheduler.SchedulerPolicy) st.PluginToStatus {
	logger := s.logger.Named("run all filter plugins")

	statuses := make(st.PluginToStatus)
	for _, plugin := range policy.Predicates {
		pl, err := factory.GetFilterPlugin(plugin.Name)
		if err != nil {
			logger.Error("Could not find filter plugin in Registry: ", plugin.Name)
			continue
		}

		logger.Infof("Going to run filter plugin: %s using state: %v ", pl.Name(), states)
		pluginStatus := s.runFilterPlugin(ctx, pl, plugin.Args, states, vpod, podID)
		if !pluginStatus.IsSuccess() {
			if !pluginStatus.IsUnschedulable() {
				errStatus := st.NewStatus(st.Error, fmt.Sprintf("running %q filter plugin for pod %q failed with: %v", pl.Name(), podID, pluginStatus.Message()))
				return map[string]*st.Status{pl.Name(): errStatus} //TODO: if one plugin fails, then no more plugins are run
			}
			statuses[pl.Name()] = pluginStatus
			return statuses
		}
	}

	return statuses
}

func (s *StatefulSetScheduler) runFilterPlugin(ctx context.Context, pl st.FilterPlugin, args interface{}, states *st.State, vpod scheduler.VPod, podID int32) *st.Status {
	status := pl.Filter(ctx, args, states, vpod.GetKey(), podID)
	return status
}

// RunScorePlugins runs the set of configured scoring plugins. It returns a list that stores for each scoring plugin name the corresponding PodScoreList(s).
// It also returns *Status, which is set to non-success if any of the plugins returns a non-success status.
func (s *StatefulSetScheduler) RunScorePlugins(ctx context.Context, states *st.State, vpod scheduler.VPod, feasiblePods []int32, policy *scheduler.SchedulerPolicy) (st.PluginToPodScores, *st.Status) {
	logger := s.logger.Named("run all score plugins")

	pluginToPodScores := make(st.PluginToPodScores, len(policy.Priorities))
	for _, plugin := range policy.Priorities {
		pl, err := factory.GetScorePlugin(plugin.Name)
		if err != nil {
			logger.Error("Could not find score plugin in registry: ", plugin.Name)
			continue
		}

		logger.Infof("Going to run score plugin: %s using state: %v ", pl.Name(), states)
		pluginToPodScores[pl.Name()] = make(st.PodScoreList, len(feasiblePods))
		for index, podID := range feasiblePods {
			score, pluginStatus := s.runScorePlugin(ctx, pl, plugin.Args, states, vpod, podID)
			if !pluginStatus.IsSuccess() {
				errStatus := st.NewStatus(st.Error, fmt.Sprintf("running %q scoring plugin for pod %q failed with: %v", pl.Name(), podID, pluginStatus.AsError()))
				return pluginToPodScores, errStatus //TODO: if one plugin fails, then no more plugins are run
			}

			score = score * plugin.Weight //WEIGHED SCORE VALUE
			logger.Infof("scoring plugin %q produced score %v for pod %q: %v", pl.Name(), score, podID, pluginStatus)
			pluginToPodScores[pl.Name()][index] = st.PodScore{
				ID:    podID,
				Score: score,
			}
		}

		status := pl.ScoreExtensions().NormalizeScore(ctx, states, pluginToPodScores[pl.Name()]) //NORMALIZE SCORES FOR ALL FEASIBLE PODS
		if !status.IsSuccess() {
			errStatus := st.NewStatus(st.Error, fmt.Sprintf("running %q scoring plugin failed with: %v", pl.Name(), status.AsError()))
			return pluginToPodScores, errStatus
		}
	}

	return pluginToPodScores, st.NewStatus(st.Success)
}

func (s *StatefulSetScheduler) runScorePlugin(ctx context.Context, pl st.ScorePlugin, args interface{}, states *st.State, vpod scheduler.VPod, podID int32) (uint64, *st.Status) {
	score, status := pl.Score(ctx, args, states, vpod.GetKey(), podID)
	return score, status
}

// HasScorePlugins returns true if at least one score plugin is defined.
func (s *StatefulSetScheduler) HasScorePlugins(state *st.State, policy *scheduler.SchedulerPolicy) bool {
	return len(policy.Priorities) > 0
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

func (s *StatefulSetScheduler) removeReplicasEvenSpread(state *st.State, diff int32, placements []duckv1alpha1.Placement, evenSpread int32) []duckv1alpha1.Placement {
	logger := s.logger.Named("remove replicas")

	newPlacements := s.removeFromExistingReplicas(state, logger, diff, placements, evenSpread)
	return newPlacements
}

func (s *StatefulSetScheduler) removeFromExistingReplicas(state *st.State, logger *zap.SugaredLogger, diff int32, placements []duckv1alpha1.Placement, evenSpread int32) []duckv1alpha1.Placement {
	var domainNames []string
	var placementsByDomain map[string][]int32
	newPlacements := make([]duckv1alpha1.Placement, 0, len(placements))

	if state.SchedulerPolicy == scheduler.EVENSPREAD_BYNODE {
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
		if state.SchedulerPolicy == scheduler.EVENSPREAD_BYNODE {
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

func (s *StatefulSetScheduler) addReplicas(states *st.State, diff int32, placements []duckv1alpha1.Placement) ([]duckv1alpha1.Placement, int32) {
	// Pod affinity algorithm: prefer adding replicas to existing pods before considering other replicas
	newPlacements := make([]duckv1alpha1.Placement, 0, len(placements))

	// Add to existing
	for i := 0; i < len(placements); i++ {
		podName := placements[i].PodName
		ordinal := st.OrdinalFromPodName(podName)

		// Is there space in PodName?
		f := states.Free(ordinal)
		if diff >= 0 && f > 0 {
			allocation := integer.Int32Min(f, diff)
			newPlacements = append(newPlacements, duckv1alpha1.Placement{
				PodName:   podName,
				VReplicas: placements[i].VReplicas + allocation,
			})

			diff -= allocation
			states.SetFree(ordinal, f-allocation)
		} else {
			newPlacements = append(newPlacements, placements[i])
		}
	}

	if diff > 0 {
		// Needs to allocate replicas to additional pods
		for ordinal := int32(0); ordinal < s.replicas; ordinal++ {
			f := states.Free(ordinal)
			if f > 0 {
				allocation := integer.Int32Min(f, diff)
				newPlacements = append(newPlacements, duckv1alpha1.Placement{
					PodName:   st.PodNameFromOrdinal(s.statefulSetName, ordinal),
					VReplicas: allocation,
				})

				diff -= allocation
				states.SetFree(ordinal, f-allocation)
			}

			if diff == 0 {
				break
			}
		}
	}

	return newPlacements, diff
}

func (s *StatefulSetScheduler) addReplicasEvenSpread(state *st.State, diff int32, placements []duckv1alpha1.Placement, evenSpread int32) ([]duckv1alpha1.Placement, int32) {
	// Pod affinity MAXFILLUP algorithm prefer adding replicas to existing pods to fill them up before adding to new pods
	// Pod affinity EVENSPREAD and EVENSPREAD_BYNODE algorithm spread replicas across pods in different regions (zone or node) for HA
	logger := s.logger.Named("add replicas")

	newPlacements, diff := s.addToExistingReplicas(state, logger, diff, placements, evenSpread)

	if diff > 0 {
		newPlacements, diff = s.addToNewReplicas(state, logger, diff, newPlacements, evenSpread)
	}
	return newPlacements, diff
}

func (s *StatefulSetScheduler) addToExistingReplicas(state *st.State, logger *zap.SugaredLogger, diff int32, placements []duckv1alpha1.Placement, evenSpread int32) ([]duckv1alpha1.Placement, int32) {
	var domainNames []string
	var placementsByDomain map[string][]int32
	newPlacements := make([]duckv1alpha1.Placement, 0, len(placements))

	if state.SchedulerPolicy == scheduler.EVENSPREAD_BYNODE {
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
		if state.SchedulerPolicy == scheduler.EVENSPREAD_BYNODE {
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

func (s *StatefulSetScheduler) addToNewReplicas(states *st.State, logger *zap.SugaredLogger, diff int32, newPlacements []duckv1alpha1.Placement, evenSpread int32) ([]duckv1alpha1.Placement, int32) {
	for ordinal := int32(0); ordinal < s.replicas; ordinal++ {
		f := states.Free(ordinal)
		if f > 0 { //here it is possible to hit pods that are in existing placements
			podName := st.PodNameFromOrdinal(s.statefulSetName, ordinal)
			zoneName, nodeName, err := states.GetPodInfo(podName)
			if err != nil {
				logger.Errorw("Error getting zone and node info from pod", zap.Error(err))
				continue //TODO: not continue?
			}

			var totalInDomain int32
			if states.SchedulerPolicy == scheduler.EVENSPREAD_BYNODE {
				totalInDomain = s.getTotalVReplicasInNode(states, newPlacements, nodeName)
			} else {
				totalInDomain = s.getTotalVReplicasInZone(states, newPlacements, zoneName)
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
			states.SetFree(ordinal, f-allocation)
		}

		if diff == 0 {
			break
		}
	}
	return newPlacements, diff
}

func (s *StatefulSetScheduler) getPlacementsByZoneKey(states *st.State, placements []duckv1alpha1.Placement) (placementsByZone map[string][]int32) {
	placementsByZone = make(map[string][]int32)
	for i := 0; i < len(placements); i++ {
		zoneName, _, _ := states.GetPodInfo(placements[i].PodName)
		placementsByZone[zoneName] = append(placementsByZone[zoneName], st.OrdinalFromPodName(placements[i].PodName))
	}
	return placementsByZone
}

func (s *StatefulSetScheduler) getPlacementsByNodeKey(states *st.State, placements []duckv1alpha1.Placement) (placementsByNode map[string][]int32) {
	placementsByNode = make(map[string][]int32)
	for i := 0; i < len(placements); i++ {
		_, nodeName, _ := states.GetPodInfo(placements[i].PodName)
		placementsByNode[nodeName] = append(placementsByNode[nodeName], st.OrdinalFromPodName(placements[i].PodName))
	}
	return placementsByNode
}

func (s *StatefulSetScheduler) getTotalVReplicasInZone(states *st.State, placements []duckv1alpha1.Placement, zoneName string) int32 {
	var totalReplicasInZone int32
	for i := 0; i < len(placements); i++ {
		pZone, _, _ := states.GetPodInfo(placements[i].PodName)
		if pZone == zoneName {
			totalReplicasInZone += placements[i].VReplicas
		}
	}
	return totalReplicasInZone
}

func (s *StatefulSetScheduler) getTotalVReplicasInNode(states *st.State, placements []duckv1alpha1.Placement, nodeName string) int32 {
	var totalReplicasInNode int32
	for i := 0; i < len(placements); i++ {
		_, pNode, _ := states.GetPodInfo(placements[i].PodName)
		if pNode == nodeName {
			totalReplicasInNode += placements[i].VReplicas
		}
	}
	return totalReplicasInNode
}

func (s *StatefulSetScheduler) getPlacementFromPodOrdinal(placements []duckv1alpha1.Placement, ordinal int32) (placement duckv1alpha1.Placement) {
	for i := 0; i < len(placements); i++ {
		if placements[i].PodName == st.PodNameFromOrdinal(s.statefulSetName, ordinal) {
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
		// When less than existing ones, recording is done to help with
		// for removal of vreps with descheduling policies
		// note: track all vreplicas, not only the new ones since
		// the next time `state()` is called some vreplicas might
		// have been committed.
		if placed != p.VReplicas {
			if _, ok := s.reserved[vpod.GetKey()]; !ok {
				s.reserved[vpod.GetKey()] = make(map[string]int32)
			}
			s.reserved[vpod.GetKey()][p.PodName] = p.VReplicas
		}
	}

}
