package source

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/logging"

	ctrlmessage "knative.dev/eventing-kafka/pkg/source/control/message"
)

type ClaimsStatus map[string]ctrlmessage.Claims

func (status ClaimsStatus) String() string {
	var strs []string
	for podIp, claims := range status {
		strs = append(strs, fmt.Sprintf("Pod %s: %v", podIp, claims))
	}
	return strings.Join(strs, "\n")
}

// TODO might be good to generalize this
type StatusUpdateStore struct {
	enqueueKey func(name types.NamespacedName)

	claims     map[types.NamespacedName]ClaimsStatus
	claimsLock sync.Mutex
}

func (sus *StatusUpdateStore) ControlMessageHandler(ctx context.Context, opcode uint8, payload []byte, podIp string, srcName types.NamespacedName) {
	logger := logging.FromContext(ctx)

	switch opcode {
	case ctrlmessage.SetupClaimsOpCode:
		claims := make(ctrlmessage.Claims)
		err := claims.UnmarshalBinary(payload)
		if err != nil {
			logger.Errorf("Cannot parse the claims (sounds like a programming error of the adapter): %w", err)
		}

		// Register the update
		sus.claimsLock.Lock()
		claimsStatus := sus.claims[srcName]
		if claimsStatus == nil {
			claimsStatus = make(ClaimsStatus)
		}
		if claimsStatus[podIp] == nil {
			claimsStatus[podIp] = make(ctrlmessage.Claims)
		}
		mergeClaimsStatus(claimsStatus[podIp], claims)
		sus.claims[srcName] = claimsStatus
		sus.claimsLock.Unlock()

		logger.Infof("Registered new claims for '%v', pod '%s': %v", srcName, podIp, claims)

		// Trigger the reconciler again
		sus.enqueueKey(srcName)
	case ctrlmessage.CleanupClaimsOpCode:
		claims := make(ctrlmessage.Claims)
		err := claims.UnmarshalBinary(payload)
		if err != nil {
			logger.Errorf("Cannot parse the claims (sounds like a programming error of the adapter): %w", err)
		}

		// Register the update
		sus.claimsLock.Lock()
		claimsStatus := sus.claims[srcName]
		if claimsStatus == nil {
			claimsStatus = make(ClaimsStatus)
		}
		removeClaimsStatus(claimsStatus[podIp], claims)
		if len(claimsStatus[podIp]) == 0 {
			delete(claimsStatus, podIp)
		}
		if len(claimsStatus) == 0 {
			delete(sus.claims, srcName)
		} else {
			sus.claims[srcName] = claimsStatus
		}
		sus.claimsLock.Unlock()

		logger.Infof("Removed claims for '%v', pod '%s': %v", srcName, podIp, claims)

		// Trigger the reconciler again
		sus.enqueueKey(srcName)
	default:
		logger.Warnw(
			"Received an unknown message, I don't know what to do with it",
			zap.Uint8("opcode", opcode),
			zap.ByteString("payload", payload),
		)
	}
}

func mergeClaimsStatus(status ctrlmessage.Claims, newClaims ctrlmessage.Claims) {
	for topic, partitions := range status {
		if newPartitions, ok := newClaims[topic]; ok {
			status[topic] = sets.NewInt32(partitions...).Insert(newPartitions...).List() // Merge partitions
			delete(newClaims, topic)
		}
	}
	for newTopic, newPartitions := range newClaims {
		status[newTopic] = newPartitions
	}
}

func removeClaimsStatus(status ctrlmessage.Claims, cleanedClaims ctrlmessage.Claims) {
	for topic, partitions := range status {
		if cleanedPartitions, ok := cleanedClaims[topic]; ok {
			newSet := sets.NewInt32(partitions...).Delete(cleanedPartitions...).List()
			if len(newSet) == 0 {
				delete(status, topic)
			} else {
				status[topic] = newSet
			}
		}
	}
}

func (sus *StatusUpdateStore) GetLastClaimsUpdate(src types.NamespacedName) (ClaimsStatus, bool) {
	sus.claimsLock.Lock()
	defer sus.claimsLock.Unlock()
	claimsStatus, ok := sus.claims[src]
	return claimsStatus, ok
}
