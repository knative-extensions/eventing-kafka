package dispatcher

import (
	"encoding/json"
	nethttp "net/http"
	"strings"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
)

// subscriptionEndpoint is serving the subscription status of the Kafka channel.
// A prober in the controller calls the endpoint to see if the subscriper is ready.
type subscriptionEndpoint struct {
	dispatcher *KafkaDispatcher
	logger     *zap.SugaredLogger
}

func (d *subscriptionEndpoint) ServeHTTP(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		w.WriteHeader(nethttp.StatusMethodNotAllowed)
		d.logger.Errorf("Received request method that wasn't GET: %s", r.Method)
		return
	}
	uriSplit := strings.Split(r.RequestURI, "/")
	if len(uriSplit) != 3 {
		w.WriteHeader(nethttp.StatusNotFound)
		d.logger.Errorf("Unable to process request: %s", r.RequestURI)
		return
	}
	channelRefNamespace, channelRefName := uriSplit[1], uriSplit[2]
	channelRef := types.NamespacedName{
		Name:      channelRefName,
		Namespace: channelRefNamespace,
	}
	if _, ok := d.dispatcher.channelSubscriptions[channelRef]; !ok {
		w.WriteHeader(nethttp.StatusNotFound)
		return
	}
	d.dispatcher.channelSubscriptions[channelRef].readySubscriptionsLock.RLock()
	defer d.dispatcher.channelSubscriptions[channelRef].readySubscriptionsLock.RUnlock()
	var subscriptions = make(map[string][]int32)
	w.Header().Set(dispatcherReadySubHeader, channelRefName)
	for s, ps := range d.dispatcher.channelSubscriptions[channelRef].channelReadySubscriptions {
		subscriptions[s] = ps.List()
	}
	jsonResult, err := json.Marshal(subscriptions)
	if err != nil {
		d.logger.Errorf("Error marshalling json for sub-status channelref: %s/%s, %w", channelRefNamespace, channelRefName, err)
		return
	}
	_, err = w.Write(jsonResult)
	if err != nil {
		d.logger.Errorf("Error writing jsonResult to serveHTTP writer: %w", err)
	}
}

func (d *subscriptionEndpoint) start() {
	d.logger.Fatal(nethttp.ListenAndServe(":8081", d))
}
