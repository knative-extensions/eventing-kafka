package dispatcher

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	klogtesting "knative.dev/pkg/logging/testing"
)

func TestServeHTTP(t *testing.T) {

	httpGet := "GET"
	httpPost := "POST"
	testCases := []struct {
		name               string
		responseReturnCode int
		desiredJson        []byte
		channelSubs        map[types.NamespacedName]*KafkaSubscription
		requestURI         string
		httpMethod         string
	}{
		{
			name:               "channelref not found",
			httpMethod:         httpGet,
			responseReturnCode: http.StatusNotFound,
			desiredJson:        []byte{},
			requestURI:         "/exist/thisDoesNot",
		}, {
			name:               "nop",
			httpMethod:         httpGet,
			responseReturnCode: http.StatusNotFound,
			desiredJson:        []byte{},
			requestURI:         "///",
		}, {
			name:               "no ready subscribers",
			httpMethod:         httpGet,
			responseReturnCode: http.StatusOK,
			desiredJson:        []byte(`{}`),
			channelSubs: map[types.NamespacedName]*KafkaSubscription{
				{Name: "foo", Namespace: "bar"}: {
					subs:                      sets.NewString(),
					channelReadySubscriptions: map[string]sets.Int32{},
				},
			},
			requestURI: "/bar/foo",
		}, {
			name:               "different channelref called from populated channref (different ns)",
			httpMethod:         httpGet,
			desiredJson:        []byte{},
			responseReturnCode: http.StatusNotFound,
			channelSubs: map[types.NamespacedName]*KafkaSubscription{
				{Name: "foo", Namespace: "baz"}: {
					subs: sets.NewString("a", "b"),
					channelReadySubscriptions: map[string]sets.Int32{
						"a": sets.NewInt32(0),
						"b": sets.NewInt32(0),
					},
				},
			},
			requestURI: "/bar/foo",
		}, {
			name:               "return correct subscription",
			httpMethod:         httpGet,
			desiredJson:        []byte(`{"a":[0],"b":[0,2,5]}`),
			responseReturnCode: http.StatusOK,
			channelSubs: map[types.NamespacedName]*KafkaSubscription{
				{Name: "foo", Namespace: "bar"}: {
					subs: sets.NewString("a", "b"),
					channelReadySubscriptions: map[string]sets.Int32{
						"a": sets.NewInt32(0),
						"b": sets.NewInt32(0, 2, 5),
					},
				},
			},
			requestURI: "/bar/foo",
		}, {
			name:               "return correct subscription from multiple chanrefs",
			httpMethod:         httpGet,
			desiredJson:        []byte(`{"a":[0],"b":[0,2,5]}`),
			responseReturnCode: http.StatusOK,
			channelSubs: map[types.NamespacedName]*KafkaSubscription{
				{Name: "table", Namespace: "flip"}: {
					subs: sets.NewString("c", "d"),
					channelReadySubscriptions: map[string]sets.Int32{
						"c": sets.NewInt32(0),
						"d": sets.NewInt32(0),
					}},
				{Name: "foo", Namespace: "bar"}: {
					subs: sets.NewString("a", "b"),
					channelReadySubscriptions: map[string]sets.Int32{
						"a": sets.NewInt32(0),
						"b": sets.NewInt32(0, 2, 5),
					},
				},
			},
			requestURI: "/bar/foo",
		}, {
			name:               "bad request uri",
			httpMethod:         httpGet,
			desiredJson:        []byte{},
			responseReturnCode: http.StatusNotFound,
			requestURI:         "/here/be/dragons/there/are/too/many/slashes",
		}, {
			name:               "bad request method (POST)",
			httpMethod:         httpPost,
			desiredJson:        []byte{},
			responseReturnCode: http.StatusMethodNotAllowed,
		},
	}
	logger := klogtesting.TestLogger(t)
	d := &KafkaDispatcher{
		channelSubscriptions: make(map[types.NamespacedName]*KafkaSubscription),
		logger:               logger,
	}
	subscriptionEndpoint := &subscriptionEndpoint{
		dispatcher: d,
		logger:     logger,
	}

	ts := httptest.NewServer(subscriptionEndpoint)
	defer ts.Close()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Running %s", t.Name())
			d.channelSubscriptions = tc.channelSubs

			request, _ := http.NewRequest(tc.httpMethod, fmt.Sprintf("%s%s", ts.URL, tc.requestURI), nil)
			//			resp, err := http.Get(fmt.Sprintf("%s%s", ts.URL, tc.requestURI))
			resp, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Errorf("Could not send request to subscriber endpoint: %v", err)
			}
			if resp.StatusCode != tc.responseReturnCode {
				t.Errorf("unepxected status returned: want: %d, got: %d", tc.responseReturnCode, resp.StatusCode)
			}
			respBody, err := ioutil.ReadAll(resp.Body)
			defer resp.Body.Close()
			if err != nil {
				t.Errorf("Could not read response from subscriber endpoint: %v", err)
			}
			if testing.Verbose() && len(respBody) > 0 {
				t.Logf("http response: %s\n", string(respBody))
			}
			if diff := cmp.Diff(tc.desiredJson, respBody); diff != "" {
				t.Errorf("unexpected readysubscriber status response: (-want, +got) = %v", diff)
			}
		})
	}
}
