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

package kafka

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/v2/types"
	"go.uber.org/zap"
	"knative.dev/eventing/pkg/adapter/v2"
	"knative.dev/pkg/source"

	"knative.dev/eventing/pkg/kncloudevents"

	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
)

func TestPostMessage_ServeHTTP_binary_mode(t *testing.T) {
	aTimestamp := time.Now()

	testCases := map[string]struct {
		sink            func(http.ResponseWriter, *http.Request)
		keyTypeMapper   string
		message         *sarama.ConsumerMessage
		expectedHeaders map[string]string
		expectedBody    string
		error           bool
	}{
		"accepted_simple": {
			sink: sinkAccepted,
			message: &sarama.ConsumerMessage{
				Key:       []byte("key"),
				Topic:     "topic1",
				Value:     mustJsonMarshal(t, map[string]string{"key": "value"}),
				Partition: 1,
				Offset:    2,
				Timestamp: aTimestamp,
			},
			expectedHeaders: map[string]string{
				"ce-specversion": "1.0",
				"ce-id":          makeEventId(1, 2),
				"ce-time":        types.FormatTime(aTimestamp),
				"ce-type":        sourcesv1beta1.KafkaEventType,
				"ce-source":      sourcesv1beta1.KafkaEventSource("test", "test", "topic1"),
				"ce-subject":     makeEventSubject(1, 2),
				"ce-key":         "key",
			},
			expectedBody: `{"key":"value"}`,
			error:        false,
		},
		"accepted_int_key": {
			sink: sinkAccepted,
			message: &sarama.ConsumerMessage{
				Key:       []byte{255, 0, 23, 23},
				Topic:     "topic1",
				Value:     mustJsonMarshal(t, map[string]string{"key": "value"}),
				Partition: 1,
				Offset:    2,
				Timestamp: aTimestamp,
			},
			expectedHeaders: map[string]string{
				"ce-specversion": "1.0",
				"ce-id":          makeEventId(1, 2),
				"ce-time":        types.FormatTime(aTimestamp),
				"ce-type":        sourcesv1beta1.KafkaEventType,
				"ce-source":      sourcesv1beta1.KafkaEventSource("test", "test", "topic1"),
				"ce-subject":     makeEventSubject(1, 2),
				"ce-key":         "-16771305",
			},
			expectedBody:  `{"key":"value"}`,
			error:         false,
			keyTypeMapper: "int",
		},
		"accepted_float_key": {
			sink: sinkAccepted,
			message: &sarama.ConsumerMessage{
				Key:       []byte{1, 10, 23, 23},
				Topic:     "topic1",
				Value:     mustJsonMarshal(t, map[string]string{"key": "value"}),
				Partition: 1,
				Offset:    2,
				Timestamp: aTimestamp,
			},
			expectedHeaders: map[string]string{
				"ce-specversion": "1.0",
				"ce-id":          makeEventId(1, 2),
				"ce-time":        types.FormatTime(aTimestamp),
				"ce-type":        sourcesv1beta1.KafkaEventType,
				"ce-source":      sourcesv1beta1.KafkaEventSource("test", "test", "topic1"),
				"ce-subject":     makeEventSubject(1, 2),
				"ce-key":         "0.00000000000000000000000000000000000002536316309005082",
			},
			expectedBody:  `{"key":"value"}`,
			error:         false,
			keyTypeMapper: "float",
		},
		"accepted_byte-array_key": {
			sink: sinkAccepted,
			message: &sarama.ConsumerMessage{
				Key:       []byte{1, 10, 23, 23},
				Topic:     "topic1",
				Value:     mustJsonMarshal(t, map[string]string{"key": "value"}),
				Partition: 1,
				Offset:    2,
				Timestamp: aTimestamp,
			},
			expectedHeaders: map[string]string{
				"ce-specversion": "1.0",
				"ce-id":          makeEventId(1, 2),
				"ce-time":        types.FormatTime(aTimestamp),
				"ce-type":        sourcesv1beta1.KafkaEventType,
				"ce-source":      sourcesv1beta1.KafkaEventSource("test", "test", "topic1"),
				"ce-subject":     makeEventSubject(1, 2),
				"ce-key":         "AQoXFw==",
			},
			expectedBody:  `{"key":"value"}`,
			error:         false,
			keyTypeMapper: "byte-array",
		},
		"accepted_complex": {
			sink: sinkAccepted,
			message: &sarama.ConsumerMessage{
				Key:   []byte("key"),
				Topic: "topic1",
				Headers: []*sarama.RecordHeader{
					{
						Key: []byte("hello"), Value: []byte("world"),
					},
					{
						Key: []byte("name"), Value: []byte("Francesco"),
					},
				},
				Value:     mustJsonMarshal(t, map[string]string{"key": "value"}),
				Partition: 1,
				Offset:    2,
				Timestamp: aTimestamp,
			},
			expectedHeaders: map[string]string{
				"ce-specversion":      "1.0",
				"ce-id":               makeEventId(1, 2),
				"ce-time":             types.FormatTime(aTimestamp),
				"ce-type":             sourcesv1beta1.KafkaEventType,
				"ce-source":           sourcesv1beta1.KafkaEventSource("test", "test", "topic1"),
				"ce-subject":          makeEventSubject(1, 2),
				"ce-key":              "key",
				"ce-kafkaheaderhello": "world",
				"ce-kafkaheadername":  "Francesco",
			},
			expectedBody: `{"key":"value"}`,
			error:        false,
		},
		"accepted_fix_bad_headers": {
			sink: sinkAccepted,
			message: &sarama.ConsumerMessage{
				Key:   []byte("key"),
				Topic: "topic1",
				Headers: []*sarama.RecordHeader{
					{
						Key: []byte("hello-bla"), Value: []byte("world"),
					},
					{
						Key: []byte("name"), Value: []byte("Francesco"),
					},
				},
				Value:     mustJsonMarshal(t, map[string]string{"key": "value"}),
				Partition: 1,
				Offset:    2,
				Timestamp: aTimestamp,
			},
			expectedHeaders: map[string]string{
				"ce-specversion":         "1.0",
				"ce-id":                  makeEventId(1, 2),
				"ce-time":                types.FormatTime(aTimestamp),
				"ce-type":                sourcesv1beta1.KafkaEventType,
				"ce-source":              sourcesv1beta1.KafkaEventSource("test", "test", "topic1"),
				"ce-subject":             makeEventSubject(1, 2),
				"ce-key":                 "key",
				"ce-kafkaheaderhellobla": "world",
				"ce-kafkaheadername":     "Francesco",
			},
			expectedBody: `{"key":"value"}`,
			error:        false,
		},
		"accepted_structured": {
			sink: sinkAccepted,
			message: &sarama.ConsumerMessage{
				Key:   []byte("key"),
				Topic: "topic1",
				Value: mustJsonMarshal(t, map[string]interface{}{
					"specversion":          "1.0",
					"type":                 "com.github.pull.create",
					"source":               "https://github.com/cloudevents/spec/pull",
					"subject":              "123",
					"id":                   "A234-1234-1234",
					"time":                 "2018-04-05T17:31:00Z",
					"comexampleextension1": "value",
					"comexampleothervalue": 5,
					"datacontenttype":      "application/json",
					"data": map[string]string{
						"hello": "Francesco",
					},
				}),
				Partition: 0,
				Offset:    0,
				Headers: []*sarama.RecordHeader{
					{
						Key: []byte("content-type"), Value: []byte("application/cloudevents+json; charset=UTF-8"),
					},
				},
				Timestamp: aTimestamp,
			},
			// Because we need to write the distributed tracing extension
			expectedHeaders: map[string]string{
				"ce-specversion":          "1.0",
				"ce-id":                   "A234-1234-1234",
				"ce-time":                 "2018-04-05T17:31:00Z",
				"ce-type":                 "com.github.pull.create",
				"ce-subject":              "123",
				"ce-source":               "https://github.com/cloudevents/spec/pull",
				"ce-comexampleextension1": "value",
				"ce-comexampleothervalue": "5",
				"content-type":            "application/json",
			},
			expectedBody: `{"hello":"Francesco"}`,
			error:        false,
		},
		"accepted_binary": {
			sink: sinkAccepted,
			message: &sarama.ConsumerMessage{
				Key:   []byte("key"),
				Topic: "topic1",
				Value: mustJsonMarshal(t, map[string]string{
					"hello": "Francesco",
				}),
				Partition: 0,
				Offset:    0,
				Headers: []*sarama.RecordHeader{{
					Key: []byte("content-type"), Value: []byte("application/json"),
				}, {
					Key: []byte("ce_specversion"), Value: []byte("1.0"),
				}, {
					Key: []byte("ce_type"), Value: []byte("com.github.pull.create"),
				}, {
					Key: []byte("ce_source"), Value: []byte("https://github.com/cloudevents/spec/pull"),
				}, {
					Key: []byte("ce_subject"), Value: []byte("123"),
				}, {
					Key: []byte("ce_id"), Value: []byte("A234-1234-1234"),
				}, {
					Key: []byte("ce_time"), Value: []byte("2018-04-05T17:31:00Z"),
				}, {
					Key: []byte("ce_comexampleextension1"), Value: []byte("value"),
				}, {
					Key: []byte("ce_comexampleothervalue"), Value: []byte("5"),
				}},
				Timestamp: aTimestamp,
			},
			expectedHeaders: map[string]string{
				"ce-specversion":          "1.0",
				"ce-id":                   "A234-1234-1234",
				"ce-time":                 "2018-04-05T17:31:00Z",
				"ce-type":                 "com.github.pull.create",
				"ce-subject":              "123",
				"ce-source":               "https://github.com/cloudevents/spec/pull",
				"ce-comexampleextension1": "value",
				"ce-comexampleothervalue": "5",
				"content-type":            "application/json",
			},
			expectedBody: `{"hello":"Francesco"}`,
			error:        false,
		},
		"rejected": {
			sink: sinkRejected,
			message: &sarama.ConsumerMessage{
				Key:       []byte("key"),
				Topic:     "topic1",
				Value:     mustJsonMarshal(t, map[string]string{"key": "value"}),
				Partition: 1,
				Offset:    2,
				Timestamp: aTimestamp,
			},
			expectedHeaders: map[string]string{
				"ce-specversion": "1.0",
				"ce-id":          makeEventId(1, 2),
				"ce-time":        types.FormatTime(aTimestamp),
				"ce-type":        sourcesv1beta1.KafkaEventType,
				"ce-source":      sourcesv1beta1.KafkaEventSource("test", "test", "topic1"),
				"ce-subject":     makeEventSubject(1, 2),
				"ce-key":         "key",
			},
			expectedBody: `{"key":"value"}`,
			error:        true,
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			h := &fakeHandler{
				handler: tc.sink,
			}
			sinkServer := httptest.NewServer(h)
			defer sinkServer.Close()

			statsReporter, _ := source.NewStatsReporter()

			// If you wanna test tracing using a local zipkin server, uncomment this
			//tracing.SetupStaticPublishing(zap.L().Sugar(), "localhost", &tracingconfig.Config{
			//	Backend:        tracingconfig.Zipkin,
			//	Debug:          true,
			//	SampleRate:     1.0,
			//	ZipkinEndpoint: "http://localhost:9411/api/v2/spans",
			//})
			//defer time.Sleep(1 * time.Second)

			s, err := kncloudevents.NewHTTPMessageSenderWithTarget(sinkServer.URL)
			if err != nil {
				t.Fatal(err)
			}

			a := &Adapter{
				config: &AdapterConfig{
					EnvConfig: adapter.EnvConfig{
						Sink:      sinkServer.URL,
						Namespace: "test",
					},
					Topics:        []string{"topic1", "topic2"},
					ConsumerGroup: "group",
					Name:          "test",
				},
				httpMessageSender: s,
				logger:            zap.NewNop().Sugar(),
				reporter:          statsReporter,
				keyTypeMapper:     getKeyTypeMapper(tc.keyTypeMapper),
			}

			_, err = a.Handle(context.TODO(), tc.message)

			if tc.error && err == nil {
				t.Errorf("expected error, but got %v", err)
			}

			// Remove headers we aren't interested to test
			h.header.Del("user-agent")
			h.header.Del("accept-encoding")
			h.header.Del("content-length")

			// Check headers
			for k, expected := range tc.expectedHeaders {
				actual := h.header.Get(k)
				if actual != expected {
					t.Errorf("Expected header with key %s: '%q', but got '%q'", k, expected, actual)
				}
				h.header.Del(k)
			}

			// Check tracing headers
			if h.header.Get("traceparent") == "" {
				t.Errorf("Expected traceparent header")
			}
			h.header.Del("traceparent")

			if len(h.header) != 0 {
				t.Errorf("Unexpected headers: %v", h.header)
			}

			// Check body
			if tc.expectedBody != string(h.body) {
				t.Errorf("Expected request body '%q', but got '%q'", tc.expectedBody, h.body)
			}
		})
	}
}

func mustJsonMarshal(t *testing.T, val interface{}) []byte {
	data, err := json.Marshal(val)
	if err != nil {
		t.Errorf("unexpected error, %v", err)
	}
	return data
}

type fakeHandler struct {
	body   []byte
	header http.Header

	handler func(http.ResponseWriter, *http.Request)
}

func (h *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.header = r.Header
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "can not read body", http.StatusBadRequest)
		return
	}
	h.body = body

	defer r.Body.Close()
	h.handler(w, r)
}

func sinkAccepted(writer http.ResponseWriter, req *http.Request) {
	writer.WriteHeader(http.StatusOK)
}

func sinkRejected(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusRequestTimeout)
}

func TestAdapter_Start(t *testing.T) { // just increase code coverage
	ctx, cancel := context.WithCancel(context.Background())

	// Increasing coverage
	_ = os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap.my-kafka-namespace:9092")

	a := NewAdapter(ctx, NewEnvConfig(), nil, nil)
	err := a.Start(ctx)
	if err == nil {
		t.Errorf("expected error, but got nil")
	}
	cancel()
}

func TestInitOffset(t *testing.T) {
	testCases := map[string]struct {
		topics       []string
		topicOffsets map[string]map[int32]int64
		cgOffsets    map[string]map[int32]int64
		wantCommit   bool
	}{
		"one topic, one partition, initialized": {
			topics: []string{"my-topic"},
			topicOffsets: map[string]map[int32]int64{
				"my-topic": {
					0: 5,
				},
			},
			cgOffsets: map[string]map[int32]int64{
				"my-topic": {
					0: 2,
				},
			},
			wantCommit: false,
		},
		"one topic, one partition, uninitialized": {
			topics: []string{"my-topic"},
			topicOffsets: map[string]map[int32]int64{
				"my-topic": {
					0: 5,
				},
			},
			cgOffsets: map[string]map[int32]int64{
				"my-topic": {
					0: -1,
				},
			},
			wantCommit: true,
		},
		"several topics, several partitions, not all initialized": {
			topics: []string{"my-topic", "my-topic-2", "my-topic-3"},
			topicOffsets: map[string]map[int32]int64{
				"my-topic":   {0: 5, 1: 7},
				"my-topic-2": {0: 5, 1: 7, 2: 9},
				"my-topic-3": {0: 5, 1: 7, 2: 2, 3: 10},
			},
			cgOffsets: map[string]map[int32]int64{
				"my-topic":   {0: -1, 1: 7},
				"my-topic-2": {0: 5, 1: -1, 2: -1},
				"my-topic-3": {0: 5, 1: 7, 2: -1, 3: 10},
			},
			wantCommit: true,
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {

			broker := sarama.NewMockBroker(t, 1)
			defer broker.Close()

			group := "my-group"

			offsetResponse := sarama.NewMockOffsetResponse(t).SetVersion(1)
			for topic, partitions := range tc.topicOffsets {
				for partition, offset := range partitions {
					offsetResponse = offsetResponse.SetOffset(topic, partition, -1, offset)
				}
			}

			offsetFetchResponse := sarama.NewMockOffsetFetchResponse(t).SetError(sarama.ErrNoError)
			for topic, partitions := range tc.cgOffsets {
				for partition, offset := range partitions {
					offsetFetchResponse = offsetFetchResponse.SetOffset(group, topic, partition, offset, "", sarama.ErrNoError)
				}
			}

			metadataResponse := sarama.NewMockMetadataResponse(t).
				SetController(broker.BrokerID()).
				SetBroker(broker.Addr(), broker.BrokerID())
			for topic, partitions := range tc.topicOffsets {
				for partition, _ := range partitions {
					metadataResponse = metadataResponse.SetLeader(topic, partition, broker.BrokerID())
				}
			}

			broker.SetHandlerByMap(map[string]sarama.MockResponse{
				"OffsetRequest":      offsetResponse,
				"OffsetFetchRequest": offsetFetchResponse,

				"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
					SetCoordinator(sarama.CoordinatorGroup, group, broker),

				"MetadataRequest": metadataResponse,
			})

			adapter := NewAdapter(context.Background(), NewEnvConfig(), nil, nil).(*Adapter)
			adapter.config.ConsumerGroup = group
			adapter.config.Topics = tc.topics

			config := sarama.NewConfig()
			config.Version = sarama.MaxVersion

			fn := adapter.InitOffsets([]string{broker.Addr()}, config)
			session := new(sampleConsumerSession)
			err := fn(session)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Check uninitialized offsets have been processed.
			for topic, partitions := range tc.cgOffsets {
				for partition, offset := range partitions {
					if offset == -1 {
						got := session.GetOffset(topic, partition)

						if got != tc.topicOffsets[topic][partition] {
							t.Errorf("wanted partition offset %d, got %d", tc.topicOffsets[topic][partition], got)
						}

					}
				}
			}

			if tc.wantCommit && !session.committed {
				t.Errorf("wanted offsets to be committed")
			}

			if !tc.wantCommit && session.committed {
				t.Errorf("wanted offsets to not be committed")
			}
		})
	}
}

type sampleConsumerSession struct {
	offsets   map[string]map[int32]int64
	committed bool
}

func (s *sampleConsumerSession) Claims() map[string][]int32 {
	return nil
}

func (s *sampleConsumerSession) MemberID() string {
	return ""
}

func (s *sampleConsumerSession) GenerationID() int32 {
	return int32(0)
}

func (s *sampleConsumerSession) GetOffset(topic string, partition int32) int64 {
	if s.offsets == nil {
		return -2
	}
	partitions, ok := s.offsets[topic]
	if !ok {
		return -2
	}
	offset, ok := partitions[partition]
	if !ok {
		return -2
	}
	return offset
}

func (s *sampleConsumerSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	if s.offsets == nil {
		s.offsets = make(map[string]map[int32]int64)
	}

	partitions, ok := s.offsets[topic]
	if !ok {
		partitions = make(map[int32]int64)
		s.offsets[topic] = partitions
	}
	partitions[partition] = offset
}

func (s *sampleConsumerSession) Commit() {
	s.committed = true
}

func (s *sampleConsumerSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {

}

func (s *sampleConsumerSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {

}

func (s *sampleConsumerSession) Context() context.Context {
	return nil
}
