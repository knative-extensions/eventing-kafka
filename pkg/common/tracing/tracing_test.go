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

package tracing

import (
	"context"
	"net/http"
	"testing"

	"github.com/Shopify/sarama"
	protocolkafka "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/tracestate"
	logtesting "knative.dev/pkg/logging/testing"
)

var (
	traceID             = trace.TraceID{75, 249, 47, 53, 119, 179, 77, 166, 163, 206, 146, 157, 14, 14, 71, 54}
	spanID              = trace.SpanID{0, 240, 103, 170, 11, 169, 2, 183}
	traceOpt            = trace.TraceOptions(1)
	entry1              = tracestate.Entry{Key: "foo", Value: "bar"}
	entry2              = tracestate.Entry{Key: "hello", Value: "world   example"}
	sampleTracestate, _ = tracestate.New(nil, entry1, entry2)
	sampleSpanContext   = trace.SpanContext{
		TraceID:      traceID,
		SpanID:       spanID,
		TraceOptions: traceOpt,
		Tracestate:   sampleTracestate,
	}
)

func TestRoundtripWithNewSpan(t *testing.T) {
	_, span := trace.StartSpan(context.TODO(), "aaa", trace.WithSpanKind(trace.SpanKindClient))
	span.AddAttributes(trace.BoolAttribute("hello", true))
	inSpanContext := span.SpanContext()

	serializedHeaders := SerializeTrace(inSpanContext)

	// Translate back to headers
	headers := make(map[string][]byte)
	for _, h := range serializedHeaders {
		headers[string(h.Key)] = h.Value
	}

	outSpanContext, ok := ParseSpanContext(headers)
	require.True(t, ok)
	require.Equal(t, inSpanContext, outSpanContext)
}

func TestRoundtrip(t *testing.T) {
	serializedHeaders := SerializeTrace(sampleSpanContext)

	// Translate back to headers
	headers := make(map[string][]byte)
	for _, h := range serializedHeaders {
		headers[string(h.Key)] = h.Value
	}

	outSpanContext, ok := ParseSpanContext(headers)
	require.True(t, ok)
	require.Equal(t, sampleSpanContext, outSpanContext)
}

// Verify that the StartTraceFromMessage call creates spans.  The verification of span
// content itself is done in the TestRoundtrip and TestRoundtripWithNewSpan functions.
func TestStartTraceFromMessage(t *testing.T) {
	logger := logtesting.TestLogger(t)

	// Verify that "message without headers" starts a new span without errors
	ctx, span := StartTraceFromMessage(logger, context.TODO(), &protocolkafka.Message{}, "kafkachannel-testTopic")
	require.NotNil(t, ctx)
	require.NotNil(t, span)

	_, span = trace.StartSpan(context.TODO(), "aaa", trace.WithSpanKind(trace.SpanKindClient))
	span.AddAttributes(trace.BoolAttribute("hello", true))
	inSpanContext := span.SpanContext()

	serializedHeaders := SerializeTrace(inSpanContext)

	// Translate back to headers
	headers := make(map[string][]byte)
	for _, h := range serializedHeaders {
		headers[string(h.Key)] = h.Value
	}

	// Verify that a span can be generated from existing span headers
	ctx, span = StartTraceFromMessage(logger, context.TODO(), &protocolkafka.Message{Headers: headers}, "kafkachannel-testTopic")
	require.NotNil(t, ctx)
	require.NotNil(t, span)
}

func TestConvertHttpHeaderToRecordHeaders(t *testing.T) {

	key1 := "K1"
	value1 := "V1"

	key2 := "K2"
	value2A := "V2A"
	value2B := "V2B"

	testCases := []struct {
		name   string
		header http.Header
		want   []sarama.RecordHeader
	}{
		{
			name:   "nil http header",
			header: nil,
			want:   []sarama.RecordHeader{},
		},
		{
			name:   "empty http header",
			header: http.Header{},
			want:   []sarama.RecordHeader{},
		},
		{
			name:   "single http header",
			header: http.Header{key1: []string{value1}},
			want:   []sarama.RecordHeader{{Key: []byte(key1), Value: []byte(value1)}},
		},
		{
			name:   "multiple http header",
			header: http.Header{key1: []string{value1}, key2: []string{value2A, value2B}},
			want: []sarama.RecordHeader{
				{Key: []byte(key1), Value: []byte(value1)},
				{Key: []byte(key2), Value: []byte(value2A)},
				{Key: []byte(key2), Value: []byte(value2B)},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := ConvertHttpHeaderToRecordHeaders(testCase.header)
			assert.ElementsMatch(t, testCase.want, actual)
		})
	}
}

func TestConvertRecordHeadersToHttpHeader(t *testing.T) {

	key1 := "K1"
	value1 := "V1"

	key2 := "K2"
	value2A := "V2A"
	value2B := "V2B"

	testCases := []struct {
		name          string
		recordHeaders []*sarama.RecordHeader
		want          http.Header
	}{
		{
			name:          "no headers",
			recordHeaders: []*sarama.RecordHeader{},
			want:          http.Header{},
		},
		{
			name: "single header",
			recordHeaders: []*sarama.RecordHeader{
				{Key: []byte(key1), Value: []byte(value1)},
			},
			want: http.Header{key1: []string{value1}},
		},
		{
			name: "multiple headers",
			recordHeaders: []*sarama.RecordHeader{
				{Key: []byte(key1), Value: []byte(value1)},
				{Key: []byte(key2), Value: []byte(value2A)},
				{Key: []byte(key2), Value: []byte(value2B)},
			},
			want: http.Header{
				key1: []string{value1},
				key2: []string{value2A, value2B},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := ConvertRecordHeadersToHttpHeader(testCase.recordHeaders)
			for wantKey, wantValues := range testCase.want {
				assert.ElementsMatch(t, wantValues, actual[wantKey])
			}
		})
	}
}

func TestFilterCeRecordHeaders(t *testing.T) {

	nonCeRecordHeader1 := &sarama.RecordHeader{Key: []byte("non_ce_key_1"), Value: []byte("non_ce_value_1")}
	nonCeRecordHeader2 := &sarama.RecordHeader{Key: []byte("non_ce_key_2"), Value: []byte("non_ce_value_2")}

	ceRecordHeader1 := &sarama.RecordHeader{Key: []byte("ce_key_1"), Value: []byte("ce_value_1")}
	ceRecordHeader2 := &sarama.RecordHeader{Key: []byte("ce_key_2"), Value: []byte("ce_value_2")}

	testCases := []struct {
		name          string
		recordHeaders []*sarama.RecordHeader
		want          []*sarama.RecordHeader
	}{
		{
			name:          "nil headers",
			recordHeaders: nil,
			want:          []*sarama.RecordHeader{},
		},
		{
			name:          "empty headers",
			recordHeaders: []*sarama.RecordHeader{},
			want:          []*sarama.RecordHeader{},
		},
		{
			name:          "no filtered headers",
			recordHeaders: []*sarama.RecordHeader{nonCeRecordHeader1, nonCeRecordHeader2},
			want:          []*sarama.RecordHeader{nonCeRecordHeader1, nonCeRecordHeader2},
		},
		{
			name:          "some filtered headers",
			recordHeaders: []*sarama.RecordHeader{ceRecordHeader1, nonCeRecordHeader1, ceRecordHeader2, nonCeRecordHeader2},
			want:          []*sarama.RecordHeader{nonCeRecordHeader1, nonCeRecordHeader2},
		},
		{
			name:          "all filtered headers",
			recordHeaders: []*sarama.RecordHeader{ceRecordHeader1, ceRecordHeader2},
			want:          []*sarama.RecordHeader{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := FilterCeRecordHeaders(testCase.recordHeaders)
			assert.ElementsMatch(t, testCase.want, actual)
		})
	}
}
