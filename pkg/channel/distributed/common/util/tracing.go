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

package util

import (
	"github.com/Shopify/sarama"
	"go.opencensus.io/plugin/ochttp/propagation/tracecontext"
	"go.opencensus.io/trace"
)

const (
	traceParentHeader = "traceparent"
	traceStateHeader  = "tracestate"
)

var format = &tracecontext.HTTPFormat{}

func SerializeTrace(spanContext trace.SpanContext) []sarama.RecordHeader {
	traceParent, traceState := format.SpanContextToHeaders(spanContext)

	if traceState != "" {
		return []sarama.RecordHeader{{
			Key:   []byte(traceParentHeader),
			Value: []byte(traceParent),
		}, {
			Key:   []byte(traceStateHeader),
			Value: []byte(traceState),
		}}
	}

	return []sarama.RecordHeader{{
		Key:   []byte(traceParentHeader),
		Value: []byte(traceParent),
	}}
}

func ParseSpanContext(headers map[string][]byte) (sc trace.SpanContext, ok bool) {
	traceParentBytes, ok := headers[traceParentHeader]
	if !ok {
		return trace.SpanContext{}, false
	}
	traceParent := string(traceParentBytes)

	traceState := ""
	if traceStateBytes, ok := headers[traceStateHeader]; ok {
		traceState = string(traceStateBytes)
	}

	return format.SpanContextFromHeaders(traceParent, traceState)
}
