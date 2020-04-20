package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	logtesting "knative.dev/pkg/logging/testing"
	"testing"
)

func TestMetricsServer_Observe(t *testing.T) {

	logger := logtesting.TestLogger(t).Desugar()
	
	m := NewMetricsServer(logger, "8888", "/metrics")

	m.Observe(statsJson)

	assertGaugeEqual(t, m.receivedMsgCountGauges, "rdkafka#consumer-2", "test", "1", 3.0)
	assertGaugeEqual(t, m.producedMsgCountGauges, "rdkafka#consumer-2", "test", "1", 215.0)

}

func assertGaugeEqual(t *testing.T, gaugeVec *prometheus.GaugeVec, name string, topicName string, partition string, expectedValue float64) {
	gauge, err := gaugeVec.GetMetricWithLabelValues(name, topicName, partition)
	if err != nil {
		t.Fatalf("Unexpected Error: %s", err)
	}
	metric := &dto.Metric{}
	gauge.Write(metric)

	assert.Equal(t, expectedValue, *metric.Gauge.Value)
}

var statsJson = `
{
  "name": "rdkafka#consumer-2",
  "client_id": "rdkafka",
  "type": "consumer",
  "ts": 5016483227792,
  "time": 1527060869,
  "replyq": 0,
  "msg_cnt": 22710,
  "msg_size": 704010,
  "msg_max": 500000,
  "msg_size_max": 1073741824,
  "simple_cnt": 0,
  "metadata_cache_cnt": 1,
  "brokers": {
    "localhost:9092/2": {
      "name": "localhost:9092/2",
      "nodeid": 2,
      "nodename": "localhost:9092",
      "source": "learned",
      "state": "UP",
      "stateage": 9057234,
      "outbuf_cnt": 0,
      "outbuf_msg_cnt": 0,
      "waitresp_cnt": 0,
      "waitresp_msg_cnt": 0,
      "tx": 320,
      "txbytes": 84283332,
      "txerrs": 0,
      "txretries": 0,
      "req_timeouts": 0,
      "rx": 320,
      "rxbytes": 15708,
      "rxerrs": 0,
      "rxcorriderrs": 0,
      "rxpartial": 0,
      "zbuf_grow": 0,
      "buf_grow": 0,
      "wakeups": 591067,
      "int_latency": {
        "min": 86,
        "max": 59375,
        "avg": 23726,
        "sum": 5694616664,
        "stddev": 13982,
        "p50": 28031,
        "p75": 36095,
        "p90": 39679,
        "p95": 43263,
        "p99": 48639,
        "p99_99": 59391,
        "outofrange": 0,
        "hdrsize": 11376,
        "cnt": 240012
      },
      "rtt": {
        "min": 1580,
        "max": 3389,
        "avg": 2349,
        "sum": 79868,
        "stddev": 474,
        "p50": 2319,
        "p75": 2543,
        "p90": 3183,
        "p95": 3199,
        "p99": 3391,
        "p99_99": 3391,
        "outofrange": 0,
        "hdrsize": 13424,
        "cnt": 34
      },
      "throttle": {
        "min": 0,
        "max": 0,
        "avg": 0,
        "sum": 0,
        "stddev": 0,
        "p50": 0,
        "p75": 0,
        "p90": 0,
        "p95": 0,
        "p99": 0,
        "p99_99": 0,
        "outofrange": 0,
        "hdrsize": 17520,
        "cnt": 34
      },
      "toppars": {
        "test-1": {
          "topic": "test",
          "partition": 1
        }
      }
    },
    "localhost:9093/3": {
      "name": "localhost:9093/3",
      "nodeid": 3,
      "nodename": "localhost:9093",
      "source": "learned",
      "state": "UP",
      "stateage": 9057209,
      "outbuf_cnt": 0,
      "outbuf_msg_cnt": 0,
      "waitresp_cnt": 0,
      "waitresp_msg_cnt": 0,
      "tx": 310,
      "txbytes": 84301122,
      "txerrs": 0,
      "txretries": 0,
      "req_timeouts": 0,
      "rx": 310,
      "rxbytes": 15104,
      "rxerrs": 0,
      "rxcorriderrs": 0,
      "rxpartial": 0,
      "zbuf_grow": 0,
      "buf_grow": 0,
      "wakeups": 607956,
      "int_latency": {
        "min": 82,
        "max": 58069,
        "avg": 23404,
        "sum": 5617432101,
        "stddev": 14021,
        "p50": 27391,
        "p75": 35839,
        "p90": 39679,
        "p95": 42751,
        "p99": 48639,
        "p99_99": 58111,
        "outofrange": 0,
        "hdrsize": 11376,
        "cnt": 240016
      },
      "rtt": {
        "min": 1704,
        "max": 3572,
        "avg": 2493,
        "sum": 87289,
        "stddev": 559,
        "p50": 2447,
        "p75": 2895,
        "p90": 3375,
        "p95": 3407,
        "p99": 3583,
        "p99_99": 3583,
        "outofrange": 0,
        "hdrsize": 13424,
        "cnt": 35
      },
      "throttle": {
        "min": 0,
        "max": 0,
        "avg": 0,
        "sum": 0,
        "stddev": 0,
        "p50": 0,
        "p75": 0,
        "p90": 0,
        "p95": 0,
        "p99": 0,
        "p99_99": 0,
        "outofrange": 0,
        "hdrsize": 17520,
        "cnt": 35
      },
      "toppars": {
        "test-0": {
          "topic": "test",
          "partition": 0
        }
      }
    },
    "localhost:9094/4": {
      "name": "localhost:9094/4",
      "nodeid": 4,
      "nodename": "localhost:9094",
      "source": "learned",
      "state": "UP",
      "stateage": 9057207,
      "outbuf_cnt": 0,
      "outbuf_msg_cnt": 0,
      "waitresp_cnt": 0,
      "waitresp_msg_cnt": 0,
      "tx": 1,
      "txbytes": 25,
      "txerrs": 0,
      "txretries": 0,
      "req_timeouts": 0,
      "rx": 1,
      "rxbytes": 272,
      "rxerrs": 0,
      "rxcorriderrs": 0,
      "rxpartial": 0,
      "zbuf_grow": 0,
      "buf_grow": 0,
      "wakeups": 4,
      "int_latency": {
        "min": 0,
        "max": 0,
        "avg": 0,
        "sum": 0,
        "stddev": 0,
        "p50": 0,
        "p75": 0,
        "p90": 0,
        "p95": 0,
        "p99": 0,
        "p99_99": 0,
        "outofrange": 0,
        "hdrsize": 11376,
        "cnt": 0
      },
      "rtt": {
        "min": 0,
        "max": 0,
        "avg": 0,
        "sum": 0,
        "stddev": 0,
        "p50": 0,
        "p75": 0,
        "p90": 0,
        "p95": 0,
        "p99": 0,
        "p99_99": 0,
        "outofrange": 0,
        "hdrsize": 13424,
        "cnt": 0
      },
      "throttle": {
        "min": 0,
        "max": 0,
        "avg": 0,
        "sum": 0,
        "stddev": 0,
        "p50": 0,
        "p75": 0,
        "p90": 0,
        "p95": 0,
        "p99": 0,
        "p99_99": 0,
        "outofrange": 0,
        "hdrsize": 17520,
        "cnt": 0
      },
      "toppars": {}
    }
  },
  "topics": {
    "test": {
      "topic": "test",
      "metadata_age": 9060,
      "batchsize": {
        "min": 99,
        "max": 391805,
        "avg": 272593,
        "sum": 18808985,
        "stddev": 180408,
        "p50": 393215,
        "p75": 393215,
        "p90": 393215,
        "p95": 393215,
        "p99": 393215,
        "p99_99": 393215,
        "outofrange": 0,
        "hdrsize": 14448,
        "cnt": 69
      },
      "batchcnt": {
        "min": 1,
        "max": 10000,
        "avg": 6956,
        "sum": 480028,
        "stddev": 4608,
        "p50": 10047,
        "p75": 10047,
        "p90": 10047,
        "p95": 10047,
        "p99": 10047,
        "p99_99": 10047,
        "outofrange": 0,
        "hdrsize": 8304,
        "cnt": 69
      },
      "partitions": {
        "0": {
          "partition": 0,
          "broker": 3,
          "leader": 3,
          "desired": false,
          "unknown": false,
          "msgq_cnt": 1,
          "msgq_bytes": 31,
          "xmit_msgq_cnt": 0,
          "xmit_msgq_bytes": 0,
          "fetchq_cnt": 0,
          "fetchq_size": 0,
          "fetch_state": "none",
          "query_offset": 0,
          "next_offset": 0,
          "app_offset": -1001,
          "stored_offset": -1001,
          "commited_offset": -1001,
          "committed_offset": -1001,
          "eof_offset": -1001,
          "lo_offset": -1001,
          "hi_offset": -1001,
          "consumer_lag": -1,
          "txmsgs": 2150617,
          "txbytes": 66669127,
          "rxmsgs": 0,
          "rxbytes": 0,
          "msgs": 2160510,
          "rx_ver_drops": 0
        },
        "1": {
          "partition": 1,
          "broker": 2,
          "leader": 2,
          "desired": false,
          "unknown": false,
          "msgq_cnt": 0,
          "msgq_bytes": 0,
          "xmit_msgq_cnt": 0,
          "xmit_msgq_bytes": 0,
          "fetchq_cnt": 0,
          "fetchq_size": 0,
          "fetch_state": "none",
          "query_offset": 0,
          "next_offset": 0,
          "app_offset": -1001,
          "stored_offset": -1001,
          "commited_offset": -1001,
          "committed_offset": -1001,
          "eof_offset": -1001,
          "lo_offset": -1001,
          "hi_offset": -1001,
          "consumer_lag": -1,
          "txmsgs": 215,
          "txbytes": 66654216,
          "rxmsgs": 3,
          "rxbytes": 0,
          "msgs": 2159735,
          "rx_ver_drops": 0
        },
        "-1": {
          "partition": -1,
          "broker": -1,
          "leader": -1,
          "desired": false,
          "unknown": false,
          "msgq_cnt": 0,
          "msgq_bytes": 0,
          "xmit_msgq_cnt": 0,
          "xmit_msgq_bytes": 0,
          "fetchq_cnt": 0,
          "fetchq_size": 0,
          "fetch_state": "none",
          "query_offset": 0,
          "next_offset": 0,
          "app_offset": -1001,
          "stored_offset": -1001,
          "commited_offset": -1001,
          "committed_offset": -1001,
          "eof_offset": -1001,
          "lo_offset": -1001,
          "hi_offset": -1001,
          "consumer_lag": -1,
          "txmsgs": 0,
          "txbytes": 0,
          "rxmsgs": 0,
          "rxbytes": 0,
          "msgs": 1177,
          "rx_ver_drops": 0
        }
      }
    }
  },
  "tx": 631,
  "tx_bytes": 168584479,
  "rx": 631,
  "rx_bytes": 31084,
  "txmsgs": 4300753,
  "txmsg_bytes": 133323343,
  "rxmsgs": 0,
  "rxmsg_bytes": 0
}
`
