package prometheus

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
	"strings"
)

// Constants
const (
	// Sarama Metrics
	RecordSendRateForTopicPrefix = "record-send-rate-for-topic-"
)

// The MetricsServer Struct
type MetricsServer struct {
	logger                 *zap.Logger
	server                 *http.Server
	httpPort               string
	path                   string
	producedMsgCountGauges *prometheus.GaugeVec
}

// MetricsServer Constructor
func NewMetricsServer(logger *zap.Logger, httpPort string, path string) *MetricsServer {

	// Create The MetricsServer Instance
	metricsServer := &MetricsServer{
		logger:   logger,
		httpPort: httpPort,
		path:     path,
		producedMsgCountGauges: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "eventing_kafka_produced_msg_count",
		}, []string{"topic"}),
	}

	// Register Produced/Received Message Count Gauges
	producedErr := prometheus.Register(metricsServer.producedMsgCountGauges)
	if producedErr != nil {
		logger.Error("Failed To Register ProducedMsgCountGauges", zap.Error(producedErr))
	}

	// Initialize The HTTP Server
	metricsServer.initializeServer()

	// Return The MetricsServer
	return metricsServer
}

// Start The Metrics HTTP Server Listening For Requests
func (m *MetricsServer) Start() {
	m.logger.Info("Starting Prometheus Metrics HTTP Server")
	go func() {
		err := m.server.ListenAndServe()
		if err != nil {
			m.logger.Info("Prometheus Metrics HTTP ListenAndServe Returned Error", zap.Error(err)) // Info log since it could just be normal shutdown
		}
	}()
}

// Stop The Metrics HTTP Server Listening For Requests
func (m *MetricsServer) Stop() {
	m.logger.Info("Stopping Prometheus Metrics HTTP Server")
	err := m.server.Shutdown(context.TODO())
	if err != nil {
		m.logger.Error("Failed To Shutdown Prometheus Metrics HTTP Server", zap.Error(err))
	}
}

// Initialize The Prometheus Metrics HTTP Server
func (m *MetricsServer) initializeServer() {

	// Create The ServeMux
	serveMux := http.NewServeMux()

	// Configure The Metrics Path To Use The Default Prometheus Handler
	serveMux.Handle(m.path, promhttp.Handler())

	// Create The Server For Configured HTTP Port
	server := &http.Server{Addr: ":" + m.httpPort, Handler: serveMux}

	// Set The Initialized HTTP Server
	m.server = server
}

//
// Track The Sarama Metrics (go-metrics) In Our Prometheus Server
//
// NOTE - Sarama provides lots of metrics which would be good to expose in Prometheus,
//        but for now we're just quickly parsing out message counts.  This is for rough
//        parity with the prior Confluent implementation and due to uncertainty around
//        integrating with Knative Observability and potentially Sarama v2 using
//        OpenTelemetry directly as described here...
//
//			https://github.com/Shopify/sarama/issues/1340
//
//        If we do decide to expose all available metrics, the following library might be useful...
//
//          https://github.com/deathowl/go-metrics-prometheus
//
//        Further the Sarama Consumer metrics don't track messages so we might need/want
//        to manually track produced/consumed messages at the Topic/Partition/ConsumerGroup
//        level.
//
func (m *MetricsServer) ObserveMetrics(metrics map[string]map[string]interface{}) {

	// Validate The Metrics
	if len(metrics) > 0 {

		// Loop Over The Observed Metrics
		for metricKey, metricValue := range metrics {

			// Only Handle Specific Metrics
			if strings.HasPrefix(metricKey, RecordSendRateForTopicPrefix) {
				topicName := strings.TrimPrefix(metricKey, RecordSendRateForTopicPrefix)
				msgCount, ok := metricValue["count"].(int64)
				if ok {
					m.producedMsgCountGauges.WithLabelValues(topicName).Set(float64(msgCount))
				} else {
					m.logger.Warn("Encountered Non Int64 'count' Field In Metric", zap.String("Metric", metricKey))
				}
			}
		}
	}
}
