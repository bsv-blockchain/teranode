package kafka

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBytesWritten          prometheus.Counter
	prometheusWriteDuration         prometheus.Histogram
	prometheusWriteErrors           prometheus.Counter
	prometheusE2EDuration           prometheus.Histogram
	prometheusBatchRecords          prometheus.Counter
	prometheusBatchCompressedBytes  prometheus.Counter
	prometheusSlowTransferDetected  *prometheus.CounterVec
	prometheusProduceRequestLatency prometheus.Histogram
	prometheusBrokerConnects        prometheus.Counter
	prometheusBrokerDisconnects     prometheus.Counter
	prometheusBackpressureSignals   *prometheus.CounterVec
	prometheusBufferedMessages      *prometheus.GaugeVec
)

var prometheusMetricsInitOnce sync.Once

func initProducerMetrics() {
	prometheusMetricsInitOnce.Do(_initProducerMetrics)
}

func _initProducerMetrics() {
	prometheusBytesWritten = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "bytes_written_total",
			Help:      "Total bytes written to Kafka brokers",
		},
	)
	prometheusWriteDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "write_duration_seconds",
			Help:      "Time to write produce requests to brokers",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusWriteErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "write_errors_total",
			Help:      "Total broker write errors encountered during produce",
		},
	)
	prometheusE2EDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "e2e_duration_seconds",
			Help:      "End-to-end duration from writing a produce request to reading its response",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)
	prometheusBatchRecords = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "batch_records_total",
			Help:      "Total records successfully produced in batches",
		},
	)
	prometheusBatchCompressedBytes = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "batch_compressed_bytes_total",
			Help:      "Total compressed bytes of successfully produced batches",
		},
	)
	prometheusSlowTransferDetected = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "slow_transfer_detected_total",
			Help:      "Number of sustained slow transfer conditions detected (transfer rate below threshold)",
		},
		[]string{"topic"},
	)
	prometheusProduceRequestLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "produce_request_latency_seconds",
			Help:      "Latency of produce requests (API key 0) including write wait",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)
	prometheusBrokerConnects = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "broker_connects_total",
			Help:      "Total broker connection events",
		},
	)
	prometheusBrokerDisconnects = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "broker_disconnects_total",
			Help:      "Total broker disconnection events",
		},
	)
	prometheusBackpressureSignals = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "backpressure_signals_total",
			Help:      "Number of times producer backlog exceeded backpressure threshold",
		},
		[]string{"topic"},
	)
	prometheusBufferedMessages = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "kafka_producer",
			Name:      "buffered_messages",
			Help:      "Current number of buffered messages in adaptive producer loop",
		},
		[]string{"topic"},
	)
}
