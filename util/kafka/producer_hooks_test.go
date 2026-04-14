package kafka

import (
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func newTestHook(t *testing.T, cfg SlowTransferConfig) (*producerMetricsHook, *mockAsyncLogger) {
	t.Helper()
	initProducerMetrics()
	logger := &mockAsyncLogger{}
	h := newProducerMetricsHook(logger, "test-topic", cfg)
	return h, logger
}

func counterValue(c prometheus.Counter) float64 {
	m := &io_prometheus_client.Metric{}
	_ = c.(prometheus.Metric).Write(m)
	return m.GetCounter().GetValue()
}

func counterVecValue(cv *prometheus.CounterVec, label string) float64 {
	c, err := cv.GetMetricWithLabelValues(label)
	if err != nil {
		return 0
	}
	return counterValue(c)
}

func histogramCount(h prometheus.Histogram) uint64 {
	m := &io_prometheus_client.Metric{}
	_ = h.(prometheus.Metric).Write(m)
	return m.GetHistogram().GetSampleCount()
}

// --- Broker connect/disconnect ---

func TestHook_BrokerConnect(t *testing.T) {
	h, _ := newTestHook(t, DefaultSlowTransferConfig())
	before := counterValue(prometheusBrokerConnects)

	h.OnBrokerConnect(kgo.BrokerMetadata{}, time.Millisecond, &net.TCPConn{}, nil)
	assert.Equal(t, before+1, counterValue(prometheusBrokerConnects))

	// Error case: should not increment
	h.OnBrokerConnect(kgo.BrokerMetadata{}, time.Millisecond, nil, net.ErrClosed)
	assert.Equal(t, before+1, counterValue(prometheusBrokerConnects))
}

func TestHook_BrokerDisconnect(t *testing.T) {
	h, _ := newTestHook(t, DefaultSlowTransferConfig())
	before := counterValue(prometheusBrokerDisconnects)

	h.OnBrokerDisconnect(kgo.BrokerMetadata{}, &net.TCPConn{})
	assert.Equal(t, before+1, counterValue(prometheusBrokerDisconnects))
}

// --- Broker write metrics ---

func TestHook_BrokerWrite_ProduceRequest(t *testing.T) {
	h, _ := newTestHook(t, DefaultSlowTransferConfig())
	bytesBefore := counterValue(prometheusBytesWritten)
	writeDurBefore := histogramCount(prometheusWriteDuration)

	h.OnBrokerWrite(kgo.BrokerMetadata{}, produceAPIKey, 4096, time.Millisecond, 5*time.Millisecond, nil)

	assert.Equal(t, bytesBefore+4096, counterValue(prometheusBytesWritten))
	assert.Equal(t, writeDurBefore+1, histogramCount(prometheusWriteDuration))
}

func TestHook_BrokerWrite_NonProduceIgnored(t *testing.T) {
	h, _ := newTestHook(t, DefaultSlowTransferConfig())
	bytesBefore := counterValue(prometheusBytesWritten)

	h.OnBrokerWrite(kgo.BrokerMetadata{}, 1, 4096, 0, time.Millisecond, nil) // key=1 is Fetch
	assert.Equal(t, bytesBefore, counterValue(prometheusBytesWritten))
}

func TestHook_BrokerWrite_Error(t *testing.T) {
	h, _ := newTestHook(t, DefaultSlowTransferConfig())
	errorsBefore := counterValue(prometheusWriteErrors)

	h.OnBrokerWrite(kgo.BrokerMetadata{}, produceAPIKey, 100, 0, time.Millisecond, net.ErrClosed)
	assert.Equal(t, errorsBefore+1, counterValue(prometheusWriteErrors))
}

// --- E2E metrics ---

func TestHook_BrokerE2E_ProduceRequest(t *testing.T) {
	h, _ := newTestHook(t, DefaultSlowTransferConfig())
	e2eBefore := histogramCount(prometheusE2EDuration)
	latBefore := histogramCount(prometheusProduceRequestLatency)

	e2e := kgo.BrokerE2E{
		BytesWritten: 1024,
		BytesRead:    256,
		WriteWait:    2 * time.Millisecond,
		TimeToWrite:  3 * time.Millisecond,
		ReadWait:     1 * time.Millisecond,
		TimeToRead:   2 * time.Millisecond,
	}
	h.OnBrokerE2E(kgo.BrokerMetadata{}, produceAPIKey, e2e)

	assert.Equal(t, e2eBefore+1, histogramCount(prometheusE2EDuration))
	assert.Equal(t, latBefore+1, histogramCount(prometheusProduceRequestLatency))
}

func TestHook_BrokerE2E_NonProduceIgnored(t *testing.T) {
	h, _ := newTestHook(t, DefaultSlowTransferConfig())
	e2eBefore := histogramCount(prometheusE2EDuration)

	e2e := kgo.BrokerE2E{TimeToWrite: time.Millisecond, TimeToRead: time.Millisecond}
	h.OnBrokerE2E(kgo.BrokerMetadata{}, 1, e2e) // Fetch key

	assert.Equal(t, e2eBefore, histogramCount(prometheusE2EDuration))
}

// --- Batch written ---

func TestHook_ProduceBatchWritten(t *testing.T) {
	h, _ := newTestHook(t, DefaultSlowTransferConfig())
	recsBefore := counterValue(prometheusBatchRecords)
	bytesBefore := counterValue(prometheusBatchCompressedBytes)

	metrics := kgo.ProduceBatchMetrics{
		NumRecords:        50,
		UncompressedBytes: 8192,
		CompressedBytes:   4096,
		CompressionType:   1,
	}
	h.OnProduceBatchWritten(kgo.BrokerMetadata{}, "test-topic", 0, metrics)

	assert.Equal(t, recsBefore+50, counterValue(prometheusBatchRecords))
	assert.Equal(t, bytesBefore+4096, counterValue(prometheusBatchCompressedBytes))
}

// --- Slow transfer detection ---

func TestHook_SlowTransfer_DetectedAfterWindow(t *testing.T) {
	cfg := SlowTransferConfig{
		ThresholdBps: 100 * 1024,
		Window:       5 * time.Second,
		Cooldown:     30 * time.Second,
	}
	h, logger := newTestHook(t, cfg)

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.nowFunc = func() time.Time { return now }

	alertsBefore := counterVecValue(prometheusSlowTransferDetected, "test-topic")

	// Simulate slow writes: 50 bytes / 1ms = 50 KB/s, below 100 KB/s threshold
	for i := 0; i < 60; i++ {
		now = now.Add(100 * time.Millisecond)
		h.OnBrokerWrite(kgo.BrokerMetadata{}, produceAPIKey, 50, 0, time.Millisecond, nil)
	}

	alertsAfter := counterVecValue(prometheusSlowTransferDetected, "test-topic")
	assert.Greater(t, alertsAfter, alertsBefore, "slow transfer alert should have fired")
	assert.Greater(t, logger.warnCount, 0, "warning should have been logged")
}

func TestHook_SlowTransfer_NotDetectedWhenFast(t *testing.T) {
	cfg := SlowTransferConfig{
		ThresholdBps: 100 * 1024,
		Window:       5 * time.Second,
		Cooldown:     30 * time.Second,
	}
	h, logger := newTestHook(t, cfg)

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.nowFunc = func() time.Time { return now }

	alertsBefore := counterVecValue(prometheusSlowTransferDetected, "test-topic")

	// Fast writes: 1MB / 1ms = 1 GB/s
	for i := 0; i < 60; i++ {
		now = now.Add(100 * time.Millisecond)
		h.OnBrokerWrite(kgo.BrokerMetadata{}, produceAPIKey, 1024*1024, 0, time.Millisecond, nil)
	}

	assert.Equal(t, alertsBefore, counterVecValue(prometheusSlowTransferDetected, "test-topic"))
	assert.Equal(t, 0, logger.warnCount)
}

func TestHook_SlowTransfer_CooldownPreventsSpam(t *testing.T) {
	cfg := SlowTransferConfig{
		ThresholdBps: 100 * 1024,
		Window:       1 * time.Second,
		Cooldown:     10 * time.Second,
	}
	h, _ := newTestHook(t, cfg)

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.nowFunc = func() time.Time { return now }

	alertsBefore := counterVecValue(prometheusSlowTransferDetected, "test-topic")

	// Slow writes for 5s — only one alert should fire within the 10s cooldown
	for i := 0; i < 50; i++ {
		now = now.Add(100 * time.Millisecond)
		h.OnBrokerWrite(kgo.BrokerMetadata{}, produceAPIKey, 10, 0, time.Millisecond, nil)
	}

	alertsAfter := counterVecValue(prometheusSlowTransferDetected, "test-topic")
	assert.Equal(t, alertsBefore+1, alertsAfter, "only one alert should fire during cooldown period")
}

func TestHook_SlowTransfer_RecoveryResetsState(t *testing.T) {
	cfg := SlowTransferConfig{
		ThresholdBps: 100 * 1024,
		Window:       2 * time.Second,
		Cooldown:     1 * time.Second,
	}
	h, _ := newTestHook(t, cfg)

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.nowFunc = func() time.Time { return now }

	// Start slow
	for i := 0; i < 30; i++ {
		now = now.Add(100 * time.Millisecond)
		h.OnBrokerWrite(kgo.BrokerMetadata{}, produceAPIKey, 10, 0, time.Millisecond, nil)
	}

	alertsAfterSlow := counterVecValue(prometheusSlowTransferDetected, "test-topic")

	// Recover with fast writes
	for i := 0; i < 30; i++ {
		now = now.Add(100 * time.Millisecond)
		h.OnBrokerWrite(kgo.BrokerMetadata{}, produceAPIKey, 1024*1024, 0, time.Millisecond, nil)
	}

	alertsAfterRecovery := counterVecValue(prometheusSlowTransferDetected, "test-topic")
	assert.Equal(t, alertsAfterSlow, alertsAfterRecovery, "no new alerts after recovery")
}

// --- Interface compliance ---

func TestHook_ImplementsFranzInterfaces(t *testing.T) {
	h, _ := newTestHook(t, DefaultSlowTransferConfig())

	var _ kgo.HookBrokerConnect = h
	var _ kgo.HookBrokerDisconnect = h
	var _ kgo.HookBrokerWrite = h
	var _ kgo.HookBrokerE2E = h
	var _ kgo.HookProduceBatchWritten = h
}

// --- Integration: hook is wired into producer ---

func TestNewKafkaAsyncProducer_HooksWired(t *testing.T) {
	logger := &mockAsyncLogger{}
	kafkaURL, err := url.Parse("memory://localhost/hooks-test")
	require.NoError(t, err)

	_, err = NewKafkaAsyncProducerFromURL(t.Context(), logger, kafkaURL, nil)
	require.NoError(t, err)

	// Metrics should be initialized (package-level init is idempotent)
	initProducerMetrics()
	assert.NotNil(t, prometheusBytesWritten)
	assert.NotNil(t, prometheusSlowTransferDetected)
}

// --- Config defaults ---

func TestDefaultSlowTransferConfig(t *testing.T) {
	cfg := DefaultSlowTransferConfig()
	assert.Equal(t, float64(100*1024), cfg.ThresholdBps)
	assert.Equal(t, 5*time.Second, cfg.Window)
	assert.Equal(t, 30*time.Second, cfg.Cooldown)
}

func TestSlowTransferConfig_ZeroUsesDefaults(t *testing.T) {
	cfg := KafkaProducerConfig{
		SlowTransfer: SlowTransferConfig{},
	}
	assert.Zero(t, cfg.SlowTransfer.ThresholdBps)

	if cfg.SlowTransfer.ThresholdBps == 0 {
		cfg.SlowTransfer = DefaultSlowTransferConfig()
	}
	assert.Equal(t, float64(100*1024), cfg.SlowTransfer.ThresholdBps)
}
