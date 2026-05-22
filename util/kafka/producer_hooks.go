package kafka

import (
	"net"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// DefaultSlowTransferThresholdBps is the default bytes-per-second threshold
	// below which a transfer is considered slow. The ticket specifies 100 KB/s.
	DefaultSlowTransferThresholdBps = 100 * 1024

	// DefaultSlowTransferWindow is the minimum duration the rolling transfer
	// rate must remain below the threshold before a slow-send alert fires.
	DefaultSlowTransferWindow = 5 * time.Second

	// DefaultSlowTransferCooldown prevents log/metric spam by enforcing a
	// minimum interval between consecutive slow-transfer alerts.
	DefaultSlowTransferCooldown = 30 * time.Second

	// maxWriteSamples is the ring buffer capacity for broker write observations.
	maxWriteSamples = 512

	// produceAPIKey is the Kafka protocol API key for Produce requests.
	produceAPIKey int16 = 0
)

// SlowTransferConfig holds tunable thresholds for slow-send detection.
type SlowTransferConfig struct {
	ThresholdBps float64
	Window       time.Duration
	Cooldown     time.Duration
}

// DefaultSlowTransferConfig returns a config with the ticket-specified defaults.
func DefaultSlowTransferConfig() SlowTransferConfig {
	return SlowTransferConfig{
		ThresholdBps: DefaultSlowTransferThresholdBps,
		Window:       DefaultSlowTransferWindow,
		Cooldown:     DefaultSlowTransferCooldown,
	}
}

// writeSample records a single broker write observation.
type writeSample struct {
	ts    time.Time
	bytes int
	dur   time.Duration
}

// producerMetricsHook implements franz-go hook interfaces to collect Prometheus
// metrics and detect sustained slow-transfer conditions on produce writes.
//
// It implements:
//   - kgo.HookBrokerConnect
//   - kgo.HookBrokerDisconnect
//   - kgo.HookBrokerWrite
//   - kgo.HookBrokerE2E
//   - kgo.HookProduceBatchWritten
type producerMetricsHook struct {
	logger ulogger.Logger
	topic  string
	cfg    SlowTransferConfig

	mu            sync.Mutex
	samples       []writeSample
	sampleIdx     int
	sampleCount   int
	lastSlowAlert time.Time
	slowOngoing   bool
	slowOnsetTime time.Time
	nowFunc       func() time.Time // pluggable clock for testing
	onSlowState   func(slow bool, rateBps float64)
}

func newProducerMetricsHook(logger ulogger.Logger, topic string, cfg SlowTransferConfig) *producerMetricsHook {
	initProducerMetrics()
	return &producerMetricsHook{
		logger:  logger,
		topic:   topic,
		cfg:     cfg,
		samples: make([]writeSample, maxWriteSamples),
		nowFunc: time.Now,
	}
}

func (h *producerMetricsHook) setSlowStateHandler(handler func(slow bool, rateBps float64)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onSlowState = handler
}

// --- kgo.HookBrokerConnect ---

func (h *producerMetricsHook) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil {
		prometheusBrokerConnects.Inc()
	}
}

// --- kgo.HookBrokerDisconnect ---

func (h *producerMetricsHook) OnBrokerDisconnect(_ kgo.BrokerMetadata, _ net.Conn) {
	prometheusBrokerDisconnects.Inc()
}

// --- kgo.HookBrokerWrite ---

func (h *producerMetricsHook) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, bytesWritten int, _ time.Duration, timeToWrite time.Duration, err error) {
	if key != produceAPIKey {
		return
	}

	prometheusBytesWritten.Add(float64(bytesWritten))
	prometheusWriteDuration.Observe(timeToWrite.Seconds())

	if err != nil {
		prometheusWriteErrors.Inc()
		return
	}

	if bytesWritten > 0 && timeToWrite > 0 {
		h.recordSample(bytesWritten, timeToWrite)
	}
}

// --- kgo.HookBrokerE2E ---

func (h *producerMetricsHook) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {
	if key != produceAPIKey {
		return
	}

	prometheusE2EDuration.Observe(e2e.DurationE2E().Seconds())
	prometheusProduceRequestLatency.Observe((e2e.WriteWait + e2e.DurationE2E()).Seconds())

	if e2e.WriteErr != nil {
		prometheusWriteErrors.Inc()
	}
}

// --- kgo.HookProduceBatchWritten ---

func (h *producerMetricsHook) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, _ int32, metrics kgo.ProduceBatchMetrics) {
	prometheusBatchRecords.Add(float64(metrics.NumRecords))
	prometheusBatchCompressedBytes.Add(float64(metrics.CompressedBytes))
}

// recordSample appends a write observation to the ring buffer and evaluates
// whether the rolling transfer rate falls below the slow-send threshold.
func (h *producerMetricsHook) recordSample(bytes int, dur time.Duration) {
	now := h.nowFunc()
	var callback func(bool, float64)
	var callbackSlow bool
	var callbackRate float64
	var fireCallback bool

	h.mu.Lock()

	h.samples[h.sampleIdx] = writeSample{ts: now, bytes: bytes, dur: dur}
	h.sampleIdx = (h.sampleIdx + 1) % maxWriteSamples
	if h.sampleCount < maxWriteSamples {
		h.sampleCount++
	}

	fireCallback, callbackSlow, callbackRate = h.evaluateSlowTransfer(now)
	callback = h.onSlowState
	h.mu.Unlock()

	if fireCallback && callback != nil {
		callback(callbackSlow, callbackRate)
	}
}

// evaluateSlowTransfer checks if the rolling average transfer rate over the
// configured window is below the threshold. Must be called with h.mu held.
func (h *producerMetricsHook) evaluateSlowTransfer(now time.Time) (bool, bool, float64) {
	cutoff := now.Add(-h.cfg.Window)

	var totalBytes int
	var totalDur time.Duration
	count := 0

	n := h.sampleCount
	for i := 0; i < n; i++ {
		idx := (h.sampleIdx - 1 - i + maxWriteSamples) % maxWriteSamples
		s := h.samples[idx]
		if s.ts.Before(cutoff) {
			break
		}
		totalBytes += s.bytes
		totalDur += s.dur
		count++
	}

	if count == 0 || totalDur == 0 {
		wasSlow := h.slowOngoing
		h.slowOngoing = false
		if wasSlow {
			h.logger.Infof("[kafka] transfer recovered on topic %s", h.topic)
			return true, false, 0
		}
		return false, false, 0
	}

	rateBps := float64(totalBytes) / totalDur.Seconds()

	if rateBps >= h.cfg.ThresholdBps {
		wasSlow := h.slowOngoing
		h.slowOngoing = false
		if wasSlow {
			h.logger.Infof("[kafka] transfer recovered on topic %s: %.1f KB/s", h.topic, rateBps/1024)
			return true, false, rateBps
		}
		return false, false, rateBps
	}

	if !h.slowOngoing {
		h.slowOngoing = true
		h.slowOnsetTime = now
		return true, true, rateBps
	}

	sustainedFor := now.Sub(h.slowOnsetTime)
	if sustainedFor < h.cfg.Window {
		return false, true, rateBps
	}

	if now.Sub(h.lastSlowAlert) < h.cfg.Cooldown {
		return false, true, rateBps
	}

	h.lastSlowAlert = now
	rateKBps := rateBps / 1024

	prometheusSlowTransferDetected.WithLabelValues(h.topic).Inc()
	h.logger.Warnf("[kafka] slow transfer detected on topic %s: %.1f KB/s (threshold %.1f KB/s) sustained for %v over %d writes",
		h.topic, rateKBps, h.cfg.ThresholdBps/1024, sustainedFor.Round(time.Millisecond), count)
	return false, true, rateBps
}

// compile-time interface assertions
var (
	_ kgo.HookBrokerConnect       = (*producerMetricsHook)(nil)
	_ kgo.HookBrokerDisconnect    = (*producerMetricsHook)(nil)
	_ kgo.HookBrokerWrite         = (*producerMetricsHook)(nil)
	_ kgo.HookBrokerE2E           = (*producerMetricsHook)(nil)
	_ kgo.HookProduceBatchWritten = (*producerMetricsHook)(nil)
)
