// Package metered provides a Prometheus wrapper for blob.Store implementations.
//
// The wrapper counts reads, writes and failures of a blob store and measures how long they take.
// Each store has a name, which becomes the store label of every metric. Not-found results are not failures.
// A call that the caller cancels is not a failure either.
package metered

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	_ blob.Store = (*Metered)(nil)

	metricsOnce sync.Once

	prometheusBlobReads         *prometheus.CounterVec
	prometheusBlobWrites        *prometheus.CounterVec
	prometheusBlobErrors        *prometheus.CounterVec
	prometheusBlobReadDuration  *prometheus.HistogramVec
	prometheusBlobWriteDuration *prometheus.HistogramVec
)

var durationBuckets = prometheus.ExponentialBuckets(0.0005, 2, 16)

func counterVec(name, help string) *prometheus.CounterVec {
	return promauto.NewCounterVec(
		prometheus.CounterOpts{Namespace: "teranode", Subsystem: "blob", Name: name, Help: help},
		[]string{"store"},
	)
}

func histogramVec(name, help string) *prometheus.HistogramVec {
	return promauto.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: "teranode", Subsystem: "blob", Name: name, Help: help, Buckets: durationBuckets},
		[]string{"store"},
	)
}

func initPrometheusMetrics() {
	metricsOnce.Do(func() {
		prometheusBlobReads = counterVec("reads_total", "Number of read calls to a blob store (Exists, Get and GetIoReader), by store")
		prometheusBlobWrites = counterVec("writes_total", "Number of write calls to a blob store (Set, SetFromReader, SetDAH and Del), by store")
		prometheusBlobErrors = counterVec("errors_total", "Number of blob store calls that failed, by store. Not-found results and cancelled calls are not counted")
		prometheusBlobReadDuration = histogramVec("read_duration_seconds", "Duration of blob store read calls in seconds, by store. GetIoReader measures the time to open the reader")
		prometheusBlobWriteDuration = histogramVec("write_duration_seconds", "Duration of blob store write calls in seconds, by store. SetFromReader includes the time to read the source")
	})
}

// Metered wraps a blob.Store and records Prometheus metrics for every call.
type Metered struct {
	store         blob.Store
	reads         prometheus.Counter
	writes        prometheus.Counter
	failures      prometheus.Counter
	readDuration  prometheus.Observer
	writeDuration prometheus.Observer
}

// New wraps store and labels its metrics with name. All series exist from the start, so an idle store reads 0.
func New(name string, store blob.Store) *Metered {
	initPrometheusMetrics()

	return &Metered{
		store:         store,
		reads:         prometheusBlobReads.WithLabelValues(name),
		writes:        prometheusBlobWrites.WithLabelValues(name),
		failures:      prometheusBlobErrors.WithLabelValues(name),
		readDuration:  prometheusBlobReadDuration.WithLabelValues(name),
		writeDuration: prometheusBlobWriteDuration.WithLabelValues(name),
	}
}

func (m *Metered) read(start time.Time, err error) {
	m.reads.Inc()
	m.readDuration.Observe(time.Since(start).Seconds())
	m.fail(err)
}

func (m *Metered) write(start time.Time, err error) {
	m.writes.Inc()
	m.writeDuration.Observe(time.Since(start).Seconds())
	m.fail(err)
}

func (m *Metered) fail(err error) {
	if err == nil || errors.Is(err, errors.ErrNotFound) || errors.Is(err, context.Canceled) {
		return
	}

	m.failures.Inc()
}

func (m *Metered) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return m.store.Health(ctx, checkLiveness)
}

func (m *Metered) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	start := time.Now()
	exists, err := m.store.Exists(ctx, key, fileType, opts...)
	m.read(start, err)

	return exists, err
}

func (m *Metered) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	start := time.Now()
	value, err := m.store.Get(ctx, key, fileType, opts...)
	m.read(start, err)

	return value, err
}

func (m *Metered) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	start := time.Now()
	reader, err := m.store.GetIoReader(ctx, key, fileType, opts...)
	m.read(start, err)

	return reader, err
}

func (m *Metered) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	start := time.Now()
	err := m.store.Set(ctx, key, fileType, value, opts...)
	m.write(start, err)

	return err
}

func (m *Metered) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	start := time.Now()
	err := m.store.SetFromReader(ctx, key, fileType, reader, opts...)
	m.write(start, err)

	return err
}

func (m *Metered) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	start := time.Now()
	err := m.store.SetDAH(ctx, key, fileType, dah, opts...)
	m.write(start, err)

	return err
}

func (m *Metered) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	start := time.Now()
	err := m.store.Del(ctx, key, fileType, opts...)
	m.write(start, err)

	return err
}

func (m *Metered) Close(ctx context.Context) error {
	return m.store.Close(ctx)
}

func (m *Metered) SetCurrentBlockHeight(height uint32) {
	m.store.SetCurrentBlockHeight(height)
}
