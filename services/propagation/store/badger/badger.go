package badger

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel"
)

func init() {
	badgerExpvarCollector := collectors.NewExpvarCollector(map[string]*prometheus.Desc{
		"badger_blocked_puts_total":   prometheus.NewDesc("badger_blocked_puts_total", "Blocked Puts", nil, nil),
		"badger_disk_reads_total":     prometheus.NewDesc("badger_disk_reads_total", "Disk Reads", nil, nil),
		"badger_disk_writes_total":    prometheus.NewDesc("badger_disk_writes_total", "Disk Writes", nil, nil),
		"badger_gets_total":           prometheus.NewDesc("badger_gets_total", "Gets", nil, nil),
		"badger_puts_total":           prometheus.NewDesc("badger_puts_total", "Puts", nil, nil),
		"badger_memtable_gets_total":  prometheus.NewDesc("badger_memtable_gets_total", "Memtable gets", nil, nil),
		"badger_lsm_size_bytes":       prometheus.NewDesc("badger_lsm_size_bytes", "LSM Size in bytes", []string{"database"}, nil),
		"badger_vlog_size_bytes":      prometheus.NewDesc("badger_vlog_size_bytes", "Value Log Size in bytes", []string{"database"}, nil),
		"badger_pending_writes_total": prometheus.NewDesc("badger_pending_writes_total", "Pending Writes", []string{"database"}, nil),
		"badger_read_bytes":           prometheus.NewDesc("badger_read_bytes", "Read bytes", nil, nil),
		"badger_written_bytes":        prometheus.NewDesc("badger_written_bytes", "Written bytes", nil, nil),
		"badger_lsm_bloom_hits_total": prometheus.NewDesc("badger_lsm_bloom_hits_total", "LSM Bloom Hits", []string{"level"}, nil),
		"badger_lsm_level_gets_total": prometheus.NewDesc("badger_lsm_level_gets_total", "LSM Level Gets", []string{"level"}, nil),
	})
	prometheus.MustRegister(badgerExpvarCollector)
}

type Badger struct {
	store  *badger.DB
	logger utils.Logger
	// mu     sync.RWMutex
}

type loggerWrapper struct {
	*gocore.Logger
}

func (l loggerWrapper) Warningf(format string, args ...interface{}) {
	l.Warnf(format, args...)
}

func New(dir string) (*Badger, error) {
	logLevel, _ := gocore.Config().Get("logLevel")
	logger := loggerWrapper{gocore.Log("bdgr", gocore.NewLogLevelFromString(logLevel))}

	opts := badger.DefaultOptions(dir).
		WithLogger(logger).
		WithLoggingLevel(badger.ERROR).WithNumMemtables(32).
		WithMetricsEnabled(true)
	s, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	badgerStore := &Badger{
		store:  s,
		logger: logger,
	}

	return badgerStore, nil
}

func (s *Badger) Close(ctx context.Context) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_badger").NewStat("Close").AddTime(start)
	}()
	span, _ := opentracing.StartSpanFromContext(ctx, "badger:Close")
	defer span.Finish()

	return s.store.Close()
}

func (s *Badger) Set(ctx context.Context, key []byte, value []byte) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_badger").NewStat("Set").AddTime(start)
	}()

	_, span := otel.Tracer("").Start(ctx, "Badger:Set")
	defer span.End()

	if err := s.store.Update(func(tx *badger.Txn) error {
		return tx.Set(key, value)
	}); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to set data: %w", err)
	}

	return nil
}

func (s *Badger) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_badger").NewStat("Get").AddTime(start)
	}()

	_, span := otel.Tracer("").Start(ctx, "Badger:Get")
	defer span.End()

	var result []byte
	err := s.store.View(func(tx *badger.Txn) error {
		data, err := tx.Get(hash)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("key not found: %w", err)
			}
			span.RecordError(err)
			return err
		}

		if err = data.Value(func(val []byte) error {
			result = val
			return nil
		}); err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to decode data: %w", err)
		}

		return nil
	})

	return result, err
}

func (s *Badger) Del(ctx context.Context, hash []byte) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_badger").NewStat("Del").AddTime(start)
	}()

	_, span := otel.Tracer("").Start(ctx, "Badger:Del")
	defer span.End()

	err := s.store.Update(func(tx *badger.Txn) error {
		return tx.Delete(hash)
	})

	if err != nil {
		span.RecordError(err)
	}

	return err
}
