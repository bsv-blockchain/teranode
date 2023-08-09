package badger

import (
	"context"
	"fmt"
	"time"

	"github.com/TAAL-GmbH/ubsv/stores/blob/options"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/dgraph-io/badger/v3"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
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
	logger := loggerWrapper{gocore.Log("bdgr")}

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
	traceSpan := tracing.Start(ctx, "Badger:Close")
	defer traceSpan.Finish()

	return s.store.Close()
}

func (s *Badger) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_badger").NewStat("Set").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "Badger:Set")
	defer traceSpan.Finish()

	setOptions := options.NewSetOptions(opts...)

	if err := s.store.Update(func(tx *badger.Txn) error {
		entry := badger.NewEntry(key, value)
		if setOptions.TTL > 0 {
			entry = entry.WithTTL(setOptions.TTL)
		}
		return tx.SetEntry(entry)
	}); err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to set data: %w", err)
	}

	return nil
}

func (s *Badger) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_badger").NewStat("SetTTL").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "Badger:SetTTL")
	defer traceSpan.Finish()

	// badger does not allow updating the TTL, so we just have to set a new object
	objectBytes, err := s.Get(ctx, key)
	if err != nil {
		traceSpan.RecordError(err)
		return fmt.Errorf("failed to get data: %w", err)
	}

	return s.Set(ctx, key, objectBytes, options.WithTTL(ttl))
}

func (s *Badger) Get(ctx context.Context, hash []byte) ([]byte, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_badger").NewStat("Get").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "Badger:Get")
	defer traceSpan.Finish()

	var result []byte
	err := s.store.View(func(tx *badger.Txn) error {
		data, err := tx.Get(hash)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("key not found: %w", err)
			}
			traceSpan.RecordError(err)
			return err
		}

		if err = data.Value(func(val []byte) error {
			result = val
			return nil
		}); err != nil {
			traceSpan.RecordError(err)
			return fmt.Errorf("failed to decode data: %w", err)
		}

		return nil
	})

	return result, err
}

func (s *Badger) Exists(ctx context.Context, hash []byte) (bool, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_badger").NewStat("Exists").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "Badger:Exists")
	defer traceSpan.Finish()

	err := s.store.View(func(tx *badger.Txn) error {
		_, err := tx.Get(hash)
		if err != nil {
			traceSpan.RecordError(err)
			return err
		}

		return nil
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (s *Badger) Del(ctx context.Context, hash []byte) error {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("prop_store_badger").NewStat("Del").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "Badger:Del")
	defer traceSpan.Finish()

	err := s.store.Update(func(tx *badger.Txn) error {
		return tx.Delete(hash)
	})

	if err != nil {
		traceSpan.RecordError(err)
	}

	return err
}
