package badger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/dgraph-io/badger/v3"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
)

func init() {
	//badgerExpvarCollector := collectors.NewExpvarCollector(map[string]*prometheus.Desc{
	//	"badger_blocked_puts_total":   prometheus.NewDesc("badger_blocked_puts_total", "Blocked Puts", nil, nil),
	//	"badger_disk_reads_total":     prometheus.NewDesc("badger_disk_reads_total", "Disk Reads", nil, nil),
	//	"badger_disk_writes_total":    prometheus.NewDesc("badger_disk_writes_total", "Disk Writes", nil, nil),
	//	"badger_gets_total":           prometheus.NewDesc("badger_gets_total", "Gets", nil, nil),
	//	"badger_puts_total":           prometheus.NewDesc("badger_puts_total", "Puts", nil, nil),
	//	"badger_memtable_gets_total":  prometheus.NewDesc("badger_memtable_gets_total", "Memtable gets", nil, nil),
	//	"badger_lsm_size_bytes":       prometheus.NewDesc("badger_lsm_size_bytes", "LSM Size in bytes", []string{"database"}, nil),
	//	"badger_vlog_size_bytes":      prometheus.NewDesc("badger_vlog_size_bytes", "Value Log Size in bytes", []string{"database"}, nil),
	//	"badger_pending_writes_total": prometheus.NewDesc("badger_pending_writes_total", "Pending Writes", []string{"database"}, nil),
	//	"badger_read_bytes":           prometheus.NewDesc("badger_read_bytes", "Read bytes", nil, nil),
	//	"badger_written_bytes":        prometheus.NewDesc("badger_written_bytes", "Written bytes", nil, nil),
	//	"badger_lsm_bloom_hits_total": prometheus.NewDesc("badger_lsm_bloom_hits_total", "LSM Bloom Hits", []string{"level"}, nil),
	//	"badger_lsm_level_gets_total": prometheus.NewDesc("badger_lsm_level_gets_total", "LSM Level Gets", []string{"level"}, nil),
	//})
	//prometheus.MustRegister(badgerExpvarCollector)
}

type Badger struct {
	store  *badger.DB
	logger ulogger.Logger
	// mu     sync.RWMutex
}

type loggerWrapper struct {
	ulogger.Logger
}

var (
	cache = expiringmap.New[string, []byte](1 * time.Minute)
)

func (l loggerWrapper) Warningf(format string, args ...interface{}) {
	l.Warnf(format, args...)
}

func New(logger ulogger.Logger, dir string) (*Badger, error) {
	bLogger := loggerWrapper{logger}
	opts := badger.DefaultOptions(dir).
		WithLogger(bLogger).
		WithLoggingLevel(badger.ERROR).WithNumMemtables(32).
		WithMetricsEnabled(true)

	// low memory options
	if gocore.Config().GetBool("badger_limitMemoryLow") {
		opts.WithBaseTableSize(1 << 20)
		opts.WithNumMemtables(1)
		opts.WithNumLevelZeroTables(1)
		opts.WithNumLevelZeroTablesStall(2)
		opts.WithSyncWrites(false)
	} else if gocore.Config().GetBool("badger_limitMemoryMedium") {
		opts.WithBaseTableSize(1 << 22)
		opts.WithNumMemtables(2)
		opts.WithNumLevelZeroTables(2)
		opts.WithNumLevelZeroTablesStall(4)
		opts.WithSyncWrites(false)
	}

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

func (s *Badger) Health(ctx context.Context) (int, string, error) {
	_, err := s.Exists(ctx, []byte("health"))
	if err != nil {
		return -1, "Badger Store", err
	}

	return 0, "Badger Store", nil
}

func (s *Badger) Close(ctx context.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_badger_blob", true).NewStat("Close").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "Badger:Close")
	defer traceSpan.Finish()

	return s.store.Close()
}

func (s *Badger) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	defer reader.Close()

	b, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data from reader: %w", err)
	}

	return s.Set(ctx, key, b, opts...)
}

func (s *Badger) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	// s.logger.Debugf("[Badger] Set: %s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack())
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_badger_blob", true).NewStat("Set").AddTime(start)
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

	cache.Set(utils.ReverseAndHexEncodeSlice(key), value)

	return nil
}

func (s *Badger) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	// s.logger.Debugf("[Badger] SetTTL: %s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack())
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_badger_blob", true).NewStat("SetTTL").AddTime(start)
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

func (s *Badger) GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error) {
	b, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewBuffer(b)), nil
}

func (s *Badger) Get(ctx context.Context, hash []byte) ([]byte, error) {
	//s.logger.Debugf("[Badger] Get: %s", utils.ReverseAndHexEncodeSlice(hash))
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_badger_blob", true).NewStat("Get").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "Badger:Get")
	defer traceSpan.Finish()

	cached, ok := cache.Get(utils.ReverseAndHexEncodeSlice(hash))
	if ok {
		s.logger.Debugf("Cache hit for: %s", utils.ReverseAndHexEncodeSlice(hash))
		return cached, nil
	}

	var result []byte
	err := s.store.View(func(tx *badger.Txn) error {
		data, err := tx.Get(hash)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ubsverrors.New(ubsverrors.ErrorConstants_NOT_FOUND, fmt.Sprintf("badger key not found [%s]", utils.ReverseAndHexEncodeSlice(hash)), err)
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

func (s *Badger) GetHead(ctx context.Context, hash []byte, nrOfBytes int) ([]byte, error) {
	b, err := s.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	if len(b) < nrOfBytes {
		return b, nil
	}

	return b[:nrOfBytes], nil
}

func (s *Badger) Exists(ctx context.Context, hash []byte) (bool, error) {
	//s.logger.Debugf("[Badger] Exists: %s", utils.ReverseAndHexEncodeSlice(hash))
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_badger_blob", true).NewStat("Exists").AddTime(start)
	}()
	traceSpan := tracing.Start(ctx, "Badger:Exists")
	defer traceSpan.Finish()

	_, ok := cache.Get(utils.ReverseAndHexEncodeSlice(hash))
	if ok {
		s.logger.Debugf("Cache hit for: %s", utils.ReverseAndHexEncodeSlice(hash))
		return true, nil
	}

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
	//s.logger.Debugf("[Badger] Del: %s", utils.ReverseAndHexEncodeSlice(hash))
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_badger_blob", true).NewStat("Del").AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "Badger:Del")
	defer traceSpan.Finish()

	err := s.store.Update(func(tx *badger.Txn) error {
		return tx.Delete(hash)
	})

	if err != nil {
		traceSpan.RecordError(err)
	}

	cache.Delete(utils.ReverseAndHexEncodeSlice(hash))

	return err
}
