package badger

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/dgraph-io/badger/v3"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type loggerWrapper struct {
	*gocore.Logger
}

type Badger struct {
	mu     sync.Mutex
	store  *badger.DB
	logger utils.Logger
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

func (b *Badger) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return b.Get(ctx, hash)
}

func (b *Badger) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_badger_txmeta", true).NewStat("Get", true).AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "Badger:Get")
	defer traceSpan.Finish()

	var result []byte
	err := b.store.View(func(tx *badger.Txn) error {
		data, err := tx.Get(hash[:])
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
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
	if err != nil {
		return nil, err
	}

	return txmeta.NewDataFromBytes(result)
}

func (b *Badger) Create(ctx context.Context, tx *bt.Tx) (*txmeta.Data, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	start := gocore.CurrentTime()
	defer func() {
		gocore.NewStat("prop_store_badger_txmeta", true).NewStat("Set", true).AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "Badger:Set")
	defer traceSpan.Finish()

	data, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}

	// check whether it already exists
	exists := true
	_ = b.store.View(func(badgerTx *badger.Txn) error {
		_, err = badgerTx.Get(tx.TxIDChainHash().CloneBytes())
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				exists = false
			}
			return err
		}

		return nil
	})
	if exists {
		return data, txmeta.ErrAlreadyExists
	}

	if err = b.store.Update(func(badgerTx *badger.Txn) error {
		entry := badger.NewEntry(tx.TxIDChainHash().CloneBytes(), data.Bytes())
		return badgerTx.SetEntry(entry)
	}); err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("failed to set data: %w", err)
	}

	return data, nil
}

func (b *Badger) SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	data, err := b.Get(ctx, hash)
	if err != nil {
		return err
	}

	if data.BlockHashes == nil {
		data.BlockHashes = []*chainhash.Hash{
			blockHash,
		}
	} else {
		data.BlockHashes = append(data.BlockHashes, blockHash)
	}

	if err = b.store.Update(func(badgerTx *badger.Txn) error {
		entry := badger.NewEntry(hash.CloneBytes(), data.Bytes())
		return badgerTx.SetEntry(entry)
	}); err != nil {
		return fmt.Errorf("failed to set data: %w", err)
	}

	return nil
}

func (b *Badger) Delete(_ context.Context, hash *chainhash.Hash) error {
	return b.store.Update(func(tx *badger.Txn) error {
		return tx.Delete(hash[:])
	})
}
