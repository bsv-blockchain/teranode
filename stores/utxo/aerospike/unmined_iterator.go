package aerospike

import (
	"context"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// unminedTxIterator implements utxo.UnminedTxIterator for Aerospike
// It scans all records in the set and yields those that are not mined (i.e., unmined/mempool)
type unminedTxIterator struct {
	store     *Store
	err       error
	done      bool
	recordset *as.Recordset
	result    <-chan *as.Result
}

func newUnminedTxIterator(store *Store) (*unminedTxIterator, error) {
	it := &unminedTxIterator{
		store: store,
	}

	stmt := as.NewStatement(store.namespace, store.setName)

	err := stmt.SetFilter(as.NewEqualFilter(fields.NotMined.String(), int64(1)))
	if err != nil {
		return nil, err
	}

	policy := as.NewQueryPolicy()
	policy.MaxRetries = 1
	policy.IncludeBinData = true

	recordset, err := store.client.Query(policy, stmt)
	if err != nil {
		return nil, err
	}

	it.recordset = recordset
	it.result = recordset.Results()

	return it, nil
}

func (it *unminedTxIterator) Next(ctx context.Context) (*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil || it.recordset == nil {
		return nil, it.err
	}

	rec, ok := <-it.result
	if !ok || rec == nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		return nil, nil
	}

	if rec.Err != nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		it.err = rec.Err

		return nil, it.err
	}

	txidVal := rec.Record.Bins[fields.TxID.String()]
	if txidVal == nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		it.err = errors.NewProcessingError("txid not found")

		return nil, it.err
	}

	txidValBytes, ok := txidVal.([]byte)
	if !ok {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		it.err = errors.NewProcessingError("txid not []byte")

		return nil, it.err
	}

	hash, err := chainhash.NewHash(txidValBytes)
	if err != nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		it.err = err

		return nil, it.err
	}

	feeVal := rec.Record.Bins[fields.Fee.String()]
	if feeVal == nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		it.err = errors.NewProcessingError("fee not found")

		return nil, it.err
	}

	fee, err := toUint64(feeVal)
	if err != nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		it.err = errors.NewProcessingError("Failed to convert fee")

		return nil, it.err
	}

	sizeVal := rec.Record.Bins[fields.SizeInBytes.String()]
	if sizeVal == nil {
		if err := it.Close(); err != nil {
			it.store.logger.Warnf("failed to close iterator: %v", err)
		}

		it.err = errors.NewProcessingError("size not found")

		return nil, it.err
	}

	size, _ := toUint64(sizeVal)

	// If the tx is external, we need to fetch it from the external store...
	var externalTx *bt.Tx

	external, ok := rec.Record.Bins[fields.External.String()].(bool)
	if ok && external {
		if externalTx, err = it.store.GetTxFromExternalStore(ctx, *hash); err != nil {
			if err := it.Close(); err != nil {
				it.store.logger.Warnf("failed to close iterator: %v", err)
			}

			it.err = err

			return nil, it.err
		}
	}

	var txInpoints meta.TxInpoints

	if external {
		txInpoints, err = meta.NewTxInpointsFromTx(externalTx)
		if err != nil {
			if err := it.Close(); err != nil {
				it.store.logger.Warnf("failed to close iterator: %v", err)
			}

			it.err = errors.NewTxInvalidError("could not process tx inpoints", err)

			return nil, it.err
		}
	} else {
		txInpoints, err = processInputsToTxInpoints(rec.Record.Bins)
		if err != nil {
			if err := it.Close(); err != nil {
				it.store.logger.Warnf("failed to close iterator: %v", err)
			}

			it.err = errors.NewTxInvalidError("could not process input interfaces", err)

			return nil, it.err
		}
	}

	return &utxo.UnminedTransaction{
		Hash:       hash,
		Fee:        fee,
		Size:       size,
		TxInpoints: txInpoints,
	}, nil
}

func (it *unminedTxIterator) Err() error {
	return it.err
}

func (it *unminedTxIterator) Close() error {
	it.done = true

	return it.recordset.Close()
}

// nolint: gosec
func toUint64(val interface{}) (uint64, error) {
	switch v := val.(type) {
	case int:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint64:
		return v, nil
	case float64:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case float32:
		return uint64(v), nil
	case nil:
		return 0, nil
	default:
		return 0, errors.NewProcessingError("unknown type for uint64 conversion")
	}
}

// GetUnminedTxIterator implements utxo.Store for Aerospike
func (s *Store) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	if s.client == nil {
		return nil, errors.NewProcessingError("aerospike client not initialized")
	}

	return newUnminedTxIterator(s)
}
