package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// These tests drive each batcher dispatch fn through its panic / BatchOperate-error
// / result paths using the batchOperateFn seam, so the failure-path branches added
// for the leak fix are exercised without a live Aerospike instance.

func panicOperate() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
	return func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
		panic("simulated batch panic")
	}
}

func errorOperate() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
	return func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
		return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
	}
}

func okOperate() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
	return func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error { return nil }
}

func requireSignalled[T any](t *testing.T, ch chan T, i int) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("item %d was orphaned: no completion signal", i)
	}
}

func TestSendIncrementBatch_NeverOrphans(t *testing.T) {
	mk := func() []*batchIncrement {
		b := make([]*batchIncrement, 4)
		for i := range b {
			h := chainhash.Hash{byte(i + 1)}
			b[i] = &batchIncrement{txID: &h, increment: 1, res: make(chan incrementSpentRecordsRes, 1)}
		}
		return b
	}

	for name, fn := range map[string]func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error{
		"panic": panicOperate, "batchOperate error": errorOperate, "ok (nil records)": okOperate,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestStoreForGet(t)
			s.batchOperateFn = fn()

			b := mk()
			require.NotPanics(t, func() { s.sendIncrementBatch(b) })

			for i, it := range b {
				requireSignalled(t, it.res, i)
			}
		})
	}
}

func TestSendSetDAHBatch_NeverOrphans(t *testing.T) {
	mk := func() []*batchDAH {
		b := make([]*batchDAH, 4)
		for i := range b {
			h := chainhash.Hash{byte(i + 1)}
			b[i] = &batchDAH{txID: &h, childIdx: uint32(i + 1), deleteAtHeight: 100, errCh: make(chan error, 1)}
		}
		return b
	}

	for name, fn := range map[string]func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error{
		"panic": panicOperate, "batchOperate error": errorOperate, "ok (nil records)": okOperate,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestStoreForGet(t)
			s.batchOperateFn = fn()

			b := mk()
			require.NotPanics(t, func() { s.sendSetDAHBatch(b) })

			for i, it := range b {
				requireSignalled(t, it.errCh, i)
			}
		})
	}
}

func TestSetLockedBatch_NeverOrphans(t *testing.T) {
	mk := func() []*batchLocked {
		b := make([]*batchLocked, 4)
		for i := range b {
			b[i] = &batchLocked{ctx: context.Background(), txHash: chainhash.Hash{byte(i + 1)}, setValue: true, errCh: make(chan error, 1)}
		}
		return b
	}

	// "ok" exercises the previously-silent missing-LuaSuccess-bin branch (the
	// mocked records carry no Record), which must now signal an error rather
	// than fall through and orphan the submitter.
	for name, fn := range map[string]func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error{
		"panic": panicOperate, "batchOperate error": errorOperate, "ok (missing response)": okOperate,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestStoreForGet(t)
			s.batchOperateFn = fn()

			b := mk()
			require.NotPanics(t, func() { s.setLockedBatch(b) })

			for i, it := range b {
				requireSignalled(t, it.errCh, i)
			}
		})
	}
}

func TestSendSpendBatchLua_PanicSignalsAllWaiters(t *testing.T) {
	s := newTestStoreForGet(t)
	s.utxoBatchSize = 2
	s.batchOperateFn = panicOperate()

	const n = 4

	batch := make([]*batchSpend, n)
	for i := range batch {
		txID := chainhash.HashH([]byte{byte(i), 't', 'x'})
		utxoHash := chainhash.HashH([]byte{byte(i), 'u'})
		spender := chainhash.HashH([]byte{byte(i), 's'})
		batch[i] = &batchSpend{
			spend: &utxo.Spend{
				TxID:         &txID,
				Vout:         uint32(i),
				UTXOHash:     &utxoHash,
				SpendingData: spendpkg.NewSpendingData(&spender, 0),
			},
			blockHeight: 100,
			errCh:       make(chan error, 1),
		}
	}

	require.NotPanics(t, func() { s.sendSpendBatchLua(batch) })

	for i, it := range batch {
		requireSignalled(t, it.errCh, i)
	}
}

func TestSendStoreBatch_PanicSignalsAllWaiters(t *testing.T) {
	s := newTestStoreForSendStoreBatch(t)
	s.batchOperateFn = panicOperate()

	const n = 3

	batch := make([]*BatchStoreItem, n)
	for i := range batch {
		tx := txWithSingleOutput(t)
		batch[i] = NewBatchStoreItem(tx.TxIDChainHash(), false, tx, 100, nil, 0, make(chan error, 1))
	}

	require.NotPanics(t, func() { s.sendStoreBatch(batch) })

	for i, item := range batch {
		requireSignalled(t, item.done, i)
	}
}
