package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
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

// requireGroupCompleted is the group-completion-protocol
// equivalent: a single group.Wait standing in for "every item in the batch
// received a completion signal, none was orphaned" (the group's counter
// only reaches zero once every item's complete() has run).
func requireGroupCompleted(t *testing.T, group *completion.Group, timeout time.Duration) {
	t.Helper()
	require.NoError(t, group.Wait(context.Background(), timeout), "group did not complete: an item was orphaned")
}

func TestSetLockedBatch_NeverOrphans(t *testing.T) {
	mk := func(group *completion.Group) []*batchLocked {
		b := make([]*batchLocked, 4)
		for i := range b {
			b[i] = &batchLocked{ctx: context.Background(), txHash: chainhash.Hash{byte(i + 1)}, setValue: true, group: group}
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

			group := completion.NewGroup(4)
			b := mk(group)
			require.NotPanics(t, func() { s.setLockedBatch(b) })

			requireGroupCompleted(t, group, 2*time.Second)
		})
	}
}

func TestSendSpendBatchLua_NeverOrphans(t *testing.T) {
	const n = 4

	mk := func(group *completion.Group) []*batchSpend {
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
				group:       group,
			}
		}
		return batch
	}

	// "ok (nil records)" exercises the missing-LuaSuccess-bin branch (the
	// mocked records carry no Record), which must complete with an error
	// rather than fall through and orphan the submitter.
	for name, fn := range map[string]func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error{
		"panic": panicOperate, "batchOperate error": errorOperate, "ok (nil records)": okOperate,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestStoreForGet(t)
			s.utxoBatchSize = 2
			s.batchOperateFn = fn()

			group := completion.NewGroup(n)
			batch := mk(group)

			require.NotPanics(t, func() { s.sendSpendBatchLua(batch) })

			requireGroupCompleted(t, group, 2*time.Second)
		})
	}
}
