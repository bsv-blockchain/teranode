package aerospike

import (
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

// TestSpendExpressions_NeverOrphans drives the expression-based spend path
// (sendSpendBatchLua -> SpendMultiWithExpressions) through the batchOperateFn
// seam, exercising the invariant that every item is completed exactly once even
// when the FILTERED_OUT-requeue-through-Lua handoff interleaves with a panic or a
// batch-level error.
//
// The invariant under test (plan §1e, design §7): each item is completed by
// exactly one of {expressions path, Lua retry path, panic sweep}. FILTERED_OUT
// items are deliberately NOT completed in processSpendBatchResultsExpressions —
// they are handed to executeLuaSpendBatch, which completes them. If a panic
// strikes the Lua retry, sendSpendBatchLua's deferred sweep must complete the
// still-open items (and be a CAS no-op for the ones already completed by the
// expressions pass), so group.Wait returns and no double-Done panic fires.
func TestSpendExpressions_NeverOrphans(t *testing.T) {
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

	// filterOutOdd marks odd-indexed records FILTERED_OUT so they take the
	// retry-through-Lua branch; even-indexed records complete in-place.
	filterOutOdd := func(records []aerospike.BatchRecordIfc) {
		for i, r := range records {
			if i%2 == 1 {
				r.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.FILTERED_OUT}
			}
		}
	}

	// Each factory returns a stateful seam: call 1 is the expressions batch, call
	// 2 (if reached) is the Lua retry of the FILTERED_OUT records.
	scenarios := map[string]func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error{
		// FILTERED_OUT half, Lua retry returns cleanly (missing-response path
		// completes the requeued items with an error — still completed, not orphaned).
		"filtered_out then lua ok": func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
			call := 0
			return func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				call++
				if call == 1 {
					filterOutOdd(records)
				}
				return nil
			}
		},
		// FILTERED_OUT half, then the Lua retry panics: the top-level sweep in
		// sendSpendBatchLua must complete the requeued items the retry never reached.
		"filtered_out then lua panic": func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
			call := 0
			return func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				call++
				if call == 1 {
					filterOutOdd(records)
					return nil
				}
				panic("simulated lua-retry panic")
			}
		},
		// Batch-level error on the expressions call: SpendMultiWithExpressions
		// sweeps the whole batch (no records reach the retry path).
		"expressions batch error": errorOperate,
		// Panic on the expressions call: sendSpendBatchLua's sweep covers everyone.
		"expressions panic": panicOperate,
	}

	for name, factory := range scenarios {
		t.Run(name, func(t *testing.T) {
			s := newTestStoreForGet(t)
			s.settings.Aerospike.EnableSpendFilterExpressions = true
			s.utxoBatchSize = 1 // required for useExpressionSpend()
			s.batchOperateFn = factory()

			require.True(t, s.useExpressionSpend(), "test must drive the expression path")

			group := completion.NewGroup(n)
			batch := mk(group)

			require.NotPanics(t, func() { s.sendSpendBatchLua(batch) })

			requireGroupCompleted(t, group, 2*time.Second)

			// Exactly-once corollary: group.Wait returning nil means every item's
			// complete() ran and drove the counter to zero; NotPanics above means
			// none over-decremented. Confirm each item's flag settled true.
			for i, it := range batch {
				require.True(t, it.completed.Load(), "item %d left uncompleted", i)
			}
		})
	}
}
