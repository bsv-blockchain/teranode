package aerospike

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestSpendSplitCompletesAllClaimsOnBackendFailure(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		name := "lua"
		if expressions {
			name = "expressions"
		}
		t.Run(name, func(t *testing.T) {
			for _, panicSecond := range []bool{false, true} {
				scenario := "errors"
				if panicSecond {
					scenario = "panic after first part"
				}
				t.Run(scenario, func(t *testing.T) {
					s := newTestStoreForGet(t)
					s.utxoBatchSize = 1
					s.settings.Aerospike.EnableSpendFilterExpressions = expressions
					group := completion.NewGroup(3)
					parent, hash := chainhash.Hash{1}, chainhash.Hash{2}
					batch := make([]*batchSpend, 3)
					for i := range batch {
						spender := chainhash.Hash{byte(i + 3)}
						batch[i] = &batchSpend{spend: &utxo.Spend{
							TxID: &parent, UTXOHash: &hash,
							SpendingData: spendpkg.NewSpendingData(&spender, 0),
						}, blockHeight: 100, group: group}
					}
					calls := 0
					fail := errorOperate()
					s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
						calls++
						if panicSecond && calls == 2 {
							panic("second part failed")
						}
						return fail(policy, records)
					}
					require.NotPanics(t, func() { s.sendSpendBatchLua(batch) })
					requireGroupCompleted(t, group, time.Second)
					expectedCalls := 3
					if panicSecond {
						expectedCalls = 2
					}
					require.Equal(t, expectedCalls, calls)
					for _, item := range batch {
						require.Error(t, item.spend.Err)
						require.NotErrorIs(t, item.spend.Err, errors.ErrSpent)
					}
				})
			}
		})
	}
}

// A batch split three ways during an Aerospike outage is one batcher batch: it
// records one breaker failure and one batch metric sample, not three.
func TestSpendSplitRecordsBreakerAndMetricsOncePerBatch(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		name := "lua"
		if expressions {
			name = "expressions"
		}
		t.Run(name, func(t *testing.T) {
			s := newSpendBreakerTestStore(t, expressions)
			parent := chainhash.Hash{1}
			batch, group := competingSpendBatch(&parent, &parent, &parent)

			calls := 0
			s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				calls++
				failRecords(records)
				return nil
			}

			samplesBefore := spendBatchSizeSamples(t)
			s.sendSpendBatchLua(batch)
			requireGroupCompleted(t, group, time.Second)

			require.Equal(t, 3, calls, "a failure without an owner sends each competing claim to the backend")
			require.Equal(t, cbStateClosed, s.spendCircuitBreaker.state, "one batch must not trip a breaker with threshold 2")
			require.Equal(t, 1, s.spendCircuitBreaker.consecutiveFailures)
			require.Equal(t, samplesBefore+1, spendBatchSizeSamples(t))
		})
	}
}

// The Lua path counts each distinct record key, as before.
func TestSpendLuaBreakerCountsDistinctKeys(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	first, second := chainhash.Hash{1}, chainhash.Hash{9}
	batch, group := competingSpendBatch(&first, &second)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		failRecords(records)
		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)

	require.Equal(t, cbStateOpen, s.spendCircuitBreaker.state, "two failed record keys reach threshold 2")
}

// One tx spending two outputs of the same parent gives two Lua records on the
// same key, because each input carries its own TxID pointer. Without a split,
// both still count, as before the batch could split.
func TestSpendLuaBreakerCountsUnsplitRecordsAsBefore(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	s.utxoBatchSize = 128
	group := completion.NewGroup(2)
	hash, spender := chainhash.Hash{2}, chainhash.Hash{3}
	batch := make([]*batchSpend, 2)
	for vout := range batch {
		parent := chainhash.Hash{1}
		batch[vout] = &batchSpend{spend: &utxo.Spend{
			TxID: &parent, Vout: uint32(vout), UTXOHash: &hash,
			SpendingData: spendpkg.NewSpendingData(&spender, vout),
		}, blockHeight: 100, group: group}
	}

	records := 0
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, batchRecords []aerospike.BatchRecordIfc) aerospike.Error {
		records += len(batchRecords)
		failRecords(batchRecords)
		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)

	require.Equal(t, 2, records)
	require.Equal(t, cbStateOpen, s.spendCircuitBreaker.state, "two failed records reach threshold 2")
}

// The Lua path records each result in the order the records come back. In
// half-open state a failure first trips the breaker, and a later success in
// open state does not close it.
func TestSpendLuaBreakerRecordsInOrder(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	s.spendCircuitBreaker = newCircuitBreaker(2, 1, time.Minute)
	s.spendCircuitBreaker.state = cbStateHalfOpen
	first, second := chainhash.Hash{1}, chainhash.Hash{9}
	batch, group := competingSpendBatch(&first, &second)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		require.Len(t, records, 2)
		failRecords(records[:1])
		records[1].BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
			LuaSuccess.String(): map[interface{}]interface{}{"status": "OK"},
		}}
		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)

	require.Equal(t, cbStateOpen, s.spendCircuitBreaker.state)
}

// A panic in a later part must not drop the breaker failure of an earlier part.
func TestSpendSplitPanicKeepsEarlierBreakerFailures(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		name := "lua"
		if expressions {
			name = "expressions"
		}
		t.Run(name, func(t *testing.T) {
			s := newSpendBreakerTestStore(t, expressions)
			s.spendCircuitBreaker = newCircuitBreaker(5, 1, time.Minute)
			parent := chainhash.Hash{1}
			batch, group := competingSpendBatch(&parent, &parent)

			calls := 0
			s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				calls++
				if calls == 2 {
					panic("second part failed")
				}
				failRecords(records)
				return nil
			}

			require.NotPanics(t, func() { s.sendSpendBatchLua(batch) })
			requireGroupCompleted(t, group, time.Second)

			require.Equal(t, 2, calls)
			require.Equal(t, 1, s.spendCircuitBreaker.consecutiveFailures)
		})
	}
}

// A Lua success after the split resets the breaker failure count once.
func TestSpendLuaBreakerRecordsSuccessForSplitBatch(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	s.spendCircuitBreaker.RecordFailure()
	parent := chainhash.Hash{1}
	batch, group := competingSpendBatch(&parent, &parent)

	calls := 0
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		calls++
		for _, record := range records {
			record.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
				LuaSuccess.String(): map[interface{}]interface{}{"status": "OK"},
			}}
		}
		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)

	require.Equal(t, 1, calls, "the second claim is refused locally once the first one owns the outpoint")
	require.NoError(t, batch[0].spend.Err)
	require.ErrorIs(t, batch[1].spend.Err, errors.ErrSpent)
	require.Equal(t, 0, s.spendCircuitBreaker.consecutiveFailures)
}

func newSpendBreakerTestStore(t *testing.T, expressions bool) *Store {
	t.Helper()

	s := newTestStoreForGet(t)
	s.utxoBatchSize = 1
	s.settings.Aerospike.EnableSpendFilterExpressions = expressions
	s.spendCircuitBreaker = newCircuitBreaker(2, 1, time.Minute)

	return s
}

// competingSpendBatch builds one claim per parent on output 0, each from a
// different spending tx.
func competingSpendBatch(parents ...*chainhash.Hash) ([]*batchSpend, *completion.Group) {
	group := completion.NewGroup(int32(len(parents)))
	hash := chainhash.Hash{2}
	batch := make([]*batchSpend, len(parents))
	for i, parent := range parents {
		spender := chainhash.Hash{byte(i + 3)}
		batch[i] = &batchSpend{spend: &utxo.Spend{
			TxID: parent, UTXOHash: &hash,
			SpendingData: spendpkg.NewSpendingData(&spender, 0),
		}, blockHeight: 100, group: group}
	}

	return batch, group
}

func failRecords(records []aerospike.BatchRecordIfc) {
	for _, record := range records {
		record.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
	}
}

func spendBatchSizeSamples(t *testing.T) uint64 {
	t.Helper()

	var metric dto.Metric
	require.NoError(t, prometheusUtxoSpendBatchSize.Write(&metric))

	return metric.GetHistogram().GetSampleCount()
}

// A competing claim with other ignore flags than the claim that showed the
// owner goes to the backend, because the backend could answer it differently.
func TestSpendSplitSendsClaimWithOtherChecksToBackend(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	parent := chainhash.Hash{1}
	batch, group := competingSpendBatch(&parent, &parent, &parent)
	batch[1].ignoreConflicting = true

	calls := 0
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		calls++
		for _, record := range records {
			record.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
				LuaSuccess.String(): map[interface{}]interface{}{"status": "OK"},
			}}
		}
		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)

	require.Equal(t, 2, calls, "the claim with ignoreConflicting reaches the backend, the plain one is refused locally")
	require.ErrorIs(t, batch[2].spend.Err, errors.ErrSpent)
}
