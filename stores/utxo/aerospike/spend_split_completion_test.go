package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
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
						}, blockHeight: 100 + uint32(i), group: group}
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

// Metrics count one batcher batch. Expression failures count once across parts,
// while Lua counts every failed record, even when the record key repeats.
func TestSpendSplitRecordsBreakerOutcomesAndOneMetricSample(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		name := "lua"
		if expressions {
			name = "expressions"
		}
		t.Run(name, func(t *testing.T) {
			s := newSpendBreakerTestStore(t, expressions)
			parent := chainhash.Hash{1}
			batch, group := competingSpendBatch(&parent, &parent, &parent)
			for i, item := range batch {
				item.blockHeight += uint32(i)
			}

			calls := 0
			s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				calls++
				failRecords(records)
				return nil
			}

			samplesBefore := spendBatchSizeSamples(t)
			s.sendSpendBatchLua(batch)
			requireGroupCompleted(t, group, time.Second)

			require.Equal(t, 3, calls, "different checks still require backend decisions")
			if expressions {
				require.Equal(t, cbStateClosed, s.spendCircuitBreaker.state)
				require.Equal(t, 1, s.spendCircuitBreaker.consecutiveFailures)
			} else {
				require.Equal(t, cbStateOpen, s.spendCircuitBreaker.state, "each Lua record failure must count")
			}
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
			batch[1].blockHeight++

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

// Expression decisions must reach the breaker before any Lua retry, including
// when the retry panics. Otherwise a successful retry can hide a failed probe.
func TestSpendExpressionBreakerBeforeLuaRetry(t *testing.T) {
	for _, state := range []cbState{cbStateClosed, cbStateHalfOpen} {
		for _, split := range []bool{false, true} {
			for _, panicRetry := range []bool{false, true} {
				name := string(state) + "/success"
				if panicRetry {
					name = string(state) + "/panic"
				}
				if split {
					name += "/split"
				}
				t.Run(name, func(t *testing.T) {
					s := newSpendBreakerTestStore(t, true)
					s.spendCircuitBreaker.state = state
					s.spendCircuitBreaker.consecutiveFailures = 1
					first, second := chainhash.Hash{1}, chainhash.Hash{9}
					parents := []*chainhash.Hash{&first, &second}
					if split {
						parents = append(parents, &first)
					}
					batch, group := competingSpendBatch(parents...)
					if split {
						batch[2].blockHeight++
					}
					calls := 0
					s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
						calls++
						if calls == 1 {
							require.Len(t, records, 2)
							failRecords(records[:1])
							records[1].BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.FILTERED_OUT}
							return nil
						}
						require.Equal(t, cbStateOpen, s.spendCircuitBreaker.state, "the expression failure must count before Lua starts")
						if panicRetry {
							panic("Lua retry failed")
						}
						require.Len(t, records, 1)
						records[0].BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
							LuaSuccess.String(): map[interface{}]interface{}{"status": "OK"},
						}}
						return nil
					}
					s.sendSpendBatchLua(batch)
					requireGroupCompleted(t, group, time.Second)
					wantCalls := 2
					if split && !panicRetry {
						wantCalls = 3
					}
					require.Equal(t, wantCalls, calls)
					require.Equal(t, cbStateOpen, s.spendCircuitBreaker.state)
					require.Error(t, batch[0].spend.Err)
					if panicRetry {
						require.Error(t, batch[1].spend.Err)
					} else {
						require.NoError(t, batch[1].spend.Err)
					}
				})
			}
		}
	}
}

// A successful record cannot suppress a later failure merely because the two
// operations share a parent record. Different flags force both claims to run.
func TestSpendLuaBreakerCountsLaterPartFailure(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	s.spendCircuitBreaker = newCircuitBreaker(1, 1, time.Minute)
	s.utxoBatchSize = 128
	parent, sameParent := chainhash.Hash{1}, chainhash.Hash{1}
	batch, group := competingSpendBatch(&parent, &parent, &sameParent)
	batch[1].ignoreLocked = true
	batch[2].spend.Vout = 1
	calls := 0
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		calls++
		if calls == 1 {
			require.Len(t, records, 1)
			records[0].BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
				LuaSuccess.String(): map[interface{}]interface{}{"status": "OK"},
			}}
		} else {
			require.Len(t, records, 2)
			for _, record := range records {
				// Only the plain record shares the first part's value ID.
				// The competing claim gets a data error, which must not count.
				if record.(*aerospike.BatchUDF).FunctionArgs[2] == aerospike.BoolValue(true) {
					record.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}
				} else {
					failRecords([]aerospike.BatchRecordIfc{record})
				}
			}
		}
		return nil
	}
	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)
	require.Equal(t, 2, calls)
	require.Equal(t, cbStateOpen, s.spendCircuitBreaker.state)
}

func TestSpendExpressionSuccessBeforeLuaFailure(t *testing.T) {
	for _, state := range []cbState{cbStateClosed, cbStateHalfOpen} {
		t.Run(string(state), func(t *testing.T) {
			s := newSpendBreakerTestStore(t, true)
			s.spendCircuitBreaker.state = state
			s.spendCircuitBreaker.consecutiveFailures = 1
			first, second := chainhash.Hash{1}, chainhash.Hash{9}
			batch, group := competingSpendBatch(&first, &second)
			calls := 0
			s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				calls++
				if calls == 1 {
					require.Len(t, records, 2)
					records[1].BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.FILTERED_OUT}
					return nil
				}
				require.Equal(t, cbStateClosed, s.spendCircuitBreaker.state)
				require.Zero(t, s.spendCircuitBreaker.consecutiveFailures, "expression success precedes Lua failure")
				failRecords(records)
				return nil
			}
			s.sendSpendBatchLua(batch)
			requireGroupCompleted(t, group, time.Second)
			require.Equal(t, 2, calls)
			require.Equal(t, cbStateClosed, s.spendCircuitBreaker.state)
			require.Equal(t, 1, s.spendCircuitBreaker.consecutiveFailures)
			require.NoError(t, batch[0].spend.Err)
			require.Error(t, batch[1].spend.Err)
		})
	}
}

// Coalescing repeated expression failures must stop at a recovery signal. A
// later timeout is new evidence after the success reset consecutiveFailures.
func TestSpendExpressionBreakerCountsFailureAfterRecovery(t *testing.T) {
	for _, luaRecovery := range []bool{false, true} {
		name := "expression recovery"
		if luaRecovery {
			name = "Lua recovery"
		}
		t.Run(name, func(t *testing.T) {
			s := newSpendBreakerTestStore(t, true)
			parent := chainhash.Hash{1}
			batch, group := competingSpendBatch(&parent, &parent, &parent)
			batch[1].ignoreLocked = true
			batch[2].ignoreConflicting = true
			calls := 0
			s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				calls++
				require.Len(t, records, 1)
				switch {
				case calls == 2 && luaRecovery:
					records[0].BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.FILTERED_OUT}
				case calls == 2:
					// Successful expression record, without follow-up bins.
				case calls == 3 && luaRecovery:
					records[0].BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
						LuaSuccess.String(): map[interface{}]interface{}{"status": "OK"},
					}}
				default:
					failRecords(records)
				}
				return nil
			}
			s.sendSpendBatchLua(batch)
			requireGroupCompleted(t, group, time.Second)
			wantCalls := 3
			if luaRecovery {
				wantCalls++
			}
			require.Equal(t, wantCalls, calls)
			require.Equal(t, cbStateClosed, s.spendCircuitBreaker.state)
			require.Equal(t, 1, s.spendCircuitBreaker.consecutiveFailures, "the final timeout must count after recovery")
			require.Error(t, batch[0].spend.Err)
			require.NoError(t, batch[1].spend.Err)
			require.Error(t, batch[2].spend.Err)
		})
	}
}

func TestSpendSplitDoesNotDispatchAfterCallerReturns(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		name := "lua"
		if expressions {
			name = "expressions"
		}
		t.Run(name, func(t *testing.T) {
			for _, cancelCaller := range []bool{false, true} {
				mode := "timeout"
				if cancelCaller {
					mode = "cancellation"
				}
				t.Run(mode, func(t *testing.T) {
					s := newSpendBreakerTestStore(t, expressions)
					s.settings.UtxoStore.SpendWaitTimeout = 200 * time.Millisecond
					parent := chainhash.Hash{1}
					tx := bt.NewTx()
					require.NoError(t, tx.From(parent.String(), 0, "76a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac", 1000))
					require.NoError(t, tx.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1))
					queued := make(chan *batchSpend, 1)
					s.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) { queued <- items[0] }, false)
					defer s.spendBatcher.Close()
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					result := make(chan error, 1)
					go func() { _, err := s.Spend(ctx, tx, 2); result <- err }()
					var later *batchSpend
					select {
					case later = <-queued:
					case <-time.After(3 * time.Second):
						t.Fatal("claim not queued")
					}
					firstBatch, _ := competingSpendBatch(&parent)
					first := firstBatch[0]
					first.spend.UTXOHash = later.spend.UTXOHash
					// Different height bypasses failure reuse, so admission itself
					// must prevent the second backend call after timeout.
					first.blockHeight = 1
					entered, release := make(chan struct{}), make(chan struct{})
					calls := 0
					s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
						calls++
						if calls == 1 {
							close(entered)
							<-release
							failRecords(records)
						}
						return nil
					}
					done := make(chan struct{})
					go func() { defer close(done); s.sendSpendBatchLua([]*batchSpend{first, later}) }()
					select {
					case <-entered:
					case <-time.After(3 * time.Second):
						t.Fatal("first part not started")
					}
					if cancelCaller {
						cancel()
					}
					select {
					case err := <-result:
						require.Error(t, err)
					case <-time.After(3 * time.Second):
						t.Fatal("caller did not stop waiting")
					}
					close(release)
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Fatal("batch did not finish")
					}
					require.Equal(t, 1, calls, "an abandoned claim must never start another write")
					require.True(t, later.published.Load())
					require.Error(t, later.err)
				})
			}
		})
	}
}
