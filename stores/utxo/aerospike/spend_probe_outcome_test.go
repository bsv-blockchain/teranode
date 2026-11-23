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

func probeBreaker(t *testing.T, halfOpenMax int) *circuitBreaker {
	t.Helper()

	cb := newCircuitBreaker(1, halfOpenMax, time.Hour)
	require.NotNil(t, cb)

	cb.RecordFailure(breakerSlot{})
	expireCooldown(cb)

	return cb
}

func admitProbes(t *testing.T, cb *circuitBreaker, batch []*batchSpend) {
	t.Helper()

	for _, item := range batch {
		allowed, slot := cb.Admit()
		require.True(t, allowed)
		require.True(t, slot.taken)

		item.breakerSlot = slot
	}
}

func distinctParents(n int) []*chainhash.Hash {
	parents := make([]*chainhash.Hash, n)
	for i := range parents {
		parents[i] = &chainhash.Hash{byte(i + 1)}
	}

	return parents
}

func TestSpend_HalfOpenProbesAnsweredWithBusinessErrorCloseTheBreaker(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	cb := probeBreaker(t, 3)
	s.spendCircuitBreaker = cb

	batch, group := competingSpendBatch(distinctParents(3)...)
	admitProbes(t, cb, batch)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		for _, record := range records {
			record.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
				LuaSuccess.String(): map[interface{}]interface{}{
					"status":    "ERROR",
					"errorCode": "SPENT",
					"message":   "utxo already spent",
				},
			}}
		}

		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)

	for _, item := range batch {
		require.Error(t, item.spend.Err)
	}

	require.True(t, admitted(cb), "probes answered with a business error must not hold half-open shut")
	require.Equal(t, cbStateClosed, cb.state, "three answered probes close the breaker")
}

func TestSpend_HalfOpenProbeWithoutUsableResponseReleasesItsSlot(t *testing.T) {
	responses := map[string]*aerospike.Record{
		"missing":    nil,
		"unparsable": {Bins: aerospike.BinMap{LuaSuccess.String(): "not a map"}},
	}

	for name, response := range responses {
		t.Run(name, func(t *testing.T) {
			s := newSpendBreakerTestStore(t, false)
			cb := probeBreaker(t, 1)
			s.spendCircuitBreaker = cb

			batch, group := competingSpendBatch(distinctParents(1)...)
			admitProbes(t, cb, batch)
			require.False(t, admitted(cb), "the only probe slot is taken")

			s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				for _, record := range records {
					record.BatchRec().Record = response
				}

				return nil
			}

			s.sendSpendBatchLua(batch)
			requireGroupCompleted(t, group, time.Second)

			require.Error(t, batch[0].spend.Err)
			require.Equal(t, cbStateHalfOpen, cb.state, "no evidence either way, so no close and no trip")
			require.True(t, admitted(cb), "the released slot must admit another probe")
		})
	}
}

func TestSpend_HalfOpenProbeAnsweredWithDataStateErrorCountsAsSuccess(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	cb := probeBreaker(t, 1)
	s.spendCircuitBreaker = cb

	batch, group := competingSpendBatch(distinctParents(1)...)
	admitProbes(t, cb, batch)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		for _, record := range records {
			record.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}
		}

		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)

	require.Error(t, batch[0].spend.Err)
	require.Equal(t, cbStateClosed, cb.state)
}

func timeoutBatch(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
	return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
}

func TestSpend_WholeBatchInfrastructureErrorTripsClosedBreaker(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		t.Run(pathName(expressions), func(t *testing.T) {
			s := newSpendBreakerTestStore(t, expressions)
			cb := newCircuitBreaker(1, 1, time.Hour)
			s.spendCircuitBreaker = cb

			batch, group := competingSpendBatch(distinctParents(2)...)
			s.batchOperateFn = timeoutBatch

			s.sendSpendBatchLua(batch)
			requireGroupCompleted(t, group, time.Second)

			require.Equal(t, cbStateOpen, cb.state, "one whole-batch TIMEOUT reaches threshold 1")
		})
	}
}

func TestSpend_WholeBatchInfrastructureErrorTripsHalfOpenProbe(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		t.Run(pathName(expressions), func(t *testing.T) {
			s := newSpendBreakerTestStore(t, expressions)
			cb := probeBreaker(t, 2)
			s.spendCircuitBreaker = cb

			batch, group := competingSpendBatch(distinctParents(2)...)
			admitProbes(t, cb, batch)
			s.batchOperateFn = timeoutBatch

			s.sendSpendBatchLua(batch)
			requireGroupCompleted(t, group, time.Second)

			require.Equal(t, cbStateOpen, cb.state, "a probe whose batch times out trips the breaker")
		})
	}
}

func TestSpend_WholeBatchNonInfrastructureErrorReleasesProbeSlots(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		t.Run(pathName(expressions), func(t *testing.T) {
			s := newSpendBreakerTestStore(t, expressions)
			cb := probeBreaker(t, 2)
			s.spendCircuitBreaker = cb

			batch, group := competingSpendBatch(distinctParents(2)...)
			admitProbes(t, cb, batch)
			s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
				return &aerospike.AerospikeError{ResultCode: types.PARAMETER_ERROR}
			}

			s.sendSpendBatchLua(batch)
			requireGroupCompleted(t, group, time.Second)

			require.Equal(t, cbStateHalfOpen, cb.state)
			require.Equal(t, 0, cb.halfOpenAttempts)
		})
	}
}

func TestSpend_ExpiredContextBeforeBatchOperateDoesNotTripBreaker(t *testing.T) {
	for _, halfOpen := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed", true: "half-open"}[halfOpen], func(t *testing.T) {
			s := newSpendBreakerTestStore(t, false)
			cb := newCircuitBreaker(1, 1, time.Hour)
			if halfOpen {
				cb = probeBreaker(t, 1)
			}
			s.spendCircuitBreaker = cb

			batch, _ := competingSpendBatch(distinctParents(1)...)
			if halfOpen {
				admitProbes(t, cb, batch)
			}
			called := false
			s.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
				called = true
				return nil
			}

			ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			defer cancel()
			require.ErrorIs(t, s.executeSpendBatchContext(ctx, nil, batch, 1), context.DeadlineExceeded)
			require.False(t, called)
			if halfOpen {
				require.Equal(t, cbStateHalfOpen, cb.state, "preflight must release the probe without tripping")
				require.Zero(t, cb.halfOpenAttempts)
			} else {
				require.Equal(t, cbStateClosed, cb.state, "closed breaker must not trip on caller preflight")
				require.Zero(t, cb.consecutiveFailures)
			}
		})
	}
}

func TestSpend_HalfOpenProbeRecordOutcomesByResultCode(t *testing.T) {
	cases := []struct {
		name      string
		code      types.ResultCode
		wantState cbState
		wantSlots int
	}{
		{"key not found counts as success", types.KEY_NOT_FOUND_ERROR, cbStateClosed, 0},
		{"device overload releases the slot", types.DEVICE_OVERLOAD, cbStateHalfOpen, 0},
		{"client side error releases the slot", types.PARAMETER_ERROR, cbStateHalfOpen, 0},
		{"timeout trips", types.TIMEOUT, cbStateOpen, 0},
	}

	for _, expressions := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(pathName(expressions)+"/"+tc.name, func(t *testing.T) {
				s := newSpendBreakerTestStore(t, expressions)
				cb := probeBreaker(t, 1)
				s.spendCircuitBreaker = cb

				batch, group := competingSpendBatch(distinctParents(1)...)
				admitProbes(t, cb, batch)

				s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
					for _, record := range records {
						record.BatchRec().Err = &aerospike.AerospikeError{ResultCode: tc.code}
					}

					return nil
				}

				s.sendSpendBatchLua(batch)
				requireGroupCompleted(t, group, time.Second)

				require.Equal(t, tc.wantState, cb.state)
				require.Equal(t, tc.wantSlots, cb.halfOpenAttempts)
			})
		}
	}
}

func TestSpend_HalfOpenProbeWithUnknownStatusReleasesItsSlot(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	cb := probeBreaker(t, 1)
	s.spendCircuitBreaker = cb

	batch, group := competingSpendBatch(distinctParents(1)...)
	admitProbes(t, cb, batch)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		for _, record := range records {
			record.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
				LuaSuccess.String(): map[interface{}]interface{}{"status": "WEIRD"},
			}}
		}

		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)

	require.ErrorContains(t, batch[0].spend.Err, `unknown response status "WEIRD"`)

	require.Equal(t, cbStateHalfOpen, cb.state)
	require.True(t, admitted(cb), "the slot of an unknown status must be released")
}

func TestSpend_HalfOpenProbeCompletedBeforeTheBackendReleasesItsSlot(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	cb := probeBreaker(t, 1)
	s.spendCircuitBreaker = cb

	batch, group := competingSpendBatch(distinctParents(1)...)
	batch[0].spend.SpendingData = nil
	admitProbes(t, cb, batch)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)

	require.Error(t, batch[0].spend.Err)
	require.Equal(t, 0, cb.halfOpenAttempts)
	require.True(t, admitted(cb))
}

func TestSpend_HalfOpenProbeReleasedWhenTheBatchPanics(t *testing.T) {
	s := newSpendBreakerTestStore(t, false)
	cb := probeBreaker(t, 1)
	s.spendCircuitBreaker = cb

	batch, group := competingSpendBatch(distinctParents(1)...)
	admitProbes(t, cb, batch)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) aerospike.Error {
		panic("client failed")
	}

	require.NotPanics(t, func() { s.sendSpendBatchLua(batch) })
	requireGroupCompleted(t, group, time.Second)

	require.Equal(t, 0, cb.halfOpenAttempts)
	require.True(t, admitted(cb))
}

func TestSpend_MixedEpochProbesInOneRecordTripOnInfrastructureError(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		t.Run(pathName(expressions), func(t *testing.T) {
			s := newSpendBreakerTestStore(t, expressions)
			s.utxoBatchSize = 128
			cb := probeBreaker(t, 1)
			s.spendCircuitBreaker = cb

			_, stale := cb.Admit()
			cb.mu.Lock()
			cb.nextAttempt = time.Now().Add(-2 * time.Hour)
			cb.mu.Unlock()

			allowed, current := cb.Admit()
			require.True(t, allowed)
			require.NotEqual(t, stale.epoch, current.epoch)

			parent := chainhash.Hash{1}
			batch, group := competingSpendBatch(&parent, &parent)
			batch[1].spend.Vout = 1
			batch[0].breakerSlot = stale
			batch[1].breakerSlot = current

			s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				failRecords(records)

				return nil
			}

			s.sendSpendBatchLua(batch)
			requireGroupCompleted(t, group, time.Second)

			require.Equal(t, cbStateOpen, cb.state, "the current probe must trip even when a stale probe shares the batch")
		})
	}
}

func TestSpendExpressions_CurrentProbeFailureTripsBeforeBatchSuccessCloses(t *testing.T) {
	s := newSpendBreakerTestStore(t, true)
	cb := newCircuitBreaker(2, 2, time.Hour)
	s.spendCircuitBreaker = cb
	cb.RecordFailure(breakerSlot{})
	cb.RecordFailure(breakerSlot{})
	expireCooldown(cb)

	_, carried := cb.Admit()
	_, hanging := cb.Admit()
	cb.RecordSuccess(carried)
	cb.mu.Lock()
	cb.nextAttempt = time.Now().Add(-2 * time.Hour)
	cb.mu.Unlock()

	batch, group := competingSpendBatch(distinctParents(2)...)
	admitProbes(t, cb, batch)
	require.NotEqual(t, hanging.epoch, batch[0].breakerSlot.epoch)
	require.Equal(t, 1, cb.consecutiveSuccess)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		require.Len(t, records, 2)
		records[1].BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)
	require.Equal(t, cbStateOpen, cb.state, "a current probe failure must trip before its batch's success can close")
}

func TestSpendExpressions_ClosedMixedBatchStillCountsFailure(t *testing.T) {
	s := newSpendBreakerTestStore(t, true)
	cb := newCircuitBreaker(2, 1, time.Hour)
	s.spendCircuitBreaker = cb
	batch, group := competingSpendBatch(distinctParents(2)...)
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		require.Len(t, records, 2)
		records[1].BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
		return nil
	}

	s.sendSpendBatchLua(batch)
	requireGroupCompleted(t, group, time.Second)
	require.Equal(t, cbStateClosed, cb.state)
	require.Equal(t, 1, cb.consecutiveFailures, "a mixed closed-state batch still records its infrastructure failure")
}

func pathName(expressions bool) string {
	if expressions {
		return "expressions"
	}

	return "lua"
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
