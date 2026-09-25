package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestResolveSpendCompletionsReportsAllSuccessfulSpends pins the input to the
// rollback decision: resolveSpendCompletions must report every spend that
// succeeded, whatever the error class of the ones that failed. Spend then rolls
// that whole set back unconditionally — partial spends are never left behind for
// a hoped-for retry, because a surviving spend names a spender whose record is
// never created (#1214).
//
// Error classes here deliberately exclude ErrTxNotFound, which takes the
// "already blessed" store lookup path; the missing-parent case is covered
// end-to-end by tests.PartialSpendRollback against a real store.
func TestResolveSpendCompletionsReportsAllSuccessfulSpends(t *testing.T) {
	store := &Store{logger: ulogger.TestLogger{}}

	newSpend := func(vout uint32, err error) *utxo.Spend {
		return &utxo.Spend{TxID: &chainhash.Hash{}, Vout: vout, Err: err}
	}

	tests := []struct {
		name          string
		failure       error
		expectSuccess int
	}{
		{name: "locked parent", failure: errors.NewTxLockedError("locked"), expectSuccess: 2},
		{name: "double spend", failure: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, spendpkg.NewSpendingData(&chainhash.Hash{}, 0)), expectSuccess: 2},
		{name: "conflicting tx", failure: errors.NewTxConflictingError("conflicting"), expectSuccess: 2},
		{name: "frozen utxo", failure: errors.NewUtxoFrozenError("frozen"), expectSuccess: 2},
		{name: "hash mismatch", failure: errors.NewUtxoHashMismatchError("mismatch"), expectSuccess: 2},
		{name: "device overload", failure: errors.NewStorageError("DEVICE_OVERLOAD"), expectSuccess: 2},
		{name: "service unavailable", failure: errors.NewServiceUnavailableError("circuit breaker open"), expectSuccess: 2},
		{name: "every spend failed", failure: errors.NewStorageError("DEVICE_OVERLOAD"), expectSuccess: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			items := make([]*batchSpend, 0, 3)

			for i := 0; i < 3; i++ {
				err := tc.failure
				if i < tc.expectSuccess {
					err = nil
				}

				items = append(items, &batchSpend{spend: newSpend(uint32(i), err)}) // nolint:gosec
			}

			result := store.resolveSpendCompletions(context.Background(), bt.NewTx(), items, false)
			require.Len(t, result.spentSpends, tc.expectSuccess,
				"every successful spend must be reported so Spend can roll it back")
		})
	}
}

// TestDecideRollback pins the three-way rollback decision, in particular the
// branch that knowingly leaves a dangling ref behind: when the existence probe
// itself fails we do NOT roll back, because wrongly clearing a slot a live
// spender owns is unrecoverable while a surviving ref is at least counted.
//
// Only a definitive ErrTxNotFound authorises reverting the spends. Every other
// error — storage, service-unavailable, timeout, cancellation — must be treated
// as "unknown", never as "absent". The blob-store case below is the one that
// mattered in review: probing with the default field set pulled the full
// transaction, so a blob read failure surfaced as a storage error and silently
// skipped the rollback; the probe now asks for a single small field.
func TestDecideRollback(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want rollbackDecision
	}{
		{name: "record present", err: nil, want: rollbackSkipSpenderExists},
		{name: "record definitively absent", err: errors.NewTxNotFoundError("not found"), want: rollbackFire},
		{name: "external blob read failed", err: errors.NewStorageError("external store read failed"), want: rollbackSkipIndeterminate},
		{name: "service unavailable", err: errors.NewServiceUnavailableError("batcher closed"), want: rollbackSkipIndeterminate},
		{name: "context canceled", err: errors.NewContextCanceledError("canceled"), want: rollbackSkipIndeterminate},
		{name: "processing error", err: errors.NewProcessingError("aerospike client not initialized"), want: rollbackSkipIndeterminate},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, decideRollback(tc.err))
		})
	}
}

// TestRollbackDecisionLabels guards the metric contract: the decision's String
// is used directly as the "outcome" label value on
// prometheusUtxoPartialSpendRollbacks, so renaming one silently breaks
// dashboards and the #1214 invariant alerting built on them.
func TestRollbackDecisionLabels(t *testing.T) {
	require.Equal(t, utxo.RollbackOutcomeFired, rollbackFire.String())
	require.Equal(t, utxo.RollbackOutcomeSpenderExists, rollbackSkipSpenderExists.String())
	require.Equal(t, utxo.RollbackOutcomeIndeterminate, rollbackSkipIndeterminate.String())
	require.Equal(t, utxo.RollbackOutcomeTransientLock, rollbackSkipTransientLock.String())
	require.Equal(t, utxo.RollbackOutcomeTransientCreating, rollbackSkipTransientCreating.String())

	// Every decision must map to a value in the shared set, and the shared set
	// must have no value no decision produces — otherwise a dashboard either
	// misses an outcome or queries a label that is never emitted. This is the
	// check that would have caught transient_lock being added to the code but
	// not to this test.
	emitted := []string{
		rollbackFire.String(),
		rollbackSkipSpenderExists.String(),
		rollbackSkipIndeterminate.String(),
		rollbackSkipTransientLock.String(),
		rollbackSkipTransientCreating.String(),
	}
	require.ElementsMatch(t, utxo.RollbackOutcomes, emitted,
		"aerospike outcome labels must be exactly utxo.RollbackOutcomes")

	// The literal strings are the dashboard contract; pin them so a rename of a
	// constant cannot silently change what is exported.
	require.ElementsMatch(t,
		[]string{"fired", "spender_exists", "indeterminate", "transient_lock", "transient_creating"},
		utxo.RollbackOutcomes)
}
