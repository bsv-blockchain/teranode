package validator

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	prometheustestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidateWithOptions_RetryPredicateByErrorType pins which errors returned
// by the UTXO store's SpendAndCreate get the TX_LOCKED/TX_CREATING retry
// treatment in ValidateWithOptions, and which break out on the first attempt.
//
// NOTE: this uses a fully mocked utxo.Store (utxo.MockUtxostore) rather than
// the sqlitememory SQL store that Validator_test.go's validator tests use. The
// sqlitememory store can genuinely produce ErrTxLocked, via the parent's
// `locked` column -- see
// TestValidator_LockedFlagNotChangedIfBlockAssemblyDidNotStoreTx in
// Validator_test.go -- but it cannot produce ErrTxCreating: that error only
// comes from Aerospike's LuaErrorCodeCreating path
// (stores/utxo/aerospike/spend.go) for a large, multi-record parent still being
// written, which the SQL store has no equivalent state for. Rather than fake
// that condition through a store that cannot express it, this test drives the
// retry predicate directly by injecting each error type at the point where the
// validator's own spend call returns it, which is what the "retry on
// locked/creating" logic actually branches on.
func TestValidateWithOptions_RetryPredicateByErrorType(t *testing.T) {
	// MANDATORY here, for the same reason as in
	// TestValidator_TwoPhaseCommitCompletesAfterTxMetaSerializationFailure
	// (Validator_test.go): the retry loop increments package-level CounterVecs that
	// only New() (via initPrometheusMetrics) populates, so without this they are nil
	// and the retry path panics. Running the whole package hides it — an earlier test
	// happens to have run the sync.Once — so the crash only shows up under -run, which
	// is the documented single-test workflow.
	initPrometheusMetrics()

	tracing.SetupMockTracer()

	// The verdict Aerospike really produces for a still-creating parent. When the
	// Lua layer reports CREATING as a top-level response, handleErrorSpends sets
	// one general error across every spend in the parent's record group
	// (aerospike/spend.go:1001), and Spend then returns them nested inside a
	// top-level utxo error (aerospike/spend.go:525).
	aerospikeCreatingErr := errors.NewTxCreatingError("parent still being written (multi-record create)")

	tests := []struct {
		name      string
		returnErr error
		// returnSpends is the []*utxo.Spend returned alongside returnErr. Only
		// the Aerospike-shaped case needs it; nil elsewhere.
		returnSpends []*utxo.Spend
		// wantSentinel is the error the retry predicate must still find after
		// validateInternal has finished rewriting the wrap chain. Defaults to
		// returnErr when nil.
		wantSentinel error
		wantRetried  bool
		// wantCondition is the value the "condition" label must carry on both
		// counters. Only meaningful when wantRetried is true. These label values
		// are promised to operators by docs/references/prometheusMetrics.md and by
		// the validator_txlocked_maxRetries help text, so they are pinned here.
		wantCondition string
	}{
		{
			name:          "TX_LOCKED is retried to the configured budget",
			returnErr:     errors.NewTxLockedError("parent still committing its own two-phase commit"),
			wantRetried:   true,
			wantCondition: "TX_LOCKED",
		},
		{
			name:          "TX_CREATING is retried to the configured budget",
			returnErr:     errors.NewTxCreatingError("parent still being written (multi-record create)"),
			wantRetried:   true,
			wantCondition: "TX_CREATING",
		},
		{
			// The cases above hand the sentinel back as the top-level error,
			// which is not the shape production takes: the store returns an
			// ErrUtxoError with the per-input verdicts nested underneath. That
			// difference matters because being an ErrUtxoError sends
			// validateInternal down the branch at Validator.go:863, which the
			// other cases never enter -- it walks spentUtxos, can divert to the
			// conflicting-create path, and calls SetWrappedErr. So this case
			// pins that the retry still fires when the sentinel is nested rather
			// than top-level, and that the branch leaves it reachable.
			//
			// SetWrappedErr appends to the end of the chain rather than
			// replacing it, so it cannot lose a sentinel that arrived via the
			// constructor -- verified, not assumed. The exposure that remains is
			// the JoinCapped(maxAggregatedSpendErrs) cap inside the store: with
			// more than ten failing inputs, a verdict sorting past the tenth is
			// dropped before the validator ever sees it. That cap is
			// pre-existing and shared with ErrTxLocked.
			name:      "TX_CREATING is retried when nested in the ErrUtxoError the store returns",
			returnErr: errors.NewUtxoError("failed to spend utxos", aerospikeCreatingErr),
			returnSpends: []*utxo.Spend{
				{Err: aerospikeCreatingErr},
				{Err: aerospikeCreatingErr},
			},
			wantSentinel:  aerospikeCreatingErr,
			wantRetried:   true,
			wantCondition: "TX_CREATING",
		},
		{
			name:        "TX_CONFLICTING breaks out on the first attempt",
			returnErr:   errors.NewTxConflictingError("tx conflicts with chain state"),
			wantRetried: false,
		},
		{
			name:        "a generic processing error breaks out on the first attempt",
			returnErr:   errors.NewProcessingError("unrelated storage failure"),
			wantRetried: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			txs := transactions.CreateTestTransactionChainWithCount(t, 3)
			childTx := txs[1]
			require.True(t, childTx.IsExtended(), "test fixture must already be extended so no PreviousOutputsDecorate call is needed")

			mockStore := &utxo.MockUtxostore{}
			mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 1, MedianTime: 1700000000})
			// One parent per input; validateInternal always fetches block-height
			// fields for the parent, extended or not.
			mockStore.On("Get", mock.Anything, mock.Anything, mock.Anything).
				Return(&meta.Data{BlockHeights: []uint32{100}}, nil)
			mockStore.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, tc.returnSpends, tc.returnErr)

			tSettings := test.CreateBaseTestSettings(t)
			// Keep the budget small so the backoff (10ms, 20ms, ...) stays cheap
			// while still proving the loop is bounded rather than unbounded or
			// single-shot for the retried cases.
			tSettings.Validator.TxLockedMaxRetries = 2

			v := &Validator{
				logger:      ulogger.TestLogger{},
				utxoStore:   mockStore,
				settings:    tSettings,
				txValidator: NewTxValidator(ulogger.TestLogger{}, tSettings),
				stats:       gocore.NewStat("validator"),
			}

			opts := &Options{}

			// Counters are package-level and shared across subtests, so compare
			// deltas rather than absolute values.
			retriesBefore := map[string]float64{}
			exhaustedBefore := map[string]float64{}
			for _, c := range []string{"TX_LOCKED", "TX_CREATING"} {
				retriesBefore[c] = prometheustestutil.ToFloat64(prometheusValidatorParentCommitRetries.WithLabelValues(c))
				exhaustedBefore[c] = prometheustestutil.ToFloat64(prometheusValidatorParentCommitExhausted.WithLabelValues(c))
			}

			start := time.Now()
			_, err := v.ValidateWithOptions(ctx, childTx, 2, opts)
			elapsed := time.Since(start)

			require.Error(t, err)

			wantSentinel := tc.wantSentinel
			if wantSentinel == nil {
				wantSentinel = tc.returnErr
			}

			require.True(t, errors.Is(err, wantSentinel), "expected the sentinel to be reachable via errors.Is through the wrap chain, got: %v", err)

			wantCalls := 1
			if tc.wantRetried {
				wantCalls = tSettings.Validator.TxLockedMaxRetries + 1 // 1 initial attempt + N retries

				// 10ms + 20ms backoff between the 3 attempts; assert a floor so a
				// regression that stops sleeping between retries (i.e. retries
				// fire back-to-back with no backoff) is caught. No sleeps beyond
				// what the retry budget itself needs.
				require.GreaterOrEqual(t, elapsed, 30*time.Millisecond, "retried case should have paid the exponential backoff between attempts")
			}
			// No upper bound on elapsed for the non-retried case. The backoff sleep is
			// only reachable between two attempts, so the call count below already
			// proves no sleep happened, and a wall-clock ceiling would only add a
			// scheduler- and GC-sensitive flake under CI contention.

			mockStore.AssertNumberOfCalls(t, "SpendAndCreate", wantCalls)

			// The counters are the observable half of this change: the docs and the
			// setting's help text tell operators to watch these two names with these
			// label values, so a rename or a mislabelled increment has to fail here.
			for _, c := range []string{"TX_LOCKED", "TX_CREATING"} {
				wantRetries, wantExhausted := 0.0, 0.0
				if tc.wantRetried && c == tc.wantCondition {
					wantRetries = float64(tSettings.Validator.TxLockedMaxRetries)
					wantExhausted = 1
				}

				require.Equal(t, wantRetries,
					prometheustestutil.ToFloat64(prometheusValidatorParentCommitRetries.WithLabelValues(c))-retriesBefore[c],
					"parent_commit_retries{condition=%q} delta", c)
				require.Equal(t, wantExhausted,
					prometheustestutil.ToFloat64(prometheusValidatorParentCommitExhausted.WithLabelValues(c))-exhaustedBefore[c],
					"parent_commit_exhausted{condition=%q} delta", c)
			}
		})
	}
}
