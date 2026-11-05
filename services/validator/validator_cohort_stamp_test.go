package validator

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	utxotests "github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

// TestValidator_CohortStamping checks both validator create paths against a
// real store: with UtxoStore.CohortStamping on, CreateInUtxoStore and
// spendAndCreateInUtxoStore must store a wall-clock cohort on the record; with
// it off, the stored label must stay cohort.Unset. The validator is built with
// New so the flag is read the way production reads it.
func TestValidator_CohortStamping(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stamping  bool
		wantClock bool
	}{
		{name: "flag on stamps a clock cohort", stamping: true, wantClock: true},
		{name: "flag off leaves the cohort unset", stamping: false, wantClock: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracing.SetupMockTracer()

			ctx := context.Background()
			logger := ulogger.TestLogger{}

			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.CohortStamping = tc.stamping
			tSettings.BlockAssembly.Disabled = true

			utxoStoreURL, err := url.Parse("sqlitememory:///cohort_stamp")
			require.NoError(t, err)

			utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
			require.NoError(t, err)

			vi, err := New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil, nil)
			require.NoError(t, err)

			v, ok := vi.(*Validator)
			require.True(t, ok)

			parent := utxotests.ParentTx
			child := utxotests.Tx
			require.Equal(t, parent.TxIDChainHash().String(), child.Inputs[0].PreviousTxIDChainHash().String(),
				"the fixture child must spend the fixture parent")

			// Path one: the plain create the validator uses for conflicting and
			// already-known transactions.
			_, err = v.CreateInUtxoStore(ctx, parent, 1000, false, false)
			require.NoError(t, err)

			// Path two: the combined spend and create every validated
			// transaction takes.
			_, _, err = v.spendAndCreateInUtxoStore(ctx, child, 1000, false, &Options{})
			require.NoError(t, err)

			for _, created := range []struct {
				path string
				tx   *bt.Tx
			}{
				{path: "CreateInUtxoStore", tx: parent},
				{path: "spendAndCreateInUtxoStore", tx: child},
			} {
				md, err := utxoStore.Get(ctx, created.tx.TxIDChainHash(), fields.Cohort)
				require.NoError(t, err)

				if tc.wantClock {
					require.True(t, cohort.ID(md.Cohort).IsClock(),
						"%s must stamp a clock cohort, got %d", created.path, md.Cohort)
				} else {
					require.Equal(t, uint32(cohort.Unset), md.Cohort,
						"%s must not stamp a cohort with the flag off", created.path)
				}
			}
		})
	}
}

// TestValidator_CohortStampFailureDoesNotFailTheTx checks the stampCohort
// fallback. With the flag on but a stamper that cannot issue an ID (its clock is
// past the end of the clock cohort space and it has no floor to fall back on),
// both create paths must still succeed, the created outputs must be spendable,
// and the stored label must be cohort.Unset, the same as the flag-off path.
func TestValidator_CohortStampFailureDoesNotFailTheTx(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.CohortStamping = true
	tSettings.BlockAssembly.Disabled = true

	utxoStoreURL, err := url.Parse("sqlitememory:///cohort_stamp_failure")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	vi, err := New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	v, ok := vi.(*Validator)
	require.True(t, ok)

	// A clock reading beyond the last representable clock cohort.
	v.cohortStamper = cohort.NewStamper(cohort.WithClock(func() time.Time { return time.Unix(1<<33, 0) }))

	_, stampErr := v.cohortStamper.Stamp()
	require.Error(t, stampErr, "pre-condition: this stamper cannot issue an ID")

	parent := utxotests.ParentTx
	child := utxotests.Tx

	_, err = v.CreateInUtxoStore(ctx, parent, 1000, false, false)
	require.NoError(t, err, "a stamp failure must not fail the plain create")

	_, _, err = v.spendAndCreateInUtxoStore(ctx, child, 1000, false, &Options{})
	require.NoError(t, err, "a stamp failure must not fail the spend and create")

	for _, created := range []*bt.Tx{parent, child} {
		md, err := utxoStore.Get(ctx, created.TxIDChainHash(), fields.Cohort)
		require.NoError(t, err)
		require.Equal(t, uint32(cohort.Unset), md.Cohort, "tx %s must be created unlabelled", created.TxIDChainHash())
	}

	spend, err := utxoStore.GetSpend(ctx, &utxo.Spend{TxID: child.TxIDChainHash(), Vout: 0})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), spend.Status, "the child's output must be spendable")
}
