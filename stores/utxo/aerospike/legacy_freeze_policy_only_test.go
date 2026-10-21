package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestLegacyFreezeIsPolicyOnly pins how an output frozen before the freeze bins existed
// reads: the 0xFF sentinel in its spending-data slot and no freeze bins. It is policy
// only — the Lua spend path holds it from the mempool but never reports the consensus
// verdict, and block validation's Go reader reports no record — exactly as the SQL store
// reads a frozen row with a NULL freezeFrom (issue #1422).
func TestLegacyFreezeIsPolicyOnly(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	cleanDB(t, client)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0}}, tSettings))

	// Rewind the record to its pre-upgrade shape: the sentinel stays, the bins go.
	keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), 0, store.GetUtxoBatchSize()) //nolint:gosec
	key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
	require.NoError(t, aErr)

	_, aErr = client.Operate(nil, key,
		aerospike.PutOp(aerospike.NewBin(fields.UtxoFreezeFrom.String(), nil)),
		aerospike.PutOp(aerospike.NewBin(fields.UtxoFreezeUntil.String(), nil)),
		aerospike.PutOp(aerospike.NewBin(fields.UtxoFreezeExp.String(), nil)))
	require.NoError(t, aErr)

	got, err := store.Get(ctx, tx.TxIDChainHash(), fields.UtxoFreezeFrom, fields.UtxoFreezeUntil, fields.UtxoFreezeExp)
	require.NoError(t, err)
	require.Empty(t, got.FreezeRecords, "a sentinel-only freeze has no consensus record")

	spendTx := utxo2.GetSpendingTx(tx, 0)

	_, err = store.Spend(ctx, spendTx, 100)
	require.ErrorIs(t, err, errors.ErrFrozen, "the policy tier still holds the coin")
	require.NotErrorIs(t, err, errors.ErrUtxoConsensusFrozen)

	// Re-freezing with a window — the zero window included — is not "already frozen":
	// it records the window and makes the freeze a consensus rule.
	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0}}, tSettings))

	_, err = store.Spend(ctx, spendTx, 100, utxo.IgnoreFlags{IgnorePolicyFreeze: true})
	require.ErrorIs(t, err, errors.ErrUtxoConsensusFrozen, "the recorded window is enforced")

	// Back to the legacy shape: a block spend of it is not a consensus violation.
	_, aErr = client.Operate(nil, key,
		aerospike.PutOp(aerospike.NewBin(fields.UtxoFreezeFrom.String(), nil)),
		aerospike.PutOp(aerospike.NewBin(fields.UtxoFreezeUntil.String(), nil)),
		aerospike.PutOp(aerospike.NewBin(fields.UtxoFreezeExp.String(), nil)))
	require.NoError(t, aErr)

	_, err = store.Spend(ctx, spendTx, 100, utxo.IgnoreFlags{IgnorePolicyFreeze: true})
	require.NoError(t, err, "a block spend of a sentinel-only freeze is not a consensus violation")
}
