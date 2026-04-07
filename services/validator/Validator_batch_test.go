//go:build !aerospike

package validator

import (
	"context"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

func setupBatchTestValidator(t *testing.T) *Validator {
	t.Helper()
	tracing.SetupMockTracer()
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = true

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_, err = utxoStore.Create(ctx, tests.ParentTx, 999)
	require.NoError(t, err)

	vi, err := New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil)
	require.NoError(t, err)

	return vi.(*Validator)
}

func setupBatchTestValidatorWithChain(t *testing.T, chainLen uint32) (*Validator, []*bt.Tx) {
	t.Helper()
	tracing.SetupMockTracer()
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = true

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// Set block height high enough for coinbase maturity (100 confirmations needed)
	_ = utxoStore.SetBlockHeight(200)

	txs := transactions.CreateTestTransactionChainWithCount(t, chainLen)
	// Create root (coinbase) at height 1 so it has 199 confirmations at height 200
	_, err = utxoStore.Create(ctx, txs[0], 1)
	require.NoError(t, err)

	vi, err := New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil)
	require.NoError(t, err)

	return vi.(*Validator), txs
}

func TestValidateBatch_EmptyBatch(t *testing.T) {
	v := setupBatchTestValidator(t)
	ctx := context.Background()

	metaResults, errs := v.ValidateBatch(ctx, []*bt.Tx{}, 0, nil)
	require.Empty(t, metaResults)
	require.Empty(t, errs)
}

func TestValidateBatch_SingleTx(t *testing.T) {
	v := setupBatchTestValidator(t)
	ctx := context.Background()

	metaResults, errs := v.ValidateBatch(ctx, []*bt.Tx{tests.Tx}, 0, &Options{})
	require.Len(t, metaResults, 1)
	require.Len(t, errs, 1)
	require.NoError(t, errs[0])
	require.NotNil(t, metaResults[0])
}

func TestValidateBatch_MultipleTxs(t *testing.T) {
	// Create a chain with enough siblings: txs[0] is root, txs[1]..txs[3] all spend txs[0]
	// Since CreateTestTransactionChainWithCount creates a linear chain (each tx spends the previous),
	// we can only validate txs[1] independently. For truly parallel batch validation we need
	// txs whose parents are already in the store.
	// Use two separate batches: first validate txs[1], then txs[2] (which needs txs[1] in store).
	v, txs := setupBatchTestValidatorWithChain(t, 4)
	ctx := context.Background()

	// First batch: validate txs[1] (parent txs[0] is in store)
	metaResults, errs := v.ValidateBatch(ctx, []*bt.Tx{txs[1]}, 0, &Options{})
	require.Len(t, errs, 1)
	require.NoError(t, errs[0], "tx 1 should succeed")
	require.NotNil(t, metaResults[0])

	// Second batch: validate txs[2] (parent txs[1] is now in store from first batch)
	metaResults, errs = v.ValidateBatch(ctx, []*bt.Tx{txs[2]}, 0, &Options{})
	require.Len(t, errs, 1)
	require.NoError(t, errs[0], "tx 2 should succeed")
	require.NotNil(t, metaResults[0])
}

func TestValidateBatch_CoinbaseRejected(t *testing.T) {
	v := setupBatchTestValidator(t)
	ctx := context.Background()

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	metaResults, errs := v.ValidateBatch(ctx, []*bt.Tx{coinbaseTx, tests.Tx}, 0, &Options{})
	require.Len(t, metaResults, 2)
	require.Len(t, errs, 2)
	require.Error(t, errs[0], "coinbase tx should be rejected")
	require.NoError(t, errs[1], "valid tx should succeed")
	require.NotNil(t, metaResults[1])
}

func TestValidateBatch_NilOptions(t *testing.T) {
	v := setupBatchTestValidator(t)
	ctx := context.Background()

	metaResults, errs := v.ValidateBatch(ctx, []*bt.Tx{tests.Tx}, 0, nil)
	require.Len(t, metaResults, 1)
	require.Len(t, errs, 1)
	require.NoError(t, errs[0])
	require.NotNil(t, metaResults[0])
}

func TestValidateBatch_ScriptFailure(t *testing.T) {
	v, txs := setupBatchTestValidatorWithChain(t, 3)
	ctx := context.Background()

	// Corrupt the unlocking script of a copy of txs[1]
	badTx := txs[1].Clone()
	badScript := bscript.Script([]byte{0xde, 0xad})
	badTx.Inputs[0].UnlockingScript = &badScript

	// Batch: corrupted tx should fail script verification; valid tx should succeed
	metaResults, errs := v.ValidateBatch(ctx, []*bt.Tx{txs[1], badTx}, 0, &Options{})
	require.Len(t, metaResults, 2)
	require.Len(t, errs, 2)
	require.NoError(t, errs[0], "valid tx should succeed")
	require.Error(t, errs[1], "tx with bad script should fail")
}
