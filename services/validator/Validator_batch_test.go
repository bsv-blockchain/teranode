package validator

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupBatchTestValidatorWithChain returns a validator over an in-memory SQL store
// holding the root of a linear chain of chainLen transactions (txs[i] spends
// txs[i-1]); txs[1:] are not in the store yet.
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

	// Block height high enough for coinbase maturity of the root created at height 1.
	require.NoError(t, utxoStore.SetBlockState(200, 1_700_000_000))

	txs := transactions.CreateTestTransactionChainWithCount(t, chainLen+1)

	_, err = utxoStore.Create(ctx, txs[0], 1)
	require.NoError(t, err)

	vi, err := New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	return vi.(*Validator), txs
}

func TestValidateBatch_EmptyBatch(t *testing.T) {
	v, _ := setupBatchTestValidatorWithChain(t, 2)

	metaResults, errs := v.ValidateBatch(context.Background(), []*bt.Tx{}, 0, nil)
	require.Empty(t, metaResults)
	require.Empty(t, errs)
}

func TestValidateBatch_SingleTx(t *testing.T) {
	v, txs := setupBatchTestValidatorWithChain(t, 2)

	metaResults, errs := v.ValidateBatch(context.Background(), []*bt.Tx{txs[1]}, 0, NewDefaultOptions())
	require.Len(t, errs, 1)
	require.NoError(t, errs[0])
	require.NotNil(t, metaResults[0])

	stored := &meta.Data{}
	require.NoError(t, v.utxoStore.GetMeta(context.Background(), txs[1].TxIDChainHash(), stored))
}

func TestValidateBatch_NilOptions(t *testing.T) {
	v, txs := setupBatchTestValidatorWithChain(t, 2)

	metaResults, errs := v.ValidateBatch(context.Background(), []*bt.Tx{txs[1]}, 0, nil)
	require.Len(t, errs, 1)
	require.NoError(t, errs[0])
	require.NotNil(t, metaResults[0])
}

func TestValidateBatch_CoinbaseAndNilRejected(t *testing.T) {
	v, txs := setupBatchTestValidatorWithChain(t, 2)

	coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	metaResults, errs := v.ValidateBatch(context.Background(), []*bt.Tx{coinbaseTx, nil, txs[1]}, 0, NewDefaultOptions())
	require.Len(t, metaResults, 3)
	require.Len(t, errs, 3)
	require.Error(t, errs[0], "coinbase tx should be rejected")
	require.Error(t, errs[1], "nil tx should be rejected")
	require.NoError(t, errs[2], "valid tx should succeed")
	require.NotNil(t, metaResults[2])
}

// TestValidateBatch_InBatchChain checks that a child whose parent is in the same
// batch is validated after the parent, whatever the order in the batch.
func TestValidateBatch_InBatchChain(t *testing.T) {
	v, txs := setupBatchTestValidatorWithChain(t, 5)

	batch := []*bt.Tx{txs[4], txs[2], txs[3], txs[1]}

	metaResults, errs := v.ValidateBatch(context.Background(), batch, 0, NewDefaultOptions())
	require.Len(t, errs, len(batch))

	for i := range batch {
		require.NoError(t, errs[i], "tx %d", i)
		require.NotNil(t, metaResults[i], "tx %d", i)
	}
}

func TestBatchWaves(t *testing.T) {
	txs := transactions.CreateTestTransactionChainWithCount(t, 6)
	batch := []*bt.Tx{txs[4], txs[2], txs[3], txs[1]}
	errs := make([]error, len(batch))

	waves := batchWaves(batch, errs)
	require.Equal(t, [][]int{{3}, {1}, {2}, {0}}, waves)

	errs[3] = assert.AnError
	waves = batchWaves(batch, errs)
	require.Equal(t, [][]int{{1}, {2}, {0}}, waves, "a tx that already failed is not scheduled and does not hold back its children")
}

// TestValidateBatch_ErrorsMatchSinglePath checks that a rejected transaction gets
// the same error from the batch as from ValidateWithOptions.
func TestValidateBatch_ErrorsMatchSinglePath(t *testing.T) {
	v, txs := setupBatchTestValidatorWithChain(t, 3)

	badTx := txs[1].Clone()
	badScript := bscript.Script([]byte{0xde, 0xad})
	badTx.Inputs[0].UnlockingScript = &badScript

	_, singleErr := v.ValidateWithOptions(context.Background(), badTx.Clone(), 0, NewDefaultOptions())
	require.Error(t, singleErr)

	metaResults, errs := v.ValidateBatch(context.Background(), []*bt.Tx{badTx, txs[1]}, 0, NewDefaultOptions())
	require.Len(t, metaResults, 2)
	require.Error(t, errs[0], "tx with bad script should fail")
	assert.Equal(t, singleErr.Error(), errs[0].Error())
	require.NoError(t, errs[1], "valid tx should succeed")
	require.NotNil(t, metaResults[1])

	// The same transaction again is an idempotent success, as on the single path.
	_, errs = v.ValidateBatch(context.Background(), []*bt.Tx{txs[1]}, 0, NewDefaultOptions())
	require.NoError(t, errs[0])

	// A double spend of the same parent output is rejected per item.
	doubleSpend := txs[1].Clone()
	doubleSpend.Outputs[0].Satoshis--

	_, singleDoubleSpendErr := v.ValidateWithOptions(context.Background(), doubleSpend.Clone(), 0, NewDefaultOptions())
	require.Error(t, singleDoubleSpendErr)

	_, errs = v.ValidateBatch(context.Background(), []*bt.Tx{doubleSpend}, 0, NewDefaultOptions())
	require.Error(t, errs[0])
}

func TestValidateBatch_CancelledContext(t *testing.T) {
	v, txs := setupBatchTestValidatorWithChain(t, 2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, errs := v.ValidateBatch(ctx, []*bt.Tx{txs[1]}, 0, NewDefaultOptions())
	require.ErrorIs(t, errs[0], context.Canceled)
}
