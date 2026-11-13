package validator

import (
	"net/url"
	"testing"
	"time"

	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestCheckStoredTxAtHeight pins the checks a block runs on a tx another block already stored:
// the ones whose verdict depends on the block's height. It writes nothing.
func TestCheckStoredTxAtHeight(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	maturity := uint32(tSettings.ChainCfgParams.CoinbaseMaturity)
	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("CHECK_STORED_TX_AT_HEIGHT_KEY!!!"))

	const coinbaseHeight = 200

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(coinbaseHeight, "/check stored tx at height/"),
		transactions.WithP2PKHOutputs(1, 100_000, publicKey),
	)
	tx := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(coinbase, 0, privateKey),
		transactions.WithP2PKHOutputs(1, 90_000, publicKey),
	)

	blockOptions := func(parentMTP uint32) *Options {
		return ProcessOptions(WithSkipPolicyChecks(true), WithCreateConflicting(true), WithCandidateParentMedianTime(parentMTP), WithCandidateBlockTime(parentMTP))
	}

	mtp := uint32(time.Now().Unix()) //nolint:gosec

	t.Run("the coinbase maturity boundary is the store's", func(t *testing.T) {
		ctx := t.Context()
		logger := ulogger.NewErrorTestLogger(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///check_stored_tx_at_height")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)
		utxoStore.RawDB().SetMaxOpenConns(1)

		_, err = utxoStore.Create(ctx, coinbase, coinbaseHeight, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1, BlockHeight: coinbaseHeight, SubtreeIdx: 0}))
		require.NoError(t, err)

		parents := []InputParent{{IsCoinbase: true, Height: coinbaseHeight}}

		immature := coinbaseHeight + maturity - 1
		_, err = utxoStore.Spend(ctx, tx, immature)
		require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "the store refuses the spend at %d: %v", immature, err)

		err = CheckStoredTxAtHeight(tx, parents, immature, tSettings, blockOptions(mtp))
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "%v", err)
		require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "%v", err)

		mature := coinbaseHeight + maturity
		require.NoError(t, CheckStoredTxAtHeight(tx, parents, mature, tSettings, blockOptions(mtp)))

		_, err = utxoStore.Spend(ctx, tx, mature)
		require.NoError(t, err, "the store allows the spend at %d", mature)
	})

	t.Run("a non-coinbase parent has no maturity", func(t *testing.T) {
		require.NoError(t, CheckStoredTxAtHeight(tx, []InputParent{{Height: coinbaseHeight}}, coinbaseHeight, tSettings, blockOptions(mtp)))
	})

	t.Run("a tx not final at the block's height is invalid", func(t *testing.T) {
		height := coinbaseHeight + maturity

		nonFinal := tx.Clone()
		nonFinal.LockTime = height
		nonFinal.Inputs[0].SequenceNumber = 0

		err := CheckStoredTxAtHeight(nonFinal, []InputParent{{IsCoinbase: true, Height: coinbaseHeight}}, height, tSettings, blockOptions(mtp))
		require.True(t, errors.Is(err, errors.ErrNonFinal), "%v", err)
		requireTopLevelTxInvalid(t, err)

		require.NoError(t, CheckStoredTxAtHeight(nonFinal, []InputParent{{IsCoinbase: true, Height: coinbaseHeight}}, height+1, tSettings, blockOptions(mtp)))
	})

	t.Run("after CSV a tx is final against the candidate parent's MTP", func(t *testing.T) {
		height := max(uint32(tSettings.ChainCfgParams.CSVHeight), coinbaseHeight+maturity)
		parents := []InputParent{{IsCoinbase: true, Height: coinbaseHeight}}

		nonFinal := tx.Clone()
		nonFinal.LockTime = mtp
		nonFinal.Inputs[0].SequenceNumber = 0

		err := CheckStoredTxAtHeight(nonFinal, parents, height, tSettings, blockOptions(mtp))
		require.True(t, errors.Is(err, errors.ErrNonFinal), "%v", err)
		requireTopLevelTxInvalid(t, err)

		require.NoError(t, CheckStoredTxAtHeight(nonFinal, parents, height, tSettings, blockOptions(mtp+1)))

		// as the validator, never a fallback to the tip's time, and no verdict without the time
		err = CheckStoredTxAtHeight(nonFinal, parents, height, tSettings, blockOptions(0))
		require.True(t, errors.Is(err, errors.ErrProcessing), "%v", err)
		require.False(t, errors.Is(err, errors.ErrTxInvalid), "%v", err)
	})

	t.Run("one parent per input", func(t *testing.T) {
		err := CheckStoredTxAtHeight(tx, nil, coinbaseHeight, tSettings, blockOptions(mtp))
		require.True(t, errors.Is(err, errors.ErrProcessing), "%v", err)
	})
}

// requireTopLevelTxInvalid asserts the verdict is err's own code, not one wrapped deeper.
func requireTopLevelTxInvalid(t *testing.T, err error) {
	t.Helper()

	var top *errors.Error
	require.True(t, errors.As(err, &top), "%v", err)
	require.Equal(t, errors.ERR_TX_INVALID, top.Code(), "%v", err)
}
