package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// CheckCoinbaseOnlyBodyBound ties a no-subtree body to its header, but it does not say
// the single transaction in that body is coinbase-shaped. The quick path's own coinbase
// checks are len(Inputs) != 0 (BlockIncomplete) and the scriptSig length bound, neither
// of which rejects an ordinary spend standing in for the coinbase.
//
// The check belongs AFTER the binding: once the header commits to this transaction, a
// non-coinbase one is the miner's own committed body and so genuinely invalid. Ahead of
// the binding the same failure would be indistinguishable from a corrupted download,
// which is the classification bitcoin-sv/teranode#4692 settled.

// buildBoundNonCoinbaseBody builds a coinbase-only block whose only transaction is a
// normal spend, with the header merkle root set to that txid so the body is genuinely
// bound.
func buildBoundNonCoinbaseBody(t *testing.T) *model.Block {
	t.Helper()

	block := testhelpers.CreateTestBlocks(t, 1)[0]

	// An ordinary spend: one input against a real outpoint, so it is not a coinbase.
	spend := bt.NewTx()
	require.NoError(t, spend.From("6a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b", 0, "76a914000000000000000000000000000000000000000088ac", 1000))
	require.NoError(t, spend.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 900))

	// A scriptSig inside the coinbase length bound (2..MaxCoinbaseScriptSigSize), so the
	// length check is not what rejects this and the shape check is what the test exercises.
	spend.Inputs[0].UnlockingScript = bscript.NewFromBytes(make([]byte, 16))

	require.False(t, spend.IsCoinbase())

	block.CoinbaseTx = spend

	// Bind it: the header commits to exactly this transaction, so the body is not corrupt.
	block.Header.HashMerkleRoot = spend.TxIDChainHash()
	block.Header.Nonce = 0
	testhelpers.MineHeader(block.Header)

	return block
}

// Both entry points are covered: quickValidateBlockAsync is the catch-up path, and a
// guard tested only through the synchronous twin can be dropped from the async pipeline
// with the suite still green.
func TestQuickValidateBlock_BoundNonCoinbaseBodyRejected(t *testing.T) {
	t.Run("quickValidateBlock", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, buildBoundNonCoinbaseBody(t), "test", "", nil)

		require.Error(t, err, "a bound body whose only transaction is not a coinbase must be rejected")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"the body is bound, so this is genuine invalidity rather than a corrupt download: got %v", err)
		suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("quickValidateBlockAsync: same body, same verdict", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		// Registered although the guard should stop short of it: without this a regression
		// that drops the guard reaches commitBlock and panics the whole test binary on an
		// unexpected mock call, instead of failing this assertion.
		suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()

		// Buffered so the async path never blocks queuing write jobs; this body queues none.
		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		_, _, err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, buildBoundNonCoinbaseBody(t), "test", "", writeJobsChan)

		require.Error(t, err, "the catch-up entry point must reject the same body")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"the body is bound, so this is genuine invalidity rather than a corrupt download: got %v", err)
		suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
}
