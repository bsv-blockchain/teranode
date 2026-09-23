package model

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// TestCheckParentExistsOnChain_SameBlockParentFreeze pins the block-level freeze check
// for a parent that is itself in the block (issue #1422). Subtree validation blesses a
// known parent/child pair on existence without re-spending the child, and the chain
// check skips same-block parents, so this is the only place an alert-system freeze on
// such a parent's output is judged.
func TestCheckParentExistsOnChain_SameBlockParentFreeze(t *testing.T) {
	ctx := context.Background()
	store := createTestUTXOStore(t)

	const (
		windowStart = 500
		windowStop  = 600
	)

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	parentTx := newTx(4242)
	parentTx.Inputs[0].PreviousTxSatoshis = 10_000 // the store computes fees on create
	parentTx.Inputs[0].PreviousTxScript = script
	parentTx.Outputs = []*bt.Output{
		{Satoshis: 1000, LockingScript: script},
		{Satoshis: 1000, LockingScript: script},
	}
	parentHash := *parentTx.TxIDChainHash()

	seedParent(t, store, parentTx, 1)

	frozenHash, err := util.UTXOHashFromOutput(&parentHash, parentTx.Outputs[1], 1)
	require.NoError(t, err)

	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{
		TxID: &parentHash, Vout: 1, UTXOHash: frozenHash, FreezeFrom: windowStart, FreezeUntil: windowStop,
	}}, settings.NewSettings()))

	childHash, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	unknownHash, _ := chainhash.NewHashFromStr("000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd")

	check := func(t *testing.T, height uint32, parent chainhash.Hash, vouts []uint32) error {
		t.Helper()

		b := &Block{Height: height}

		_, err := b.checkParentExistsOnChain(ctx, ulogger.TestLogger{}, store, missingParentTx{
			parentTxHash: parent,
			txHash:       *childHash,
			vouts:        vouts,
			sameBlock:    true,
		}, map[uint32]struct{}{})

		return err
	}

	t.Run("spending the frozen output inside the window is block-invalid", func(t *testing.T) {
		err := check(t, windowStart, parentHash, []uint32{0, 1})
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrBlockInvalid)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
		require.ErrorIs(t, err, errors.ErrUtxoConsensusFrozen)
	})

	t.Run("spending only the unfrozen output is fine", func(t *testing.T) {
		require.NoError(t, check(t, windowStart, parentHash, []uint32{0}))
	})

	t.Run("outside the window the frozen output may be spent", func(t *testing.T) {
		require.NoError(t, check(t, windowStart-1, parentHash, []uint32{1}))
		require.NoError(t, check(t, windowStop, parentHash, []uint32{1}))
	})

	t.Run("a same-block parent the store does not hold is passed over", func(t *testing.T) {
		require.NoError(t, check(t, windowStart, *unknownHash, []uint32{1}))
	})

	t.Run("the same parent as an out-of-block edge is still chain-checked", func(t *testing.T) {
		// Sanity: the flag is what selects the freeze-only path. Without it an unknown
		// parent is the ordinary "not in the store" incomplete verdict.
		b := &Block{Height: windowStart}

		_, err := b.checkParentExistsOnChain(ctx, ulogger.TestLogger{}, store, missingParentTx{
			parentTxHash: *unknownHash,
			txHash:       *childHash,
			vouts:        []uint32{1},
		}, map[uint32]struct{}{})
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrBlockIncomplete)
	})
}
