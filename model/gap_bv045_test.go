package model

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// gap045Coinbase builds a coinbase that satisfies every rule except possibly finality: one
// consensus-shaped input, a scriptSig that starts with the BIP34 height push and is well inside
// [2,100] bytes, one output of 50 BTC (the regtest subsidy below the first halving), with the
// given nLockTime and input sequence.
func gap045Coinbase(t *testing.T, height, lockTime, sequence uint32) *bt.Tx {
	t.Helper()

	scriptSig := append(util.EncodeCoinbaseHeightPush(height), []byte("/gap-bv-045/")...)

	cb := bt.NewTx()
	require.NoError(t, cb.From("0000000000000000000000000000000000000000000000000000000000000000", 0xFFFFFFFF, "", 0))
	cb.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)
	cb.Inputs[0].SequenceNumber = sequence
	require.NoError(t, cb.AddP2PKHOutputFromAddress("1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn", 50*100_000_000))
	cb.LockTime = lockTime

	require.True(t, IsConsensusCoinbase(cb), "fixture precondition: consensus-shaped coinbase")

	return cb
}

// gap045Block wraps cb in a bound, coinbase-only block at the given height (header merkle root =
// coinbase txid, so CheckCoinbaseOnlyBodyBound succeeds and every failure below is a verdict on the
// block hash).
func gap045Block(t *testing.T, cb *bt.Tx, height uint32) *Block {
	t.Helper()

	hdr := minedHeaderOnParent(t, 4, cb.TxIDChainHash(), regtestGenesisHeader(t).Hash())

	block, err := NewBlock(hdr, cb, []*chainhash.Hash{}, 1, uint64(cb.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}

// TestGapBV045_NonFinalCoinbaseRejected asserts BV-BODY-019 / AC-BV-BODY-019.1.
//
// GAP-BV-045: block-level validation has no coinbase finality check (bitcoin-sv applies IsFinalTx
// to the coinbase in ContextualCheckBlock; Teranode's only finality check is in the Validator,
// which never sees a coinbase). A bound block at height H whose coinbase has nLockTime = H and
// input sequence 0 is non-final (nLockTime is not < H, and the sequence is not 0xFFFFFFFF) and
// MUST be rejected with ERR_BLOCK_INVALID. The control with sequence 0xFFFFFFFF is final and must
// be accepted. Fails today because model/Block.go ValidWithBinding (steps 4a-4b, BIP34, reward)
// never inspects Coinbase LockTime/SequenceNumber, so the non-final block is returned valid.
func TestGapBV045_NonFinalCoinbaseRejected(t *testing.T) {
	const height = uint32(100) // regtest: below the first halving (150); no checkpoints configured

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = bip34ReorderSettings(t)

	validate := func(t *testing.T, block *Block) (bool, error) {
		t.Helper()

		return block.Valid(context.Background(), ulogger.TestLogger{}, blobmemory.New(), createTestUTXOStore(t),
			txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{regtestGenesisHeader(t)}, []uint32{0}, tSettings, nil)
	}

	t.Run("control: sequence 0xFFFFFFFF makes the coinbase final", func(t *testing.T) {
		block := gap045Block(t, gap045Coinbase(t, height, height, 0xFFFFFFFF), height)

		valid, err := validate(t, block)
		require.NoError(t, err, "fixture must satisfy every other block rule, so the only variable is finality")
		require.True(t, valid)
	})

	t.Run("nLockTime == height with sequence 0 is non-final and must be INVALID", func(t *testing.T) {
		block := gap045Block(t, gap045Coinbase(t, height, height, 0), height)

		valid, err := validate(t, block)
		require.Error(t, err, "BV-BODY-019: a non-final coinbase must be rejected (bitcoin-sv bad-txns-nonfinal)")
		require.False(t, valid)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "must be ERR_BLOCK_INVALID on a bound body, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err), "a bound body must not be classified corrupt")
	})
}
