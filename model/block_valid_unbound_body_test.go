package model

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// unboundBodyHeight is above 0 (so the fee/reward arithmetic runs) and far below the regtest BIP34
// activation height (so the coinbase-height check stays out of the way).
const unboundBodyHeight = uint32(100)

// unboundBodyCoinbase returns a coinbase paying exactly satoshis in a single output.
func unboundBodyCoinbase(t *testing.T, satoshis uint64) *bt.Tx {
	t.Helper()

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = coinbase.Outputs[:1]
	coinbase.Outputs[0].Satoshis = satoshis

	require.True(t, coinbase.IsCoinbase())

	return coinbase
}

// honestSubtreeBody builds the honest two-transaction body for coinbase — one subtree carrying the
// coinbase placeholder plus one other transaction paying 1 satoshi of fee — with the header merkle
// root computed over that body. The subtree is written into the returned store so
// GetAndValidateSubtrees can load it. Emptying the returned block's subtree list reproduces the
// truncated wire body: the header still commits the full two-transaction block.
func honestSubtreeBody(t *testing.T, coinbase *bt.Tx) (*Block, *blobmemory.Memory) {
	t.Helper()

	txHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	require.NoError(t, err)

	st, merkleRoot := buildSubtreeAndMerkleRoot(t, coinbase, *txHash)

	block, err := NewBlock(minedHeaderVersion(t, 1, merkleRoot), coinbase,
		[]*chainhash.Hash{st.RootHash()}, 2, 123, unboundBodyHeight, 0)
	require.NoError(t, err)

	store := blobmemory.New()
	storeSubtree(t, store, st)

	return block, store
}

// unboundBodySettings returns regtest settings; the subsidy at unboundBodyHeight is returned too so
// each case can build a coinbase that either matches it exactly or overpays by one satoshi.
func unboundBodySettings(t *testing.T) (*settings.Settings, uint64) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	return tSettings, util.GetBlockSubsidyForHeight(unboundBodyHeight, tSettings.ChainCfgParams)
}

// TestBlock_Valid_UnboundBody covers freemans13 item 1 (bitcoin-sv/teranode#4692). The block message
// carries the transaction count, the size and the subtree count as three independent untrusted
// varints, so a body whose subtree list has been emptied is accepted by the decoder. Before the fix
// such a body reached the fee/reward arithmetic completely unbound to the header, where it either
// produced an INVALID verdict against an honest hash (poisoning it) or — when the honest block's
// real fees happened to be zero — passed outright, accepting a block with no transactions.
//
// The fix binds a subtree-less body against the header before any of that: for a single-transaction
// block the merkle root IS the coinbase txid, so the body's claim is checkable with no subtree store
// at all. Everything downstream is additionally classified by whether the body was bound.
func TestBlock_Valid_UnboundBody(t *testing.T) {
	tSettings, subsidy := unboundBodySettings(t)

	callValid := func(t *testing.T, block *Block, subtreeStore *blobmemory.Memory) (bool, error) {
		t.Helper()

		// A nil *blobmemory.Memory would still be a non-nil interface value, so pass the store
		// through an explicit branch to keep the "no subtree store" case genuinely nil.
		if subtreeStore == nil {
			return block.Valid(context.Background(), ulogger.TestLogger{}, nil, &panicTxMetaStore{},
				txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)
		}

		return block.Valid(context.Background(), ulogger.TestLogger{}, subtreeStore, &panicTxMetaStore{},
			txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)
	}

	t.Run("emptied subtree list on an honest hash is corrupt, never poisoned", func(t *testing.T) {
		// The honest block pays subsidy + the 1 satoshi of fee its subtree carries. With the subtree
		// list emptied the accumulated fees drop to zero, so the arithmetic would condemn the honest
		// hash as inflating — the poisoning this case exists to prevent.
		block, store := honestSubtreeBody(t, unboundBodyCoinbase(t, subsidy+1))
		block.Subtrees = nil
		block.SubtreeSlices = nil

		ok, err := callValid(t, block, store)
		require.False(t, ok)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "a truncated subtree list must be corrupt (re-download), got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an honest hash must never be poisoned by a truncated body")
	})

	t.Run("forged transaction count does not evade the binding", func(t *testing.T) {
		// Rejecting on "no subtrees but TransactionCount > 1" alone would be evaded here, because
		// TransactionCount is the same attacker-supplied varint the emptied subtree list came from.
		// The merkle binding is what actually catches this.
		block, store := honestSubtreeBody(t, unboundBodyCoinbase(t, subsidy+1))
		block.Subtrees = nil
		block.SubtreeSlices = nil
		block.TransactionCount = 1

		ok, err := callValid(t, block, store)
		require.False(t, ok)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "a forged transaction count must not evade the binding, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid))
		require.Contains(t, err.Error(), "merkle root is not the coinbase txid")
	})

	t.Run("zero-fee body with an emptied subtree list is no longer ACCEPTED", func(t *testing.T) {
		// The worse half of the finding: when the coinbase claims exactly the subsidy the
		// arithmetic passes against zero accumulated fees, so before the fix Block.Valid returned
		// true for a body carrying no transactions at all. Asserting ok == false is the pin.
		block, store := honestSubtreeBody(t, unboundBodyCoinbase(t, subsidy))
		block.Subtrees = nil
		block.SubtreeSlices = nil

		ok, err := callValid(t, block, store)
		require.False(t, ok, "a body with no transactions must never be accepted")
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid))
	})

	t.Run("a genuine coinbase-only block is bound and still condemnable", func(t *testing.T) {
		// Proves the binding did not weaken anything: a real coinbase-only block whose merkle root
		// is its coinbase txid IS bound, so overpaying the subsidy is genuine invalidity.
		coinbase := unboundBodyCoinbase(t, subsidy+1)

		block, err := NewBlock(minedHeaderVersion(t, 1, coinbase.TxIDChainHash()), coinbase,
			[]*chainhash.Hash{}, 1, 123, unboundBodyHeight, 0)
		require.NoError(t, err)

		ok, validErr := callValid(t, block, blobmemory.New())
		require.False(t, ok)
		require.Error(t, validErr)
		require.True(t, errors.Is(validErr, errors.ErrBlockInvalid), "a bound coinbase-only body must be condemnable, got: %v", validErr)
		require.False(t, errors.IsBlockCorrupt(validErr))
		require.Contains(t, validErr.Error(), "greater than the fees + block subsidy")
	})

	t.Run("fee verdict on an unbound body is corrupt, not invalid", func(t *testing.T) {
		// The internal-caller shape: subtrees declared but no subtree store, so CheckMerkleRoot
		// never runs and the body is never bound. The fee verdict must follow the bind rule.
		block, _ := honestSubtreeBody(t, unboundBodyCoinbase(t, subsidy+1))

		ok, err := callValid(t, block, nil)
		require.False(t, ok)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "an unbound fee verdict must be corrupt, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an unbound body must never be poisoned by the fee check")
		require.Contains(t, err.Error(), "greater than the fees + block subsidy")
	})
}
