package model

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestBlock_Valid_DuplicateCheckPrecedesFeeCheck pins the check ordering inside Block.Valid against
// bitcoin-sv (freemans13 item 6 / bitcoin-sv/teranode#4692): svnode runs the CVE-2012-2459 mutation
// check in CheckBlock, ahead of ContextualCheckBlock's coinbase and fee arithmetic. Teranode ran the
// fee arithmetic first, so a body that is BOTH mutated and fee-wrong was classified by the weaker of
// the two rules.
//
// The fixture is deliberately merkle-BOUND, which is what makes this a real discriminator: on a
// bound body the fee verdict is ErrBlockInvalid (condemn the hash) while the duplicate verdict is
// ErrBlockCorrupt (re-download). If the dedup check is ever moved back below the fee check this test
// fails, because the error flips from corrupt to invalid.
func TestBlock_Valid_DuplicateCheckPrecedesFeeCheck(t *testing.T) {
	const blockHeight = uint32(100) // above 0 so the fee check runs, far below the regtest BIP34 height

	tSettings := test.CreateBaseTestSettings(t)
	subsidy := util.GetBlockSubsidyForHeight(blockHeight, tSettings.ChainCfgParams)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	// Overpay well beyond the subsidy plus the subtree's 3 satoshis of fees, so the fee check would
	// definitely fail if it were reached first.
	coinbase.Outputs = coinbase.Outputs[:1]
	coinbase.Outputs[0].Satoshis = subsidy + 1_000

	duplicate := chainhash.HashH([]byte("duplicated-transaction"))
	other := chainhash.HashH([]byte("other-transaction"))

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(duplicate, 1, 100))
	require.NoError(t, subtree.AddNode(other, 1, 100))
	require.NoError(t, subtree.AddNode(duplicate, 1, 100)) // CVE-2012-2459 shape

	// Bind the body to the header: the merkle root is computed over this exact subtree with the
	// coinbase placeholder replaced, which is what CheckMerkleRoot recomputes.
	merkleRoot, err := subtree.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec
	require.NoError(t, err)

	block, err := NewBlock(minedHeaderVersion(t, 1, merkleRoot), coinbase,
		[]*chainhash.Hash{subtree.RootHash()}, 4, 123, blockHeight, 0)
	require.NoError(t, err)

	// Pre-set (non-nil, len matches Subtrees) so GetAndValidateSubtrees skips loading and the binding
	// runs against this in-memory body.
	block.SubtreeSlices = []*subtreepkg.Subtree{subtree}

	valid, err := block.Valid(context.Background(), ulogger.TestLogger{}, &mockSubtreeStore{}, nil,
		txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)

	require.False(t, valid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate transaction", "the mutation check must be reported, not the fee arithmetic")
	require.True(t, errors.IsBlockCorrupt(err), "a mutated body is corrupt (re-download), got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid),
		"the fee check must not have run first — a bound body would have been condemned invalid")
}
