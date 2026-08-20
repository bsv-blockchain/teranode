package blockvalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidateBlock_TruncatedSubtreeList_IsCorruptNotPoisoned is the wire-shape regression for
// bitcoin-sv/teranode#4692. The finding exists BECAUSE the block message carries
// the transaction count, the size and the subtree count as three independent untrusted varints, so
// the test drives the real encoder and the real decoder rather than hand-building a model.Block: an
// in-memory-only fixture would prove the check but not that it covers the actual ingress shape.
//
// The honest hash is a two-transaction block. Served with its subtree list emptied (and the
// transaction count forged down to 1, the evasion that a TransactionCount-based rejection would
// miss), the body no longer matches the header the miner committed. It must be classified CORRUPT —
// re-downloaded from another peer, the serving peer struck — and the honest hash must never be
// marked invalid or persisted, because a truncated body is evidence about the transfer, not about
// the block.
func TestValidateBlock_TruncatedSubtreeList_IsCorruptNotPoisoned(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false // synchronous block.Valid path

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Subtree validation passes; the only failure must come from block.Valid's body binding.
	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// A coinbase encoding this block's own height, so nothing in the fixture is incidentally
	// BIP34-bad — the corrupt verdict under test must come from the emptied subtree list alone.
	const blockHeight = uint32(100)

	coinbaseTx := coinbaseAtHeight(t, blockHeight)

	// The honest body: one subtree holding the coinbase placeholder plus one other transaction. The
	// header commits that body, so its merkle root is NOT the coinbase txid.
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(chainhash.Hash{0xAB}, 100, 0))

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	merkleRoot := replicated.RootHash()

	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, merkleRoot)

	honest, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()), blockHeight, 0) //nolint:gosec
	require.NoError(t, err)

	// The tampered delivery: same header (same hash, same proof of work), subtree list emptied and
	// the transaction count forged to 1. Built through the real constructor so only wire-encoded
	// fields differ from the honest block.
	tampered, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, honest.SizeInBytes, honest.Height, 0)
	require.NoError(t, err)

	raw, err := tampered.Bytes()
	require.NoError(t, err)

	// The decoder ACCEPTING this shape is the premise of the finding, so assert it explicitly: if a
	// future change makes the decoder reject it, this test should say the premise moved rather than
	// pass for the wrong reason.
	decoded, err := model.NewBlockFromBytes(raw)
	require.NoError(t, err, "the wire decoder accepts an emptied subtree list — that is the premise of this test")
	require.Empty(t, decoded.Subtrees, "the delivered body carries no subtrees")
	require.True(t, decoded.Header.HashMerkleRoot.IsEqual(honest.Header.HashMerkleRoot),
		"the header still commits the honest two-transaction body")
	require.True(t, decoded.Hash().IsEqual(honest.Hash()), "the tampering does not change the block hash")

	fake := &corruptStrikeP2PClient{}

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeVal)
	bv.p2pClient = fake

	err = bv.ValidateBlockWithOptions(ctx, decoded, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "a truncated subtree list must be corrupt (re-download), got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "the honest hash must never be condemned on an unbound body")

	// Nothing persisted: no invalid=true row, no existence record.
	exists, existsErr := blockchainClient.GetBlockExists(ctx, decoded.Header.Hash())
	require.NoError(t, existsErr)
	require.False(t, exists, "a corrupt body must not be stored, so the honest hash is not poisoned")

	// The peer that served the truncated body is struck.
	calls := fake.recorded()
	require.Len(t, calls, 1, "the serving peer must be struck exactly once")
	require.Equal(t, "peer-serving", calls[0].peerID)
}
