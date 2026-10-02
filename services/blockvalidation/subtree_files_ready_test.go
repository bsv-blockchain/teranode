package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// newSubtreesNotSetHarness builds a *BlockValidation wired to a real sqlitememory
// blockchain store and a real in-memory blob store (no mocks, per AGENTS.md), plus a
// block chained from genesis with subtreeCount fake subtree hashes, added with
// addBlockOpts (subtrees_set=false either way, since none of the tests below pass
// WithSubtreesSet). With no options this mirrors an ordinary peer block the instant it
// lands in the store - every validation path writes its subtree files before adding the
// block, so this is the same state a real block is in whether or not its files happen to
// exist yet; WithInvalid(true) mirrors storeInvalidBlock's record of a rejected one. Neither
// option models the blockchain service's raw RevalidateBlock (its /revalidate/:hash endpoint
// or a direct gRPC call), which is the one route that un-invalidates a block without writing
// its files - see the doc comment on subtreeFilesReady in BlockValidation.go.
//
// The BlockValidation is built as a bare struct, not via NewBlockValidation, so that
// start()'s background goroutines (setMined worker, periodic ticker) never run - this
// test only exercises processSubtreesNotSet / subtreeFilesReady directly and does not
// want a concurrent setMined pass trying to parse the placeholder subtree bytes below.
func newSubtreesNotSetHarness(t *testing.T, subtreeCount int, addBlockOpts ...blockchainoptions.StoreBlockOption) (bv *BlockValidation, block *model.Block, subtreeStore *blobmemory.Memory, ctx context.Context) {
	t.Helper()

	ctx = context.Background()
	logger := ulogger.TestLogger{}
	tSettings := testutil.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	subtreeStore = blobmemory.New()

	bv = &BlockValidation{
		logger:           logger,
		settings:         tSettings,
		blockchainClient: blockchainClient,
		subtreeStore:     subtreeStore,
	}

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From(
		"0000000000000000000000000000000000000000000000000000000000000000",
		0xffffffff, "", 0,
	))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(
		[]byte{0x03, 0x01, 0x00, 0x00, '/', 'T', 'e', 's', 't'},
	)
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))

	subtreeHashes := make([]*chainhash.Hash, subtreeCount)
	for i := range subtreeHashes {
		h := chainhash.HashH([]byte{byte(i + 1)})
		subtreeHashes[i] = &h
	}

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	// Block 1 chains from the genesis block already in the sqlitememory store.
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: coinbaseTx.TxIDChainHash(), // single coinbase is its own merkle root
		Timestamp:      uint32(time.Now().Unix()),  //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}
	// Grind for a valid proof-of-work; the regression-net minimum difficulty
	// (207fffff) converges in at most a few thousand iterations.
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++
		if blockHeader.Nonce > 2_000_000 {
			t.Fatal("failed to find a valid nonce within iteration budget")
		}
	}

	const blockHeight = uint32(1)

	block, err = model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		1,                         // transaction count (coinbase only; the fake subtree hashes are not decoded here)
		uint64(coinbaseTx.Size()), //nolint:gosec
		blockHeight,
		0, // ID is auto-assigned by AddBlock below
	)
	require.NoError(t, err)

	// No WithSubtreesSet option is ever passed here, so subtrees_set=false regardless of
	// addBlockOpts, matching how a block (valid or invalid) lands in the store before its
	// subtrees are known to be validated.
	require.NoError(t, blockchainClient.AddBlock(ctx, block, "test-peer", addBlockOpts...))

	return bv, block, subtreeStore, ctx
}

// subtreesSetFlag reads the persisted subtrees_set flag back from the blockchain store,
// rather than trusting a call was made, so the tests below assert end state.
func subtreesSetFlag(t *testing.T, ctx context.Context, bv *BlockValidation, hash *chainhash.Hash) bool {
	t.Helper()

	_, meta, err := bv.blockchainClient.GetBlockHeader(ctx, hash)
	require.NoError(t, err)
	require.NotNil(t, meta)

	return meta.SubtreesSet
}

// runSweep drives processSubtreesNotSet exactly as the periodic ticker in start() does:
// a fresh errgroup per sweep, waited out before the caller inspects end state.
func runSweep(ctx context.Context, bv *BlockValidation) {
	g, gCtx := errgroup.WithContext(ctx)
	bv.processSubtreesNotSet(gCtx, g)
	_ = g.Wait()
}

func TestProcessSubtreesNotSet_SetsFlagOnceFilesExist(t *testing.T) {
	bv, block, subtreeStore, ctx := newSubtreesNotSetHarness(t, 2)

	for _, h := range block.Subtrees {
		require.NoError(t, subtreeStore.Set(ctx, h[:], fileformat.FileTypeSubtree, []byte("subtree-bytes")))
	}

	runSweep(ctx, bv)

	require.True(t, subtreesSetFlag(t, ctx, bv, block.Hash()),
		"subtrees_set must become true once every referenced subtree file exists")
}

func TestProcessSubtreesNotSet_LeavesFlagFalseWhenOneFileMissing(t *testing.T) {
	bv, block, subtreeStore, ctx := newSubtreesNotSetHarness(t, 2)

	// Only the first subtree file is written; the second is missing, e.g. a partial write
	// or a storage hiccup - see subtreeFilesReady for why a missing file is normally this
	// kind of anomaly rather than "still validating."
	require.NoError(t, subtreeStore.Set(ctx, block.Subtrees[0][:], fileformat.FileTypeSubtree, []byte("subtree-bytes")))

	runSweep(ctx, bv)

	require.False(t, subtreesSetFlag(t, ctx, bv, block.Hash()),
		"subtrees_set must stay false while a referenced subtree file is missing")
}

func TestProcessSubtreesNotSet_SetsFlagOnLaterSweepOnceFileAppears(t *testing.T) {
	bv, block, subtreeStore, ctx := newSubtreesNotSetHarness(t, 2)

	require.NoError(t, subtreeStore.Set(ctx, block.Subtrees[0][:], fileformat.FileTypeSubtree, []byte("subtree-bytes")))

	runSweep(ctx, bv)
	require.False(t, subtreesSetFlag(t, ctx, bv, block.Hash()), "sanity: still false before the second file appears")

	// The second file lands, e.g. the earlier storage hiccup resolves or the file is
	// rewritten.
	require.NoError(t, subtreeStore.Set(ctx, block.Subtrees[1][:], fileformat.FileTypeSubtree, []byte("subtree-bytes")))

	runSweep(ctx, bv)

	require.True(t, subtreesSetFlag(t, ctx, bv, block.Hash()),
		"subtrees_set must be set on a later sweep once every file exists")
}

// TestProcessSubtreesNotSet_ExcludesInvalidBlocks pins the regression found in review of
// #1829: storeInvalidBlock persists a rejected block with subtrees_set=false and
// invalid=true, and its subtree files are usually never written. Before the
// "invalid = false" filter in GetBlocksSubtreesNotSet, such a block could never satisfy
// subtreeFilesReady, so it stayed in the sweep's result set and was re-fetched (and
// re-warned about) every minute forever. Asserted via the store, not a log capture: the
// block must never even be a candidate the sweep sees, which is what actually stops the
// loop.
func TestProcessSubtreesNotSet_ExcludesInvalidBlocks(t *testing.T) {
	bv, block, _, ctx := newSubtreesNotSetHarness(t, 2, blockchainoptions.WithInvalid(true))
	// No subtree files are written - storeInvalidBlock's block usually has none.

	blocksBefore, err := bv.blockchainClient.GetBlocksSubtreesNotSet(ctx)
	require.NoError(t, err)
	require.Empty(t, blocksBefore, "an invalid block must never be a subtrees-not-set sweep candidate")

	runSweep(ctx, bv)

	require.False(t, subtreesSetFlag(t, ctx, bv, block.Hash()),
		"subtrees_set must stay false for an invalid block with no subtree files")

	blocksAfter, err := bv.blockchainClient.GetBlocksSubtreesNotSet(ctx)
	require.NoError(t, err)
	require.Empty(t, blocksAfter, "the invalid block must still not be a candidate after a sweep, so nothing loops")
}

// existsErrorStore is the real in-memory blob store with Exists forced to fail, standing in
// for a storage backend that cannot answer (network blip, permission error).
type existsErrorStore struct {
	*blobmemory.Memory
}

func (s *existsErrorStore) Exists(context.Context, []byte, fileformat.FileType, ...bloboptions.FileOption) (bool, error) {
	return false, errors.NewStorageError("simulated Exists failure")
}

// TestProcessSubtreesNotSet_LeavesFlagFalseWhenExistsFails pins the fail-closed side of
// subtreeFilesReady: when the store cannot say whether a file exists, the sweep must not
// set subtrees_set, even though every file is in fact present, and must leave the block
// a candidate so a later sweep can retry.
func TestProcessSubtreesNotSet_LeavesFlagFalseWhenExistsFails(t *testing.T) {
	bv, block, subtreeStore, ctx := newSubtreesNotSetHarness(t, 2)

	for _, h := range block.Subtrees {
		require.NoError(t, subtreeStore.Set(ctx, h[:], fileformat.FileTypeSubtree, []byte("subtree-bytes")))
	}

	bv.subtreeStore = &existsErrorStore{Memory: subtreeStore}

	runSweep(ctx, bv)

	require.False(t, subtreesSetFlag(t, ctx, bv, block.Hash()),
		"subtrees_set must stay false when the subtree store cannot confirm the files exist")

	candidates, err := bv.blockchainClient.GetBlocksSubtreesNotSet(ctx)
	require.NoError(t, err)
	require.Len(t, candidates, 1, "the block must stay a sweep candidate so a later sweep retries it")
	require.Equal(t, block.Hash().String(), candidates[0].Hash().String())
}
