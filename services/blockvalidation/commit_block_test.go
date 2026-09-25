package blockvalidation

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newCommitBlockHarness builds the minimal in-memory harness for TestCommitBlock_AddsBlockAndSetsExists.
//
// It creates:
//   - a sqlitememory blockchain store (genesis auto-inserted)
//   - a LocalClient wrapping that store (provides real GetBlockExists/GetBlockIsMined)
//   - a BlockValidation wired to the LocalClient
//   - a mined block chained from genesis with a valid proof-of-work and block.ID pre-assigned
//
// The block has no subtrees so unlockSubtreeTransactionsIfNeeded is a no-op.
func newCommitBlockHarness(t *testing.T) (*BlockValidation, *model.Block, context.Context) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := testutil.CreateBaseTestSettings(t)

	// Build a real sqlitememory blockchain store; genesis is auto-inserted on New.
	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Minimal stores — commitBlock does not use subtreeStore or utxoStore directly
	// (the subtree unlock is a no-op when block.SubtreeSlices is empty).
	subtreeStore := blobmemory.New()
	mockUTXO := &utxo.MockUtxostore{}

	bv := NewBlockValidation(ctx, logger, tSettings, blockchainClient, subtreeStore, nil, mockUTXO, nil, nil)

	// Build a coinbase paying block-1 subsidy; the private key / address are
	// throwaway since we never validate scripts here.
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

	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		nil,                       // no subtrees — unlock is a no-op
		1,                         // transaction count (coinbase only)
		uint64(coinbaseTx.Size()), //nolint:gosec
		blockHeight,
		0, // ID will be assigned below
	)
	require.NoError(t, err)

	// Assign the block ID that processBlockSubtrees normally provides before
	// calling commitBlock.
	assignedID, err := blockchainClient.AssignBlockID(ctx, block.Hash())
	require.NoError(t, err)
	id32, err := blockIDToUint32(assignedID, block.Hash().String())
	require.NoError(t, err)
	block.ID = id32

	return bv, block, ctx
}

// TestCommitBlock_AddsBlockAndSetsExists verifies the extracted commit tail:
// AddBlock stores the block with MinedSet+SubtreesSet+ID, and SetBlockExists
// records it. Uses the same in-memory harness as the quick-validate tests.
func TestCommitBlock_AddsBlockAndSetsExists(t *testing.T) {
	// Build a *BlockValidation and a valid below-checkpoint block (no subtrees)
	// with block.ID already assigned, mirroring the state processBlockSubtrees
	// leaves before commitBlock runs.
	u, block, ctx := newCommitBlockHarness(t)

	require.NoError(t, u.commitBlock(ctx, block, "test-peer", "commitBlock"))

	// AddBlock committed it: GetBlockExists / blockchainClient reports it present + mined
	exists, err := u.blockchainClient.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, exists)

	mined, err := u.blockchainClient.GetBlockIsMined(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, mined, "commitBlock must AddBlock with MinedSet=true")
}

// TestCommitBlock_UnlockFailureStillFinishesTheCommit pins the post-commit
// unlock rule: once AddBlock has stored the block, a failed release of the
// create-phase lock is logged and counted, and the commit tail still runs. The
// caller must not see a stored block as a failed one, and the block-exists
// cache, which runs after the unlock, must still be set.
func TestCommitBlock_UnlockFailureStillFinishesTheCommit(t *testing.T) {
	u, block, ctx := newCommitBlockHarness(t)

	// One subtree: the coinbase placeholder plus one transaction, so the
	// unlock pass has a record to release.
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(chainhash.HashH([]byte("locked-tx")), 1, 1))

	block.SubtreeSlices = []*subtreepkg.Subtree{subtree}

	mockUTXO, ok := u.utxoStore.(*utxo.MockUtxostore)
	require.True(t, ok)
	mockUTXO.On("SetLocked", mock.Anything, mock.Anything, false).Return(errors.NewStorageError("store overloaded"))

	initPrometheusMetrics()
	before := promtestutil.ToFloat64(prometheusQuickValidatePostCommitUnlockFailures)

	require.NoError(t, u.commitBlock(ctx, block, "test-peer", "commitBlock"),
		"a failed unlock after AddBlock must not fail the committed block")

	mockUTXO.AssertNumberOfCalls(t, "SetLocked", postCommitUnlockAttempts)
	require.Equal(t, float64(1), promtestutil.ToFloat64(prometheusQuickValidatePostCommitUnlockFailures)-before)

	_, cached := u.blockExistsCache.Get(*block.Hash())
	require.True(t, cached, "the commit tail after the unlock still ran")

	exists, err := u.blockchainClient.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, exists)
}

// TestCommitBlock_UnlockRetriesDetachedFromCallerContext pins why the
// post-commit unlock can swallow its error: nothing re-runs it, so it retries
// on its own, on a context the caller cannot cancel. The caller's context is
// cancelled while the first attempt is in flight and that attempt fails; the
// retry must still reach the store on a live context and release the lock.
func TestCommitBlock_UnlockRetriesDetachedFromCallerContext(t *testing.T) {
	u, block, baseCtx := newCommitBlockHarness(t)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(chainhash.HashH([]byte("locked-tx")), 1, 1))

	block.SubtreeSlices = []*subtreepkg.Subtree{subtree}

	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	mockUTXO, ok := u.utxoStore.(*utxo.MockUtxostore)
	require.True(t, ok)

	var retryCtxErr error

	mockUTXO.On("SetLocked", mock.Anything, mock.Anything, false).
		Run(func(mock.Arguments) { cancel() }).
		Return(errors.NewStorageError("shutdown raced the commit")).Once()
	mockUTXO.On("SetLocked", mock.Anything, mock.Anything, false).
		Run(func(args mock.Arguments) { retryCtxErr = args.Get(0).(context.Context).Err() }).
		Return(nil).Once()

	initPrometheusMetrics()
	before := promtestutil.ToFloat64(prometheusQuickValidatePostCommitUnlockFailures)

	require.NoError(t, u.commitBlock(ctx, block, "test-peer", "commitBlock"))

	mockUTXO.AssertNumberOfCalls(t, "SetLocked", 2)
	require.NoError(t, retryCtxErr, "the retry must not inherit the caller's cancellation")
	require.Zero(t, promtestutil.ToFloat64(prometheusQuickValidatePostCommitUnlockFailures)-before,
		"a retry that succeeds is not a failure")
}

// countingSubtreesSetClient is the real local client with SetBlockSubtreesSet counted. Every
// other call goes straight to the sqlitememory store, so the block row read back below is the
// store's own answer, not a mock's.
type countingSubtreesSetClient struct {
	blockchain.ClientI
	subtreesSetCalls atomic.Int32
}

func (c *countingSubtreesSetClient) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	c.subtreesSetCalls.Add(1)

	return c.ClientI.SetBlockSubtreesSet(ctx, blockHash)
}

// TestCommitBlock_WritesSubtreesSetOnceAtInsert pins that the quick commit sets subtrees_set
// through AddBlock alone. The insert already writes the column, so a second SetBlockSubtreesSet
// call would re-UPDATE a row that is already true, commit on its own, clear the blockchain
// response cache and notify every subscriber, all inside the one stage every block passes
// through in order: 2.6 ms a block measured on a mainnet sync.
func TestCommitBlock_WritesSubtreesSetOnceAtInsert(t *testing.T) {
	u, block, ctx := newCommitBlockHarness(t)

	counting := &countingSubtreesSetClient{ClientI: u.blockchainClient}
	u.blockchainClient = counting

	require.NoError(t, u.commitBlock(ctx, block, "test-peer", "commitBlock"))

	require.Equal(t, int32(0), counting.subtreesSetCalls.Load(), "the quick commit must not send SetBlockSubtreesSet")

	_, meta, err := u.blockchainClient.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, meta.SubtreesSet, "AddBlock writes subtrees_set at insert")
	require.True(t, meta.MinedSet, "and mined_set with it")
}
