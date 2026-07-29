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
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// buildFloaterBlock assembles a structurally-valid candidate block whose only
// non-coinbase tx (childTx) spends an EXTERNAL parent (parentTx) that lives in
// the UTXO store WITH EMPTY BlockIDs and is NOT one of the block's transactions
// — the exact floater shape the P0 fix targets:
//
//   - the parent is unconfirmed (no recorded block membership), and
//   - the parent is absent from this block,
//
// so model.Block.Valid → checkParentsExistOnChain → getParentTxMetaBlockIDs
// returns ErrBlockIncomplete ("has no block IDs"). Subtree validation itself
// succeeds (the child blesses fail-open under the candidate-height option), so
// the only failure signal is that ErrBlockIncomplete from block.Valid — which
// the FSM-gated handlers must reclassify as a consensus violation in RUNNING
// and leave retryable in catchup.
//
// The parent and child are created in the supplied utxoStore so the same store
// can be threaded into NewBlockValidation, and the subtree/subtreeMeta are
// written into subtreeStore for block.Valid to read.
func buildFloaterBlock(t *testing.T, utxoStore utxostore.Store, subtreeStore blob.Store) (*model.Block, *bt.Tx) {
	t.Helper()

	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)

	// Coinbase paying exactly the genesis+1 subsidy so checkBlockRewardAndFees passes.
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))
	_, _, err = utxoStore.SpendAndCreate(ctx, coinbaseTx, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)

	// The FLOATER PARENT: in the UTXO store, but created WITHOUT mined-block
	// info so its BlockIDs stay empty. It is deliberately NOT added to the block.
	_, _, err = utxoStore.SpendAndCreate(ctx, parentTx, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)

	parentMeta, err := utxoStore.Get(ctx, parentTx.TxIDChainHash())
	require.NoError(t, err)
	require.Empty(t, parentMeta.BlockIDs, "fixture invariant: floater parent must have no recorded block IDs")

	// Child spends the external floater parent. newTx(seed, parentHash) wires a
	// single input referencing parentTx, so getParentTxMetaBlockIDs resolves to
	// the unconfirmed parent rather than to an in-block tx via b.txMap.
	childTx := newTx(7, parentTx.TxIDChainHash())
	_, _, err = utxoStore.SpendAndCreate(ctx, childTx, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*childTx.TxIDChainHash(), 100, 0))

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}

	// Merkle root with coinbase swapped into the placeholder position.
	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	calculatedMerkleRootHash := replicatedSubtree.RootHash()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: calculatedMerkleRootHash,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++
		if blockHeader.Nonce > 1_000_000 {
			t.Fatal("failed to grind a valid nonce for the floater block header")
		}
	}

	block, err := model.NewBlock(
		blockHeader,
		coinbaseTx,
		subtreeHashes,
		uint64(subtree.Length()), //nolint:gosec
		uint64(coinbaseTx.Size()+childTx.Size()), //nolint:gosec
		100, 0,
	)
	require.NoError(t, err)

	return block, childTx
}

// TestBlockValidation_OptimisticFloaterInvalidatedWhenCaughtUp is the PRIMARY
// regression test for the P0: it is the test that would have caught the
// fail-open floater on the optimistic-mining (default) block-validation path.
//
// With OptimisticMining=true the block is AddBlock'd BEFORE block.Valid runs in
// the background goroutine. Pre-fix, the background block.Valid returned
// ErrBlockIncomplete for the floater, the optimistic handler only invalidated
// on ErrBlockInvalid, so the floater stayed permanently ACCEPTED. Post-fix, the
// caught-up (RUNNING) branch reclassifies ErrBlockIncomplete as a floater and
// rolls the block back via markBlockAsInvalid -> InvalidateBlock.
//
// LocalClient hardwires FSM=RUNNING, so isCaughtUp() is true here without any
// override; the tracking wrapper observes InvalidateBlock.
func TestBlockValidation_OptimisticFloaterInvalidatedWhenCaughtUp(t *testing.T) {
	initPrometheusMetrics()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true // exercise the optimistic background path

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	tracker := newTrackingBlockchainClient(localClient)

	// Subtree validation succeeds — the only failure must come from block.Valid's
	// parent-existence check in the background goroutine.
	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	block, _ := buildFloaterBlock(t, utxoStore, subtreeStore)

	// Pass an already-cancelled lifecycle context so the revalidate WORKER never
	// starts. This ISOLATES the optimistic background goroutine's branch: the only
	// path that can call InvalidateBlock is the caught-up floater rollback we are
	// pinning. (Were the worker running, it would re-validate the same in-memory
	// block whose pooled []Node slices the optimistic failure path released — a
	// "first subtree has no nodes" ErrBlockInvalid that the reValidateBlock branch
	// would also invalidate, masking whether the optimistic branch itself fired.)
	// ValidateBlock runs on the request ctx (context.Background()), so it and its
	// decoupled background goroutine run fully regardless.
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	lifecycleCancel()

	blockValidation := NewBlockValidation(lifecycleCtx, ulogger.TestLogger{}, tSettings, tracker, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	// Optimistic path: ValidateBlock returns nil immediately after AddBlock; the
	// floater is caught asynchronously.
	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost")
	require.NoError(t, err, "optimistic mining returns before background validation")

	// The block was optimistically added.
	exists, existsErr := localClient.GetBlockExists(context.Background(), block.Header.Hash())
	require.NoError(t, existsErr)
	require.True(t, exists, "optimistic mining must AddBlock before background validation")

	// The background goroutine must roll the floater back via InvalidateBlock.
	select {
	case <-tracker.invalidateCalled:
		// floater rolled back as required
	case <-time.After(10 * time.Second):
		t.Fatal("caught-up optimistic floater must be invalidated by the background goroutine (InvalidateBlock not called)")
	}
}

// TestBlockValidation_OptimisticFloaterRetriedDuringCatchup is the #1031
// regression GUARD: the same floater-shaped block (a not-yet-absorbed parent
// with empty BlockIDs) must stay retryable and must NEVER be invalidated while
// the FSM is in a sync state (CATCHINGBLOCKS). isCaughtUp() returns false, so
// the optimistic background goroutine takes the ReValidateBlock retry branch
// instead of markBlockAsInvalid.
//
// If this test fails (InvalidateBlock observed in catchup), the fix has
// reopened #1031: a parent we simply have not absorbed yet would be persisted
// invalid instead of retried, poisoning the DB and stalling sync.
func TestBlockValidation_OptimisticFloaterRetriedDuringCatchup(t *testing.T) {
	initPrometheusMetrics()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Force CATCHINGBLOCKS so isCaughtUp() is false: the empty-BlockIDs parent is
	// a transient ordering state, NOT a floater.
	tracker := newTrackingBlockchainClient(localClient).withFSMState(blockchain.FSMStateCATCHINGBLOCKS)

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	block, _ := buildFloaterBlock(t, utxoStore, subtreeStore)

	// Pass an already-cancelled lifecycle context so the revalidate WORKER never
	// starts. We are pinning the background goroutine's BRANCH DECISION (retry vs
	// invalidate), not worker behaviour. (The worker, if left running, would
	// re-validate the same in-memory block whose pooled []Node slices the
	// optimistic failure path released via releaseBlockNodes — a "first subtree
	// has no nodes" ErrBlockInvalid that is a test-object-lifecycle artifact, not
	// the #1031 floater scenario. Disabling the worker isolates the decision.)
	// ValidateBlock runs on the request ctx (context.Background()), so it and its
	// decoupled background goroutine run fully regardless of the cancelled
	// lifecycle ctx.
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	lifecycleCancel()

	blockValidation := NewBlockValidation(lifecycleCtx, ulogger.TestLogger{}, tSettings, tracker, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost")
	require.NoError(t, err, "optimistic mining returns before background validation")

	// Positive signal: the background goroutine must route the catchup floater to
	// ReValidateBlock (enqueue onto revalidateBlockChan), NOT to markBlockAsInvalid.
	// With the worker disabled, nothing drains the channel, so we read it directly.
	select {
	case <-blockValidation.revalidateBlockChan:
		// catchup floater routed to retry as required (#1031 preserved)
	case <-time.After(10 * time.Second):
		t.Fatal("catchup floater must be routed to ReValidateBlock (retry) by the background goroutine")
	}

	// And it must NEVER have been invalidated.
	require.False(t, tracker.invalidateWasCalled(),
		"#1031 regression: a not-yet-absorbed parent in CATCHINGBLOCKS must be retried, NOT invalidated")
}

// TestBlockValidation_NonOptimisticFloaterInvalidatedWhenCaughtUp pins
// criterion (c) for the SYNCHRONOUS (OptimisticMining=false) block.Valid path
// using the precise floater shape (parent IN the store with empty BlockIDs, not
// in the block) — complementing the existing
// TestBlockValidation_FloaterPersistedInvalidWhenCaughtUp, which uses the
// absent-parent (ErrTxNotFound) variant. In RUNNING the block must surface
// BLOCK_INVALID and be persisted invalid via storeInvalidBlock.
func TestBlockValidation_NonOptimisticFloaterInvalidatedWhenCaughtUp(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false // synchronous block.Valid path

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	block, _ := buildFloaterBlock(t, utxoStore, subtreeStore)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, localClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid),
		"caught-up floater on the synchronous path must surface BLOCK_INVALID, got: %v", err)

	exists, existsErr := localClient.GetBlockExists(context.Background(), block.Header.Hash())
	require.NoError(t, existsErr)
	require.True(t, exists, "caught-up floater must be persisted as invalid (storeInvalidBlock)")
}

// TestBlockValidation_NonOptimisticFloaterRetriedDuringCatchup is the
// synchronous-path #1031 GUARD: with the FSM in CATCHINGBLOCKS the same floater
// shape must stay BLOCK_INCOMPLETE (retryable) and must NOT be persisted
// invalid. This pins that the synchronous handler's isCaughtUp gate preserves
// the #1031 catchup-ordering contract.
func TestBlockValidation_NonOptimisticFloaterRetriedDuringCatchup(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	tracker := newTrackingBlockchainClient(localClient).withFSMState(blockchain.FSMStateCATCHINGBLOCKS)

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	block, _ := buildFloaterBlock(t, utxoStore, subtreeStore)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, tracker, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockIncomplete),
		"#1031: a not-yet-absorbed parent in CATCHINGBLOCKS must stay BLOCK_INCOMPLETE, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid),
		"#1031: catchup floater must NOT be reclassified as BLOCK_INVALID")

	require.False(t, tracker.invalidateWasCalled(),
		"#1031: catchup floater must not be invalidated on the synchronous path")
}

// TestBlockValidation_NonOptimisticFloaterFSMErrorFailsSafe pins criterion (d):
// when GetFSMCurrentState errors, isCaughtUp() must fail safe (return false), so
// the floater is treated as a transient catchup state (BLOCK_INCOMPLETE/retry),
// never wrongly invalidated. An FSM-query hiccup must never poison a block.
func TestBlockValidation_NonOptimisticFloaterFSMErrorFailsSafe(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	tracker := newTrackingBlockchainClient(localClient).withFSMError(errors.NewServiceError("fsm unavailable"))

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	block, _ := buildFloaterBlock(t, utxoStore, subtreeStore)

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, tracker, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	err = blockValidation.ValidateBlock(context.Background(), block, "http://localhost")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockIncomplete),
		"FSM-error fail-safe: floater must stay BLOCK_INCOMPLETE when FSM state is unknown, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid),
		"FSM-error fail-safe: must never reclassify as BLOCK_INVALID on an FSM query error")

	require.False(t, tracker.invalidateWasCalled(),
		"FSM-error fail-safe: must not invalidate when FSM state is unknown")
}
