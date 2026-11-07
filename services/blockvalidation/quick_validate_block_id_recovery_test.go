package blockvalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// These tests cover how quick validation recovers a block's id from the block ids
// recorded on its first non-coinbase transaction, when that transaction is also in
// another block. The other block stamps its own id on the transaction, and StoreBlock
// refuses to write a block under an id another block is stored under or has
// reserved, so taking the first recorded id blindly leaves the block unable to
// commit.
//
// Everything is real: a sqlitememory UTXO store and a SQLite blockchain store behind
// the ordinary LocalClient, via preBindHarness. Only the lookup-fault test wraps the
// blockchain client, to make GetBlockByID fail.

// quickDriver is one of the three processing drivers that call setQuickBlockID.
// Each has its own call site, so each is exercised.
type quickDriver struct {
	name          string
	prefetchDepth int
	async         bool
}

var quickDrivers = []quickDriver{
	{name: "sequential", prefetchDepth: 0},
	{name: "pipeline", prefetchDepth: 2},
	{name: "async", prefetchDepth: 2, async: true},
}

// stampWithoutCommit runs d's processing driver over block and stops before the
// commit, as an attempt does when it fails after its transactions were stamped. The
// async driver's write jobs are drained without being written.
func (h *preBindHarness) stampWithoutCommit(d quickDriver, block *model.Block) {
	h.t.Helper()

	h.bv.settings.BlockValidation.SubtreeBatchPrefetchDepth = d.prefetchDepth

	if !d.async {
		_, err := h.bv.processBlockSubtrees(h.ctx, block, false)
		require.NoError(h.t, err)

		return
	}

	writeJobsChan := make(chan *SubtreeWriteJob, 16)
	drained := make(chan struct{})

	go func() {
		drainWriteJobs(writeJobsChan)
		close(drained)
	}()

	_, _, _, err := h.bv.processBlockSubtreesPipelineAsync(h.ctx, block, d.prefetchDepth, writeJobsChan, false)

	close(writeJobsChan)
	<-drained

	require.NoError(h.t, err)
}

// quickValidateWith runs the whole quick validation of block, commit included, on
// d's driver. The async driver gets a real write worker.
func (h *preBindHarness) quickValidateWith(d quickDriver, block *model.Block) error {
	h.t.Helper()

	h.bv.settings.BlockValidation.SubtreeBatchPrefetchDepth = d.prefetchDepth

	if !d.async {
		return h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	}

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	_, _, err := h.bv.quickValidateBlockAsync(h.ctx, block, "peer", "", writeJobsChan)

	close(writeJobsChan)
	require.NoError(h.t, g.Wait())

	return err
}

// storeForkHolding stores a fork block at preBindHeight, a sibling of whatever the
// test validates, and records tx in the UTXO store as mined in it. It returns the
// fork's block id.
func (h *preBindHarness) storeForkHolding(tx *bt.Tx, coinbaseNonce byte) uint32 {
	h.t.Helper()

	coinbase := preBindCoinbase(h.t, coinbaseNonce)
	fork := h.newPreBindBlock(coinbase, nil, coinbase.TxIDChainHash(), 1)

	require.NoError(h.t, h.chain.AddBlock(h.ctx, fork, "peer"))

	_, meta, err := h.chain.GetBlockHeader(h.ctx, fork.Hash())
	require.NoError(h.t, err)
	require.NotZero(h.t, meta.ID, "the fork must have a real block id for the test to mean anything")

	_, err = h.utxoStore.Create(h.ctx, tx, preBindHeight, utxo.WithSkipExtendedInputs(true),
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: meta.ID, BlockHeight: preBindHeight, SubtreeIdx: 0}))
	require.NoError(h.t, err)

	return meta.ID
}

// forkSharingBlock builds a block whose only non-coinbase transaction is child, and
// stores its body.
func (h *preBindHarness) forkSharingBlock(child *bt.Tx, coinbaseNonce byte) *model.Block {
	h.t.Helper()

	coinbase := preBindCoinbase(h.t, coinbaseNonce)
	st := h.oneSubtreeBody(coinbase, child)

	return h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{st.RootHash()},
		composeBlockMerkleRoot(h.t, []chainhash.Hash{coinbaseSubstitutedRoot(h.t, st, coinbase)}),
		2)
}

// sameBlock returns a fresh value for the same block, with no id set, as a retry
// receives it.
func sameBlock(block *model.Block) *model.Block {
	return &model.Block{
		Header:           block.Header,
		CoinbaseTx:       block.CoinbaseTx,
		Height:           block.Height,
		Subtrees:         block.Subtrees,
		TransactionCount: block.TransactionCount,
	}
}

// requireStoredUnder asserts the blockchain store holds block under id.
func (h *preBindHarness) requireStoredUnder(block *model.Block, id uint32) {
	h.t.Helper()

	_, meta, err := h.chain.GetBlockHeader(h.ctx, block.Hash())
	require.NoError(h.t, err, "the block must be stored")
	require.Equal(h.t, id, meta.ID)
}

// openFileChain opens a SQLite blockchain store in a file under the test's data
// folder and points the harness at it. Opening it again reads the same database
// with an empty in-process reservation cache, which is what a restart, or a
// reservation cache entry outliving its TTL, leaves behind.
func (h *preBindHarness) openFileChain() blockchainstore.Store {
	h.t.Helper()

	logger := ulogger.NewErrorTestLogger(h.t)

	store, err := blockchainstore.NewStore(logger, &url.URL{Scheme: "sqlite", Path: "/blockchain"}, h.bv.settings)
	require.NoError(h.t, err)

	localClient, err := blockchain.NewLocalClient(logger, h.bv.settings, store, nil, nil)
	require.NoError(h.t, err)

	h.chain = &countingBlockchainClient{ClientI: localClient}
	h.bv.blockchainClient = h.chain

	return store
}

// TestQuickValidate_ForkHoldsFirstTx_FirstAttemptCommits is the case the shortcut
// broke on a first attempt, with no retry involved: the first non-coinbase
// transaction already carries a stored fork block's id, the block took that id, and
// StoreBlock refused it.
//
// Mutation target: taking recorded BlockIDs[0] without checking which block holds it,
// at any one of the three driver call sites. block.ID then equals the fork's id and
// commitBlock's AddBlock is refused.
func TestQuickValidate_ForkHoldsFirstTx_FirstAttemptCommits(t *testing.T) {
	for _, d := range quickDrivers {
		t.Run(d.name, func(t *testing.T) {
			h := newPreBindHarness(t, nil)

			parent := h.storeGenuineParent(0x41)
			child := preBindSpendOf(t, parent, 9_000)
			forkID := h.storeForkHolding(child, 0x40)

			block := h.forkSharingBlock(child, 0x42)

			require.NoError(t, h.quickValidateWith(d, block), "the block must commit under an id the store accepts")

			require.NotEqual(t, forkID, block.ID, "the block must not take the id of the fork it shares a transaction with")
			require.Equal(t, 1, h.chain.assignCount(), "with only the fork's id recorded, the block takes its id from AssignBlockID")
			h.requireStoredUnder(block, block.ID)
		})
	}
}

// TestQuickValidate_ForkHoldsFirstTx_RetryKeepsOwnID covers the retry: a first
// attempt stamped the transaction and failed before it committed, so the transaction
// carries both the fork's id and this block's. The retry must pick this block's id,
// not the fork's.
//
// Mutation targets: taking BlockIDs[0] at any driver call site, which picks the
// fork's id and is refused. Falling back to AssignBlockID whenever the transaction
// carries more than one id is caught only by the assignCount check here, because
// the reservation still exists and AssignBlockID returns the same id;
// TestQuickValidate_ForkHoldsFirstTx_RetryAfterSweepKeepsOwnID catches it on the id.
func TestQuickValidate_ForkHoldsFirstTx_RetryKeepsOwnID(t *testing.T) {
	for _, d := range quickDrivers {
		t.Run(d.name, func(t *testing.T) {
			h := newPreBindHarness(t, nil)

			parent := h.storeGenuineParent(0x51)
			child := preBindSpendOf(t, parent, 9_000)
			forkID := h.storeForkHolding(child, 0x50)

			block := h.forkSharingBlock(child, 0x52)

			h.stampWithoutCommit(d, block)

			firstID := block.ID
			require.NotEqual(t, forkID, firstID)

			recorded, err := h.utxoStore.Get(h.ctx, child.TxIDChainHash())
			require.NoError(t, err)
			require.ElementsMatch(t, []uint32{forkID, firstID}, recorded.BlockIDs,
				"precondition: the transaction must carry both ids, or the retry is not ambiguous")

			retry := sameBlock(block)

			require.NoError(t, h.quickValidateWith(d, retry))

			require.Equal(t, firstID, retry.ID, "the retry must keep the id its transactions were stamped with")
			require.Equal(t, 1, h.chain.assignCount(), "the retry must recover the id, not call AssignBlockID again")
			h.requireStoredUnder(retry, firstID)

			// Once committed, the block's own stored id is recognised by hash.
			again := sameBlock(block)

			recovered, err := h.bv.setQuickBlockID(h.ctx, again, child, "test")
			require.NoError(t, err)
			require.Equal(t, uint64(firstID), recovered)
			require.Equal(t, firstID, again.ID)
		})
	}
}

// TestQuickValidate_ForkHoldsFirstTx_RetryAfterSweepKeepsOwnID is the case the
// single-candidate path exists for. The first attempt stamped the transaction and
// failed, then sweepStaleReservations deleted the block's reservation. AssignBlockID
// would now hand out a new id the stamped transaction does not carry, so the retry
// must take its id from the transaction.
//
// The sweep is simulated by deleting the block's reservation row, and the expired
// in-process reservation cache by reopening the store over the same database file.
//
// Mutation target: falling back to AssignBlockID with a single candidate. The retry
// then commits under a new id, and the id assertion fails.
func TestQuickValidate_ForkHoldsFirstTx_RetryAfterSweepKeepsOwnID(t *testing.T) {
	h := newPreBindHarness(t, nil)
	h.bv.settings.DataFolder = t.TempDir()
	store := h.openFileChain()

	parent := h.storeGenuineParent(0x81)
	child := preBindSpendOf(t, parent, 9_000)
	forkID := h.storeForkHolding(child, 0x80)

	block := h.forkSharingBlock(child, 0x82)

	h.stampWithoutCommit(quickDrivers[1], block)

	firstID := block.ID
	require.NotEqual(t, forkID, firstID)

	res, err := store.GetDB().ExecContext(h.ctx, `DELETE FROM block_id_reservations WHERE hash = $1`, block.Hash()[:])
	require.NoError(t, err)

	deleted, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted, "precondition: the block's reservation must exist before the sweep")

	// Asking AssignBlockID here whether it now hands out a different id would itself
	// reserve that id for the block and change what the retry sees. It does: under
	// the mutation below the retry commits under a newer id than firstID.
	h.openFileChain()

	retry := sameBlock(block)

	require.NoError(t, h.quickValidateWith(quickDrivers[1], retry))

	require.Equal(t, firstID, retry.ID, "the retry must keep the id its transactions were stamped with")
	h.requireStoredUnder(retry, firstID)
}

// TestQuickValidate_TwoUnstoredStamps_FallsBackToReservation pins the several-
// candidates case: two blocks that are both still uncommitted have stamped the same
// transaction, and nothing recorded says which id is whose. The block must then take
// its own reservation from AssignBlockID rather than guess.
//
// The two stamps are written directly. Running both blocks' first attempts through
// processBlockSubtrees cannot produce them: the second attempt finds the first
// block's id as the lone candidate and adopts it.
//
// Mutation target: returning candidates[0] in the several-candidates case. The block
// then takes its sibling's reserved id and StoreBlock refuses it.
func TestQuickValidate_TwoUnstoredStamps_FallsBackToReservation(t *testing.T) {
	h := newPreBindHarness(t, nil)

	parent := h.storeGenuineParent(0x91)
	child := preBindSpendOf(t, parent, 9_000)

	sibling := h.forkSharingBlock(child, 0x90)
	block := h.forkSharingBlock(child, 0x92)

	siblingAssigned, err := h.chain.AssignBlockID(h.ctx, sibling.Hash())
	require.NoError(t, err)

	blockAssigned, err := h.chain.AssignBlockID(h.ctx, block.Hash())
	require.NoError(t, err)

	siblingID, err := blockIDToUint32(siblingAssigned, sibling.Hash().String())
	require.NoError(t, err)

	blockID, err := blockIDToUint32(blockAssigned, block.Hash().String())
	require.NoError(t, err)

	_, err = h.utxoStore.Create(h.ctx, child, preBindHeight, utxo.WithSkipExtendedInputs(true),
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: siblingID, BlockHeight: preBindHeight, SubtreeIdx: 0}))
	require.NoError(t, err)

	_, err = h.utxoStore.SetMinedMulti(h.ctx, []*chainhash.Hash{child.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: preBindHeight, SubtreeIdx: 0})
	require.NoError(t, err)

	recorded, err := h.utxoStore.Get(h.ctx, child.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{siblingID, blockID}, recorded.BlockIDs,
		"precondition: the sibling's id must come first, or taking the first candidate would pass by luck")

	retry := sameBlock(block)

	require.NoError(t, h.quickValidateWith(quickDrivers[1], retry))

	require.Equal(t, blockID, retry.ID, "the block must take its own reservation, not its sibling's id")
	h.requireStoredUnder(retry, blockID)
}

// failingBlockByIDClient makes GetBlockByID fail with a storage error, which is not
// the same as "no block has this id".
type failingBlockByIDClient struct {
	*countingBlockchainClient
}

func (c *failingBlockByIDClient) GetBlockByID(context.Context, uint64) (*model.Block, error) {
	return nil, errors.NewStorageError("injected GetBlockByID fault")
}

// TestSetQuickBlockID_LookupFault_ReturnsError pins that a failed lookup is not read
// as "no block holds this id". If it were, the fork's id would look free and the
// block would take it, which is the wedge again.
//
// Mutation target: treating every GetBlockByID error as not-found.
func TestSetQuickBlockID_LookupFault_ReturnsError(t *testing.T) {
	h := newPreBindHarness(t, nil)

	parent := h.storeGenuineParent(0x61)
	child := preBindSpendOf(t, parent, 9_000)
	h.storeForkHolding(child, 0x60)

	h.bv.blockchainClient = &failingBlockByIDClient{countingBlockchainClient: h.chain}

	block := h.forkSharingBlock(child, 0x62)

	_, err := h.bv.setQuickBlockID(h.ctx, block, child, "test")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "the lookup fault must surface, got %v", err)
	require.Zero(t, block.ID, "no id may be set when the lookup failed")
	require.Zero(t, h.chain.assignCount(), "AssignBlockID must not be reached")
}
