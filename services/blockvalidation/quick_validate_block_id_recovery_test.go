package blockvalidation

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// These tests cover the id quick validation gives a block whose first non-coinbase
// transaction is also in another block. The other block stamps its own id on the
// transaction, and StoreBlock refuses to write a block under an id another block
// holds, so the block must take its id from its own reservation, never from the
// transaction.
//
// Everything is real: a sqlitememory UTXO store and a SQLite blockchain store behind
// the ordinary LocalClient, via preBindHarness.

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

// TestQuickValidate_ForkHoldsFirstTx_FirstAttemptCommits: the first non-coinbase
// transaction already carries a stored fork block's id. Taking that id made
// StoreBlock refuse the block on every attempt.
//
// Mutation target: reading the id back from the transaction's recorded BlockIDs at
// any one of the three driver call sites. block.ID then equals the fork's id and
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
			h.requireStoredUnder(block, block.ID)
		})
	}
}

// TestQuickValidate_ForkHoldsFirstTx_RetryKeepsOwnID covers the retry: a first
// attempt stamped the transaction and failed before it committed, so the transaction
// carries both the fork's id and this block's. While the reservation exists,
// AssignBlockID returns the same id, so the retry commits under the id its
// transactions were stamped with.
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
				"precondition: the transaction must carry both ids")

			retry := sameBlock(block)

			require.NoError(t, h.quickValidateWith(d, retry))

			require.Equal(t, firstID, retry.ID, "the retry must keep the id its first attempt reserved")
			h.requireStoredUnder(retry, firstID)
		})
	}
}

// TestQuickValidate_RetryAfterSweep_RestampsNewID: the first attempt stamped the
// transaction and failed, then sweepStaleReservations deleted the block's
// reservation. The retry gets a new id from AssignBlockID, commits under it, and
// restamps the transaction with it, so the transaction carries the id the block is
// stored under.
//
// The sweep is simulated by deleting the block's reservation row, and the expired
// in-process reservation cache by reopening the store over the same database file.
//
// Mutation target: dropping the SetMinedMulti restamp of existing transactions in
// createAndSpendUTXOsForBatch. The transaction then lacks the new id.
func TestQuickValidate_RetryAfterSweep_RestampsNewID(t *testing.T) {
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

	h.openFileChain()

	retry := sameBlock(block)

	require.NoError(t, h.quickValidateWith(quickDrivers[1], retry))

	require.NotEqual(t, firstID, retry.ID, "precondition: with the reservation swept, the retry gets a new id")
	require.NotEqual(t, forkID, retry.ID)
	h.requireStoredUnder(retry, retry.ID)

	recorded, err := h.utxoStore.Get(h.ctx, child.TxIDChainHash())
	require.NoError(t, err)
	require.Contains(t, recorded.BlockIDs, retry.ID, "the transaction must carry the id the block is stored under")
}
