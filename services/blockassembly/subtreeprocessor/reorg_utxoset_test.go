package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/chainancestry/chainancestrytest"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// These tests run a reorg through block assembly against the utxoset store, and follow each
// transaction of the two competing blocks until the pruner's stamp has written a block onto
// its UTXOs. They pin that block assembly does not have to record the winning block itself:
// block validation records containment for every block it validates, forks included, the
// trailing MarkTransactionsOnLongestChain clears the unmined marker, and the stamp asks the
// chain which block won. The network is regtest, which has no checkpoints, so every create
// takes the identity route the tip takes.

const (
	utxosetReorgHeight        = uint32(2)
	utxosetMoveBackBlockID    = uint32(11)
	utxosetMoveForwardBlockID = uint32(22)
)

// utxosetReorgFixture is a subtree processor on a utxoset store, with a raw pool on the same
// database for the assertions that read tables no interface method exposes.
type utxosetReorgFixture struct {
	ctx       context.Context
	store     *utxoset.Store
	pool      *pgxpool.Pool
	blobStore *blob_memory.Memory
	stp       *SubtreeProcessor
}

func newUtxosetReorgFixture(t *testing.T) *utxosetReorgFixture {
	t.Helper()

	ctx := context.Background()

	dsn, cleanup, err := tpostgres.SetupTestPostgresContainer()
	if err != nil {
		test.SkipIfContainerUnavailable(t, err)
		t.Fatalf("postgres container unavailable: %v", err)
	}

	t.Cleanup(func() { _ = cleanup() })

	storeURL, err := url.Parse(dsn)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	store, err := utxoset.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(context.Background()) })

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	t.Cleanup(pool.Close)

	blobStore := blob_memory.New()
	newSubtreeChan := make(chan NewSubtreeRequest, 10)

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBlockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.AnythingOfType("*chainhash.Hash"), mock.AnythingOfType("[]bool")).Return(nil)
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(prevBlockHeader, &model.BlockHeaderMeta{}, nil)
	mockBlockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, blobStore, mockBlockchainClient, store, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)

	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	return &utxosetReorgFixture{ctx: ctx, store: store, pool: pool, blobStore: blobStore, stp: stp}
}

// unminedTx creates a transaction the way the validator does, with no block: an identity row
// with the unmined marker set, and UTXOs with no block on them.
func (f *utxosetReorgFixture) unminedTx(t *testing.T, satoshis uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.Version = 1
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis))

	_, err := f.store.Create(f.ctx, tx, utxosetReorgHeight-1)
	require.NoError(t, err)

	return tx
}

// validateBlock makes the call block validation makes for a block it has validated: record
// mined, for every non-coinbase transaction, with the block's place on the chain at that time.
func (f *utxosetReorgFixture) validateBlock(t *testing.T, blockID uint32, onLongestChain bool, txs ...*bt.Tx) {
	t.Helper()

	hashes := make([]*chainhash.Hash, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.TxIDChainHash()
	}

	_, err := f.store.SetMinedMulti(f.ctx, hashes, utxostore.MinedBlockInfo{
		BlockID:        blockID,
		BlockHeight:    utxosetReorgHeight,
		OnLongestChain: onLongestChain,
	})
	require.NoError(t, err)
}

// block builds a block at the reorg height with one subtree holding txs after the coinbase
// placeholder.
func (f *utxosetReorgFixture) block(t *testing.T, id uint32, nonce uint32, coinbase *bt.Tx, txs ...*bt.Tx) *model.Block {
	t.Helper()

	nodes := make([]subtree.Node, len(txs))
	for i, tx := range txs {
		nodes[i] = subtree.Node{Hash: *tx.TxIDChainHash(), Fee: 100, SizeInBytes: 250}
	}

	subtreeHash := storeReorgSubtree(t, f.ctx, f.blobStore, nodes)

	return &model.Block{
		ID:         id,
		Height:     utxosetReorgHeight,
		CoinbaseTx: coinbase,
		Subtrees:   []*chainhash.Hash{subtreeHash},
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevBlockHeader.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1900000000 + nonce,
			Bits:           model.NBit{},
			Nonce:          nonce,
		},
	}
}

// applyAsTip creates the block's coinbase and sets it as the subtree processor's tip, the
// condition block assembly is in after it moved the block forward.
func (f *utxosetReorgFixture) applyAsTip(t *testing.T, block *model.Block) {
	t.Helper()

	require.NoError(t, f.stp.processCoinbaseUtxos(f.ctx, block))
	f.stp.InitCurrentBlockHeader(block.Header)
}

// stamp runs the pruner's stamp over the reorg height's partition, with a chain that names
// winnerID at the reorg height, the way the pruner service's stamp worker drives it.
func (f *utxosetReorgFixture) stamp(t *testing.T, winnerID uint32) {
	t.Helper()

	stamper := f.store.Stamper()
	tip := stamper.WindowBlocks() - 1 + stamper.StampDepth()
	require.NoError(t, f.store.SetBlockHeight(tip))

	drain, floors, err := stamper.OpenDrain(f.ctx)
	require.NoError(t, err)

	defer func() { require.NoError(t, drain.Close()) }()

	anc := chainancestrytest.Chain(t, floors.StampCompleteFloor, tip, map[uint32]uint32{utxosetReorgHeight: winnerID})

	state, err := drain.BeginWindow(f.ctx, floors.StampCompleteFloor, anc)
	require.NoError(t, err)
	require.Contains(t, []pruner.StampWindowState{pruner.StampWindowReady, pruner.StampWindowResumed}, state,
		"the partition holding the reorg height must be deep enough to stamp")

	for page := 0; page < drain.PagesPerWindow(); page++ {
		_, err = drain.StampPage(f.ctx, floors.StampCompleteFloor, anc, page)
		require.NoError(t, err)
	}

	require.NoError(t, drain.CompleteWindow(f.ctx, floors.StampCompleteFloor, anc, tip))
}

// marker reads a transaction's unmined marker; set is false when it is clear. The identity row
// must exist.
func (f *utxosetReorgFixture) marker(t *testing.T, tx *bt.Tx) (set bool) {
	t.Helper()

	var since *int32
	require.NoError(t, f.pool.QueryRow(f.ctx,
		`SELECT off_chain_since FROM tx_ident WHERE txid = $1`, txidBytes(tx)).Scan(&since))

	return since != nil
}

func (f *utxosetReorgFixture) identExists(t *testing.T, tx *bt.Tx) bool {
	t.Helper()

	var n int
	require.NoError(t, f.pool.QueryRow(f.ctx,
		`SELECT count(*) FROM tx_ident WHERE txid = $1`, txidBytes(tx)).Scan(&n))

	return n > 0
}

// minedBlocks lists the blocks the containment table says contain the transaction.
func (f *utxosetReorgFixture) minedBlocks(t *testing.T, tx *bt.Tx) []uint32 {
	t.Helper()

	rows, err := f.pool.Query(f.ctx,
		`SELECT block_id FROM tx_mined WHERE txid = $1 ORDER BY block_id`, txidBytes(tx))
	require.NoError(t, err)

	defer rows.Close()

	ids := []uint32{}

	for rows.Next() {
		var id int32
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, uint32(id)) //nolint:gosec // a block id is never negative
	}

	require.NoError(t, rows.Err())

	return ids
}

// utxoBlock reads the (mined_height, block_id) pair off each live UTXO of the transaction and
// requires them to agree. ok is false when the transaction has no live UTXO.
func (f *utxosetReorgFixture) utxoBlock(t *testing.T, tx *bt.Tx) (height, blockID int32, ok bool) {
	t.Helper()

	rows, err := f.pool.Query(f.ctx,
		`SELECT DISTINCT mined_height, block_id FROM utxo WHERE txid = $1`, txidBytes(tx))
	require.NoError(t, err)

	defer rows.Close()

	for rows.Next() {
		require.False(t, ok, "the UTXOs of one transaction must carry one block")
		require.NoError(t, rows.Scan(&height, &blockID))

		ok = true
	}

	require.NoError(t, rows.Err())

	return height, blockID, ok
}

func txidBytes(tx *bt.Tx) []byte {
	h := tx.TxIDChainHash()

	return h[:]
}

// TestUtxosetReorgSettlesEachTransactionOnTheWinningBlock is the general reorg: block M is on
// the longest chain, fork block F at the same height was validated while it lost, and then F's
// branch wins.
//
//   - bothTx is in M and in F, the common condition in a reorg.
//   - forwardOnlyTx is only in F. Block validation recorded it with the longest-chain flag
//     false, so its marker stayed set until the reorg.
//   - backOnlyTx is only in M, and goes back to block assembly.
//   - F's coinbase is new, so block assembly creates it on the move forward. M's coinbase is
//     removed on the move back.
//
// After the reorg each transaction must carry the correct marker and the correct containment
// rows. After the stamp each one must carry F, or no block at all for backOnlyTx.
func TestUtxosetReorgSettlesEachTransactionOnTheWinningBlock(t *testing.T) {
	f := newUtxosetReorgFixture(t)

	bothTx := f.unminedTx(t, 1001)
	backOnlyTx := f.unminedTx(t, 1002)
	forwardOnlyTx := f.unminedTx(t, 1003)

	blockM := f.block(t, utxosetMoveBackBlockID, 902, coinbaseTx2, bothTx, backOnlyTx)
	blockF := f.block(t, utxosetMoveForwardBlockID, 903, coinbaseTx3, bothTx, forwardOnlyTx)

	f.validateBlock(t, utxosetMoveBackBlockID, true, bothTx, backOnlyTx)
	f.applyAsTip(t, blockM)
	f.validateBlock(t, utxosetMoveForwardBlockID, false, bothTx, forwardOnlyTx)

	require.True(t, f.marker(t, forwardOnlyTx), "precondition: F was a fork when it was validated, so its only transaction is unmined")

	require.NoError(t, f.stp.Reorg([]*model.Block{blockM}, []*model.Block{blockF}))

	require.False(t, f.marker(t, bothTx), "a transaction in the two blocks stays mined")
	require.False(t, f.marker(t, forwardOnlyTx), "a transaction only the winning block carries is mined after the reorg")
	require.True(t, f.marker(t, backOnlyTx), "a transaction only the losing block carries is unmined after the reorg")
	require.False(t, f.marker(t, coinbaseTx3), "the winning block's coinbase is mined")

	require.Equal(t, []uint32{utxosetMoveBackBlockID, utxosetMoveForwardBlockID}, f.minedBlocks(t, bothTx))
	require.Equal(t, []uint32{utxosetMoveForwardBlockID}, f.minedBlocks(t, forwardOnlyTx))
	require.Equal(t, []uint32{utxosetMoveBackBlockID}, f.minedBlocks(t, backOnlyTx))
	require.Equal(t, []uint32{utxosetMoveForwardBlockID}, f.minedBlocks(t, coinbaseTx3))

	_, _, ok := f.utxoBlock(t, coinbaseTx2)
	require.False(t, ok, "the losing block's coinbase has no live UTXO after the reorg")

	f.stamp(t, utxosetMoveForwardBlockID)

	for _, tc := range []struct {
		name string
		tx   *bt.Tx
	}{
		{"in the two blocks", bothTx},
		{"only in the winning block", forwardOnlyTx},
		{"the winning block's coinbase", coinbaseTx3},
	} {
		height, blockID, ok := f.utxoBlock(t, tc.tx)
		require.True(t, ok, tc.name)
		require.Equal(t, int32(utxosetReorgHeight), height, tc.name)
		require.Equal(t, int32(utxosetMoveForwardBlockID), blockID, "%s: the stamp writes the block that won", tc.name)
		require.False(t, f.identExists(t, tc.tx), "%s: the stamp removes the identity row", tc.name)
	}

	height, blockID, ok := f.utxoBlock(t, backOnlyTx)
	require.True(t, ok)
	require.Equal(t, int32(0), height, "a transaction no winning block carries gets no block")
	require.Equal(t, int32(0), blockID)
	require.True(t, f.marker(t, backOnlyTx), "and stays unmined")
	require.Empty(t, f.minedBlocks(t, backOnlyTx), "its containment row in the losing block is removed by the stamp")
}

// TestUtxosetReorgWithTheSameCoinbaseInBothBlocks is the one condition in which the winning
// block's coinbase can already be in the store when block assembly moves the block forward:
// the losing and the winning block carry the identical coinbase transaction. That is possible
// at the same height, because the coinbase commits only to the height, and a miner can mine
// the two blocks with one coinbase.
//
// The reorg moves M back before it moves F forward, and the move back removes M's coinbase, so
// processCoinbaseUtxos creates F's coinbase like a new one. If the row were still there, the
// refused create would record F instead, as TestUtxosetReorgToABlockWhoseCoinbaseIsAlreadyStored
// shows. The two routes give the same result.
func TestUtxosetReorgWithTheSameCoinbaseInBothBlocks(t *testing.T) {
	f := newUtxosetReorgFixture(t)

	tx := f.unminedTx(t, 1001)
	forwardOnlyTx := f.unminedTx(t, 1003)

	// The two blocks must carry different subtrees, so F has a transaction M does not.
	blockM := f.block(t, utxosetMoveBackBlockID, 902, coinbaseTx2, tx)
	blockF := f.block(t, utxosetMoveForwardBlockID, 903, coinbaseTx2, tx, forwardOnlyTx)

	f.validateBlock(t, utxosetMoveBackBlockID, true, tx)
	f.applyAsTip(t, blockM)
	f.validateBlock(t, utxosetMoveForwardBlockID, false, tx, forwardOnlyTx)

	require.Equal(t, []uint32{utxosetMoveBackBlockID}, f.minedBlocks(t, coinbaseTx2), "precondition: the coinbase is in the store, recorded in M")

	require.NoError(t, f.stp.Reorg([]*model.Block{blockM}, []*model.Block{blockF}))

	require.False(t, f.marker(t, coinbaseTx2), "the coinbase is mined after the reorg")
	require.Contains(t, f.minedBlocks(t, coinbaseTx2), utxosetMoveForwardBlockID,
		"the coinbase was created again on the move forward, recorded in the winning block")

	f.stamp(t, utxosetMoveForwardBlockID)

	height, blockID, ok := f.utxoBlock(t, coinbaseTx2)
	require.True(t, ok)
	require.Equal(t, int32(utxosetReorgHeight), height)
	require.Equal(t, int32(utxosetMoveForwardBlockID), blockID, "the stamp writes the block that won onto the coinbase")
	require.Equal(t, []uint32{utxosetMoveForwardBlockID}, f.minedBlocks(t, coinbaseTx2))
}

// TestUtxosetReorgToABlockWhoseCoinbaseIsAlreadyStored is the condition the removed
// stampMoveForwardBlockAsMined named as its own: the winning block's coinbase is already in the
// store when block assembly moves the block forward, so processCoinbaseUtxos gets
// ErrTxExists and skips its create. Here the coinbase was stored under a third block at the
// same height, which the move back of M does not remove.
//
// The utxoset create writes the containment row for the new block before it refuses the
// duplicate, so the store records F with no help from block assembly.
//
// The removed test seeded the coinbase with no block at all. No production path does that:
// the validator refuses coinbases, and block assembly, quick validation and legacy sync each
// create a coinbase with its block.
func TestUtxosetReorgToABlockWhoseCoinbaseIsAlreadyStored(t *testing.T) {
	const otherBlockID = uint32(33)

	f := newUtxosetReorgFixture(t)

	tx := f.unminedTx(t, 1001)
	forwardOnlyTx := f.unminedTx(t, 1003)

	blockM := f.block(t, utxosetMoveBackBlockID, 902, coinbaseTx2, tx)
	blockF := f.block(t, utxosetMoveForwardBlockID, 903, coinbaseTx3, tx, forwardOnlyTx)

	f.validateBlock(t, utxosetMoveBackBlockID, true, tx)
	f.applyAsTip(t, blockM)
	f.validateBlock(t, utxosetMoveForwardBlockID, false, tx, forwardOnlyTx)

	_, _, err := f.store.SpendAndCreate(f.ctx, coinbaseTx3, utxosetReorgHeight, utxostore.WithCreateOnly(),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: otherBlockID, BlockHeight: utxosetReorgHeight}))
	require.NoError(t, err)
	require.Equal(t, []uint32{otherBlockID}, f.minedBlocks(t, coinbaseTx3), "precondition: the coinbase is stored under another block")

	require.NoError(t, f.stp.Reorg([]*model.Block{blockM}, []*model.Block{blockF}))

	require.Equal(t, []uint32{utxosetMoveForwardBlockID, otherBlockID}, f.minedBlocks(t, coinbaseTx3),
		"the refused create records the winning block for the coinbase")
	require.False(t, f.marker(t, coinbaseTx3), "the coinbase is mined after the reorg")

	f.stamp(t, utxosetMoveForwardBlockID)

	height, blockID, ok := f.utxoBlock(t, coinbaseTx3)
	require.True(t, ok)
	require.Equal(t, int32(utxosetReorgHeight), height)
	require.Equal(t, int32(utxosetMoveForwardBlockID), blockID, "the stamp writes the block that won onto the coinbase")
	require.Equal(t, []uint32{utxosetMoveForwardBlockID}, f.minedBlocks(t, coinbaseTx3), "the other block's row is removed as a loser")
}
