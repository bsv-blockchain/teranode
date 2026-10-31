package blockvalidation

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobopts "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// overtakeTestStore wraps the UTXO store so a test can act at precise points:
// afterSetMined runs once, right after the first SetMinedMulti for blockID,
// failMarkFalse makes the next N MarkTransactionsOnLongestChain(false) calls fail, and
// failAfterSetMined makes SetMinedMulti fail after its write landed: a setTxMined that
// cleared unmined_since and then failed, leaving the block's mined_set unwritten.
type overtakeTestStore struct {
	utxostore.Store

	blockID           uint32
	once              sync.Once
	afterSetMined     func()
	failMarkFalse     atomic.Int32
	failAfterSetMined atomic.Bool

	markedTrueMu sync.Mutex
	markedTrue   []chainhash.Hash // every hash passed to MarkTransactionsOnLongestChain(true)
}

func (s *overtakeTestStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxostore.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	res, err := s.Store.SetMinedMulti(ctx, hashes, minedBlockInfo)
	if err == nil && s.failAfterSetMined.Load() {
		return nil, errors.NewStorageError("injected failure after the write")
	}

	if err == nil && s.afterSetMined != nil && minedBlockInfo.BlockID == s.blockID {
		s.once.Do(s.afterSetMined)
	}

	return res, err
}

func (s *overtakeTestStore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool, opts ...utxostore.MarkLongestChainOption) error {
	if onLongestChain {
		s.markedTrueMu.Lock()
		s.markedTrue = append(s.markedTrue, txHashes...)
		s.markedTrueMu.Unlock()
	}

	if !onLongestChain && s.failMarkFalse.Load() > 0 {
		s.failMarkFalse.Add(-1)
		return errors.NewStorageError("injected mark failure")
	}

	return s.Store.MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain, opts...)
}

// coinbaseOnlyBlock builds a mined block with no subtrees on parent. tag makes the
// coinbase, and so the block, unique.
func coinbaseOnlyBlock(t *testing.T, parent *chainhash.Hash, nBits *model.NBit, height uint32, tag byte) *model.Block {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, byte(height), 0x00, 0x00, tag, '/', 'T', 'e', 's', 't'})
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("1MUMxUTXcPQ1kAqB7MtJWneeAwVW4cHzzp", 50*100000000))

	header := mineBlockHeader(t, parent, coinbaseTx.TxIDChainHash(), nBits)

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}

// overtakeFixture is a chain with block B on genesis mining two mempool txs:
// bOnly (only ever in B) and shared (also mined by the competing chain).
type overtakeFixture struct {
	ctx                     context.Context
	tSettings               *settings.Settings
	store                   *overtakeTestStore
	blockchainClient        blockchain.ClientI
	subtreeStore            blob.Store
	txStore                 blob.Store
	subtreeValidationClient subtreevalidation.Interface
	nBits                   *model.NBit
	blockB                  *model.Block
	blockBID                uint32
	bOnlyTx                 *bt.Tx
	bOnly                   *chainhash.Hash
	shared                  *chainhash.Hash
	later                   *chainhash.Hash // only with newOvertakeFixtureWithLaterSubtree
}

func newOvertakeFixture(t *testing.T) *overtakeFixture {
	t.Helper()

	return newOvertakeFixtureSubtrees(t, false)
}

// newOvertakeFixtureWithLaterSubtree is newOvertakeFixture with a second subtree in B that
// mines one more mempool tx, later, only ever in B.
func newOvertakeFixtureWithLaterSubtree(t *testing.T) *overtakeFixture {
	t.Helper()

	return newOvertakeFixtureSubtrees(t, true)
}

func newOvertakeFixtureSubtrees(t *testing.T, withLater bool) *overtakeFixture {
	t.Helper()
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	innerUtxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	t.Cleanup(deferFunc)

	tSettings := test.CreateBaseTestSettings(t)
	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// Two unrelated mempool txs, created unmined. The sql store stamps unmined_since with
	// the create height, so create above 0 to make "unmined" observable.
	coinbaseB, bOnlyTx, _, _ := createCoinbaseAndChildTx(t)
	_, sharedTx, _, _ := createCoinbaseAndChildTx(t)

	_, _, err = innerUtxoStore.SpendAndCreate(ctx, bOnlyTx, 1, utxostore.WithCreateOnly())
	require.NoError(t, err)
	_, _, err = innerUtxoStore.SpendAndCreate(ctx, sharedTx, 1, utxostore.WithCreateOnly())
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*bOnlyTx.TxIDChainHash(), uint64(bOnlyTx.Size()), 0))   //nolint:gosec
	require.NoError(t, subtree.AddNode(*sharedTx.TxIDChainHash(), uint64(sharedTx.Size()), 0)) //nolint:gosec

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeHashes := []*chainhash.Hash{subtree.RootHash()}
	txCount := uint64(subtree.Length())                                      //nolint:gosec
	totalSize := uint64(coinbaseB.Size() + bOnlyTx.Size() + sharedTx.Size()) //nolint:gosec

	var later *chainhash.Hash

	if withLater {
		_, laterTx, _, _ := createCoinbaseAndChildTx(t)
		_, _, err = innerUtxoStore.SpendAndCreate(ctx, laterTx, 1, utxostore.WithCreateOnly())
		require.NoError(t, err)

		laterSubtree, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, laterSubtree.AddNode(*laterTx.TxIDChainHash(), uint64(laterTx.Size()), 0)) //nolint:gosec

		laterBytes, err := laterSubtree.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(ctx, laterSubtree.RootHash()[:], fileformat.FileTypeSubtree, laterBytes))

		subtreeHashes = append(subtreeHashes, laterSubtree.RootHash())
		txCount += uint64(laterSubtree.Length()) //nolint:gosec
		totalSize += uint64(laterTx.Size())      //nolint:gosec
		later = laterTx.TxIDChainHash()
	}

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseB.TxIDChainHash(), 0, uint64(coinbaseB.Size())) //nolint:gosec

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	headerB := mineBlockHeader(t, tSettings.ChainCfgParams.GenesisHash, replicated.RootHash(), nBits)
	blockB, err := model.NewBlock(headerB, coinbaseB, subtreeHashes, txCount, totalSize, 1, 0)
	require.NoError(t, err)

	require.NoError(t, blockchainClient.AddBlock(ctx, blockB, "test", blockchainoptions.WithSubtreesSet(true)))

	_, metaB, err := blockchainClient.GetBlockHeader(ctx, blockB.Hash())
	require.NoError(t, err)

	return &overtakeFixture{
		ctx:                     ctx,
		tSettings:               tSettings,
		store:                   &overtakeTestStore{Store: innerUtxoStore, blockID: metaB.ID},
		blockchainClient:        blockchainClient,
		subtreeStore:            subtreeStore,
		txStore:                 txStore,
		subtreeValidationClient: subtreeValidationClient,
		nBits:                   nBits,
		blockB:                  blockB,
		blockBID:                metaB.ID,
		bOnlyTx:                 bOnlyTx,
		bOnly:                   bOnlyTx.TxIDChainHash(),
		shared:                  sharedTx.TxIDChainHash(),
		later:                   later,
	}
}

// newBlockValidation returns a BlockValidation whose background workers have stopped, so
// the test drives every repair slice itself (drainTipRepair) and sees them in order.
func (f *overtakeFixture) newBlockValidation(t *testing.T) *BlockValidation {
	t.Helper()

	ctx, stop := context.WithCancel(f.ctx)
	bv := f.newRunningBlockValidation(ctx)
	stop()
	bv.backgroundTasks.Wait()

	return bv
}

// newRunningBlockValidation returns a BlockValidation with its background workers running:
// the setMined worker runs the repair on its own, as in production.
func (f *overtakeFixture) newRunningBlockValidation(ctx context.Context) *BlockValidation {
	return NewBlockValidation(ctx, ulogger.TestLogger{}, f.tSettings, f.blockchainClient, f.subtreeStore, f.txStore, f.store, nil, f.subtreeValidationClient)
}

// drainTipRepair runs repair slices until none is pending, as the setMined worker would
// between blocks, and returns the number of slices. It stops at the first error.
func drainTipRepair(t *testing.T, bv *BlockValidation) (int, error) {
	t.Helper()

	for slices := 1; slices < 1000; slices++ {
		more, err := bv.tipRepairSlice(context.Background(), bv.blockchainClient)
		if err != nil || !more {
			return slices, err
		}
	}

	t.Fatal("repair did not settle")

	return 0, nil
}

// overtakeB adds the competing chain B' -> C' on genesis, which overtakes B, and returns C'.
// B' also mines shared, so shared is mined on the winning chain.
func (f *overtakeFixture) overtakeB(t *testing.T) *model.Block {
	t.Helper()

	blockC2, blockB2ID := f.overtakeBPendingSharedWrite(t)
	f.mineSharedIn(t, blockB2ID)

	return blockC2
}

// mineSharedIn writes shared as mined on the longest chain in blockID: the competing
// block's own setTxMined.
func (f *overtakeFixture) mineSharedIn(t *testing.T, blockID uint32) {
	t.Helper()

	_, err := f.store.Store.SetMinedMulti(f.ctx, []*chainhash.Hash{f.shared}, utxostore.MinedBlockInfo{
		BlockID:        blockID,
		BlockHeight:    1,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)
}

// mineIn writes hash as mined on the longest chain in blockID.
func (f *overtakeFixture) mineIn(t *testing.T, hash *chainhash.Hash, blockID uint32) {
	t.Helper()

	_, err := f.store.Store.SetMinedMulti(f.ctx, []*chainhash.Hash{hash}, utxostore.MinedBlockInfo{
		BlockID:        blockID,
		BlockHeight:    1,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)
}

// overtakeBPendingSharedWrite is overtakeB before B' has written shared as mined. It returns
// C' and B”s id.
func (f *overtakeFixture) overtakeBPendingSharedWrite(t *testing.T) (*model.Block, uint32) {
	t.Helper()

	// Header timestamps must not decrease along the chain.
	time.Sleep(10 * time.Millisecond)

	blockB2 := coinbaseOnlyBlock(t, f.tSettings.ChainCfgParams.GenesisHash, f.nBits, 1, 0xb2)
	blockC2 := coinbaseOnlyBlock(t, blockB2.Hash(), f.nBits, 2, 0xc2)

	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockB2, "test", blockchainoptions.WithSubtreesSet(true)))
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockC2, "test", blockchainoptions.WithSubtreesSet(true)))

	_, metaB2, err := f.blockchainClient.GetBlockHeader(f.ctx, blockB2.Hash())
	require.NoError(t, err)

	require.False(t, f.onChain(t, f.blockBID), "precondition: the competing chain overtook B")

	return blockC2, metaB2.ID
}

// extendB makes B's chain the longest again: B -> D2 -> D3 outworks B' -> C'.
func (f *overtakeFixture) extendB(t *testing.T) {
	t.Helper()

	time.Sleep(10 * time.Millisecond)

	blockD2 := coinbaseOnlyBlock(t, f.blockB.Hash(), f.nBits, 2, 0xd2)
	blockD3 := coinbaseOnlyBlock(t, blockD2.Hash(), f.nBits, 3, 0xd3)

	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockD2, "test", blockchainoptions.WithSubtreesSet(true)))
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockD3, "test", blockchainoptions.WithSubtreesSet(true)))

	require.True(t, f.onChain(t, f.blockBID), "precondition: B is back on the longest chain")
}

func (f *overtakeFixture) onChain(t *testing.T, blockID uint32) bool {
	t.Helper()

	onChain, err := f.blockchainClient.CheckBlockIsInCurrentChain(f.ctx, []uint32{blockID})
	require.NoError(t, err)

	return onChain
}

func (f *overtakeFixture) unminedSince(t *testing.T, hash *chainhash.Hash) uint32 {
	t.Helper()

	m, err := f.store.Store.Get(f.ctx, hash, fields.UnminedSince)
	require.NoError(t, err)

	return m.UnminedSince
}

// TestRepairOvertakenTips_OvertakenAfterSetMined pins the repair for a tip overtaken after
// its setTxMined finished (issue 2051).
//
// setTxMined clears unmined_since on the txs of the block that is the tip when it runs. If
// a competing chain overtakes that block before block assembly moves onto it, block
// assembly reaches the new tip by a pure catch-up, never sees the block as a moveBack
// block, and nothing in block assembly restores unmined_since. At scale block assembly lags
// by minutes, so this is the common case in a fork storm. The txs would sit in the mempool
// looking mined: prunable, and invisible to loadUnminedTransactions. The repair must
// re-mark the txs not mined on the new chain, and leave a tx the winning chain mined.
func TestRepairOvertakenTips_OvertakenAfterSetMined(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.True(t, f.onChain(t, f.blockBID), "precondition: B is the tip when setTxMined runs")
	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	require.Zero(t, f.unminedSince(t, f.bOnly), "precondition: setTxMined marked B's txs mined")

	_ = f.overtakeB(t)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)

	require.NotZero(t, f.unminedSince(t, f.bOnly), "a tx mined only in the overtaken block must be unmined again")
	require.Zero(t, f.unminedSince(t, f.shared), "a tx also mined on the winning chain must stay mined")
}

// TestRepairOvertakenTips_OvertakenDuringSetMined covers the overtake landing while B's own
// setTxMined is running: the write still uses onLongestChain=true, so B must be tracked.
func TestRepairOvertakenTips_OvertakenDuringSetMined(t *testing.T) {
	f := newOvertakeFixture(t)
	f.store.afterSetMined = func() { _ = f.overtakeB(t) }
	bv := f.newBlockValidation(t)

	require.True(t, f.onChain(t, f.blockBID), "precondition: B is the tip when setTxMined starts")
	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)

	bOnlyMeta, err := f.store.Store.Get(f.ctx, f.bOnly, fields.UnminedSince, fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{f.blockBID}, bOnlyMeta.BlockIDs)
	require.NotZero(t, bOnlyMeta.UnminedSince, "a tx mined only in the overtaken block must be unmined again")
	require.Zero(t, f.unminedSince(t, f.shared), "a tx also mined on the winning chain must stay mined")
}

// TestRepairOvertakenTips_DuplicateSetMinedKeepsTracking: a second setTxMined for B while
// B's write is in flight is turned away as a duplicate. That must not stop B being tracked:
// the write in flight is what clears its txs.
func TestRepairOvertakenTips_DuplicateSetMinedKeepsTracking(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	f.store.afterSetMined = func() {
		require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()), "the duplicate is skipped, not an error")
	}

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "B was still tracked, so it is repaired")
}

// TestRepairOvertakenTips_SharedTxWrittenAfterRepair: the other order for a tx B shares with
// the winning chain. The repair runs before the winning block's setTxMined has written the
// tx, so the tx still reads as only in B and is stamped; that setTxMined then clears it.
// Both run on the setMined worker, so they never overlap.
func TestRepairOvertakenTips_SharedTxWrittenAfterRepair(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_, blockB2ID := f.overtakeBPendingSharedWrite(t)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.shared), "precondition: shared read as only in B, so it was stamped")

	f.mineSharedIn(t, blockB2ID)

	require.Zero(t, f.unminedSince(t, f.shared), "the winning block's write leaves shared mined")
	require.NotZero(t, f.unminedSince(t, f.bOnly))
}

// TestRepairOvertakenTips_Slices: a block larger than one slice is repaired across several
// slices, each one a separate step of the setMined worker.
func TestRepairOvertakenTips_Slices(t *testing.T) {
	f := newOvertakeFixture(t)
	f.tSettings.BlockValidation.TipRepairSliceSize = 1
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	steps, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	// Slices are over subtree nodes, the coinbase placeholder included: start the job, one
	// step per node (the first also loads the subtree, the last finishes), find nothing left.
	require.Equal(t, 5, steps)

	require.NotZero(t, f.unminedSince(t, f.bOnly))
	require.Zero(t, f.unminedSince(t, f.shared))
}

// TestRepairOvertakenTips_RepairsAgainAfterFlipBack: a block whose repair had nothing to
// re-mark (the winning chain mined all its txs) and that then comes back onto the chain must
// be repaired again when it leaves a second time, for a chain that mines none of them.
func TestRepairOvertakenTips_RepairsAgainAfterFlipBack(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))

	_, blockB2ID := f.overtakeBPendingSharedWrite(t)
	f.mineSharedIn(t, blockB2ID)
	f.mineIn(t, f.bOnly, blockB2ID)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.Zero(t, f.unminedSince(t, f.bOnly), "precondition: B' mined every tx of B, so nothing was re-marked")

	f.extendB(t)

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)

	// A third chain, longer than B -> D2 -> D3, that mines neither tx.
	time.Sleep(10 * time.Millisecond)

	parent := f.tSettings.ChainCfgParams.GenesisHash
	for height := uint32(1); height <= 4; height++ {
		block := coinbaseOnlyBlock(t, parent, f.nBits, height, 0xe0+byte(height))
		require.NoError(t, f.blockchainClient.AddBlock(f.ctx, block, "test", blockchainoptions.WithSubtreesSet(true)))
		parent = block.Hash()
	}

	require.False(t, f.onChain(t, f.blockBID), "precondition: B is overtaken again")

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "B is repaired again: no block on the new chain mines bOnly")
}

// TestRepairOvertakenTips_RetriesAfterFailure: a failed repair leaves the block tracked and
// unrepaired, so the next slice retries it. No later setTxMined can do it: B's mined_set is
// already written, and a retried setTxMined would see onLongestChain=false.
func TestRepairOvertakenTips_RetriesAfterFailure(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	f.store.failMarkFalse.Store(1 << 30)

	for i := 0; i < 2*tipRepairMaxAttempts; i++ {
		_, err := drainTipRepair(t, bv)
		require.Error(t, err)
	}

	require.Zero(t, f.unminedSince(t, f.bOnly), "precondition: the failed repairs changed nothing")
	_, tracked := bv.minedOnChain.snapshot()[*f.blockB.Hash()]
	require.True(t, tracked, "a transient failure never gives the block up")

	f.store.failMarkFalse.Store(0)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "the next attempt repairs the block")
}

// blockGoneClient answers GetBlock with block-not-found.
type blockGoneClient struct {
	blockchain.ClientI
}

func (c *blockGoneClient) GetBlock(_ context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	return nil, errors.NewBlockNotFoundError(blockHash.String())
}

// extraSubtreeClient returns blocks with an extra, never-stored subtree listed first.
type extraSubtreeClient struct {
	blockchain.ClientI
}

func (c *extraSubtreeClient) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	block, err := c.ClientI.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	missing := chainhash.HashH([]byte("never stored"))

	return model.NewBlock(block.Header, block.CoinbaseTx, append([]*chainhash.Hash{&missing}, block.Subtrees...),
		block.TransactionCount, block.SizeInBytes, block.Height, block.ID)
}

// TestRepairOvertakenTips_SkipsMissingSubtree: a subtree that is gone must not stop the
// repair of the block's other subtrees, nor keep the block retrying.
func TestRepairOvertakenTips_SkipsMissingSubtree(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	client := &extraSubtreeClient{ClientI: f.blockchainClient}
	bv.blockchainClient = client

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "the readable subtree is repaired")

	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].done, "and the block is done, not retried")
}

// failingGetBlockClient fails GetBlock while fail is set: a blockchain service that is
// briefly unreachable.
type failingGetBlockClient struct {
	blockchain.ClientI
	fail atomic.Bool
}

func (c *failingGetBlockClient) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	if c.fail.Load() {
		return nil, errors.NewServiceError("blockchain service unavailable")
	}

	return c.ClientI.GetBlock(ctx, blockHash)
}

// TestRepairOvertakenTips_TransientLoadFailureNeverGivesUp: failing to load the overtaken
// block because a service is briefly unavailable must not count toward giving the block up.
func TestRepairOvertakenTips_TransientLoadFailureNeverGivesUp(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	client := &failingGetBlockClient{ClientI: f.blockchainClient}
	client.fail.Store(true)
	bv.blockchainClient = client

	for i := 0; i < 2*tipRepairMaxAttempts; i++ {
		_, err := drainTipRepair(t, bv)
		require.Error(t, err)
	}

	_, tracked := bv.minedOnChain.snapshot()[*f.blockB.Hash()]
	require.True(t, tracked, "a transient failure never gives the block up")

	client.fail.Store(false)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly))
}

// TestRepairOvertakenTips_GivesUpAfterMaxAttempts: a block that can never be repaired (it is
// gone from the blockchain store) must not be retried, and logged, forever.
func TestRepairOvertakenTips_GivesUpAfterMaxAttempts(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	bv.blockchainClient = &blockGoneClient{ClientI: f.blockchainClient}

	before := testutil.ToFloat64(prometheusBlockValidationTipRepairAbandoned.WithLabelValues("block"))

	for i := 0; i < tipRepairMaxAttempts; i++ {
		_, err := drainTipRepair(t, bv)
		require.Error(t, err, "attempt %d", i+1)
	}

	_, tracked := bv.minedOnChain.snapshot()[*f.blockB.Hash()]
	require.False(t, tracked, "the block is dropped after the last attempt")
	require.InDelta(t, before+1, testutil.ToFloat64(prometheusBlockValidationTipRepairAbandoned.WithLabelValues("block")), 0, "and counted")

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err, "and no longer retried")
}

// TestRepairOvertakenTips_FlipBackRemines: a repaired block whose chain becomes the longest
// again must have its txs marked mined again.
func TestRepairOvertakenTips_FlipBackRemines(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "precondition: repaired while B is off chain")

	f.extendB(t)

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)
	require.Zero(t, f.unminedSince(t, f.bOnly), "B is on the longest chain again, so its txs are mined again")
	require.Equal(t, []chainhash.Hash{*f.bOnly}, f.store.markedTrue, "only the tx the repair stamped is written back")
}

// TestRepairOvertakenTips_SeedAfterRestart: the tracker is in memory. A block written mined
// before a restart and found off chain after it must still be repaired. The seed runs at the
// first slice after the restarted process marks a block mined, here the new tip C'.
func TestRepairOvertakenTips_SeedAfterRestart(t *testing.T) {
	f := newOvertakeFixture(t)

	require.NoError(t, f.newBlockValidation(t).setTxMinedStatus(f.ctx, f.blockB.Hash()))
	blockC2 := f.overtakeB(t)

	restarted := f.newBlockValidation(t)

	_, err := drainTipRepair(t, restarted)
	require.NoError(t, err)
	require.Zero(t, f.unminedSince(t, f.bOnly), "precondition: nothing tracked yet, so no repair has run")

	require.NoError(t, restarted.setTxMinedStatus(f.ctx, blockC2.Hash()))

	_, err = drainTipRepair(t, restarted)
	require.NoError(t, err)

	require.NotZero(t, f.unminedSince(t, f.bOnly), "a block overtaken across a restart must be repaired")
	require.Zero(t, f.unminedSince(t, f.shared), "a tx also mined on the winning chain must stay mined")
}

// TestRepairOvertakenTips_PartialWriteSeededBeforeRetry: B's setTxMined clears its txs and
// then fails, so B's mined_set stays unwritten. The node restarts, the competing chain
// overtakes B, and the restarted process seeds the tracker before B's retry. The retry runs
// with onLongestChain=false, which leaves unmined_since alone, so the cleared txs would keep
// looking mined unless the seed tracks B although its mined_set is not written.
func TestRepairOvertakenTips_PartialWriteSeededBeforeRetry(t *testing.T) {
	f := newOvertakeFixture(t)

	f.store.failAfterSetMined.Store(true)
	require.Error(t, f.newBlockValidation(t).setTxMinedStatus(f.ctx, f.blockB.Hash()))
	f.store.failAfterSetMined.Store(false)

	_, metaB, err := f.blockchainClient.GetBlockHeader(f.ctx, f.blockB.Hash())
	require.NoError(t, err)
	require.False(t, metaB.MinedSet, "precondition: B's mined_set is not written")
	require.Zero(t, f.unminedSince(t, f.bOnly), "precondition: the failed write cleared B's txs")

	blockC2 := f.overtakeB(t)

	restarted := f.newBlockValidation(t)

	require.NoError(t, restarted.setTxMinedStatus(f.ctx, blockC2.Hash()))
	_, err = drainTipRepair(t, restarted)
	require.NoError(t, err)

	require.NoError(t, restarted.setTxMinedStatus(f.ctx, f.blockB.Hash()), "B's retry, now off the longest chain")
	_, err = drainTipRepair(t, restarted)
	require.NoError(t, err)

	require.NotZero(t, f.unminedSince(t, f.bOnly), "a tx cleared by the failed write must be unmined again")
	require.Zero(t, f.unminedSince(t, f.shared), "a tx also mined on the winning chain must stay mined")
}

// TestRepairOvertakenTips_SkipsPrunedTxs: a tx of the overtaken block that is no longer in
// the store (pruned) must not stop the repair of the others.
func TestRepairOvertakenTips_SkipsPrunedTxs(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	require.NoError(t, f.store.Store.Delete(f.ctx, f.shared))

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "the remaining txs are repaired")
}

// TestRepairOvertakenTips_StillOnChainNoRemark is the control: when B stays the tip its txs
// stay mined.
func TestRepairOvertakenTips_StillOnChainNoRemark(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)

	require.Zero(t, f.unminedSince(t, f.bOnly), "a tx mined in the tip must stay mined")
}

// TestRepairOvertakenTips_RunsOnTheSetMinedWorker drives the production path end to end, with
// no test call into setTxMined or the repair: the running setMined worker marks B mined,
// the overtaking blocks' notifications ask it for repair slices, and it repairs B.
func TestRepairOvertakenTips_RunsOnTheSetMinedWorker(t *testing.T) {
	f := newOvertakeFixture(t)
	_ = f.newRunningBlockValidation(f.ctx)

	// The overtaking blocks' notifications must reach the worker, so wait for it to subscribe.
	require.Eventually(t, func() bool {
		subscribers, err := f.blockchainClient.GetSubscribers(f.ctx)
		return err == nil && slices.Contains(subscribers, blockchain.SubscriberBlockValidation)
	}, 10*time.Second, 10*time.Millisecond)

	// The worker's mined-not-set pass picks B up and writes it as on the longest chain.
	require.Eventually(t, func() bool {
		_, meta, err := f.blockchainClient.GetBlockHeader(f.ctx, f.blockB.Hash())
		return err == nil && meta.MinedSet
	}, 10*time.Second, 10*time.Millisecond, "the setMined worker marks B mined")
	require.Zero(t, f.unminedSince(t, f.bOnly), "precondition: B's txs are mined")

	// B' writes shared mined before C' overtakes B. The test writes it straight to the
	// store, outside the worker, so a repair slice running in between would race it.
	time.Sleep(10 * time.Millisecond)

	blockB2 := coinbaseOnlyBlock(t, f.tSettings.ChainCfgParams.GenesisHash, f.nBits, 1, 0xb2)
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockB2, "test", blockchainoptions.WithSubtreesSet(true)))
	require.True(t, f.onChain(t, f.blockBID), "precondition: B' alone does not overtake B")

	_, metaB2, err := f.blockchainClient.GetBlockHeader(f.ctx, blockB2.Hash())
	require.NoError(t, err)
	f.mineSharedIn(t, metaB2.ID)

	blockC2 := coinbaseOnlyBlock(t, blockB2.Hash(), f.nBits, 2, 0xc2)
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockC2, "test", blockchainoptions.WithSubtreesSet(true)))
	require.False(t, f.onChain(t, f.blockBID), "precondition: the competing chain overtook B")

	require.Eventually(t, func() bool {
		m, err := f.store.Store.Get(f.ctx, f.bOnly, fields.UnminedSince)
		return err == nil && m.UnminedSince != 0
	}, 10*time.Second, 20*time.Millisecond, "the setMined worker repairs the overtaken block")

	require.Zero(t, f.unminedSince(t, f.shared), "a tx also mined on the winning chain must stay mined")
}

// grpcLikeBestHeaderClient returns GetBestBlockHeader metas without the block id, as the
// gRPC blockchain client does.
type grpcLikeBestHeaderClient struct {
	blockchain.ClientI
}

func (c *grpcLikeBestHeaderClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	header, meta, err := c.ClientI.GetBestBlockHeader(ctx)
	if meta != nil {
		stripped := *meta
		stripped.ID = 0
		meta = &stripped
	}

	return header, meta, err
}

// TestMainChainIDs_ExtensionWithoutBestHeaderID: when the best block only extends the one
// seen last time, its id must still be added to the main-chain set even though
// GetBestBlockHeader over gRPC carries no id. Adding id 0 instead made the new tip look off
// the chain, and the repair stamped its txs unmined.
func TestMainChainIDs_ExtensionWithoutBestHeaderID(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)
	client := &grpcLikeBestHeaderClient{ClientI: f.blockchainClient}

	_, _, err := bv.mainChainIDsLocked(f.ctx, client)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	child := coinbaseOnlyBlock(t, f.blockB.Hash(), f.nBits, 2, 0xa2)
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, child, "test", blockchainoptions.WithSubtreesSet(true)))

	_, childMeta, err := f.blockchainClient.GetBlockHeader(f.ctx, child.Hash())
	require.NoError(t, err)

	onMainChain, _, err := bv.mainChainIDsLocked(f.ctx, client)
	require.NoError(t, err)
	require.True(t, onMainChain[childMeta.ID], "the new tip is on the main chain")
	require.True(t, onMainChain[f.blockBID])
}

func TestTipRepairExpired(t *testing.T) {
	// GetBlockHeaderIDs(best, tipRepairWindow) covers best down to best-tipRepairWindow+1.
	require.False(t, tipRepairExpired(1001, 2000), "inside the main chain ID window")
	require.True(t, tipRepairExpired(1000, 2000), "outside it: treating it as off chain would read every tx")
	require.False(t, tipRepairExpired(5, 10), "young chain")
}

func TestMinedOnChainTracker_NewWriteStartsOver(t *testing.T) {
	tracker := newMinedOnChainTracker()
	hash := chainhash.HashH([]byte("b"))

	tracker.track(hash, 1, 1)
	require.True(t, tracker.markWritten(hash, true))

	gen := tracker.snapshot()[hash].gen
	tracker.update(hash, gen, func(e *minedOnChainBlock) {
		e.stamped = true
		e.done = true
	})

	// setTxMined writes the block as on the longest chain again (its mined_set was cleared).
	tracker.track(hash, 1, 1)

	b := tracker.snapshot()[hash]
	require.NotEqual(t, gen, b.gen, "a new write is a new generation; a repair in progress for the old one is abandoned")
	require.False(t, b.done, "the new write clears the txs again, so the block needs repairing again")
	require.True(t, b.stamped, "kept in case the new write fails before clearing the re-marked txs")

	tracker.update(hash, gen, func(e *minedOnChainBlock) { e.done = true })
	require.False(t, tracker.snapshot()[hash].done, "an update for the old generation is ignored")
}

func TestMinedOnChainTracker_Nil(t *testing.T) {
	var tracker *minedOnChainTracker
	hash := chainhash.HashH([]byte("b"))

	tracker.track(hash, 1, 1)
	tracker.seed(hash, 1, 1)
	require.False(t, tracker.markWritten(hash, true))
	tracker.update(hash, 1, func(e *minedOnChainBlock) { e.done = true })
	tracker.drop(hash)
	require.True(t, tracker.empty())
	require.Nil(t, tracker.snapshot())
}

// TestRepairOvertakenTips_DefaultSliceSize: an unset slice size falls back to the default.
func TestRepairOvertakenTips_DefaultSliceSize(t *testing.T) {
	f := newOvertakeFixture(t)
	f.tSettings.BlockValidation.TipRepairSliceSize = 0
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly))
}

// TestRunTipRepairSlice_RetriesAfterError: a failed slice leaves the retry flag set, so the
// periodic tick asks again; the next good slice clears it.
func TestRunTipRepairSlice_RetriesAfterError(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	client := &failingGetBlockClient{ClientI: f.blockchainClient}
	client.fail.Store(true)

	bv.runTipRepairSlice(f.ctx, client)
	require.True(t, bv.tipRepairRetry.Load())

	client.fail.Store(false)

	for i := 0; i < 10 && bv.tipRepairPending.Swap(false); i++ {
		bv.runTipRepairSlice(f.ctx, client)
	}

	bv.runTipRepairSlice(f.ctx, client)
	require.False(t, bv.tipRepairRetry.Load())
	require.NotZero(t, f.unminedSince(t, f.bOnly))
}

// seedFailClient fails the one blockchain call named by failOn.
type seedFailClient struct {
	blockchain.ClientI
	failOn string
}

func (c *seedFailClient) err(name string) error {
	if c.failOn == name {
		return errors.NewServiceError("injected %s failure", name)
	}

	return nil
}

func (c *seedFailClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if err := c.err("GetBestBlockHeader"); err != nil {
		return nil, nil, err
	}

	return c.ClientI.GetBestBlockHeader(ctx)
}

func (c *seedFailClient) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	if err := c.err("GetBlockHeaders"); err != nil {
		return nil, nil, err
	}

	return c.ClientI.GetBlockHeaders(ctx, hash, n)
}

func (c *seedFailClient) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	if err := c.err("GetLastNBlocks"); err != nil {
		return nil, err
	}

	return c.ClientI.GetLastNBlocks(ctx, n, includeOrphans, fromHeight)
}

func (c *seedFailClient) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if err := c.err("GetBlockHeader"); err != nil {
		return nil, nil, err
	}

	return c.ClientI.GetBlockHeader(ctx, hash)
}

func (c *seedFailClient) GetBlockHeaderIDs(ctx context.Context, hash *chainhash.Hash, n uint64) ([]uint32, error) {
	if err := c.err("GetBlockHeaderIDs"); err != nil {
		return nil, err
	}

	return c.ClientI.GetBlockHeaderIDs(ctx, hash, n)
}

// TestRepairOvertakenTips_SeedFailures: a blockchain call failing during the restart seed or
// the main-chain read fails the slice and leaves the seed to run again.
func TestRepairOvertakenTips_SeedFailures(t *testing.T) {
	for _, failOn := range []string{"GetBestBlockHeader", "GetBlockHeaders", "GetLastNBlocks", "GetBlockHeader"} {
		t.Run(failOn, func(t *testing.T) {
			f := newOvertakeFixture(t)
			bv := f.newBlockValidation(t)

			require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
			_ = f.overtakeB(t)

			// The seed must run in this slice.
			bv.tipRepairSeeded = false

			_, err := bv.tipRepairSlice(f.ctx, &seedFailClient{ClientI: f.blockchainClient, failOn: failOn})
			require.Error(t, err)

			_, err = drainTipRepair(t, bv)
			require.NoError(t, err)
			require.NotZero(t, f.unminedSince(t, f.bOnly), "the next slice repairs the block")
		})
	}
}

// farBestClient reports the best block as much higher than it is, so tracked blocks look
// older than the repair window.
type farBestClient struct {
	blockchain.ClientI
}

func (c *farBestClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	header, meta, err := c.ClientI.GetBestBlockHeader(ctx)
	if meta != nil {
		far := *meta
		far.Height += 2 * tipRepairWindow
		meta = &far
	}

	return header, meta, err
}

// TestRepairOvertakenTips_ExpiredBlockDropped: a block that left the window before its repair
// is dropped, not repaired.
func TestRepairOvertakenTips_ExpiredBlockDropped(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	bv.blockchainClient = &farBestClient{ClientI: f.blockchainClient}

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.True(t, bv.minedOnChain.empty(), "the expired block is dropped")
	require.Zero(t, f.unminedSince(t, f.bOnly), "and not repaired")
}

// TestRepairOvertakenTips_InvalidBlockDropped: an invalidated block belongs to unsetMined,
// so the repair drops it.
func TestRepairOvertakenTips_InvalidBlockDropped(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))

	_, err := f.blockchainClient.InvalidateBlock(f.ctx, f.blockB.Hash())
	require.NoError(t, err)

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)

	_, tracked := bv.minedOnChain.snapshot()[*f.blockB.Hash()]
	require.False(t, tracked, "the invalid block is dropped")
}

// TestRepairOvertakenTips_SubtreeToCheckOnly: setTxMined reads a subtree from its
// .subtreeToCheck file when the .subtree file is not there, so the repair must too, or a
// block marked mined that way is never repaired.
func TestRepairOvertakenTips_SubtreeToCheckOnly(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	subtreeHash := f.blockB.Subtrees[0]
	subtreeBytes, err := f.subtreeStore.Get(f.ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.NoError(t, f.subtreeStore.Set(f.ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))
	require.NoError(t, f.subtreeStore.Del(f.ctx, subtreeHash[:], fileformat.FileTypeSubtree))

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "the subtree is read from its .subtreeToCheck file")
}

// truncatingSubtreeStore returns a truncated reader for the next truncate reads: a subtree
// read while another service moves the file.
type truncatingSubtreeStore struct {
	blob.Store
	truncate atomic.Int32
}

func (s *truncatingSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (io.ReadCloser, error) {
	reader, err := s.Store.GetIoReader(ctx, key, fileType, opts...)
	if err != nil || s.truncate.Load() <= 0 {
		return reader, err
	}

	s.truncate.Add(-1)

	return struct {
		io.Reader
		io.Closer
	}{io.LimitReader(reader, 10), reader}, nil
}

// TestRepairOvertakenTips_TruncatedSubtreeReadRetried: a truncated subtree read must be
// retried, not taken for a subtree that is gone, which would skip its txs for good.
func TestRepairOvertakenTips_TruncatedSubtreeReadRetried(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	store := &truncatingSubtreeStore{Store: f.subtreeStore}
	store.truncate.Store(1)
	bv.subtreeStore = store

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "the subtree is read again and repaired")
}

// TestRepairOvertakenTips_StuckJobYields: a job that keeps failing must not hold up the
// repair of other blocks.
func TestRepairOvertakenTips_StuckJobYields(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	f.store.failMarkFalse.Store(1 << 30)

	// B's job starts first and gets stuck.
	_, err := bv.tipRepairSlice(f.ctx, bv.blockchainClient)
	require.NoError(t, err)
	require.NotNil(t, bv.tipRepairJob)
	require.Equal(t, *f.blockB.Hash(), bv.tipRepairJob.hash)

	// A second overtaken block, written mined while it was on the chain. It has no txs, so
	// its repair finishes in one step.
	time.Sleep(10 * time.Millisecond)

	fork := coinbaseOnlyBlock(t, f.tSettings.ChainCfgParams.GenesisHash, f.nBits, 1, 0xf1)
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, fork, "test", blockchainoptions.WithSubtreesSet(true)))

	_, forkMeta, err := f.blockchainClient.GetBlockHeader(f.ctx, fork.Hash())
	require.NoError(t, err)
	require.False(t, f.onChain(t, forkMeta.ID))

	bv.minedOnChain.track(*fork.Hash(), forkMeta.ID, forkMeta.Height)
	require.True(t, bv.minedOnChain.markWritten(*fork.Hash(), true))

	for i := 0; i < 3*tipRepairMaxAttempts; i++ {
		_, _ = bv.tipRepairSlice(f.ctx, bv.blockchainClient)
	}

	require.True(t, bv.minedOnChain.snapshot()[*fork.Hash()].done, "the other block is repaired while B's job keeps failing")
	require.False(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].done, "B is not done")

	f.store.failMarkFalse.Store(0)

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "B is repaired once the failure clears")
}

// TestRepairOvertakenTips_RemineSkipsTxNotInBlock: a tx of a re-marked block that was pruned
// and created again unmined while the block was off the chain no longer carries the block's
// id. Marking it mined when the block comes back would leave it looking mined in no block.
func TestRepairOvertakenTips_RemineSkipsTxNotInBlock(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].stamped, "precondition: B's txs were re-marked")

	require.NoError(t, f.store.Store.Delete(f.ctx, f.bOnly))
	_, _, err = f.store.Store.SpendAndCreate(f.ctx, f.bOnlyTx, 1, utxostore.WithCreateOnly())
	require.NoError(t, err)

	f.extendB(t)

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)

	m, err := f.store.Store.Get(f.ctx, f.bOnly, fields.UnminedSince, fields.BlockIDs)
	require.NoError(t, err)
	require.Empty(t, m.BlockIDs, "precondition: the re-created tx is in no block")
	require.NotZero(t, m.UnminedSince, "a tx in no block must stay unmined")
}

// TestRepairOvertakenTips_PartialWriteRetryKeepsFailing: B's setTxMined clears its txs and
// then fails, and every retry fails too, so B's mined_set is never written. Once the
// competing chain overtakes B, the cleared txs must be re-marked without waiting for a retry
// to succeed, whether the process kept B's tracking or restarted and seeded it.
func TestRepairOvertakenTips_PartialWriteRetryKeepsFailing(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "in process", true: "after restart"}[restart], func(t *testing.T) {
			f := newOvertakeFixture(t)
			bv := f.newBlockValidation(t)

			f.store.failAfterSetMined.Store(true)
			require.Error(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
			require.Zero(t, f.unminedSince(t, f.bOnly), "precondition: the failed write cleared B's txs")

			blockC2 := f.overtakeB(t)

			if restart {
				bv = f.newBlockValidation(t)

				f.store.failAfterSetMined.Store(false)
				require.NoError(t, bv.setTxMinedStatus(f.ctx, blockC2.Hash()))
				f.store.failAfterSetMined.Store(true)
			}

			require.Error(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()), "B's retry, off the longest chain, fails again")

			_, err := drainTipRepair(t, bv)
			require.NoError(t, err)

			require.NotZero(t, f.unminedSince(t, f.bOnly), "a tx cleared by the failed write must be unmined again")
			require.Zero(t, f.unminedSince(t, f.shared), "a tx also mined on the winning chain must stay mined")
		})
	}
}

// failingSubtreeStore fails the next fail subtree reads with a storage error: a blob store
// that is briefly unavailable.
type failingSubtreeStore struct {
	blob.Store
	fail atomic.Int32
}

func (s *failingSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (io.ReadCloser, error) {
	if s.fail.Add(-1) >= 0 {
		return nil, errors.NewStorageError("injected subtree store failure")
	}

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}

// TestRepairOvertakenTips_TransientSubtreeReadErrorRetried: a subtree store that stays
// unavailable through every read attempt of a step must not get the subtree skipped as gone;
// the step is retried once the store is back.
func TestRepairOvertakenTips_TransientSubtreeReadErrorRetried(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	store := &failingSubtreeStore{Store: f.subtreeStore}
	store.fail.Store(tipRepairSubtreeReadAttempts)
	bv.subtreeStore = store

	_, err := drainTipRepair(t, bv)
	require.Error(t, err, "the step fails while the store is down")
	require.False(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].done, "the subtree is not skipped")

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "the subtree is repaired once the store is back")
}

// TestRepairOvertakenTips_ShortReadsOutlastAttemptsRetried: short subtree reads through every
// read attempt of a step must not get the subtree skipped as corrupt, which would leave its
// txs looking mined for good; the step is retried and recovers once reads are whole again.
func TestRepairOvertakenTips_ShortReadsOutlastAttemptsRetried(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	store := &truncatingSubtreeStore{Store: f.subtreeStore}
	store.truncate.Store(tipRepairSubtreeReadAttempts)
	bv.subtreeStore = store

	_, err := drainTipRepair(t, bv)
	require.Error(t, err, "the step fails while reads are short")
	require.False(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].done, "the subtree is not skipped")

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "the subtree is repaired once reads are whole")
}

// TestRepairOvertakenTips_CorruptSubtreeSkippedAfterMaxAttempts: a subtree that can never be
// deserialized must not hold the job forever, which would leave the block's later subtrees
// unrepaired. It is retried for tipRepairMaxAttempts steps, then set aside and the job
// finishes, with the block not done and the subtree pending for its next job.
func TestRepairOvertakenTips_CorruptSubtreeSkippedAfterMaxAttempts(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	store := &truncatingSubtreeStore{Store: f.subtreeStore}
	store.truncate.Store(1 << 30)
	bv.subtreeStore = store

	for i := 1; i < tipRepairMaxAttempts; i++ {
		_, err := drainTipRepair(t, bv)
		require.Error(t, err, "step %d fails", i)
		require.False(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].done, "the subtree is not skipped after %d steps", i)
	}

	_, err := bv.tipRepairSlice(f.ctx, bv.blockchainClient)
	require.NoError(t, err)
	require.Nil(t, bv.tipRepairJob, "the subtree is set aside and the job finishes")

	b := bv.minedOnChain.snapshot()[*f.blockB.Hash()]
	require.False(t, b.done)
	require.True(t, b.deferred, "other blocks go first")
	require.Equal(t, []*chainhash.Hash{f.blockB.Subtrees[0]}, b.pending)
}

// shortReadSubtreeStore returns a truncated reader for the next short reads of one subtree;
// set short to 1<<30 for a subtree file that is corrupt on disk.
type shortReadSubtreeStore struct {
	blob.Store
	key   []byte
	short atomic.Int32
}

func (s *shortReadSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (io.ReadCloser, error) {
	reader, err := s.Store.GetIoReader(ctx, key, fileType, opts...)
	if err != nil || !bytes.Equal(key, s.key) || s.short.Add(-1) < 0 {
		return reader, err
	}

	return struct {
		io.Reader
		io.Closer
	}{io.LimitReader(reader, 10), reader}, nil
}

// shortReads wraps store so the next n reads of the block's subtree idx are short.
func (f *overtakeFixture) shortReads(store blob.Store, idx int, n int32) *shortReadSubtreeStore {
	s := &shortReadSubtreeStore{Store: store, key: f.blockB.Subtrees[idx][:]}
	s.short.Store(n)

	return s
}

// settleTipRepair runs the repair, as the worker's periodic tick would, until settled holds
// or a bound is hit, ignoring failed steps.
func settleTipRepair(t *testing.T, bv *BlockValidation, settled func() bool) {
	t.Helper()

	for i := 0; i < 10*tipRepairMaxAttempts && !settled(); i++ {
		_, _ = drainTipRepair(t, bv)
	}
}

// TestRepairOvertakenTips_CorruptSubtreeDoesNotBlockLaterSubtrees: a corrupt earlier
// subtree of the overtaken block must not keep its later, intact subtrees from being
// repaired, and must stay pending: the block is not done while it is unread.
func TestRepairOvertakenTips_CorruptSubtreeDoesNotBlockLaterSubtrees(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	require.Zero(t, f.unminedSince(t, f.later), "precondition: setTxMined marked the later subtree's tx mined")
	_ = f.overtakeB(t)

	bv.subtreeStore = f.shortReads(f.subtreeStore, 0, 1<<30)

	for i := 1; i < tipRepairMaxAttempts; i++ {
		_, err := drainTipRepair(t, bv)
		require.Error(t, err, "step %d fails at the corrupt subtree", i)
		require.Zero(t, f.unminedSince(t, f.later), "the corrupt subtree is retried, not skipped, after %d steps", i)
	}

	settleTipRepair(t, bv, func() bool { return f.unminedSince(t, f.later) != 0 })
	require.NotZero(t, f.unminedSince(t, f.later), "the later, intact subtree is repaired")

	b := bv.minedOnChain.snapshot()[*f.blockB.Hash()]
	require.False(t, b.done, "the block is not done while a subtree is unread")
	require.Equal(t, []*chainhash.Hash{f.blockB.Subtrees[0]}, b.pending, "the corrupt subtree is read again later")
}

// TestRepairOvertakenTips_ShortReadsThenRecoveryRepairsSkippedSubtree: a subtree set aside
// after a long run of short reads must be repaired once its reads are whole again, on the
// next pass after the back-off, not left with its txs looking mined. The later subtree is
// repaired meanwhile.
func TestRepairOvertakenTips_ShortReadsThenRecoveryRepairsSkippedSubtree(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)
	clock := f.fakeTipRepairClock(bv)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	bv.subtreeStore = f.shortReads(f.subtreeStore, 0, tipRepairSubtreeReadAttempts*tipRepairMaxAttempts)

	settleTipRepair(t, bv, func() bool { return bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending != nil })
	require.NotZero(t, f.unminedSince(t, f.later), "the later subtree is repaired while subtree 0 is set aside")

	clock.advance(tipRepairPendingBaseBackoff)
	settleTipRepair(t, bv, func() bool { return bv.minedOnChain.snapshot()[*f.blockB.Hash()].done })

	require.NotZero(t, f.unminedSince(t, f.later), "the later subtree is repaired")
	require.NotZero(t, f.unminedSince(t, f.bOnly), "the skipped subtree is repaired once its reads are whole")
	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].done)
}

// TestRepairOvertakenTips_SkipFailuresCountedPerSubtree: the failed steps that got a corrupt
// subtree skipped must not count toward skipping the next one. One step of short reads on
// the intact later subtree, right after the skip, must leave it to be retried.
func TestRepairOvertakenTips_SkipFailuresCountedPerSubtree(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	corrupt := f.shortReads(f.subtreeStore, 0, 1<<30)
	bv.subtreeStore = f.shortReads(corrupt, 1, tipRepairSubtreeReadAttempts)

	settleTipRepair(t, bv, func() bool { return f.unminedSince(t, f.later) != 0 })

	require.NotZero(t, f.unminedSince(t, f.later), "the later subtree is retried and repaired, not skipped")
}

// TestRepairOvertakenTips_PendingClearedBackOnChain: a block with a subtree set aside that
// comes back onto the chain must be repaired in full if it leaves again: its other txs may
// have been marked mined again meanwhile.
func TestRepairOvertakenTips_PendingClearedBackOnChain(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	bv.subtreeStore = f.shortReads(f.subtreeStore, 0, 1<<30)
	settleTipRepair(t, bv, func() bool { return bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending != nil })
	require.NotNil(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending, "precondition: a subtree is set aside")

	f.extendB(t)

	_, _ = bv.tipRepairSlice(f.ctx, bv.blockchainClient)
	require.Nil(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending)
}

// TestMinedOnChainTracker_WriteClearsPending: a completed setTxMined write may have cleared
// any of the block's txs, so the next repair reads the whole block, not only the subtrees
// set aside.
func TestMinedOnChainTracker_WriteClearsPending(t *testing.T) {
	tracker := newMinedOnChainTracker()
	hash := chainhash.HashH([]byte("b"))
	subtree := chainhash.HashH([]byte("s"))

	tracker.track(hash, 1, 1)

	gen := tracker.snapshot()[hash].gen
	tracker.update(hash, gen, func(e *minedOnChainBlock) { e.pending = []*chainhash.Hash{&subtree} })

	require.True(t, tracker.markWritten(hash, true))
	require.Nil(t, tracker.snapshot()[hash].pending)
}

// tipRepairClock is a clock for the overtaken-tip repair's back-off that moves only when
// the test advances it.
type tipRepairClock struct {
	offset atomic.Int64
	base   time.Time
}

func (c *tipRepairClock) now() time.Time {
	return c.base.Add(time.Duration(c.offset.Load()))
}

func (c *tipRepairClock) advance(d time.Duration) {
	c.offset.Add(int64(d))
}

// fakeTipRepairClock has bv's tracker time the back-off on a clock the test moves.
func (f *overtakeFixture) fakeTipRepairClock(bv *BlockValidation) *tipRepairClock {
	clock := &tipRepairClock{base: time.Now()}
	bv.minedOnChain.now = clock.now

	return clock
}

// TestRepairOvertakenTips_PendingPassesBackOffByTime: a block whose subtree stays
// unreadable must not be read again on every slice until it leaves the window. The next
// pass over its pending subtrees waits on the clock, twice as long each time. The wait is
// measured in time, not blocks: a subtree that recovers must be read again long before
// the pruner can delete a parent its off-chain txs spent.
func TestRepairOvertakenTips_PendingPassesBackOffByTime(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)
	clock := f.fakeTipRepairClock(bv)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	store := f.shortReads(f.subtreeStore, 0, 1<<30)
	bv.subtreeStore = store

	settleTipRepair(t, bv, func() bool {
		b := bv.minedOnChain.snapshot()[*f.blockB.Hash()]
		return b.pending != nil && bv.tipRepairJob == nil
	})
	require.NotNil(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending, "precondition: the corrupt subtree is set aside")

	left := store.short.Load()

	for i := 0; i < 5; i++ {
		_, _ = drainTipRepair(t, bv)
	}

	require.Equal(t, left, store.short.Load(), "no new pass before the back-off has passed")

	clock.advance(tipRepairPendingBaseBackoff)

	_, _ = drainTipRepair(t, bv)
	require.Less(t, store.short.Load(), left, "a new pass reads the pending subtree once the back-off has passed")
}

// TestRepairOvertakenTips_PendingBackOffCapped: however many passes a subtree has stayed
// unreadable for, the next one is at most tipRepairPendingMaxBackoff away, so a subtree
// that recovers late in the window is still repaired within that time.
func TestRepairOvertakenTips_PendingBackOffCapped(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)
	clock := f.fakeTipRepairClock(bv)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))

	hash := *f.blockB.Hash()
	b := bv.minedOnChain.snapshot()[hash]

	for pass := 1; pass <= 40; pass++ {
		bv.finishJob(&tipRepairJob{kind: unmarkJob, hash: hash, id: b.id, gen: b.gen, unreadable: f.blockB.Subtrees[:1]})

		wait := bv.minedOnChain.snapshot()[hash].nextPass.Sub(clock.now())
		require.Positive(t, wait, "pass %d", pass)
		require.LessOrEqual(t, wait, tipRepairPendingMaxBackoff, "pass %d", pass)
	}
}

// extendWinning adds one block on tip, the winning chain's tip at height 2, which moves the
// best height.
func (f *overtakeFixture) extendWinning(t *testing.T, tip *model.Block) {
	t.Helper()

	time.Sleep(10 * time.Millisecond)

	blockD2 := coinbaseOnlyBlock(t, tip.Hash(), f.nBits, 3, 0xd4)
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockD2, "test", blockchainoptions.WithSubtreesSet(true)))
}

// TestRepairOvertakenTips_PendingPassOnNewBlock: a subtree set aside whose reads recover must
// be repaired on the next block even while the back-off is still running. The pruner works
// block by block, so a parent the subtree's txs spent can reach its prune height on any
// block; the back-off must not hold the repair past one.
func TestRepairOvertakenTips_PendingPassOnNewBlock(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)
	_ = f.fakeTipRepairClock(bv)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	blockC2 := f.overtakeB(t)

	bv.subtreeStore = f.shortReads(f.subtreeStore, 0, tipRepairSubtreeReadAttempts*tipRepairMaxAttempts)

	settleTipRepair(t, bv, func() bool { return bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending != nil })
	require.NotNil(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending, "precondition: subtree 0 is set aside")

	_, _ = drainTipRepair(t, bv)
	require.Zero(t, f.unminedSince(t, f.bOnly), "precondition: no pass before the back-off or a new block")

	f.extendWinning(t, blockC2)

	_, _ = drainTipRepair(t, bv)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "the recovered subtree is repaired on the next block")
	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].done)
}

// TestRepairOvertakenTips_PendingPassWakesIdleWorker: on an idle chain, with no block and no
// write to wake it, the running setMined worker must still make the pending pass once the
// back-off has passed, and repair a subtree whose reads recovered.
func TestRepairOvertakenTips_PendingPassWakesIdleWorker(t *testing.T) {
	f := newOvertakeFixture(t)
	f.tSettings.BlockValidation.PeriodicProcessingInterval = 20 * time.Millisecond

	reads := f.shortReads(f.subtreeStore, 0, 0)
	f.subtreeStore = reads

	// Registered after the fixture's cleanups, so it runs first: the worker stops before the
	// stores it uses are closed.
	ctx, stop := context.WithCancel(f.ctx)
	bv := f.newRunningBlockValidation(ctx)
	t.Cleanup(func() {
		stop()
		bv.backgroundTasks.Wait()
	})

	require.Eventually(t, func() bool {
		subscribers, err := f.blockchainClient.GetSubscribers(f.ctx)
		return err == nil && slices.Contains(subscribers, blockchain.SubscriberBlockValidation)
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		_, meta, err := f.blockchainClient.GetBlockHeader(f.ctx, f.blockB.Hash())
		return err == nil && meta.MinedSet
	}, 10*time.Second, 10*time.Millisecond, "the setMined worker marks B mined")

	// Exactly enough short reads to set subtree 0 aside, then whole reads.
	reads.short.Store(tipRepairSubtreeReadAttempts * tipRepairMaxAttempts)
	_ = f.overtakeB(t)

	require.Eventually(t, func() bool { return reads.short.Load() <= 0 }, 30*time.Second, 10*time.Millisecond,
		"precondition: the worker read the subtree until it was set aside")

	require.Eventually(t, func() bool { return f.unminedSince(t, f.bOnly) != 0 }, 10*tipRepairPendingBaseBackoff, 20*time.Millisecond,
		"the idle worker repairs the recovered subtree once the back-off has passed")
}

// erroringSubtreeStore fails the next fail reads of one subtree with err.
type erroringSubtreeStore struct {
	blob.Store
	key  []byte
	err  error
	fail atomic.Int32
}

func (s *erroringSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (io.ReadCloser, error) {
	if bytes.Equal(key, s.key) && s.fail.Add(-1) >= 0 {
		return nil, s.err
	}

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}

// erroringReads wraps store so the next n reads of the block's subtree idx fail with err.
func (f *overtakeFixture) erroringReads(store blob.Store, idx int, n int32, err error) *erroringSubtreeStore {
	s := &erroringSubtreeStore{Store: store, key: f.blockB.Subtrees[idx][:], err: err}
	s.fail.Store(n)

	return s
}

// TestRepairOvertakenTips_StorageErrorOverNotFoundRetried: a storage error is the store
// failing while it looked, whatever it wraps, so one over a not-found must not get the
// subtree skipped as gone (the subtreeAbsent rule). The step is retried once the store is
// back, and the subtree is repaired.
func TestRepairOvertakenTips_StorageErrorOverNotFoundRetried(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	storageErr := errors.NewStorageError("store failed while reading", errors.NewNotFoundError("not found"))
	bv.subtreeStore = f.erroringReads(f.subtreeStore, 0, 2*tipRepairSubtreeReadAttempts, storageErr)

	settleTipRepair(t, bv, func() bool { return bv.minedOnChain.snapshot()[*f.blockB.Hash()].done })

	require.NotZero(t, f.unminedSince(t, f.bOnly), "the subtree is repaired once the store is back, not skipped")
}

// TestRepairOvertakenTips_PersistentStoreErrorSetAside: a subtree whose reads keep failing
// with any error, not only a deserialize failure, is set aside after tipRepairMaxAttempts
// failed loads, so the later subtrees are repaired and the job does not start over from the
// first subtree each time it is deferred.
func TestRepairOvertakenTips_PersistentStoreErrorSetAside(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)
	_ = f.fakeTipRepairClock(bv)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	bv.subtreeStore = f.erroringReads(f.subtreeStore, 0, 1<<30, errors.NewStorageError("injected subtree store failure"))

	settleTipRepair(t, bv, func() bool { return f.unminedSince(t, f.later) != 0 })

	require.NotZero(t, f.unminedSince(t, f.later), "the later subtree is repaired")

	b := bv.minedOnChain.snapshot()[*f.blockB.Hash()]
	require.False(t, b.done)
	require.Equal(t, []*chainhash.Hash{f.blockB.Subtrees[0]}, b.pending, "the failing subtree is read again later")
}

// TestRepairOvertakenTips_BackOnChainClearsDeferred: a block whose subtrees were set aside
// and which is back on the chain must not stay deferred. If it leaves again, its new repair
// has nothing to do with the old unreadable subtrees and must not wait behind every other
// block.
func TestRepairOvertakenTips_BackOnChainClearsDeferred(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	bv.subtreeStore = f.shortReads(f.subtreeStore, 0, 1<<30)
	settleTipRepair(t, bv, func() bool { return bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending != nil })
	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].deferred, "precondition: the block is deferred")

	f.extendB(t)

	_, _ = bv.tipRepairSlice(f.ctx, bv.blockchainClient)
	require.False(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].deferred)
}

// warnRecorder records the format of every overtaken-tip repair Warnf; the background
// workers winding down log their own.
type warnRecorder struct {
	ulogger.TestLogger
	mu    sync.Mutex
	warns []string
}

func (l *warnRecorder) Warnf(format string, _ ...interface{}) {
	if !strings.HasPrefix(format, "[repairOvertakenTips]") {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.warns = append(l.warns, format)
}

// TestFinishJob_WarnsOnlyWhenTxsMarked: an unmark job that re-marked none of the block's
// txs, such as one for a fork block the restart seed tracks that was never on the chain, is
// not reported as a block that left the chain after its txs were marked mined.
func TestFinishJob_WarnsOnlyWhenTxsMarked(t *testing.T) {
	f := newOvertakeFixture(t)
	logger := &warnRecorder{}

	ctx, stop := context.WithCancel(f.ctx)
	bv := NewBlockValidation(ctx, logger, f.tSettings, f.blockchainClient, f.subtreeStore, f.txStore, f.store, nil, f.subtreeValidationClient)
	stop()
	bv.backgroundTasks.Wait()

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))

	hash := *f.blockB.Hash()
	b := bv.minedOnChain.snapshot()[hash]

	bv.finishJob(&tipRepairJob{kind: unmarkJob, hash: hash, id: b.id, gen: b.gen, total: 3})
	require.Empty(t, logger.warns, "no warning when no tx was marked")

	bv.finishJob(&tipRepairJob{kind: unmarkJob, hash: hash, id: b.id, gen: b.gen, marked: 2, total: 3})
	require.Len(t, logger.warns, 1, "a warning when txs were marked")

	bv.finishJob(&tipRepairJob{kind: unmarkJob, hash: hash, id: b.id, gen: b.gen, unreadable: f.blockB.Subtrees[:1]})
	require.Len(t, logger.warns, 2, "a warning when subtrees were set aside, though none was marked yet")
}

// blockWithShared returns a block on parent whose one subtree holds shared, with its
// subtree stored, as the competing chain's block that also mines shared.
func (f *overtakeFixture) blockWithShared(t *testing.T, parent *chainhash.Hash, height uint32, tag byte) *model.Block {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, byte(height), 0x00, 0x00, tag, '/', 'T', 'e', 's', 't'})
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("1MUMxUTXcPQ1kAqB7MtJWneeAwVW4cHzzp", 50*100000000))

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*f.shared, 100, 0))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, f.subtreeStore.Set(f.ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	header := mineBlockHeader(t, parent, replicated.RootHash(), f.nBits)

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{subtree.RootHash()}, uint64(subtree.Length()), uint64(coinbaseTx.Size())+100, height, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}

// TestRepairOvertakenTips_ThirdChainRepairsBlockJoinedThroughAChild: B's repair leaves
// shared mined because B', which also holds it, is on the chain. B' got there through its
// child C', so no on-chain setTxMined of B' tracked it. When a third chain overtakes
// B' -> C', shared is in no block on the chain any more, and the repair of B' itself must mark it.
func TestRepairOvertakenTips_ThirdChainRepairsBlockJoinedThroughAChild(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	blockB2 := f.blockWithShared(t, f.tSettings.ChainCfgParams.GenesisHash, 1, 0xb2)
	blockC2 := coinbaseOnlyBlock(t, blockB2.Hash(), f.nBits, 2, 0xc2)
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockB2, "test", blockchainoptions.WithSubtreesSet(true)))
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockC2, "test", blockchainoptions.WithSubtreesSet(true)))
	require.False(t, f.onChain(t, f.blockBID), "precondition: the competing chain overtook B")

	_, metaB2, err := f.blockchainClient.GetBlockHeader(f.ctx, blockB2.Hash())
	require.NoError(t, err)
	f.mineSharedIn(t, metaB2.ID)

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)
	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].done, "precondition: B is repaired")
	require.Zero(t, f.unminedSince(t, f.shared), "precondition: shared is mined in B' on the chain")

	time.Sleep(10 * time.Millisecond)

	parent := f.tSettings.ChainCfgParams.GenesisHash

	for height := uint32(1); height <= 3; height++ {
		block := coinbaseOnlyBlock(t, parent, f.nBits, height, 0xf0+byte(height))
		require.NoError(t, f.blockchainClient.AddBlock(f.ctx, block, "test", blockchainoptions.WithSubtreesSet(true)))
		parent = block.Hash()
	}

	require.False(t, f.onChain(t, metaB2.ID), "precondition: the third chain overtook B'")

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.shared), "shared is in no block on the chain and must look unmined")
}

// TestMinedOnChainTracker_TrackKeepsStamped: a new on-chain write of a block whose txs were
// re-marked not on the longest chain must not forget that they were: if the write fails
// before clearing them, the remine job is the only thing left to mark them mined again.
func TestMinedOnChainTracker_TrackKeepsStamped(t *testing.T) {
	tracker := newMinedOnChainTracker()
	hash := chainhash.HashH([]byte("b"))

	tracker.track(hash, 1, 1)

	gen := tracker.snapshot()[hash].gen
	tracker.update(hash, gen, func(e *minedOnChainBlock) { e.stamped = true })

	tracker.track(hash, 1, 1)
	require.True(t, tracker.snapshot()[hash].stamped)
}

// TestRepairOvertakenTips_ReorgTracksBlocksJoiningThroughAChild: B' reaches the chain only
// through its child C', so no on-chain setTxMined of B' tracks it. The reorg that brings it
// in must, so that when B' leaves, its own repair marks the txs other blocks' repairs left
// mined because B' was on the chain, whether those repairs finished, are still running, or
// wait on subtrees set aside.
func TestRepairOvertakenTips_ReorgTracksBlocksJoiningThroughAChild(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)

	blockC2 := f.overtakeB(t)

	_, err = bv.tipRepairSlice(f.ctx, bv.blockchainClient)
	require.NoError(t, err)

	_, tracked := bv.minedOnChain.snapshot()[*blockC2.Header.HashPrevBlock]
	require.True(t, tracked, "B' is tracked once the reorg brings it onto the chain")
}

// TestRepairOvertakenTips_BackOffFromPassEnd: a pass over a subtree that keeps failing spans
// several blocks. The next pass is due a block after the pass ended, not after it started,
// or it starts again at once and a hopeless subtree holds the repair's one job slot.
func TestRepairOvertakenTips_BackOffFromPassEnd(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)
	_ = f.fakeTipRepairClock(bv)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	blockC2 := f.overtakeB(t)

	store := f.shortReads(f.subtreeStore, 0, 1<<30)
	bv.subtreeStore = store

	_, err := drainTipRepair(t, bv)
	require.Error(t, err, "precondition: the pass is under way, failing")

	f.extendWinning(t, blockC2)

	settleTipRepair(t, bv, func() bool {
		return bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending != nil && bv.tipRepairJob == nil
	})
	require.NotNil(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending, "precondition: the subtree is set aside")

	require.Equal(t, 1, bv.minedOnChain.snapshot()[*f.blockB.Hash()].passes,
		"the block that arrived during the pass does not start another one as soon as it ends")

	left := store.short.Load()

	for i := 0; i < 5; i++ {
		_, _ = drainTipRepair(t, bv)
	}

	require.Equal(t, left, store.short.Load(), "no new pass before a block arrives after the last one ended")
}

// TestRepairOvertakenTips_RemineSetsAsideKeepsStamped: a remine job whose subtree keeps
// failing to load sets it aside and re-mines the later subtrees, but the block stays
// stamped: the set-aside subtree's re-mine is still owed, and a later pass, once its reads
// recover, does it.
func TestRepairOvertakenTips_RemineSetsAsideKeepsStamped(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)
	clock := f.fakeTipRepairClock(bv)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].stamped, "precondition: B's txs were re-marked")
	require.NotZero(t, f.unminedSince(t, f.bOnly), "precondition")
	require.NotZero(t, f.unminedSince(t, f.later), "precondition")

	f.extendB(t)

	store := f.erroringReads(f.subtreeStore, 0, 1<<30, errors.NewStorageError("injected subtree store failure"))
	bv.subtreeStore = store

	for i := 0; i <= tipRepairMaxAttempts; i++ {
		_, _ = drainTipRepair(t, bv)
	}

	require.Zero(t, f.unminedSince(t, f.later), "the later subtree is re-mined past the unreadable one")
	require.NotZero(t, f.unminedSince(t, f.bOnly), "precondition: subtree 0 is not re-mined yet")
	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].stamped, "the re-mine of subtree 0 is still owed")

	store.fail.Store(0)
	clock.advance(tipRepairPendingMaxBackoff)

	_, err = drainTipRepair(t, bv)
	require.NoError(t, err)
	require.Zero(t, f.unminedSince(t, f.bOnly), "subtree 0 is re-mined once it reads")
	require.False(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].stamped)
}

// failHeadersOnceClient fails the next GetBlockHeaders call once armed.
type failHeadersOnceClient struct {
	blockchain.ClientI
	fail atomic.Bool
}

func (c *failHeadersOnceClient) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	if c.fail.Swap(false) {
		return nil, nil, errors.NewServiceError("injected GetBlockHeaders failure")
	}

	return c.ClientI.GetBlockHeaders(ctx, hash, n)
}

// TestRepairOvertakenTips_ReorgTrackingSurvivesFailedRead: a failed read on the slice that
// first sees the reorg must not lose it: the next slice still tracks B', which reached the
// chain through its child.
func TestRepairOvertakenTips_ReorgTrackingSurvivesFailedRead(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)
	client := &failHeadersOnceClient{ClientI: f.blockchainClient}

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_, err := bv.tipRepairSlice(f.ctx, client)
	require.NoError(t, err)

	blockC2 := f.overtakeB(t)

	client.fail.Store(true)
	_, err = bv.tipRepairSlice(f.ctx, client)
	require.Error(t, err, "precondition: the slice that sees the reorg fails")

	_, err = bv.tipRepairSlice(f.ctx, client)
	require.NoError(t, err)

	_, tracked := bv.minedOnChain.snapshot()[*blockC2.Header.HashPrevBlock]
	require.True(t, tracked, "B' is tracked on the retried slice")
}

// TestMinedOnChainTracker_OffChainWriteKeepsDone: a write as off the longest chain clears no
// tx, so a finished or backed-off repair is not opened again by it.
func TestMinedOnChainTracker_OffChainWriteKeepsDone(t *testing.T) {
	tracker := newMinedOnChainTracker()
	hash := chainhash.HashH([]byte("b"))
	subtree := chainhash.HashH([]byte("s"))

	tracker.track(hash, 1, 1)
	tracker.update(hash, tracker.snapshot()[hash].gen, func(e *minedOnChainBlock) {
		e.done = true
		e.pending = []*chainhash.Hash{&subtree}
	})

	require.True(t, tracker.markWritten(hash, false))

	b := tracker.snapshot()[hash]
	require.True(t, b.done)
	require.NotNil(t, b.pending)
}

// TestMinedOnChainTracker_OnChainWriteClearsStamped: a completed write of the block as on
// the longest chain cleared every one of its txs, so no remine is owed. A write as off the
// chain cleared none.
func TestMinedOnChainTracker_OnChainWriteClearsStamped(t *testing.T) {
	tracker := newMinedOnChainTracker()
	hash := chainhash.HashH([]byte("b"))

	tracker.track(hash, 1, 1)
	tracker.update(hash, tracker.snapshot()[hash].gen, func(e *minedOnChainBlock) { e.stamped = true })

	require.True(t, tracker.markWritten(hash, false))
	require.True(t, tracker.snapshot()[hash].stamped, "an off-chain write leaves the remine owed")

	require.True(t, tracker.markWritten(hash, true))
	require.False(t, tracker.snapshot()[hash].stamped, "an on-chain write did the remine's work")
}

// emptyHeadersClient returns no headers from GetBlockHeaders.
type emptyHeadersClient struct {
	blockchain.ClientI
}

func (c *emptyHeadersClient) GetBlockHeaders(context.Context, *chainhash.Hash, uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
}

// TestMainChainIDs_EmptyIsAnError: no headers for the best block is a failed read, not a
// chain with no blocks on it; taken as one, every tracked block would look off the chain.
func TestMainChainIDs_EmptyIsAnError(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	_, _, err := bv.mainChainIDsLocked(f.ctx, &emptyHeadersClient{ClientI: f.blockchainClient})
	require.Error(t, err)
	require.Nil(t, bv.tipRepairMainChain, "nothing is cached")
}

// TestRepairOvertakenTips_OnChainClearsDeferred: a block deferred after its job kept failing
// that is on the chain with no remine owed is not deferred any more, so if it leaves the
// chain its repair does not wait behind every other block.
func TestRepairOvertakenTips_OnChainClearsDeferred(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))

	hash := *f.blockB.Hash()
	bv.minedOnChain.update(hash, bv.minedOnChain.snapshot()[hash].gen, func(e *minedOnChainBlock) { e.deferred = true })

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.False(t, bv.minedOnChain.snapshot()[hash].deferred)
}

// TestRepairOvertakenTips_CleanPassEndsBackOff: once a pass reads the subtrees set aside and
// the block is done, nothing is owed: no pending pass may be reported, or the worker's wake
// timer fires at once on every slice while the block stays off the chain.
func TestRepairOvertakenTips_CleanPassEndsBackOff(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)
	clock := f.fakeTipRepairClock(bv)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	bv.subtreeStore = f.shortReads(f.subtreeStore, 0, tipRepairSubtreeReadAttempts*tipRepairMaxAttempts)

	settleTipRepair(t, bv, func() bool { return bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending != nil })
	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].stamped, "precondition: the first pass re-marked txs")

	clock.advance(tipRepairPendingBaseBackoff)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].done, "precondition: the clean pass finished the block")

	_, ok := bv.minedOnChain.nextPendingPass()
	require.False(t, ok, "no pass is owed by a done block off the chain")
}

// minedSetProbe runs at on each SetBlockMinedSet of block, before the write: the moment the
// pruner may start processing that height. calls counts the runs of at that have returned.
type minedSetProbe struct {
	blockchain.ClientI
	block chainhash.Hash
	at    func()
	calls atomic.Int32
}

func (c *minedSetProbe) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	if *blockHash == c.block {
		c.at()
		c.calls.Add(1)
	}

	return c.ClientI.SetBlockMinedSet(ctx, blockHash)
}

// heldBlocks returns the blocks the pruning hold in the blockchain state lists.
func (f *overtakeFixture) heldBlocks(t *testing.T) []chainhash.Hash {
	t.Helper()

	data, err := f.blockchainClient.GetState(f.ctx, model.OvertakenTipRepairHoldKey)
	if err != nil {
		return nil
	}

	held, err := model.DecodeOvertakenTipRepairHold(data)
	require.NoError(t, err)

	hashes := make([]chainhash.Hash, 0, len(held))
	for _, b := range held {
		hashes = append(hashes, b.Hash)
	}

	return hashes
}

// TestSetTxMinedStatus_HoldsOvertakenBlockBeforeMinedSet: the pruner processes a height only
// once its block has mined_set. So the setTxMined of the block that overtook B must put B in
// the pruning hold before it sets mined_set, or the pruner can delete a parent of a tx of B
// that still looks mined. It must not wait for B's repair to do so.
func TestSetTxMinedStatus_HoldsOvertakenBlockBeforeMinedSet(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	blockC2 := f.overtakeB(t)

	var heldAtMinedSet []chainhash.Hash

	probe := &minedSetProbe{ClientI: f.blockchainClient, block: *blockC2.Hash(), at: func() { heldAtMinedSet = f.heldBlocks(t) }}
	bv.blockchainClient = probe

	require.NoError(t, bv.setTxMinedStatus(f.ctx, blockC2.Hash()))
	require.Equal(t, int32(1), probe.calls.Load(), "precondition: C' was set mined")
	require.Equal(t, []chainhash.Hash{*f.blockB.Hash()}, heldAtMinedSet, "B is held before C' gets mined_set")
	require.Zero(t, f.unminedSince(t, f.bOnly), "the hold does not wait for B's repair")

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "precondition: B is repaired")
	require.Empty(t, f.heldBlocks(t), "the hold is released once B is repaired")
}

// TestRepairOvertakenTips_SetAsideSubtreeHoldsPruning: a subtree that cannot be read cannot
// be repaired, and the parents of its txs cannot be named. The hold stays while it is set
// aside, across new blocks, and is released only once the subtree reads and is repaired.
func TestRepairOvertakenTips_SetAsideSubtreeHoldsPruning(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)
	clock := f.fakeTipRepairClock(bv)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	blockC2 := f.overtakeB(t)

	store := f.shortReads(f.subtreeStore, 0, 1<<30)
	bv.subtreeStore = store

	settleTipRepair(t, bv, func() bool { return bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending != nil })
	require.NotNil(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending, "precondition: subtree 0 is set aside")
	require.NotZero(t, f.unminedSince(t, f.later), "precondition: the readable subtree is repaired")
	require.Equal(t, []chainhash.Hash{*f.blockB.Hash()}, f.heldBlocks(t), "a block with a subtree set aside is held")

	time.Sleep(10 * time.Millisecond)

	blockD2 := coinbaseOnlyBlock(t, blockC2.Hash(), f.nBits, 3, 0xd4)
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockD2, "test", blockchainoptions.WithSubtreesSet(true)))
	require.NoError(t, bv.setTxMinedStatus(f.ctx, blockD2.Hash()), "a new block is set mined while the subtree stays unreadable")

	settleTipRepair(t, bv, func() bool { return false })
	require.Equal(t, []chainhash.Hash{*f.blockB.Hash()}, f.heldBlocks(t), "still held after a new block and more passes")

	store.short.Store(0)
	clock.advance(tipRepairPendingMaxBackoff)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "the subtree is repaired once it reads")
	require.Empty(t, f.heldBlocks(t), "and the hold is released")
}

// cappedHeadersClient rejects header and block-summary requests over the configured caps,
// as the blockchain gRPC server does; LocalClient enforces neither.
type cappedHeadersClient struct {
	blockchain.ClientI
	maxHeaders, maxBlocks uint64
}

func (c *cappedHeadersClient) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	if n > c.maxHeaders {
		return nil, nil, errors.NewInvalidArgumentError("[Blockchain][GetBlockHeaders] %d headers requested, maximum is %d", n, c.maxHeaders)
	}

	return c.ClientI.GetBlockHeaders(ctx, hash, n)
}

func (c *cappedHeadersClient) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	if uint64(n) > c.maxBlocks { //nolint:gosec // test
		return nil, errors.NewInvalidArgumentError("[Blockchain][GetLastNBlocks] invalid count %d, maximum is %d", n, c.maxBlocks)
	}

	return c.ClientI.GetLastNBlocks(ctx, n, includeOrphans, fromHeight)
}

// TestRepairOvertakenTips_ReadsWithinConfiguredCaps: blockchain_maxBlockHeadersPerRequest and
// blockchain_maxBlocksByHeightRange may be set below the repair window. The restart seed and
// the main-chain read must page within them, not fail every slice.
func TestRepairOvertakenTips_ReadsWithinConfiguredCaps(t *testing.T) {
	f := newOvertakeFixture(t)
	f.tSettings.BlockChain.MaxBlockHeadersPerRequest = 1
	f.tSettings.BlockChain.MaxBlocksByHeightRange = 1

	bv := f.newBlockValidation(t)
	client := &cappedHeadersClient{ClientI: f.blockchainClient, maxHeaders: 1, maxBlocks: 1}

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	blockC2 := f.overtakeB(t)

	// The seed runs in the first slice, then the main-chain read.
	bv.tipRepairSeeded = false

	_, err := bv.tipRepairSlice(f.ctx, client)
	require.NoError(t, err)

	_, tracked := bv.minedOnChain.snapshot()[*blockC2.Header.HashPrevBlock]
	require.True(t, tracked, "B' is tracked from a main-chain read paged within the cap")

	for i := 0; i < 20; i++ {
		if more, err := bv.tipRepairSlice(f.ctx, client); err != nil || !more {
			require.NoError(t, err)
			break
		}
	}

	require.NotZero(t, f.unminedSince(t, f.bOnly), "B is repaired with every read within the caps")
}

// TestRepairOvertakenTips_SetAsideSubtreesGauge: a subtree set aside cannot be repaired, and
// its txs' parents cannot be protected, until it reads again. Operators see how many there
// are, and the count drops once they are read.
func TestRepairOvertakenTips_SetAsideSubtreesGauge(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)
	clock := f.fakeTipRepairClock(bv)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	store := f.shortReads(f.subtreeStore, 0, 1<<30)
	bv.subtreeStore = store

	settleTipRepair(t, bv, func() bool { return bv.minedOnChain.snapshot()[*f.blockB.Hash()].pending != nil })
	require.InDelta(t, 1, testutil.ToFloat64(prometheusBlockValidationTipRepairSubtreesSetAside), 0)

	store.short.Store(0)
	clock.advance(tipRepairPendingMaxBackoff)

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)
	require.InDelta(t, 0, testutil.ToFloat64(prometheusBlockValidationTipRepairSubtreesSetAside), 0)
}

// TestRepairOvertakenTips_SeedPagesForkBlocksWithinCap: the restart seed finds fork blocks
// near the tip with GetLastNBlocks, which blockchain_maxBlocksByHeightRange may cap below the
// seed's reach. It must page down past full pages of blocks at one height, not stop there:
// with a cap of 2, the fork blocks X at height 2 and Y at height 1 each sit below a full page.
func TestRepairOvertakenTips_SeedPagesForkBlocksWithinCap(t *testing.T) {
	f := newOvertakeFixture(t)
	f.tSettings.BlockChain.MaxBlocksByHeightRange = 2

	bv := f.newBlockValidation(t)
	client := &cappedHeadersClient{ClientI: f.blockchainClient, maxHeaders: tipRepairWindow, maxBlocks: 2}

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))

	time.Sleep(10 * time.Millisecond)

	// genesis - B - D - E, with forks D - F, B - X and genesis - Y.
	blockY := coinbaseOnlyBlock(t, f.tSettings.ChainCfgParams.GenesisHash, f.nBits, 1, 0xb0)
	blockD := coinbaseOnlyBlock(t, f.blockB.Hash(), f.nBits, 2, 0xd0)
	blockX := coinbaseOnlyBlock(t, f.blockB.Hash(), f.nBits, 2, 0xa0)
	blockE := coinbaseOnlyBlock(t, blockD.Hash(), f.nBits, 3, 0xe0)
	blockF := coinbaseOnlyBlock(t, blockD.Hash(), f.nBits, 3, 0xf0)

	for _, block := range []*model.Block{blockY, blockD, blockX, blockE, blockF} {
		require.NoError(t, f.blockchainClient.AddBlock(f.ctx, block, "test", blockchainoptions.WithSubtreesSet(true)))
	}

	bv.tipRepairSeeded = false

	_, err := bv.tipRepairSlice(f.ctx, client)
	require.NoError(t, err)

	_, tracked := bv.minedOnChain.snapshot()[*blockX.Hash()]
	require.True(t, tracked, "fork block X, below a full page at height 3, is seeded")

	_, tracked = bv.minedOnChain.snapshot()[*blockY.Hash()]
	require.True(t, tracked, "fork block Y, below a full page at height 2, is seeded")
}

// TestSetTxMinedStatus_FailingRepairDoesNotStopHealthyBlocks: a repair that keeps failing
// holds pruning, not the chain. A healthy new block is still set mined, so its children pass
// the parent-mined check, while the overtaken block stays held.
func TestSetTxMinedStatus_FailingRepairDoesNotStopHealthyBlocks(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	blockC2 := f.overtakeB(t)

	// Every attempt to re-mark B's txs fails, as a store fault that does not clear would.
	f.store.failMarkFalse.Store(1 << 30)

	settleTipRepair(t, bv, func() bool { return false })
	require.Zero(t, f.unminedSince(t, f.bOnly), "precondition: B's repair keeps failing")

	time.Sleep(10 * time.Millisecond)

	blockD2 := coinbaseOnlyBlock(t, blockC2.Hash(), f.nBits, 3, 0xd4)
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, blockD2, "test", blockchainoptions.WithSubtreesSet(true)))

	require.NoError(t, bv.setTxMinedStatus(f.ctx, blockD2.Hash()))

	_, meta, err := f.blockchainClient.GetBlockHeader(f.ctx, blockD2.Hash())
	require.NoError(t, err)
	require.True(t, meta.MinedSet, "the healthy block is set mined")
	require.Equal(t, []chainhash.Hash{*f.blockB.Hash()}, f.heldBlocks(t), "B stays held while its repair fails")
}

// TestRepairOvertakenTips_HoldSurvivesRestart: the blocks held when the process stops are read
// back from the hold when it starts again, so their repair finishes even when they are too
// deep below the tip for the seed to find, and the hold is not released before.
func TestRepairOvertakenTips_HoldSurvivesRestart(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	blockC2 := f.overtakeB(t)

	f.store.failMarkFalse.Store(1 << 30)
	settleTipRepair(t, bv, func() bool { return false })
	require.Equal(t, []chainhash.Hash{*f.blockB.Hash()}, f.heldBlocks(t), "precondition: B is held")

	// The winning chain grows past the seed's fork depth while the process is down.
	time.Sleep(10 * time.Millisecond)

	tip := blockC2
	for height := uint32(3); height <= 3+tipRepairSeedForkDepth; height++ {
		next := coinbaseOnlyBlock(t, tip.Hash(), f.nBits, height, 0xe0)
		require.NoError(t, f.blockchainClient.AddBlock(f.ctx, next, "test", blockchainoptions.WithSubtreesSet(true)))
		tip = next
	}

	f.store.failMarkFalse.Store(0)

	restarted := f.newBlockValidation(t)
	require.NoError(t, restarted.setTxMinedStatus(f.ctx, tip.Hash()))

	_, tracked := restarted.minedOnChain.snapshot()[*f.blockB.Hash()]
	require.True(t, tracked, "B is tracked again from the hold")
	require.Equal(t, []chainhash.Hash{*f.blockB.Hash()}, f.heldBlocks(t), "the hold is not released before B is repaired")

	_, err := drainTipRepair(t, restarted)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "B is repaired after the restart")
	require.Empty(t, f.heldBlocks(t), "and the hold is released")
}

// TestRepairOvertakenTips_GoneSubtreeGivenUp: a subtree of an overtaken block that is gone
// from the store can never be read again, so nothing can name its txs: holding pruning for it
// would only delay the same loss. It is given up as a gone block is, counted so operators see
// that some txs may stay marked mined, and the rest of the block is repaired and released.
func TestRepairOvertakenTips_GoneSubtreeGivenUp(t *testing.T) {
	f := newOvertakeFixtureWithLaterSubtree(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	bv.subtreeStore = f.erroringReads(f.subtreeStore, 0, 1<<30, errors.NewNotFoundError("subtree is gone"))

	before := testutil.ToFloat64(prometheusBlockValidationTipRepairAbandoned.WithLabelValues("subtree"))

	_, err := drainTipRepair(t, bv)
	require.NoError(t, err)

	require.True(t, bv.minedOnChain.snapshot()[*f.blockB.Hash()].done, "the block is done without the gone subtree")
	require.NotZero(t, f.unminedSince(t, f.later), "the readable subtree is repaired")
	require.Empty(t, f.heldBlocks(t), "a gone subtree does not hold pruning")
	require.InDelta(t, before+1, testutil.ToFloat64(prometheusBlockValidationTipRepairAbandoned.WithLabelValues("subtree")), 0, "and is counted")
}

// TestRepairOvertakenTips_HoldReadBackBeforeAnyOnChainWrite: after a restart, the blocks a
// hold recorded are read back on the first setTxMined, even when that block is off the chain
// and tracks nothing, so their repair, and the release of the hold, does not wait for the
// first block written on the chain.
func TestRepairOvertakenTips_HoldReadBackBeforeAnyOnChainWrite(t *testing.T) {
	f := newOvertakeFixture(t)
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))
	_ = f.overtakeB(t)

	f.store.failMarkFalse.Store(1 << 30)
	settleTipRepair(t, bv, func() bool { return false })
	require.Equal(t, []chainhash.Hash{*f.blockB.Hash()}, f.heldBlocks(t), "precondition: B is held")

	f.store.failMarkFalse.Store(0)

	time.Sleep(10 * time.Millisecond)

	fork := coinbaseOnlyBlock(t, f.tSettings.ChainCfgParams.GenesisHash, f.nBits, 1, 0xf7)
	require.NoError(t, f.blockchainClient.AddBlock(f.ctx, fork, "test", blockchainoptions.WithSubtreesSet(true)))

	restarted := f.newBlockValidation(t)
	require.NoError(t, restarted.setTxMinedStatus(f.ctx, fork.Hash()))

	_, tracked := restarted.minedOnChain.snapshot()[*f.blockB.Hash()]
	require.True(t, tracked, "B is tracked again from the hold")

	_, err := drainTipRepair(t, restarted)
	require.NoError(t, err)
	require.NotZero(t, f.unminedSince(t, f.bOnly), "B is repaired")
	require.Empty(t, f.heldBlocks(t), "and the hold is released")
}

// wireStateClient sends SetState through a protobuf round trip, as the gRPC blockchain client
// does, before the store sees it: empty bytes arrive as nil.
type wireStateClient struct {
	blockchain.ClientI
}

func (c *wireStateClient) SetState(ctx context.Context, key string, data []byte) error {
	wire, err := proto.Marshal(&blockchain_api.SetStateRequest{Key: key, Data: data})
	if err != nil {
		return err
	}

	var req blockchain_api.SetStateRequest
	if err = proto.Unmarshal(wire, &req); err != nil {
		return err
	}

	return c.ClientI.SetState(ctx, req.Key, req.Data)
}

// TestSetTxMinedStatus_EmptyHoldSurvivesTheWire: the hold with nothing in it is still written
// through the gRPC blockchain client, where empty bytes arrive as nil and the SQL state table
// refuses a NULL. A hold that cannot be written stops every block from being set mined.
func TestSetTxMinedStatus_EmptyHoldSurvivesTheWire(t *testing.T) {
	f := newOvertakeFixture(t)
	f.blockchainClient = &wireStateClient{ClientI: f.blockchainClient}
	bv := f.newBlockValidation(t)

	require.NoError(t, bv.setTxMinedStatus(f.ctx, f.blockB.Hash()))

	_, meta, err := f.blockchainClient.GetBlockHeader(f.ctx, f.blockB.Hash())
	require.NoError(t, err)
	require.True(t, meta.MinedSet)
	require.Empty(t, f.heldBlocks(t))
}
