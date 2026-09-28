package blockassembly

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin the heartbeat beats inside a reset and inside the
// unmined reload that ends every reset (issue 1447). A reset runs inside one
// pass of the main select, so without them the heartbeat ages by the whole
// reset, and a probe kill part-way through restarts straight into the same
// reset.
//
// Each test observes the heartbeat at a call that sits between two steps and
// then backdates it, so the next observation shows only the beats that ran in
// between. That is what lets each test name the one beat it depends on:
// deleting that beat leaves the next observation an hour stale.

const probeStaleBy = time.Hour

// heartbeatProbe records the heartbeat's age at each observation and then
// backdates it past any plausible stall timeout.
type heartbeatProbe struct {
	mu   sync.Mutex
	ba   *BlockAssembler
	ages []time.Duration
}

func (p *heartbeatProbe) observe() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ages = append(p.ages, p.ba.heartbeat.Age())
	p.ba.heartbeat.SetLastBeatForTest(time.Now().Add(-probeStaleBy))
}

// backdate stales the heartbeat without recording. It also arms it:
// BeatIfStarted is a no-op until the loop owns the heartbeat.
func (p *heartbeatProbe) backdate() {
	p.ba.heartbeat.SetLastBeatForTest(time.Now().Add(-probeStaleBy))
}

func (p *heartbeatProbe) seen() []time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]time.Duration(nil), p.ages...)
}

// probeSubtreeStore observes every subtree read, which in reset is the one
// store call per block.
type probeSubtreeStore struct {
	blob.Store
	onRead func()
}

func (s *probeSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if fileType == fileformat.FileTypeSubtree {
		s.onRead()
	}

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}

// probeUtxoStore observes the moved-back unmined mark and can stand in a
// synthetic unmined iterator.
type probeUtxoStore struct {
	utxo.Store
	onMarkUnmined func()
	iterator      utxo.UnminedTxIterator
}

func (s *probeUtxoStore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	if !onLongestChain && s.onMarkUnmined != nil {
		s.onMarkUnmined()
	}

	return s.Store.MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain)
}

func (s *probeUtxoStore) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	if s.iterator != nil {
		return s.iterator, nil
	}

	return s.Store.GetUnminedTxIterator()
}

// probeIterator serves fixed batches and observes each Next.
type probeIterator struct {
	batches [][]*utxo.UnminedTransaction
	onNext  func()
}

func (it *probeIterator) Next(context.Context) ([]*utxo.UnminedTransaction, error) {
	if it.onNext != nil {
		it.onNext()
	}

	if len(it.batches) == 0 {
		return nil, nil
	}

	batch := it.batches[0]
	it.batches = it.batches[1:]

	return batch, nil
}

func (it *probeIterator) Err() error   { return nil }
func (it *probeIterator) Close() error { return nil }

func syntheticUnmined(n int) []*utxo.UnminedTransaction {
	txs := make([]*utxo.UnminedTransaction, n)

	for i := range txs {
		var h chainhash.Hash
		h[0], h[1], h[2], h[3] = byte(i), byte(i>>8), byte(i>>16), 0xa5

		txs[i] = &utxo.UnminedTransaction{
			Node:       &subtree.Node{Hash: h, Fee: 1, SizeInBytes: 250},
			TxInpoints: &subtree.TxInpoints{},
			CreatedAt:  i,
		}
	}

	return txs
}

func seedSubtree(t *testing.T, items *baTestItems, txHash chainhash.Hash) *chainhash.Hash {
	t.Helper()

	st, err := subtree.NewTreeByLeafCount(64)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddSubtreeNode(subtree.Node{Hash: txHash, Fee: 100, SizeInBytes: 250}))

	stBytes, err := st.Serialize()
	require.NoError(t, err)

	rootHash := st.RootHash()
	require.NoError(t, items.blobStore.Set(t.Context(), rootHash[:], fileformat.FileTypeSubtree, stBytes))
	require.NoError(t, items.blobStore.Set(t.Context(), rootHash[:], fileformat.FileTypeSubtreeMeta, []byte{}))

	return rootHash
}

// TestLivenessResetBeatsPerBlock drives the real BlockAssembler.reset through a
// fork, the same shape as TestReset_MoveForwardGetSubtreesFailure_PreservesMined:
// genesis, h1, then main h2A, h3A and side h2B, h3B, with the assembler parked
// on the side tip and h3B invalidated. Reset then reads subtrees for moveForward
// [h2A, h3A] and moveBack [h3B, h2B] and marks the moved-back transaction
// unmined.
//
// Three beats are pinned, one per assertion:
//   - the moveForward loop: the second moveForward read
//   - the moveBack loop: the moveBack read
//   - the beat before the unmined mark: the mark itself
func TestLivenessResetBeatsPerBlock(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	ctx := t.Context()
	ba := items.blockAssembler

	ba.settings.BlockValidation.IsParentMinedRetryMaxRetry = 1

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{})
	mockStp.On("GetCurrentBlockHeader").Return(model.GenesisBlockHeader)
	injectMockStp(t, items, mockStp)

	reads := &heartbeatProbe{ba: ba}
	marks := &heartbeatProbe{ba: ba}

	ba.subtreeStore = &probeSubtreeStore{Store: items.blobStore, onRead: reads.observe}
	ba.utxoStore = &probeUtxoStore{Store: items.utxoStore, onMarkUnmined: marks.observe}

	txMovedBack := chainhash.HashH([]byte("moved back only"))
	subtreeB := seedSubtree(t, items, txMovedBack)
	subtreeA2 := seedSubtree(t, items, chainhash.HashH([]byte("main h2A")))
	subtreeA3 := seedSubtree(t, items, chainhash.HashH([]byte("main h3A")))

	require.NoError(t, items.addBlock(ctx, blockHeader1))
	h1 := blockHeader1

	h2A := &model.BlockHeader{Version: 1, HashPrevBlock: h1.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 22, Bits: *bits}
	h3A := &model.BlockHeader{Version: 1, HashPrevBlock: h2A.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 23, Bits: *bits}
	h2B := &model.BlockHeader{Version: 1, HashPrevBlock: h1.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 32, Bits: *bits}
	h3B := &model.BlockHeader{Version: 1, HashPrevBlock: h2B.Hash(), HashMerkleRoot: &chainhash.Hash{}, Nonce: 33, Bits: *bits}

	coinbaseTx, _ := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")

	for _, blk := range []*model.Block{
		{Header: h2A, CoinbaseTx: coinbaseTx, TransactionCount: 1, Subtrees: []*chainhash.Hash{subtreeA2}},
		{Header: h3A, CoinbaseTx: coinbaseTx, TransactionCount: 1, Subtrees: []*chainhash.Hash{subtreeA3}},
		{Header: h2B, CoinbaseTx: coinbaseTx, TransactionCount: 1, Subtrees: []*chainhash.Hash{subtreeB}},
		{Header: h3B, CoinbaseTx: coinbaseTx, TransactionCount: 1, Subtrees: []*chainhash.Hash{}},
	} {
		require.NoError(t, items.blockchainClient.AddBlock(ctx, blk, ""))
	}

	ba.setBestBlockHeader(h3B, 3)

	_, err := items.blockchainClient.InvalidateBlock(ctx, h3B.Hash())
	require.NoError(t, err)

	reads.backdate()

	require.NoError(t, ba.reset(ctx))

	// The fixture must have taken the path under test: three subtree reads (h2A,
	// h3A, h2B; h3B is invalid and skipped) and one unmined mark.
	readAges := reads.seen()
	require.Len(t, readAges, 3, "reset must read subtrees for h2A, h3A and h2B")
	require.Len(t, marks.seen(), 1, "reset must mark the moved-back transaction unmined")

	// readAges[0] follows earlier beats (the block fetch, the mined_set wait) and
	// pins nothing on its own.
	require.Less(t, readAges[1], time.Minute, "the moveForward subtree loop must beat per block")
	require.Less(t, readAges[2], time.Minute, "the moveBack subtree loop must beat per block")
	require.Less(t, marks.seen()[0], time.Minute, "reset must beat before marking moved-back transactions unmined")
}

// newReloadTest builds an assembler whose unmined reload reads a synthetic
// iterator and hands its result to a mocked subtree processor, so each batch
// boundary is observable.
func newReloadTest(t *testing.T, txs []*utxo.UnminedTransaction, perBatch int) (*BlockAssembler, *heartbeatProbe, *subtreeprocessor.MockSubtreeProcessor) {
	t.Helper()

	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	ba.settings.BlockAssembly.OnRestartValidateParentChain = false

	probe := &heartbeatProbe{ba: ba}

	var batches [][]*utxo.UnminedTransaction
	for start := 0; start < len(txs); start += perBatch {
		batches = append(batches, txs[start:min(start+perBatch, len(txs))])
	}

	ba.utxoStore = &probeUtxoStore{
		Store:    items.utxoStore,
		iterator: &probeIterator{batches: batches, onNext: probe.observe},
	}

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	injectMockStp(t, items, mockStp)

	probe.backdate()

	return ba, probe, mockStp
}

// TestLivenessUnminedReloadBeatsPerIteratorBatch pins the per-batch beat while
// reading the unmined set, which scales with the mempool. Every Next after the
// first proves the previous batch was handed to the workers.
func TestLivenessUnminedReloadBeatsPerIteratorBatch(t *testing.T) {
	ba, probe, mockStp := newReloadTest(t, syntheticUnmined(3), 1)
	mockStp.On("AddNodesDirectly", mock.Anything, mock.Anything).Return(nil)

	require.NoError(t, ba.loadUnminedTransactions(t.Context(), false))

	ages := probe.seen()
	require.Len(t, ages, 4, "three batches and the empty read that ends them")

	for i, age := range ages {
		require.Less(t, age, time.Minute, "iterator read %d must follow a beat", i)
	}
}

// TestLivenessUnminedReloadBeatsPerAddBatch pins the beat per batch handed to
// the subtree processor. The iterator's last read backdates the heartbeat, so
// the first add is stale unless the add loop beats.
func TestLivenessUnminedReloadBeatsPerAddBatch(t *testing.T) {
	ba, probe, mockStp := newReloadTest(t, syntheticUnmined(3), 3)
	ba.settings.BlockAssembly.UnminedLoadingBatchSize = 1

	adds := &heartbeatProbe{ba: ba}
	mockStp.On("AddNodesDirectly", mock.Anything, mock.Anything).Run(func(mock.Arguments) { adds.observe() }).Return(nil)

	require.NoError(t, ba.loadUnminedTransactions(t.Context(), false))
	require.NotEmpty(t, probe.seen())

	ages := adds.seen()
	require.Len(t, ages, 3, "one add per transaction at a batch size of 1")

	for i, age := range ages {
		require.Less(t, age, time.Minute, "add batch %d must follow a beat", i)
	}
}

// addDirectlyProbe observes the heartbeat on the first AddDirectly (to backdate
// it) and on the one right after a 10,000-transaction boundary.
func addDirectlyProbe(mockStp *subtreeprocessor.MockSubtreeProcessor, adds *heartbeatProbe) {
	var calls int

	mockStp.On("AddDirectly", mock.Anything, mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		calls++

		switch calls {
		case 1:
			adds.backdate()
		case 10_001:
			adds.observe()
		}
	}).Return(nil)
}

// TestLivenessUnminedReloadBeatsInSequentialAdd pins the beat every 10,000
// transactions when the reload adds one transaction at a time
// (blockassembly_unminedLoadingBatchSize 0).
func TestLivenessUnminedReloadBeatsInSequentialAdd(t *testing.T) {
	ba, _, mockStp := newReloadTest(t, syntheticUnmined(10_001), 5_000)
	ba.settings.BlockAssembly.UnminedLoadingBatchSize = 0

	adds := &heartbeatProbe{ba: ba}
	addDirectlyProbe(mockStp, adds)

	require.NoError(t, ba.loadUnminedTransactions(t.Context(), false))

	ages := adds.seen()
	require.Len(t, ages, 1)
	require.Less(t, ages[0], time.Minute, "the sequential add must beat every 10,000 transactions")
}

// TestLivenessDiskSortReloadBeats pins the same two beats on the disk-sort
// reload path: one per iterator batch, and one every 10,000 transactions read
// back and added.
func TestLivenessDiskSortReloadBeats(t *testing.T) {
	ba, probe, mockStp := newReloadTest(t, syntheticUnmined(10_001), 5_000)
	ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
	ba.settings.BlockAssembly.UnminedTxDiskSortPath = t.TempDir()

	adds := &heartbeatProbe{ba: ba}
	addDirectlyProbe(mockStp, adds)

	require.NoError(t, ba.loadUnminedTransactions(t.Context(), false))

	reads := probe.seen()
	require.Len(t, reads, 4, "three batches and the empty read that ends them")

	for i, age := range reads {
		require.Less(t, age, time.Minute, "disk-sort iterator read %d must follow a beat", i)
	}

	ages := adds.seen()
	require.Len(t, ages, 1)
	require.Less(t, ages[0], time.Minute, "the disk-sort read-back must beat every 10,000 transactions")
}

// TestLivenessWaitForBlockMinedSetDoesNotBeatWhenTheCallFails pins that only an
// answer from block validation counts as progress. A failed mined-status call
// must leave the heartbeat alone, or a wait on an unreachable dependency would
// keep the probe at 200.
func TestLivenessWaitForBlockMinedSetDoesNotBeatWhenTheCallFails(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler

	probe := &heartbeatProbe{ba: ba}
	probe.backdate()

	unknown := chainhash.HashH([]byte("no such block"))
	require.Error(t, ba.waitForBlockMinedSet(t.Context(), &unknown))

	require.Greater(t, ba.heartbeat.Age(), probeStaleBy/2, "a failed mined_set call must not beat")
}
