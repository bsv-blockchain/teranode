package netsync

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestPipelineSink_WritesTheSubtreeFiles is the whole claim: a block arrives as
// bytes on a reader and leaves as files in the store, without ever being a whole
// object in memory.
func TestPipelineSink_WritesTheSubtreeFiles(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	// wireBlockWithTxs leaves PrevBlock at its zero value, since it builds a
	// fixture with no real chain context. The sink resolves height by looking
	// the parent up, so point it at the one header a fresh store already
	// holds: its genesis. See pipelineHeaderFixture.
	pipelineHeaderFixture(t, sm, blk)
	// Below-checkpoint gating now also demands the header be PROVEN (an ancestry
	// proof to a pinned checkpoint hash, GHSA-gggq-8f59-4jm9), not merely below the
	// checkpoint height, or the sink writes .subtreeToCheck instead of the .subtree
	// this test checks for. See proveBlockOrigin.
	proveBlockOrigin(t, sm, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	require.True(t, converted, "a well-formed block below the checkpoint must convert cleanly")

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err, "the converted record the sink just wrote must read back cleanly")
	require.NotNil(t, got, "a verified block must be recorded")

	hashes := got.Subtrees
	require.NotEmpty(t, hashes, "a 20-transaction block at 8 per subtree must produce subtrees")

	for _, h := range hashes {
		exists, err := store.Exists(ctx, h[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)
		require.True(t, exists, "every subtree the sink reported must be in the store")
	}
}

// TestPipelineSink_RejectsAWrongMerkleRoot is the integrity floor. The header is
// the only thing binding a peer-supplied body to work somebody paid for, so a body
// that does not match it must be refused.
func TestPipelineSink_RejectsAWrongMerkleRoot(t *testing.T) {
	store := memory.New()
	sm := newPipelineManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	bad := blk.MsgBlock().Header
	bad.MerkleRoot[0] ^= 0xFF

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &bad, bytes.NewReader(body), int64(len(body)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "merkle root")
	require.False(t, converted, "a rejected block must never report having converted")
}

// TestPipelineSink_DeletesWhatItWroteOnAWrongMerkleRoot pins the cleanup. Nothing
// reads these files until the block is handed over, so a failed block must leave
// nothing behind.
func TestPipelineSink_DeletesWhatItWroteOnAWrongMerkleRoot(t *testing.T) {
	ctx := context.Background()
	failStore := memory.New()
	sm := newPipelineManager(t, failStore, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	bad := blk.MsgBlock().Header
	bad.MerkleRoot[0] ^= 0xFF

	_, _ = sm.pipelineBlockSink(*blk.Hash(), &bad, bytes.NewReader(body), int64(len(body)))

	// The failed run reports no subtrees, so ask a good run which hashes the block
	// produces and require every one of them to be absent from the failed store.
	good := newPipelineParkManager(t, memory.New(), 8)
	pipelineHeaderFixture(t, good, blk)
	goodConverted, goodErr := good.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, goodErr, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, goodConverted, "sanity: the good run must actually convert, or this test asserts nothing")

	goodBlock, err := good.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err, "the good run's converted record must read back cleanly")
	require.NotNil(t, goodBlock, "sanity: the good run must record a verified block, or this test asserts nothing")

	produced := goodBlock.Subtrees
	require.NotEmpty(t, produced, "sanity: the good run must produce subtrees, or this test asserts nothing")

	for _, h := range produced {
		for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
			exists, err := failStore.Exists(ctx, h[:], ft)
			require.NoError(t, err)
			require.False(t, exists, "a block that failed its merkle check must leave no %s behind", ft)
		}
	}
}

// TestPipelineSink_RejectsADuplicateTransaction pins the CVE-2012-2459 floor on
// the receive path. The merkle root cannot catch it, because the duplicate is what
// reproduces the root; and a checkpoint anchors the block hash, not the
// peer-supplied body. See model/check_duplicate_txs.go:23.
func TestPipelineSink_RejectsADuplicateTransaction(t *testing.T) {
	store := memory.New()
	sm := newPipelineManager(t, store, 8)

	blk := wireBlockWithTxs(t, 6, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyWithADuplicate(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
	require.False(t, converted, "a rejected block must never report having converted")
}

// TestPipelineSink_RecordsAWholeBlockModel pins what the sink hands on. The
// committer needs a header, a coinbase, a transaction count, a size, a height and
// a subtree list; recording only the subtree hashes would mean the committer had
// to reconstruct the rest from a block nobody kept.
func TestPipelineSink_RecordsAWholeBlockModel(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)

	header := &blk.MsgBlock().Header
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing about the recorded block")

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err, "the converted record the sink just wrote must read back cleanly")
	require.NotNil(t, got, "a verified block must be recorded")

	require.Equal(t, header.MerkleRoot.String(), got.Header.HashMerkleRoot.String(),
		"the recorded header must be the block's own")
	require.Equal(t, uint64(20), got.TransactionCount)
	require.NotNil(t, got.CoinbaseTx, "the committer serializes the coinbase from this")
	require.NotEmpty(t, got.Subtrees, "and reads the subtree list from this")
	require.Equal(t, uint64(len(body)), got.SizeInBytes)
}

// TestPipelineSink_TheRecordedBlockSurvivesASerializationRoundTrip is what makes
// Task 2 possible: the record is stored and recovered as bytes, so a field the
// model does not serialize would be silently lost after a restart.
func TestPipelineSink_TheRecordedBlockSurvivesASerializationRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)

	header := &blk.MsgBlock().Header
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing about the recorded block")

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err, "the converted record the sink just wrote must read back cleanly")
	require.NotNil(t, got, "a verified block must be recorded")

	raw, err := got.Bytes()
	require.NoError(t, err)

	back, err := model.NewBlockFromBytes(raw)
	require.NoError(t, err)

	require.Equal(t, got.Header.Hash().String(), back.Header.Hash().String())
	require.Equal(t, got.TransactionCount, back.TransactionCount)
	require.Equal(t, got.SizeInBytes, back.SizeInBytes)
	require.Equal(t, len(got.Subtrees), len(back.Subtrees), "the subtree list must survive the round trip")

	for i := range got.Subtrees {
		require.Equal(t, got.Subtrees[i].String(), back.Subtrees[i].String(),
			"subtree %d must survive the round trip", i)
	}

	require.Equal(t, got.CoinbaseTx.TxIDChainHash().String(), back.CoinbaseTx.TxIDChainHash().String())
}

// TestPipelineSink_CoinbaseOnlyBlockProducesAnEmptyRecord pins the fix for the
// live-node stall this task exists to close: every mainnet block below roughly
// height 100 carries only its coinbase, and the streaming builder refuses that
// shape outright (block_stream_builder.go's newBlockStreamBuilder) rather than
// emit the corrupt, colliding subtree it would otherwise produce. Before this
// fix that refusal reached the wire layer as an unreadable message, so every
// peer that sent block 1 got disconnected for "malformed message" and the node
// never got past height 0.
//
// The non-streaming path's own early return for the identical case
// (prepareSubtrees, handle_block.go: "if txCount <= 1 { return subtrees, nil,
// blockID, nil }") produces zero subtrees and zero subtree files, so that is
// the shape this test requires from the streaming sink too — not a weakened
// guard, a matched one. The merkle root check is not skipped: for a
// coinbase-only block the root IS the coinbase transaction's own hash, and
// the header here carries exactly that, computed independently of the sink
// under test (bsvutil.Tx.Hash(), not the sink's own accumulator).
func TestPipelineSink_CoinbaseOnlyBlockProducesAnEmptyRecord(t *testing.T) {
	ctx := context.Background()

	// subtreeStore is deliberately its own memory store, separate from the
	// park's, mirroring production (manager.go passes tempStore and
	// subtreeStore to newBlockPark and the SyncManager as two distinct
	// stores) and letting this test prove "no subtree files were written" by
	// reading the store's own Set counter directly, rather than guessing at a
	// subtree hash that a fixed sink never produces.
	subtreeStore := memory.New()
	sm := newPipelineManager(t, subtreeStore, 8)

	park, _ := newTestPark(t, "")
	require.NotNil(t, park, "the fixture must build a real park over a real file store")
	sm.blockPark = park

	blk := wireBlockWithTxs(t, 1, false)
	// wireBlockWithTxs leaves PrevBlock at its zero value; point it at the one
	// header a fresh blockchain store already holds, its genesis, the same way
	// pipelineHeaderFixture does for every other test in this file.
	blk.MsgBlock().Header.PrevBlock = *sm.chainParams.GenesisHash
	blk.MsgBlock().Header.MerkleRoot = *blk.Transactions()[0].Hash()

	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed coinbase-only block must convert cleanly")
	require.True(t, converted, "a coinbase-only block must still report having converted")

	require.Equal(t, 0, subtreeStore.Counters["set"], "a coinbase-only block must write no subtree files")

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err, "the converted record the sink just wrote must read back cleanly")
	require.NotNil(t, got, "a verified coinbase-only block must still be recorded")

	require.Empty(t, got.Subtrees, "a coinbase-only record must name no subtrees")
	require.Equal(t, uint64(1), got.TransactionCount)

	require.True(t, sm.holdsBlock(ctx, *blk.Hash()),
		"the completeness check must treat a record with an empty subtree list as complete, not as missing")
}

// pipelineManagerStoreCounter gives each newPipelineManager call its own
// sqlitememory database name, so one test's blockchain client cannot see
// another's state.
var pipelineManagerStoreCounter atomic.Int64

// newPipelineManager builds a SyncManager wired for pipelineBlockSink: a real
// settings/chainParams pair, the subtree store under test, and a real
// sqlitememory-backed blockchain client (never a mock, per AGENTS.md) so the
// sink's parent-height lookup runs against a real store rather than a narrowed
// stand-in.
//
// The checkpoint is set high (1000) purely so a fixture block, whose resolved
// height sits at 1 (one past the fresh store's genesis), reads as below
// checkpoint: that is what makes the writer choose FileTypeSubtree over
// FileTypeSubtreeToCheck, which the tests above assert on.
func newPipelineManager(t *testing.T, store blob.Store, maxItems int) *SyncManager {
	t.Helper()

	ctx := context.Background()

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1000}}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &params
	tSettings.BlockAssembly.MaximumMerkleItemsPerSubtree = maxItems

	// Task 13 removed pipelineBlockSink's own legacyUnified gate: every block
	// converts now, whether or not it is eligible for the unified route (see
	// pipeline_parent_height_test.go's TestPipelineSink_NotUnifiedRouteStillConverts).
	// These two stay on regardless, because they are still what
	// quickValidationAllowed and the structure-file-type choice below the
	// checkpoint depend on, and other tests in this package (handle_converted_block_test.go)
	// still exercise the unified commit route on top of a converted record.
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true

	dbName := fmt.Sprintf("pipeline_sink_%d", pipelineManagerStoreCounter.Add(1))

	storeURL, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)

	bcStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bcStore.Close(ctx) })

	bcClient, err := blockchain2.NewLocalClient(ulogger.TestLogger{}, tSettings, bcStore, nil, nil)
	require.NoError(t, err)

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &params,
		ctx:              ctx,
		subtreeStore:     store,
		blockchainClient: bcClient,
		// SupportsOutpointOnlySpend() true is the other conjunct
		// legacyOutpointOnly (and so legacyUnified) needs; a nil store
		// answers false, which would make every fixture ineligible above.
		utxoStore: &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
	}

	// New always builds this gate unconditionally now (the streaming pipeline
	// is the only route left), so a manager built as a struct literal needs it
	// too, or admitPipelineSink's AcquireBlockPrefetch panics on a nil map.
	// Sized generously (64, matching the shipped default of
	// MaxBlocksInTransitPerPeer x pipelineBlockSlotPeerAllowance) so it never
	// binds unless a test deliberately narrows it.
	enablePrefetchBudgetForTest(t, sm, 64)

	return sm
}

// pipelineHeaderFixture points blk's header at a parent the manager's fresh
// blockchain store actually holds, and sets the header's merkle root to the one
// this block's own transactions produce.
//
// wireBlockWithTxs (task 2's fixture builder) leaves both fields at their zero
// value, because it was written for prepareSubtrees comparisons that only ever
// look at subtree root hashes, never the header. pipelineBlockSink uses
// PrevBlock only to look up the parent's height — it never checks that the
// parent relates to the block's body — so overwriting it with the store's
// genesis hash is enough to resolve a height without seeding a whole parent
// block.
//
// The root is computed with the same builder pipelineBlockSink itself drives,
// over a throwaway store, at the same MaximumMerkleItemsPerSubtree the manager
// under test uses. That makes this fixture self-consistent, which is all a
// "good" run needs; it is NOT where the claim that this root is the canonical
// one (the one the real model.Block.CheckMerkleRoot would accept) is proven.
// That claim is carried elsewhere, against the real function rather than a
// second computation of the same code path: TestMerkleAccumulator_MatchesCheckMerkleRoot
// (merkle_accumulator_test.go) feeds the accumulator subtree shapes covering
// both this file's 20-at-8 and single-partial-subtree cases and requires
// CheckMerkleRoot to accept the result, and
// TestBlockStreamBuilder_RootMatchesTheAllAtOnceComputation
// (block_stream_builder_test.go) pins the same 20-at-8 shape through Finish()
// on the real builder. (An earlier version of this comment cited
// TestPipeline_ProducesTheSameFilesAsPrepareSubtrees for this; that test
// discarded its root and never checked it against anything until this same
// review round wired checkMerkleRootAgainst into it.)
//
// Called before the first call to blk.Hash(), so the block's cached hash
// reflects the header actually passed to the sink.
func pipelineHeaderFixture(t *testing.T, sm *SyncManager, blk *bsvutil.Block) {
	t.Helper()

	blk.MsgBlock().Header.PrevBlock = *sm.chainParams.GenesisHash

	txs := blk.Transactions()
	coinbase, _ := btTxFromWireTx(t, txs[0])

	discard := memory.New()
	writer := newSubtreeWriter(sm.logger, sm.settings, discard, 1, true)

	dedup := txmap.NewSplitSwissMapUint64(uint32(len(txs))) //nolint:gosec // test tx count is small

	builder, err := newBlockStreamBuilder(len(txs), sm.settings.BlockAssembly.MaximumMerkleItemsPerSubtree, coinbase, writer.Emit(context.Background()), dedup)
	require.NoError(t, err)

	for i := 1; i < len(txs); i++ {
		tx, hash := btTxFromWireTx(t, txs[i])
		require.NoError(t, builder.AddTx(tx, hash))
	}

	root, _, err := builder.Finish()
	require.NoError(t, err)

	blk.MsgBlock().Header.MerkleRoot = *root
}

// blockBodyBytes serializes exactly what the wire layer hands the sink: the
// transaction count varint followed by every transaction, in block order.
// NOT the header — pipelineBlockSink receives that as parsed structure, not
// bytes.
func blockBodyBytes(t *testing.T, blk *bsvutil.Block) []byte {
	t.Helper()

	txs := blk.Transactions()

	var buf bytes.Buffer
	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, uint64(len(txs))))

	for _, tx := range txs {
		require.NoError(t, tx.MsgTx().Serialize(&buf))
	}

	return buf.Bytes()
}

// blockBodyWithADuplicate is blockBodyBytes with one non-coinbase transaction
// repeated in place of another, and the declared count left at the block's
// real count — the CVE-2012-2459 shape.
func blockBodyWithADuplicate(t *testing.T, blk *bsvutil.Block) []byte {
	t.Helper()

	txs := blk.Transactions()
	require.Greater(t, len(txs), 2, "need a coinbase plus at least two non-coinbase transactions to duplicate one")

	var buf bytes.Buffer
	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, uint64(len(txs))))

	for i, tx := range txs {
		toWrite := tx.MsgTx()
		if i == len(txs)-1 {
			toWrite = txs[1].MsgTx() // repeat an earlier non-coinbase transaction
		}

		require.NoError(t, toWrite.Serialize(&buf))
	}

	return buf.Bytes()
}
