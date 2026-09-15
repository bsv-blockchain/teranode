package netsync

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/stretchr/testify/require"
)

// existsFaultStore wraps a real blob store and, when armed, fails every Exists
// call with an error of the test's choosing, leaving every other method (Get,
// GetIoReader, Set, Del, ...) untouched. blockPark.IsConverted is the only
// park method that calls Exists; fix-round item 1 stopped the commit path
// (commitParkedBlock, parkedRun) from calling it at all, routing instead on
// the entry's own converted field, so this now proves the negative — that a
// faulted Exists no longer has anywhere left on that path to bite.
type existsFaultStore struct {
	blob.Store
	err error
}

func (s *existsFaultStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	if s.err != nil {
		return false, s.err
	}

	return s.Store.Exists(ctx, key, fileType, opts...)
}

// mineRegtestPoW finds a nonce for blk's header that satisfies
// HasMetTargetDifficulty against blk's own Bits. Regtest's PowLimitBits
// (0x207fffff, set by the caller before this runs) accepts roughly half of all
// nonces, so this is a handful of iterations, not a real proof-of-work search.
// commitPreparedBlock's PoW check is real code in this test file, not mocked,
// so the header needs an actual passing nonce rather than a hand-waved one.
//
// Must run before the first call to blk.Hash(): bsvutil.Block caches its hash,
// and mining changes the header the hash is taken over.
func mineRegtestPoW(t *testing.T, blk *bsvutil.Block) {
	t.Helper()

	header := &blk.MsgBlock().Header

	for nonce := uint32(0); nonce < 1_000_000; nonce++ {
		header.Nonce = nonce

		var buf bytes.Buffer
		require.NoError(t, header.Serialize(&buf))

		modelHeader, err := model.NewBlockHeaderFromBytes(buf.Bytes())
		require.NoError(t, err)

		if ok, _, _ := modelHeader.HasMetTargetDifficulty(); ok {
			return
		}
	}

	t.Fatalf("could not find a nonce meeting the trivial regtest target")
}

// convertedRouteSpyValidation stands in for the real blockvalidation.Server so
// this test can drive HandleConvertedBlock without wiring the server's own
// dependencies (subtree fetch, UTXO create/spend, kafka, gRPC). It records what
// it was called with and reports success; it does not itself write anything to
// a blockchain store or a UTXO store. See
// TestHandleConvertedBlock_CommitsWithoutTheBlock's own doc comment for exactly
// what that means this test does, and does not, prove.
type convertedRouteSpyValidation struct {
	blockvalidation.MockBlockValidation

	mu    sync.Mutex
	calls []convertedRouteProcessBlockCall
}

type convertedRouteProcessBlockCall struct {
	block       *model.Block
	blockHeight uint32
	peerID      string
	baseURL     string
	blockID     uint32
}

func (v *convertedRouteSpyValidation) ProcessBlock(_ context.Context, block *model.Block, blockHeight uint32, peerID, baseURL string, blockID uint32) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.calls = append(v.calls, convertedRouteProcessBlockCall{
		block: block, blockHeight: blockHeight, peerID: peerID, baseURL: baseURL, blockID: blockID,
	})

	return nil
}

func (v *convertedRouteSpyValidation) callCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()

	return len(v.calls)
}

func (v *convertedRouteSpyValidation) lastCall() convertedRouteProcessBlockCall {
	v.mu.Lock()
	defer v.mu.Unlock()

	return v.calls[len(v.calls)-1]
}

// TestHandleConvertedBlock_CommitsWithoutTheBlock is the point of the plan: the
// committer is handed a record of a few hundred bytes and the block is never
// reconstructed.
//
// What this test honestly proves: a real pipelineBlockSink conversion — against a
// real subtree store and a real sqlitememory-backed blockchain client, the same
// infrastructure pipeline_sink_test.go and pipeline_park_test.go use — produces a
// converted record; the whole block is never written to the park at all
// (asserted directly below via the store, not inferred from behaviour);
// HandleConvertedBlock reads that record, re-resolves and verifies its height
// against the real chain, and hands the exact record to
// blockValidation.ProcessBlock with the correct height and blockID (0, "assign
// server-side" — see WriteConvertedBlock's own comment).
//
// What it does NOT prove: that the block's transactions validate or land in a
// real UTXO store. blockValidation is a spy here (convertedRouteSpyValidation),
// standing in for the real blockvalidation.Server the same way this package's own
// TestSyncManager_HandleBlockDirect does — and that test is skipped outright,
// because driving a block through the real server needs a live SV Node / the
// full service wiring this package's tests do not build. That distance between a
// netsync-package unit test and a real commit is unchanged by this task; what
// changed is that the record, not a decoded whole block, is what now reaches the
// boundary this test can honestly check.
//
// The merkle and duplicate checks that HandleBlockDirect runs after
// prepareSubtrees are deliberately NOT run here. They are not skipped blindly:
// the pipeline sink verified the merkle root against the header before the
// record was ever written (TestPipelineSink_RejectsAWrongMerkleRoot pins that it
// refuses a mismatch), and the stream builder's duplicate map is mandatory and
// cannot be disabled (TestPipelineSink_RejectsADuplicateTransaction pins that
// too), so both guarantees hold earlier rather than not at all — see also
// TestHandleConvertedBlock_HasNoSubtreeSlicesToRecheck below, which pins that a
// converted record carries nothing the re-check could even run against.
// Re-running either check here would need the transactions, which is exactly
// what this route exists not to read.
func TestHandleConvertedBlock_CommitsWithoutTheBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	// legacyUnified's own conjuncts: the outpoint-only gate (flag + a store that
	// supports it) and the unified flag itself. Both must hold, or
	// HandleConvertedBlock's own guard refuses the record outright — see that
	// guard's comment for why a converted record is only ever correct on this
	// route.
	sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}

	spy := &convertedRouteSpyValidation{}
	sm.blockValidation = spy
	// sm.blockAssembly stays nil: WaitForBlockAssemblyReady treats a nil client
	// as "skip" (util/blockassemblyutil), which is the honest way to leave block
	// assembly readiness out of a test that is not about it.

	blk := wireBlockWithTxs(t, 6, false)
	// wireBlockWithTxs' Bits (0x1d00ffff) is mainnet genesis-era difficulty: a
	// real, hard target no unmined test nonce will ever satisfy. commitPreparedBlock's
	// proof-of-work check is real code, not mocked here, so the header needs a
	// target this test can actually clear — regtest's own PowLimitBits, chosen so
	// any hash satisfies it.
	blk.MsgBlock().Header.Bits = 0x207fffff
	pipelineHeaderFixture(t, sm, blk)
	mineRegtestPoW(t, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing")

	isConverted, err := sm.blockPark.IsConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.True(t, isConverted, "the park must carry a converted record, or the route under test is unreachable")

	noWholeBlock, err := sm.blockPark.store.Exists(ctx, blk.Hash()[:], fileformat.FileTypeMsgBlock)
	require.NoError(t, err)
	require.False(t, noWholeBlock, "the whole block must never have been written — HandleConvertedBlock has no fallback to read one")

	record, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.NotNil(t, record)

	err = sm.HandleConvertedBlock(ctx, nil, *blk.Hash(), record)
	require.NoError(t, err, "a valid converted record below the checkpoint, on the unified route, must commit")

	require.Equal(t, 1, spy.callCount(), "HandleConvertedBlock must hand the record to blockValidation exactly once")

	call := spy.lastCall()
	require.Equal(t, record.Header.Hash().String(), call.block.Header.Hash().String(), "the committed block must be the record's own")
	require.Equal(t, record.Height, call.blockHeight, "the committed height must be the record's own height, re-verified against the chain")
	require.Equal(t, uint32(0), call.blockID, "the unified route assigns the block ID server-side, inside quickValidateBlock")
	require.Equal(t, uint64(6), call.block.TransactionCount, "the record's transaction count must reach the committer unchanged")
	require.Equal(t, len(record.Subtrees), len(call.block.Subtrees), "the record's subtree list must reach the committer unchanged")
}

// TestHandleConvertedBlock_HasNoSubtreeSlicesToRecheck pins why the merkle and
// duplicate re-check inside commitPreparedBlock is skipped for a converted
// block rather than merely never called: ReadConverted's result carries no
// in-memory subtree slices for it to run against in the first place.
// Re-deriving them would mean rebuilding the subtrees from the block's
// transactions — the exact read this route exists not to do.
func TestHandleConvertedBlock_HasNoSubtreeSlicesToRecheck(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 6, false)
	blk.MsgBlock().Header.Bits = 0x207fffff
	pipelineHeaderFixture(t, sm, blk)
	mineRegtestPoW(t, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing")

	record, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.NotNil(t, record)

	require.Nil(t, record.SubtreeSlices, "a converted record never carries in-memory subtree slices, so commitPreparedBlock's merkle/duplicate re-check has nothing to run against — it is skipped because it cannot be done cheaply, not merely because nobody called it")
}

// TestHandleConvertedBlock_AHeightDisagreementIsTransientNotBlockInvalid is
// fix-round item 4. The height check twenty lines below the eligibility
// assertion used to return a BlockInvalidError, which parkCommitFailure reads
// as parkDispositionBlockRejected: delete the converted record — its only
// copy, there is no whole block to fall back to — rewind the cursor, blame
// the peer, and fail the block at that height forever.
//
// A disagreement here is between two things THIS node computed about its own
// chain view: the record's height, resolved once at conversion time
// (pipelineParentHeight), against the parent's CURRENT height from the store.
// It says nothing about what the peer sent — the header chain and merkle root
// are checked elsewhere — so it must fail the way the eligibility assertion
// beside it already does: a ServiceError, which parkCommitFailure reads as
// parkDispositionRetryLater (keep the blob, no rewind, no blame) instead.
//
// The record's own height is bumped after a genuine conversion, rather than
// building an inconsistent record by hand, so what disagrees is exactly the
// thing a stale record after a reorg would disagree about.
func TestHandleConvertedBlock_AHeightDisagreementIsTransientNotBlockInvalid(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}

	spy := &convertedRouteSpyValidation{}
	sm.blockValidation = spy

	blk := wireBlockWithTxs(t, 6, false)
	blk.MsgBlock().Header.Bits = 0x207fffff
	pipelineHeaderFixture(t, sm, blk)
	mineRegtestPoW(t, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing")

	record, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err)

	// pipelineHeaderFixture parents this block on genesis, so the real,
	// correct height the store agrees to is 1. Bumping the record's own copy
	// of it is what disagrees, standing in for a record gone stale between
	// conversion and commit — a reorg, or simply time passing while it sat
	// parked.
	record.Height += 7

	err = sm.HandleConvertedBlock(ctx, nil, *blk.Hash(), record)
	require.Error(t, err, "a height disagreement must still fail the commit")
	require.True(t, errors.IsTransientLocalError(err),
		"a height disagreement is this node's own chain view moving, not a peer's claim, so it must be transient-local (parkDispositionRetryLater), not block-invalid (parkDispositionBlockRejected, which would delete the record's only copy and blame the peer)")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid),
		"must not be classified as a bad block: the peer sent nothing wrong here, this node's own height bookkeeping disagreed with itself")

	require.Zero(t, spy.callCount(), "a routing failure must never reach the committer")
}

// TestCommitParkedBlock_RoutesAConvertedEntryWithoutReadingAWholeBlock exercises
// Step 5's actual call site rather than calling HandleConvertedBlock directly:
// commitParkedBlock (block_park_drain.go), the serial drain's committer, given a
// parked entry that IsConverted reports true for.
//
// blockValidation is the same kind of spy TestHandleConvertedBlock_CommitsWithoutTheBlock
// uses, and for the same reason: this proves the routing and the post-commit
// bookkeeping (isCheckpointHash, applyParkDisposition, noteCommittedParkedBlock),
// not that the block's transactions validate. See that test's doc comment for the
// full statement of what a spy blockValidation does and does not prove.
func TestCommitParkedBlock_RoutesAConvertedEntryWithoutReadingAWholeBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)

	sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}

	spy := &convertedRouteSpyValidation{}
	sm.blockValidation = spy

	blk := wireBlockWithTxs(t, 6, false)
	blk.MsgBlock().Header.Bits = 0x207fffff
	pipelineHeaderFixture(t, sm, blk)
	mineRegtestPoW(t, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing")

	// commitParkedBlock takes an entry already detached from the park's index —
	// see TakeChildren/Take in production — so it is built directly here rather
	// than round-tripped through Admit, which this test has no need of.
	// converted: true is what AdoptWritten would have set from the sink's own
	// BlockBody.Converted (fix-round item 1) — commitParkedBlock now reads that
	// field instead of asking the store, so a test driving it directly has to
	// set it the same way.
	entry := parkedBlock{hash: *blk.Hash(), peer: nil, converted: true}

	ok := sm.commitParkedBlock(entry)
	require.True(t, ok, "a valid converted entry, below the checkpoint on the unified route, must commit")

	require.Equal(t, 1, spy.callCount(), "commitParkedBlock must route the converted entry to HandleConvertedBlock, which commits through blockValidation exactly once")

	noWholeBlock, err := store.Exists(ctx, blk.Hash()[:], fileformat.FileTypeMsgBlock)
	require.NoError(t, err)
	require.False(t, noWholeBlock, "commitParkedBlock must never have written or read a whole block for a converted entry")

	// The disposition for a commit deletes the converted record too (blockPark.Delete
	// removes both FileTypeMsgBlock and FileTypeBlock for the hash), so by now
	// neither representation is left on disk.
	noRecord, err := store.Exists(ctx, blk.Hash()[:], fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.False(t, noRecord, "a committed entry's converted record must be deleted, the same as a committed whole block's blob")
}

// TestBlockDispatcher_ParkedRunRoutesAConvertedEntryWithoutReadingAWholeBlock is
// fix-round item 3: the dispatcher's parked worker (block_dispatcher.go:336-366)
// had no test driving its real bd.parkedRun closure over a converted entry —
// every other dispatcher test either exercises the whole-block branch only, or
// replaces parkedRun wholesale with a stub. This calls the real closure
// directly (not through bd.dispatch's frontier/worker-pool machinery, which
// this task did not touch and which TestParkDispatch_ADispatchedParkedBlockCommitsAndTakesTheParkedTail
// already covers for the whole-block branch), the same way
// TestCommitParkedBlock_RoutesAConvertedEntryWithoutReadingAWholeBlock drives
// the serial drain's equivalent, commitParkedBlock.
func TestBlockDispatcher_ParkedRunRoutesAConvertedEntryWithoutReadingAWholeBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)

	sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}

	spy := &convertedRouteSpyValidation{}
	sm.blockValidation = spy

	blk := wireBlockWithTxs(t, 6, false)
	blk.MsgBlock().Header.Bits = 0x207fffff
	pipelineHeaderFixture(t, sm, blk)
	mineRegtestPoW(t, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing")

	bd := newBlockDispatcher(sm)

	d := &blockDispatch{parked: &parkedBlock{hash: *blk.Hash(), peer: nil, converted: true}}

	runErr := bd.parkedRun(ctx, d)
	require.NoError(t, runErr, "a valid converted entry, below the checkpoint on the unified route, must commit")
	require.Nil(t, d.readErr, "a successful commit must not record a read error")

	require.Equal(t, 1, spy.callCount(), "parkedRun must route the converted entry to HandleConvertedBlock, which commits through blockValidation exactly once")

	noWholeBlock, err := store.Exists(ctx, blk.Hash()[:], fileformat.FileTypeMsgBlock)
	require.NoError(t, err)
	require.False(t, noWholeBlock, "parkedRun must never have written or read a whole block for a converted entry")
}

// TestBlockDispatcher_ParkedRunNeverConsultsTheStoreToRoute is fix-round item
// 1. Before it, parkedRun asked blockPark.IsConverted — a store Exists call —
// before every parked commit, unconditionally: one of the file store's 768
// process-wide read permits, held for the store's configured timeout, on top
// of the read that already followed it. A store fault on that one check used
// to fail the whole commit (TestBlockDispatcher_ParkedRunKeepsTheBlockWhenIsConvertedFails
// pinned exactly that, and is gone along with the check it pinned).
//
// This arms the same existsFaultStore that test used — every Exists call
// fails — and requires the commit to succeed anyway: d.parked.converted, set
// at AdoptWritten from the sink's own return value, is what routes this now,
// and it costs nothing the store can refuse.
func TestBlockDispatcher_ParkedRunNeverConsultsTheStoreToRoute(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	realStore := memory.New()
	sm := newPipelineParkManager(t, realStore, 8)
	sm.rejectedTxns = txmap.NewSyncedMap[chainhash.Hash, struct{}](100)

	sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}

	spy := &convertedRouteSpyValidation{}
	sm.blockValidation = spy

	blk := wireBlockWithTxs(t, 6, false)
	blk.MsgBlock().Header.Bits = 0x207fffff
	pipelineHeaderFixture(t, sm, blk)
	mineRegtestPoW(t, blk)
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing")

	// Armed only now, after the real conversion above has already written the
	// record: with the fix in place nothing on the commit path below should
	// ever call Exists again, so arming it earlier would prove nothing either
	// way.
	faulted := &existsFaultStore{Store: realStore, err: errors.NewProcessingError("the store is having a bad day")}
	sm.blockPark.store = faulted

	bd := newBlockDispatcher(sm)
	d := &blockDispatch{parked: &parkedBlock{hash: *blk.Hash(), peer: nil, converted: true}}

	runErr := bd.parkedRun(ctx, d)
	require.NoError(t, runErr, "a faulted Exists must not affect the commit once routing no longer asks the store")
	require.Nil(t, d.readErr, "no read error either — the fault is on Exists, which routing must never call")

	require.Equal(t, 1, spy.callCount(), "the commit must still go through, proving the fault store was armed for nothing routing does")
}
