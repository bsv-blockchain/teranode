package netsync

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// warnCaptureLogger records every Warnf call so a test can assert on whether a
// warning was actually logged, not merely on a function's return value —
// fix-round item 4's whole point is that pipelineBlockDelete used to swallow a
// real read failure without logging anything, so the assertion has to be able
// to see the log line itself.
type warnCaptureLogger struct {
	ulogger.TestLogger
	warnings []string
}

func (l *warnCaptureLogger) Warnf(format string, args ...interface{}) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

// TestInstallStreamingBlockPath_ChoosesTheSinkAndDeleteTogether is FIX 3's
// wiring claim.
//
// Before this change, installStreamingBlockPath always installed
// sm.streamingBlockDelete as the orphan-delete callback, whichever sink was
// active. That deletes from the park's blob store; the pipeline sink never
// writes there, so when the wire layer's own post-sink checks failed after a
// successful pipeline conversion, the delete callback removed a park body
// that was never written while the subtree files the pipeline sink actually
// wrote were left behind forever.
//
// Streaming is now unconditional, so there is only one sink/delete pair to
// install any more: this proves it is always pipelineBlockDelete, never
// streamingBlockDelete alone.
//
// The installed sink is no longer sm.pipelineBlockSink itself: it is wrapped
// in admitPipelineSink to reach the download-admission budget that used to be
// unreachable from this route (AcquireBlockPrefetch was only ever called from
// OnBlock, which the pipeline route never dispatches through). A wrapper
// closure's reflect code pointer is never equal to the method value it
// wraps — confirmed separately, not assumed — so the claim is proved
// behaviourally instead: the installed sink, given a well-formed block, must
// convert it exactly as calling sm.pipelineBlockSink directly would.
func TestInstallStreamingBlockPath_ChoosesTheSinkAndDeleteTogether(t *testing.T) {
	sm := newPipelineParkManager(t, memory.New(), 8)

	var gotSink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)
	var gotGate func(chainhash.Hash, *wire.BlockHeader) error
	var gotDelete func(chainhash.Hash, bool) error

	sm.installStreamingBlockPath(func(
		sink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error),
		gate func(chainhash.Hash, *wire.BlockHeader) error,
		del func(chainhash.Hash, bool) error,
	) {
		gotSink = sink
		gotGate = gate
		gotDelete = del
	})

	wantDelete := reflect.ValueOf(sm.pipelineBlockDelete).Pointer()
	require.Equal(t, wantDelete, reflect.ValueOf(gotDelete).Pointer(), "must install pipelineBlockDelete, not streamingBlockDelete alone")
	require.NotNil(t, gotGate, "the gate must always be installed alongside a sink")

	blk := wireBlockWithTxs(t, 9, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	converted, err := gotSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "the installed sink must convert a well-formed block cleanly")
	require.True(t, converted, "the installed sink must behave like the pipeline sink, not the plain body-write path")
}

// TestPipelineBlockDelete_RemovesTheSubtreeFilesTheSinkWrote is FIX 3's
// functional claim.
//
// Simulates the wire layer's own post-sink checks failing after
// pipelineBlockSink itself already succeeded — the short-body check or the
// transaction-count read in readBlockMessage
// (services/legacy/peer/wire_streaming.go) — which calls the installed
// delete callback with the block's hash. Before this fix that callback was
// always streamingBlockDelete, which only knows about the park's store and
// would find nothing there to remove.
func TestPipelineBlockDelete_RemovesTheSubtreeFilesTheSinkWrote(t *testing.T) {
	ctx := t.Context()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	converted, sinkErr := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, sinkErr, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion to test its cleanup")

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err, "the converted record the sink just wrote must read back cleanly")
	require.NotNil(t, got, "sanity: the sink must have recorded a verified block, or this test asserts nothing")

	hashes := got.Subtrees
	require.NotEmpty(t, hashes, "sanity: the sink must have produced subtrees, or this test asserts nothing")

	// converted: true — exactly the value the sink call above just returned,
	// which is what the wire layer would actually pass through here (see
	// BlockBody.Converted / deleteOrphanedBody). Fix-round item 2 gates the
	// subtree-file cleanup on this being true for THIS call, rather than on
	// whether a converted record merely exists for the hash.
	require.NoError(t, sm.pipelineBlockDelete(*blk.Hash(), true))

	for _, h := range hashes {
		for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
			exists, err := store.Exists(ctx, h[:], ft)
			require.NoError(t, err)
			require.False(t, exists, "pipelineBlockDelete must remove every subtree artefact the sink wrote, got %s still present for subtree %s", ft, h)
		}
	}

	isConverted, err := sm.blockPark.IsConverted(ctx, *blk.Hash())
	require.NoError(t, err, "checking for a converted record must not itself fail")
	require.False(t, isConverted, "the converted record must be cleared too, not just the subtree files")
}

// TestPipelineBlockDelete_AlsoCleansUpTheFallbackParkWrite covers FIX 2's
// out-of-order fallback. pipelineBlockSink itself no longer has an
// unresolvable-parent fallback — it converts every block, resolved or not
// (task 13) — but admitPipelineSink's own admission-budget fallback (a
// duplicate hash already in flight, or the acquire timing out) still defers to
// streamingBlockSink, which writes the raw body to the park under the
// pipeline path's own name for what "the active sink wrote". The delete
// callback installed for the pipeline path must still clean that up. This
// calls streamingBlockSink directly to stand in for that fallback without
// needing to reproduce the admission race that reaches it.
func TestPipelineBlockDelete_AlsoCleansUpTheFallbackParkWrite(t *testing.T) {
	ctx := t.Context()

	subtreeStore := memory.New()
	parkStore := memory.New()
	sm := newPipelineManagerWithPark(t, subtreeStore, parkStore, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	converted, sinkErr := sm.streamingBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, sinkErr, "the fallback body write must succeed")
	require.False(t, converted, "the fallback never converts, it only writes the raw body")

	exists, err := parkStore.Exists(ctx, blk.Hash()[:], parkFileType)
	require.NoError(t, err)
	require.True(t, exists, "sanity: the fallback must have written the body to the park, or this test asserts nothing")

	// converted: false — the fallback never converts (asserted above), so this
	// is what the sink's own return value would actually be for this delivery.
	// The raw body must still be cleaned up unconditionally, regardless of
	// converted.
	require.NoError(t, sm.pipelineBlockDelete(*blk.Hash(), false))

	exists, err = parkStore.Exists(ctx, blk.Hash()[:], parkFileType)
	require.NoError(t, err)
	require.False(t, exists, "the pipeline delete callback must also remove a body the fallback wrote to the park, not just subtree files")
}

// TestPipelineBlockDelete_RemovesSubtreeToCheckFilesAboveCheckpoint is
// fix-round item 6. Every other test in this file resolves its block to
// height 1 under a checkpoint at 1000 (newPipelineManager), so
// quickValidationAllowed is always true and FileTypeSubtree is always the
// structure type pipelineBlockDelete deletes — the FileTypeSubtreeToCheck
// branch, which is what a mainnet block above the highest checkpoint takes,
// had no test at all.
//
// Task 13 removed pipelineBlockSink's own above-checkpoint refusal, so this
// now drives the real sink rather than building a record by hand: an
// above-checkpoint block is the ordinary case the sink converts, not a
// refusal to work around.
func TestPipelineBlockDelete_RemovesSubtreeToCheckFilesAboveCheckpoint(t *testing.T) {
	ctx := t.Context()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	// BelowCheckpoint (model/checkpoint.go) requires highest > 0, so a
	// checkpoint height of 0 means "no checkpoint reaches this chain", and
	// every height — including this fixture's resolved height of 1 — reads as
	// above it, so quickValidationAllowed is false and the sink must choose
	// FileTypeSubtreeToCheck.
	sm.chainParams.Checkpoints = []chaincfg.Checkpoint{{Height: 0}}

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	converted, sinkErr := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, sinkErr, "an above-checkpoint block must convert cleanly, not be refused")
	require.True(t, converted, "sanity: this test needs a real conversion above the checkpoint")

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.NotNil(t, got)

	hashes := got.Subtrees
	require.NotEmpty(t, hashes, "sanity: the builder must have produced subtrees, or this test asserts nothing")

	// Sanity on the writer's own choice, not yet on the delete: above the
	// checkpoint it must have used FileTypeSubtreeToCheck, and NOT
	// FileTypeSubtree, or the delete assertion below would pass even with the
	// wrong structure type hardcoded.
	for _, h := range hashes {
		toCheck, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, existsErr)
		require.True(t, toCheck, "sanity: above the checkpoint the writer must use FileTypeSubtreeToCheck")

		quick, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtree)
		require.NoError(t, existsErr)
		require.False(t, quick, "sanity: above the checkpoint the writer must NOT also write FileTypeSubtree")
	}

	// converted: true — this test stands in for a delivery that converted a
	// record above the checkpoint (writeConvertedRecordDirect exists because
	// the sink itself can no longer produce one, see its own doc comment).
	require.NoError(t, sm.pipelineBlockDelete(*blk.Hash(), true))

	for _, h := range hashes {
		for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
			exists, existsErr := store.Exists(ctx, h[:], ft)
			require.NoError(t, existsErr)
			require.False(t, exists, "pipelineBlockDelete must remove every subtree artefact the sink wrote, got %s still present for subtree %s", ft, h)
		}
	}
}

// TestPipelineBlockDelete_LogsARealReadFailureInsteadOfSwallowingIt is
// fix-round item 4. Before this fix, err == nil && record != nil was the only
// branch pipelineBlockDelete did anything in: a store timeout, a permit-pool
// wait that ran out, an undecodable record, and ReadConverted's own
// hash-mismatch refusal all fell through in total silence, reporting clean
// cleanup while any subtree files the sink actually wrote were never deleted.
// This forces the hash-mismatch case — the corruption ReadConverted's own doc
// comment exists to catch — and requires a logged warning, not silence.
func TestPipelineBlockDelete_LogsARealReadFailureInsteadOfSwallowingIt(t *testing.T) {
	ctx := t.Context()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	warnings := &warnCaptureLogger{}
	sm.logger = warnings

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	converted, sinkErr := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, sinkErr, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs a real converted record to corrupt")

	// A foreign block's own converted bytes: valid and decodable on their own,
	// but under blk's key they hash to someone else's block, exactly the
	// corruption ReadConverted's hash check exists to catch. A different
	// transaction count (6, not 20) is deliberate: wireBlockWithTxs is
	// otherwise fully deterministic, and the block header's own timestamp
	// field is second-resolution, so two blocks built moments apart with the
	// same transaction count can hash identically — which would make this
	// "foreign" record indistinguishable from blk's own and defeat the whole
	// test.
	foreign := wireBlockWithTxs(t, 6, false)
	foreignSM := newPipelineParkManager(t, memory.New(), 8)
	pipelineHeaderFixture(t, foreignSM, foreign)
	foreignBody := blockBodyBytes(t, foreign)

	foreignConverted, foreignErr := foreignSM.pipelineBlockSink(*foreign.Hash(), &foreign.MsgBlock().Header, bytes.NewReader(foreignBody), int64(len(foreignBody)))
	require.NoError(t, foreignErr, "sanity: the foreign block must also convert cleanly")
	require.True(t, foreignConverted, "sanity: this test needs a second, genuinely different converted record")

	foreignRaw, err := foreignSM.blockPark.store.Get(ctx, foreign.Hash()[:], fileformat.FileTypeBlock)
	require.NoError(t, err, "sanity: the foreign record must read back from its own store")

	// The real record is already there under blk's own hash from the sink call
	// above, and Set refuses to overwrite by default, so it has to be removed
	// before the foreign bytes can take its place. This reaches the store
	// directly with no options, which only lines up with parkOpts's key by
	// accident of the in-memory store's own option-blindness — see the note
	// at the top of pipeline_park_test.go.
	require.NoError(t, store.Del(ctx, blk.Hash()[:], fileformat.FileTypeBlock))
	require.NoError(t, store.Set(ctx, blk.Hash()[:], fileformat.FileTypeBlock, foreignRaw),
		"sanity: overwriting the record under blk's own hash with a foreign block's bytes")

	require.Empty(t, warnings.warnings, "sanity: nothing has failed yet")

	// converted: true — this delivery's own sink call above genuinely
	// converted blk, so the read failure below (the store now holds a foreign
	// block's bytes under blk's key) must still be treated as THIS call's own
	// record having gone bad, not silently ignored.
	require.NoError(t, sm.pipelineBlockDelete(*blk.Hash(), true),
		"pipelineBlockDelete's own return is unaffected by a read failure here: it still runs streamingBlockDelete, which is what owns the fallback park write")

	require.NotEmpty(t, warnings.warnings, "a read failure that is not \"not found\" must be logged, not swallowed in silence")
	require.Contains(t, warnings.warnings[0], "pipelineBlockDelete", "the warning must name where it came from")
}

// TestPipelineBlockDelete_DoesNotTouchAnotherDeliverysSubtreeFiles is
// fix-round item 2's regression. Before this fix, pipelineBlockDelete found
// its subtree files to remove by reading whatever converted record sat under
// the hash — an inference by mere existence, not by this call's own
// knowledge — so an UNRELATED delivery's failure could delete a genuinely
// different, still-parked delivery's subtree files out from under it.
//
// The concrete shape, per the review: peer A delivers a block, it converts
// and parks (still waiting on its own parent — genuine, needed). Ownership is
// released at intake, so the same hash is immediately re-requestable, and the
// streaming gate accepts any hash asked for within the last hour. Peer B then
// delivers the SAME hash and its own body ends short, so B's own sink call
// returns converted=false; the wire layer's orphan-delete callback still runs
// for B's failed delivery, with the hash B claimed to be delivering — which is
// the hash A's genuine, still-parked delivery already owns.
//
// This builds A's real conversion first (real subtree files, real record),
// then calls pipelineBlockDelete for the same hash with converted=false —
// exactly what B's own failed sink call would report — and requires A's
// subtree files to survive untouched.
func TestPipelineBlockDelete_DoesNotTouchAnotherDeliverysSubtreeFiles(t *testing.T) {
	ctx := t.Context()

	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	// Below-checkpoint gating now also demands the header be PROVEN (an ancestry
	// proof to a pinned checkpoint hash, GHSA-gggq-8f59-4jm9), not merely below the
	// checkpoint height, or the sink writes .subtreeToCheck instead of the .subtree
	// this test's assertion checks for. See proveBlockOrigin.
	proveBlockOrigin(t, sm, blk)
	body := blockBodyBytes(t, blk)

	// Peer A: a genuine, successful conversion, parked and still needed.
	converted, sinkErr := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, sinkErr, "sanity: A's delivery must convert cleanly, or this test asserts nothing about protecting it")
	require.True(t, converted)

	record, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees, "sanity: A's delivery must have produced subtrees, or this test asserts nothing")

	// Peer B: an unrelated, failed delivery of the SAME hash. converted=false
	// is exactly what B's own sink call would have returned — see
	// pipelineBlockSink's every error-path return — so this is what the wire
	// layer's deleteOrphanedBody would actually pass through for B's failure.
	require.NoError(t, sm.pipelineBlockDelete(*blk.Hash(), false))

	for _, h := range record.Subtrees {
		for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
			exists, existsErr := store.Exists(ctx, h[:], ft)
			require.NoError(t, existsErr)
			require.True(t, exists,
				"peer B's own failed delivery (converted=false) must never delete peer A's DIFFERENT, still-parked delivery's subtree file %s for subtree %s", ft, h)
		}
	}
}
