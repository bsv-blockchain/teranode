package netsync

import (
	"bytes"
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestBlockPark_RecoveryAdoptsAConvertedRecord is Task 5. Before this task,
// Recover's case for a ".block" file discarded it unconditionally — see that
// case's own comment in block_park.go — so a block the pipeline had already
// converted, but whose parent had not yet committed, vanished on every
// restart: not adopted, and (before this case even existed) not revisited by
// anything else in the loop either. The download walk paid to fetch it again.
//
// This drives the actual conversion path, pipelineBlockSink, over a real
// file-backed store — not a hand-built record — so what Recover reads back on
// the way in is exactly what WriteConvertedBlock put on disk on the way out.
// Recover needs a real directory to scan (os.ReadDir), which is why this test
// cannot use the in-memory blob store the way the rest of this package's
// pipeline tests do; see pipeline_park_test.go's own note on that store
// ignoring the subdirectory/hash-prefix options parkOpts always passes.
//
// The "restart" is a second, independent blockPark built over the same store
// and the same settings, sharing nothing in memory with the one that wrote
// the record — the same shape TestBlockPark_RecoveryGivesUpRatherThanHoldingUpTheStart
// and TestBlockPark_RecoveryKeepsABlockItCouldNotRead already use.
func TestBlockPark_RecoveryAdoptsAConvertedRecord(t *testing.T) {
	ctx := context.Background()

	root := t.TempDir()

	storeURL, err := url.Parse("file://" + root)
	require.NoError(t, err)

	// The subtree writer stamps every file it writes with a delete-at-height
	// (subtree_writer.go), and the real file store needs a deletion scheduler
	// configured before it will honour one at all — see
	// TestBlockPark_NeverSchedulesAParkedBlobForDeletion's own use of this
	// recordingDeletionScheduler for the same reason. What that scheduler does
	// with the booking is irrelevant here: this test is about the converted
	// record recovery reads back, not about DAH accounting.
	store, err := file.New(ulogger.TestLogger{}, storeURL,
		options.WithBlobDeletionScheduler(&recordingDeletionScheduler{}),
		options.WithStoreType(storetypes.TEMPSTORE),
	)
	require.NoError(t, err)

	// Mirrors newPipelineManager's fixture (pipeline_sink_test.go): a
	// checkpoint set high enough that the fixture block, at height 1, reads as
	// below it, which is what makes pipelineBlockSink eligible to convert
	// rather than fall back to the whole-block path.
	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1000}}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &params
	tSettings.BlockAssembly.MaximumMerkleItemsPerSubtree = 8
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.Legacy.TempStore = storeURL

	bcStoreURL, err := url.Parse("sqlitememory:///pipeline_park_recovery")
	require.NoError(t, err)

	bcStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, bcStoreURL, tSettings)
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
		// SupportsOutpointOnlySpend() true, the other conjunct legacyUnified
		// needs alongside BelowCheckpoint; a nil store would read false and
		// pipelineBlockSink would fall back instead of converting.
		utxoStore: &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
	}

	sm.blockPark = mustNewBlockPark(t, sm.logger, tSettings, store)
	require.NotNil(t, sm.blockPark, "the park must actually be enabled, or this test proves nothing about Recover")

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)

	header := &blk.MsgBlock().Header
	body := blockBodyBytes(t, blk)
	hash := *blk.Hash()

	converted, err := sm.pipelineBlockSink(hash, header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing about recovery")

	// Ground truth for the record's own size, read back through the store the
	// same way handleBlockOnDiskMsg does (convertedRecordSize) rather than
	// assumed from what pipelineBlockSink was handed.
	expectedSize, exists, err := sm.blockPark.convertedRecordSize(ctx, hash)
	require.NoError(t, err)
	require.True(t, exists, "sanity: the converted record must be on disk before recovery can be asked to find it")
	require.NotEqual(t, int64(len(body)), expectedSize,
		"sanity: a converted record must be a different size than the whole block, or the size assertion below would pass for the wrong reason")

	// The restart. A fresh blockPark over the same store and directory,
	// standing in for the process that comes back up and finds this file
	// already there.
	restarted := mustNewBlockPark(t, sm.logger, tSettings, store)
	require.NotNil(t, restarted)

	restarted.Recover(ctx, sm.subtreeStore)

	entry, ok := restarted.Take(hash)
	require.True(t, ok, "recovery must adopt a converted record left by a previous run, not silently drop it")
	require.Equal(t, header.PrevBlock.String(), entry.prevBlock.String(),
		"the entry's previous-block hash must come from the record's own header, not a wire block that does not exist on disk")
	require.Equal(t, expectedSize, entry.size,
		"the entry's size must come from the converted record, not the whole block")

	require.True(t, parkHasConvertedRecord(t, restarted, hash), "recovery adopting a converted record must not delete it")
}

// TestBlockPark_RecoveryDiscardsAConvertedRecordWhoseSubtreeFilesAreGone is
// fix-round item 5. The converted record itself carries no delete-at-height
// and so never expires, but the subtree files it names do
// (subtree_writer.go), so a record can outlive them — reachable after a long
// enough park plus a restart, or simply the retention window elapsing while
// the record sat waiting for a parent that never came. Before this fix,
// Recover adopted the record regardless; committing it would then fail inside
// validation once the missing subtree was reached, landing on the same
// destructive path a genuinely bad block does.
//
// This builds a real converted record and then deletes its first subtree's
// structure file before Recover runs, standing in for that file having
// already reached its delete-at-height. Recovery must discard the record
// rather than adopt a commit that can only fail, and must log exactly once so
// a soak can tell whether this path ever actually fires.
func TestBlockPark_RecoveryDiscardsAConvertedRecordWhoseSubtreeFilesAreGone(t *testing.T) {
	ctx := context.Background()

	root := t.TempDir()

	storeURL, err := url.Parse("file://" + root)
	require.NoError(t, err)

	store, err := file.New(ulogger.TestLogger{}, storeURL,
		options.WithBlobDeletionScheduler(&recordingDeletionScheduler{}),
		options.WithStoreType(storetypes.TEMPSTORE),
	)
	require.NoError(t, err)

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 1000}}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &params
	tSettings.BlockAssembly.MaximumMerkleItemsPerSubtree = 8
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.Legacy.TempStore = storeURL

	bcStoreURL, err := url.Parse("sqlitememory:///pipeline_park_recovery_gone_subtree")
	require.NoError(t, err)

	bcStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, bcStoreURL, tSettings)
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
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
	}

	sm.blockPark = mustNewBlockPark(t, sm.logger, tSettings, store)
	require.NotNil(t, sm.blockPark, "the park must actually be enabled, or this test proves nothing about Recover")

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	// Below-checkpoint gating now also demands the header be PROVEN (an ancestry
	// proof to a pinned checkpoint hash, GHSA-gggq-8f59-4jm9), not merely below the
	// checkpoint height, or pipelineBlockSink writes .subtreeToCheck instead of the
	// .subtree this test deletes below — and hasCompleteRecord accepts either type,
	// so the sanity that "the file recovery must notice missing" is only true when
	// the deleted type is the one actually written. See proveBlockOrigin.
	proveBlockOrigin(t, sm, blk)

	body := blockBodyBytes(t, blk)
	hash := *blk.Hash()

	converted, err := sm.pipelineBlockSink(hash, &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing about recovery")

	record, err := sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees, "sanity: the converted record must name at least one subtree, or this test asserts nothing")

	firstSubtree := record.Subtrees[0]

	// Below the checkpoint with quick validation on, subtreeWriter's structure
	// file is FileTypeSubtree — see its own doc comment. Removing exactly that
	// file, not the whole subtree's data/meta, is what Recover's own check
	// looks at.
	require.NoError(t, store.Del(ctx, firstSubtree[:], fileformat.FileTypeSubtree),
		"sanity: the file recovery must notice missing has to actually be gone")

	stillExists, err := store.Exists(ctx, firstSubtree[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, stillExists, "sanity: the subtree file must actually be gone before Recover runs")

	warnings := &warnCaptureLogger{}

	restarted := mustNewBlockPark(t, warnings, tSettings, store)
	require.NotNil(t, restarted)

	restarted.Recover(ctx, sm.subtreeStore)

	_, ok := restarted.Take(hash)
	require.False(t, ok, "a converted record whose first subtree is gone must not be adopted; committing it could only fail inside validation")

	require.False(t, parkHasConvertedRecord(t, restarted, hash), "the record itself must be discarded too, not left to be found again on every future restart")

	require.NotEmpty(t, warnings.warnings, "recovery must log when this fires, so a soak can tell whether it ever does")

	found := false

	for _, w := range warnings.warnings {
		if strings.Contains(w, "gone") {
			found = true
			break
		}
	}

	require.True(t, found, "the logged warning must describe a missing subtree, got %v", warnings.warnings)
}
