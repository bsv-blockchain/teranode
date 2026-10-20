package seeder

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestProcessHeaders_ReRun_IsIdempotentOnAlreadyStoredBlock is the
// reproduction for the first blocker in the -force recovery: a -force re-run
// still runs processHeaders, which re-stores every block from the headers file,
// including ones a prior run already stored. StoreBlock reports those as
// ErrBlockExists; if that is fatal the run fails before it reaches the
// BlockAssembler checkpoint write. It must be treated as an idempotent replay.
func TestProcessHeaders_ReRun_IsIdempotentOnAlreadyStoredBlock(t *testing.T) {
	ctx := context.Background()
	store := newTestBlockchainStore(t)

	genesis, err := store.GetBlockByID(ctx, 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytes(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	coinbase := makeCoinbaseTx(t, 1, 1)
	block1 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000001,
			Nonce:          1,
			HashPrevBlock:  genesis.Header.Hash(),
			HashMerkleRoot: coinbase.TxIDChainHash(),
			Bits:           genesis.Header.Bits,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		Height:           1,
	}
	block1Record, err := blockToIndexBytes(t, block1, block1.Header.Hash())
	require.NoError(t, err)

	path := writeUtxoHeadersFile(t, 1, genesisRecord, block1Record)

	// First pass: a complete, successful header import.
	require.NoError(t, processHeaders(ctx, ulogger.TestLogger{}, store, path))

	// Recovery re-run against the identical, complete file: this is exactly
	// what re-running with -force does. It must not fail on blocks the first
	// pass already stored.
	err = processHeaders(ctx, ulogger.TestLogger{}, store, path)
	require.NoError(t, err, "a re-run over already-stored blocks must be idempotent, not fatal")
}

// writeUtxoSetPreamble writes a minimal but complete .utxo-set file
// containing only the preamble readUTXOSetTip depends on (file header, tip
// hash, tip height, previous-block-hash placeholder) and no UTXO records.
// Sufficient for exercising the cheap checkpoint-recovery path, which never
// reads past the preamble.
func writeUtxoSetPreamble(t *testing.T, tipHash chainhash.Hash, tipHeight uint32) string {
	t.Helper()

	header := fileformat.NewHeader(fileformat.FileTypeUtxoSet)

	heightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(heightBytes, tipHeight)

	var previousBlockHash [32]byte

	data := make([]byte, 0, len(header.Bytes())+len(tipHash)+len(heightBytes)+len(previousBlockHash))
	data = append(data, header.Bytes()...)
	data = append(data, tipHash[:]...)
	data = append(data, heightBytes...)
	data = append(data, previousBlockHash[:]...)

	path := filepath.Join(t.TempDir(), "test.utxo-set")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

// TestReadUTXOSetTip_ReadsPreambleWithoutFullImport is the positive-path
// coverage for the cheapest of the three recovery options the review offered:
// the utxo-set file's own preamble already carries the tip hash and height,
// so the missing checkpoint can be recovered by reading a handful of bytes -
// no re-import of the (already complete) UTXO set required.
func TestReadUTXOSetTip_ReadsPreambleWithoutFullImport(t *testing.T) {
	var tipHash chainhash.Hash
	tipHash[0] = 0xab
	tipHash[31] = 0xcd

	path := writeUtxoSetPreamble(t, tipHash, 42)

	tip, err := readUTXOSetTip(path)
	require.NoError(t, err)
	require.Equal(t, uint32(42), tip.height)
	require.Equal(t, tipHash.String(), tip.hash.String())
}

// warnRecorder is a test logger that remembers its warnings.
type warnRecorder struct {
	ulogger.TestLogger
	warnings []string
}

func (w *warnRecorder) Warnf(format string, args ...interface{}) {
	w.warnings = append(w.warnings, fmt.Sprintf(format, args...))
}

// markImported records a completed import the way processUTXOs does: the
// lastProcessed.dat height marker plus the imported-tip record with the hash.
func markImported(t *testing.T, ctx context.Context, blockStore blob.Store, height uint32, hash chainhash.Hash) {
	t.Helper()

	require.NoError(t, blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte(fmt.Sprintf("%d\n", height)),
		bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix()))
	require.NoError(t, blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte(fmt.Sprintf("%d %s\n", height, hash.String())),
		bloboptions.WithFilename(importedTipFilename), bloboptions.WithNoHashPrefix()))
}

// TestCheckpointRecoverable_MarkerWithoutCheckpoint_ReportsRecoverable covers
// the decision the -recover-checkpoint path uses to avoid re-importing an already-complete
// UTXO set: lastProcessed.dat present with no BlockAssembler checkpoint is
// exactly the state a cheap, preamble-only recovery applies to.
func TestCheckpointRecoverable_MarkerWithoutCheckpoint_ReportsRecoverable(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	markImported(t, ctx, blockStore, 1, chainhash.Hash{})

	utxoFile := writeUtxoSetPreamble(t, chainhash.Hash{}, 1)

	recoverable, err := checkpointRecoverable(ctx, ulogger.TestLogger{}, blockStore, blockchainStore, utxoFile)
	require.NoError(t, err)
	require.True(t, recoverable)
}

// TestCheckpointRecoverable_NoMarker_NotRecoverable covers the fresh-seed
// case: no lastProcessed.dat means there is nothing to recover - the normal
// full import must run instead.
func TestCheckpointRecoverable_NoMarker_NotRecoverable(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	utxoFile := writeUtxoSetPreamble(t, chainhash.Hash{}, 1)

	recoverable, err := checkpointRecoverable(ctx, ulogger.TestLogger{}, blockStore, blockchainStore, utxoFile)
	require.NoError(t, err)
	require.False(t, recoverable)
}

// TestCheckpointRecoverable_MarkerWithCheckpoint_NotRecoverable covers the
// cleanly-completed case: both the marker and the checkpoint are present, so
// there is nothing left to recover.
func TestCheckpointRecoverable_MarkerWithCheckpoint_NotRecoverable(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	markImported(t, ctx, blockStore, 1, chainhash.Hash{})

	block := storeBlockAboveGenesis(t, ctx, blockchainStore)
	require.NoError(t, writeBlockAssemblerState(ctx, ulogger.TestLogger{}, blockchainStore, &utxoSetTip{hash: *block.Hash(), height: 1}))

	utxoFile := writeUtxoSetPreamble(t, chainhash.Hash{}, 1)

	recoverable, err := checkpointRecoverable(ctx, ulogger.TestLogger{}, blockStore, blockchainStore, utxoFile)
	require.NoError(t, err)
	require.False(t, recoverable)
}

// TestCheckpointRecoverable_MarkerHeightMismatchesUtxoFile_NotRecoverable is
// the reproduction for the snapshot-agnostic marker regression: the marker only
// proves *an* import finished, not that it was for the utxoFile this run was
// invoked with. Sequence: seed from snapshot A (tip 800000), the header pass
// rejects A's truncated utxo-headers file after the UTXO pass already wrote
// lastProcessed.dat, the operator fetches a complete snapshot B (tip 810000)
// and re-runs with -recover-checkpoint. checkpointRecoverable must see that the marker's
// recorded height does not match snapshot B's preamble height and refuse the
// cheap path - taking it here would commit a BlockAssembler checkpoint at B's
// tip over a UTXO store that still holds A's set.
func TestCheckpointRecoverable_MarkerHeightMismatchesUtxoFile_NotRecoverable(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	// Marker left behind by the import of snapshot A (tip height 800000).
	err := blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte("800000\n"),
		bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix())
	require.NoError(t, err)

	// The -force re-run is now pointed at snapshot B (tip height 810000).
	utxoFile := writeUtxoSetPreamble(t, chainhash.Hash{}, 810000)

	recoverable, err := checkpointRecoverable(ctx, ulogger.TestLogger{}, blockStore, blockchainStore, utxoFile)
	require.NoError(t, err)
	require.False(t, recoverable, "a marker for a different snapshot must not be treated as proof this utxoFile was already imported")
}

// forceSeederSettings builds settings for a processUTXOs run against an
// in-memory UTXO and blockchain store and an on-disk block store, so the
// lastProcessed.dat marker is a real blob the test can pre-populate and read
// back.
func forceSeederSettings(t *testing.T) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	blockchainStoreURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings.BlockChain.StoreURL = blockchainStoreURL

	utxoStoreURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings.UtxoStore.UtxoStore = utxoStoreURL

	blockStoreURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	tSettings.Block.BlockStore = blockStoreURL

	return tSettings
}

func openTestBlockStore(t *testing.T, tSettings *settings.Settings) blob.Store {
	t.Helper()

	store, err := blob.NewStore(ulogger.TestLogger{}, tSettings.Block.BlockStore)
	require.NoError(t, err)

	return store
}

func readMarker(t *testing.T, ctx context.Context, store blob.Store) string {
	t.Helper()

	b, err := store.Get(ctx, nil, fileformat.FileTypeDat, bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix())
	require.NoError(t, err)

	return strings.TrimSpace(string(b))
}

// TestProcessUTXOs_Force_OverExistingMarkerOfOtherSnapshot_ReimportsAndRewritesMarker
// covers two things at once: a marker recording a different snapshot's height
// must not take the cheap recovery path (the UTXO store would otherwise keep
// the other snapshot's set), and the full re-import that follows must be able
// to rewrite lastProcessed.dat, which already exists. Without
// WithAllowOverwrite on the marker write the re-import fails with
// BlobAlreadyExists after the whole import has run.
func TestProcessUTXOs_Force_OverExistingMarkerOfOtherSnapshot_ReimportsAndRewritesMarker(t *testing.T) {
	ctx := context.Background()
	tSettings := forceSeederSettings(t)
	blockStore := openTestBlockStore(t, tSettings)

	require.NoError(t, blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte("800000\n"),
		bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix()))

	utxoFile := writeUtxoSetPreamble(t, chainhash.Hash{}, 810000)

	tip, err := processUTXOs(ctx, ulogger.TestLogger{}, tSettings, newTestBlockchainStore(t), utxoFile, "", true, false)
	require.NoError(t, err)
	require.NotNil(t, tip)
	require.Equal(t, uint32(810000), tip.height)
	require.Equal(t, "810000", readMarker(t, ctx, blockStore), "the full re-import must have rewritten the marker")
}

// TestProcessUTXOs_RecoverCheckpoint_CompletedImport_RecoversTipWithoutImport
// covers the cheap recovery: the marker and the tip record both name the
// utxo-set file's tip, and no checkpoint exists, so the tip comes straight from
// the preamble. The file holds a UTXO that a re-import would store, and the
// store must not have it: that is how this test tells recovery from a re-import.
func TestProcessUTXOs_RecoverCheckpoint_CompletedImport_RecoversTipWithoutImport(t *testing.T) {
	ctx := context.Background()
	tSettings := forceSeederSettings(t)
	pointUtxoStoreAt(t, tSettings, "recover_completed")
	blockStore := openTestBlockStore(t, tSettings)

	utxoFile, txid := snapshotWithOneTx(t, "recover-completed")

	var tipHash chainhash.Hash
	tipHash[0] = 0xcc // writeCompleteSnapshotFile's tip

	markImported(t, ctx, blockStore, 200, tipHash)

	tip, err := processUTXOs(ctx, ulogger.TestLogger{}, tSettings, newTestBlockchainStore(t), utxoFile, "", false, true)
	require.NoError(t, err)
	require.NotNil(t, tip)
	require.Equal(t, uint32(200), tip.height)
	require.Equal(t, tipHash.String(), tip.hash.String())
	require.False(t, utxoStoreHasTx(ctx, t, tSettings, "recover_completed", &txid), "-recover-checkpoint must not import")
}

// TestProcessUTXOs_RecoverCheckpoint_IdentityMismatch_RefusesAndWritesNothing
// covers the guard on the dangerous path: a marker whose height disagrees with
// the utxo-set file, a tip record whose hash disagrees (a reorg sibling at the
// same height), and a missing tip record (an import by an older seeder). Each
// must refuse, import nothing and leave the marker alone.
func TestProcessUTXOs_RecoverCheckpoint_IdentityMismatch_RefusesAndWritesNothing(t *testing.T) {
	var tipHash chainhash.Hash
	tipHash[0] = 0xcc // writeCompleteSnapshotFile's tip

	var siblingHash chainhash.Hash
	siblingHash[0] = 0xee

	tests := []struct {
		name  string
		setup func(t *testing.T, ctx context.Context, blockStore blob.Store)
	}{
		{"marker height differs", func(t *testing.T, ctx context.Context, bs blob.Store) { markImported(t, ctx, bs, 199, tipHash) }},
		{"marker height differs but tip record matches", func(t *testing.T, ctx context.Context, bs blob.Store) {
			markImported(t, ctx, bs, 200, tipHash)
			require.NoError(t, bs.Set(ctx, nil, fileformat.FileTypeDat, []byte("199\n"),
				bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix(), bloboptions.WithAllowOverwrite(true)))
		}},
		{"tip hash differs at same height", func(t *testing.T, ctx context.Context, bs blob.Store) { markImported(t, ctx, bs, 200, siblingHash) }},
		{"tip record height differs", func(t *testing.T, ctx context.Context, bs blob.Store) {
			markImported(t, ctx, bs, 200, tipHash)
			require.NoError(t, bs.Set(ctx, nil, fileformat.FileTypeDat, []byte("199 "+tipHash.String()+"\n"),
				bloboptions.WithFilename(importedTipFilename), bloboptions.WithNoHashPrefix(), bloboptions.WithAllowOverwrite(true)))
		}},
		{"no tip record", func(t *testing.T, ctx context.Context, bs blob.Store) {
			require.NoError(t, bs.Set(ctx, nil, fileformat.FileTypeDat, []byte("200\n"),
				bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix()))
		}},
		{"unparsable tip record", func(t *testing.T, ctx context.Context, bs blob.Store) {
			markImported(t, ctx, bs, 200, tipHash)
			require.NoError(t, bs.Set(ctx, nil, fileformat.FileTypeDat, []byte("garbage"),
				bloboptions.WithFilename(importedTipFilename), bloboptions.WithNoHashPrefix(), bloboptions.WithAllowOverwrite(true)))
		}},
		{"no marker", func(t *testing.T, ctx context.Context, bs blob.Store) {}},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			tSettings := forceSeederSettings(t)
			name := fmt.Sprintf("recover_refuse_%d", i)
			pointUtxoStoreAt(t, tSettings, name)
			blockStore := openTestBlockStore(t, tSettings)

			utxoFile, txid := snapshotWithOneTx(t, name)

			tc.setup(t, ctx, blockStore)

			tip, err := processUTXOs(ctx, ulogger.TestLogger{}, tSettings, newTestBlockchainStore(t), utxoFile, "", false, true)
			require.Error(t, err)
			require.ErrorContains(t, err, "-recover-checkpoint refused")
			require.ErrorContains(t, err, "-force")
			require.Nil(t, tip, "no tip means no checkpoint is written")
			require.False(t, utxoStoreHasTx(ctx, t, tSettings, name, &txid), "a refused recovery must not import either")
		})
	}
}

// TestProcessUTXOs_RecoverCheckpoint_CheckpointExists_Refuses: with a
// checkpoint already in place there is nothing to recover and the flag must not
// hand back a tip that would overwrite it.
func TestProcessUTXOs_RecoverCheckpoint_CheckpointExists_Refuses(t *testing.T) {
	ctx := context.Background()
	tSettings := forceSeederSettings(t)
	pointUtxoStoreAt(t, tSettings, "recover_checkpoint_exists")
	blockStore := openTestBlockStore(t, tSettings)

	utxoFile, _ := snapshotWithOneTx(t, "recover-exists")

	var tipHash chainhash.Hash
	tipHash[0] = 0xcc

	markImported(t, ctx, blockStore, 200, tipHash)

	blockchainStore := newTestBlockchainStore(t)
	block := storeBlockAboveGenesis(t, ctx, blockchainStore)
	require.NoError(t, writeBlockAssemblerState(ctx, ulogger.TestLogger{}, blockchainStore, &utxoSetTip{hash: *block.Hash(), height: 1}))

	tip, err := processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, utxoFile, "", false, true)
	require.ErrorContains(t, err, "-recover-checkpoint refused")
	require.Nil(t, tip)
}

// TestProcessUTXOs_WipedUtxoStore_ForceReimports is the end-to-end form of the
// failure that motivated splitting the flags. A seed completes; the operator
// wipes and recreates the UTXO store, as the seeder's own messages tell them to;
// the block-store marker survives; a plain re-run is refused and points at
// -force; -force must put the UTXOs into the new store.
func TestProcessUTXOs_WipedUtxoStore_ForceReimports(t *testing.T) {
	ctx := context.Background()
	tSettings := forceSeederSettings(t)
	blockchainStore := newTestBlockchainStore(t)

	utxoFile, txid := snapshotWithOneTx(t, "wiped")

	// First seed into the original UTXO store.
	pointUtxoStoreAt(t, tSettings, "wiped_original")

	tip, err := processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, utxoFile, "", false, false)
	require.NoError(t, err)
	require.NotNil(t, tip)
	require.True(t, utxoStoreHasTx(ctx, t, tSettings, "wiped_original", &txid))

	// Wipe and recreate the UTXO store: a different, empty database. The block
	// store, and so the marker, is untouched. The checkpoint was never written
	// by processUTXOs, which is the state a plain re-run is refused in.
	pointUtxoStoreAt(t, tSettings, "wiped_recreated")
	require.False(t, utxoStoreHasTx(ctx, t, tSettings, "wiped_recreated", &txid))

	_, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, utxoFile, "", false, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "-force")

	tip, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, utxoFile, "", true, false)
	require.NoError(t, err)
	require.NotNil(t, tip)
	require.True(t, utxoStoreHasTx(ctx, t, tSettings, "wiped_recreated", &txid), "-force must have re-imported the UTXOs into the recreated store")
}

// TestProcessUTXOs_Reimport_InvalidatesTipRecordUntilComplete: a re-import that
// dies part-way must not leave the previous import's tip record vouching for a
// half-written UTXO store.
func TestProcessUTXOs_Reimport_ClearsTipRecordBeforeImporting(t *testing.T) {
	ctx := context.Background()
	tSettings := forceSeederSettings(t)
	pointUtxoStoreAt(t, tSettings, "reimport_clears")
	blockStore := openTestBlockStore(t, tSettings)

	// A truncated snapshot: the import fails after the tip record would have
	// been cleared.
	utxoFile, _ := snapshotWithOneTx(t, "reimport-clears")
	data, err := os.ReadFile(utxoFile)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(utxoFile, data[:len(data)-8], 0o600))

	var tipHash chainhash.Hash
	tipHash[0] = 0xcc

	markImported(t, ctx, blockStore, 200, tipHash)

	_, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, newTestBlockchainStore(t), utxoFile, "", true, false)
	require.Error(t, err, "the truncated snapshot must fail the import")

	exists, err := blockStore.Exists(ctx, nil, fileformat.FileTypeDat, bloboptions.WithFilename(importedTipFilename), bloboptions.WithNoHashPrefix())
	require.NoError(t, err)
	require.False(t, exists, "a failed re-import must leave no tip record behind")
}

// TestProcessUTXOs_Reimport_ClearsLastProcessedBeforeImporting: lastProcessed.dat
// lives in the block store and survives a UTXO-store wipe, so a re-import that
// dies part-way must not leave it behind vouching for a half-written store.
func TestProcessUTXOs_Reimport_ClearsLastProcessedBeforeImporting(t *testing.T) {
	ctx := context.Background()
	tSettings := forceSeederSettings(t)
	pointUtxoStoreAt(t, tSettings, "reimport_clears_marker")
	blockStore := openTestBlockStore(t, tSettings)

	utxoFile, _ := snapshotWithOneTx(t, "reimport-clears-marker")
	data, err := os.ReadFile(utxoFile)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(utxoFile, data[:len(data)-8], 0o600))

	var tipHash chainhash.Hash
	tipHash[0] = 0xcd

	markImported(t, ctx, blockStore, 200, tipHash)

	_, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, newTestBlockchainStore(t), utxoFile, "", true, false)
	require.Error(t, err, "the truncated snapshot must fail the import")

	exists, err := blockStore.Exists(ctx, nil, fileformat.FileTypeDat, bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix())
	require.NoError(t, err)
	require.False(t, exists, "a failed re-import must leave no lastProcessed.dat behind")
}

// TestSeeder_ForceAndRecoverCheckpoint_AreMutuallyExclusive: -force re-imports,
// -recover-checkpoint never imports; asking for both is a mistake, not a mode.
func TestSeeder_ForceAndRecoverCheckpoint_AreMutuallyExclusive(t *testing.T) {
	err := Seeder(ulogger.TestLogger{}, forceSeederSettings(t), t.TempDir(), "abc", true, true, true, true, true)
	require.ErrorContains(t, err, "mutually exclusive")
}

// TestSeeder_SkipUTXOs_RejectsForceAndRecoverCheckpoint: both flags do their
// work inside the UTXO pass, so combining either with -skipUTXOs must be
// refused rather than silently doing nothing.
func TestSeeder_SkipUTXOs_RejectsForceAndRecoverCheckpoint(t *testing.T) {
	tests := []struct {
		name              string
		force             bool
		recoverCheckpoint bool
	}{
		{"recover-checkpoint", false, true},
		{"force", true, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Seeder(ulogger.TestLogger{}, forceSeederSettings(t), t.TempDir(), "abc", true, true, tc.force, tc.recoverCheckpoint, true)
			require.ErrorContains(t, err, "-skipUTXOs")
		})
	}
}

// TestCheckpointRecoverable_UnparsableMarker_NotRecoverable: a marker whose
// contents are does not hold a height proves nothing about this utxoFile, so it is not
// recoverable, and the operator is told why rather than getting no signal.
func TestCheckpointRecoverable_UnparsableMarker_NotRecoverable(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	require.NoError(t, blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte("does not hold a height\n"),
		bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix()))

	utxoFile := writeUtxoSetPreamble(t, chainhash.Hash{}, 1)

	logger := &warnRecorder{}

	recoverable, err := checkpointRecoverable(ctx, logger, blockStore, blockchainStore, utxoFile)
	require.NoError(t, err)
	require.False(t, recoverable)
	require.Len(t, logger.warnings, 1, "an unparsable marker must be logged, not silently ignored")
	require.Contains(t, logger.warnings[0], "does not hold a height")
}

// TestCheckSkipUTXOImport_MarkerWithoutCheckpoint_SteersToForce guards the
// error message operators actually see after wiping the UTXO store: it must
// name -force as the safe choice and mention -recover-checkpoint only as the
// opt-in for an operator who is certain the UTXO store is intact.
func TestCheckSkipUTXOImport_MarkerWithoutCheckpoint_SteersToForce(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	markLastProcessed(t, ctx, blockStore)

	_, err := checkSkipUTXOImport(ctx, blockStore, blockchainStore)
	require.Error(t, err)

	msg := err.Error()
	require.Contains(t, msg, "-force to re-import the UTXO set")
	require.Contains(t, msg, "wiped or recreated")
	require.Contains(t, msg, "-recover-checkpoint")
	require.Less(t, strings.Index(msg, "-force"), strings.Index(msg, "-recover-checkpoint"), "the safe flag must be named first")
}

// pointUtxoStoreAt switches the settings to the named sqlite UTXO store, a file
// under the test's data folder. (sqlitememory gives every store a random private
// database, so a test could never look inside the one processUTXOs opens.)
// Distinct names are distinct databases, so changing the name between two runs
// is what "wipe and recreate the UTXO store" looks like to the seeder.
func pointUtxoStoreAt(t *testing.T, tSettings *settings.Settings, name string) {
	t.Helper()

	u, err := url.Parse("sqlite:///" + name)
	require.NoError(t, err)

	tSettings.UtxoStore.UtxoStore = u
}

// utxoStoreHasTx reports whether the named sqlite UTXO store holds txid.
func utxoStoreHasTx(ctx context.Context, t *testing.T, tSettings *settings.Settings, name string, txid *chainhash.Hash) bool {
	t.Helper()

	u, err := url.Parse("sqlite:///" + name)
	require.NoError(t, err)

	store, err := utxosql.New(ctx, ulogger.TestLogger{}, tSettings, u)
	require.NoError(t, err)

	got, err := store.Get(ctx, txid)
	if err != nil {
		require.ErrorIs(t, err, errors.ErrTxNotFound)

		return false
	}

	return got != nil && got.Tx != nil
}

// snapshotWithOneTx writes a complete snapshot (tip height 200) holding a
// single tx and returns the file path and that tx's id.
func snapshotWithOneTx(t *testing.T, seed string) (string, chainhash.Hash) {
	t.Helper()

	w := benchWrappers(seed, 1, 0)

	return writeCompleteSnapshotFile(t, w), w[0].TxID
}

// TestProcessUTXOs_Force_OverMatchingMarker_ReimportsUTXOs is the regression
// test for the -force data-loss path: -force always re-imports the UTXO set,
// even when lastProcessed.dat and the imported-tip record already name this
// snapshot's tip. They live in the block store, so they prove nothing about what
// the UTXO store currently holds; skipping the import on them left a node with a
// BlockAssembler checkpoint over an empty UTXO store.
func TestProcessUTXOs_Force_OverMatchingMarker_ReimportsUTXOs(t *testing.T) {
	ctx := context.Background()
	tSettings := forceSeederSettings(t)
	pointUtxoStoreAt(t, tSettings, "force_matching_marker")
	blockStore := openTestBlockStore(t, tSettings)

	utxoFile, txid := snapshotWithOneTx(t, "force-matching")

	var tipHash chainhash.Hash
	tipHash[0] = 0xcc // writeCompleteSnapshotFile's tip

	markImported(t, ctx, blockStore, 200, tipHash)

	tip, err := processUTXOs(ctx, ulogger.TestLogger{}, tSettings, newTestBlockchainStore(t), utxoFile, "", true, false)
	require.NoError(t, err)
	require.NotNil(t, tip)
	require.True(t, utxoStoreHasTx(ctx, t, tSettings, "force_matching_marker", &txid), "-force must import the UTXOs, not skip on the marker")
}
