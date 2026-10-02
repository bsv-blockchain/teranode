package netsync

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// minedBlocks returns n solved regtest blocks building on each other, so the
// park's proof-of-work and merkle checks are exercised against blocks that
// really do satisfy them.
func minedBlocks(t *testing.T, n int) []*bsvutil.Block {
	t.Helper()

	chainParams := chaincfg.RegressionNetParams

	address, _, err := GenerateAnyoneCanspendAddress(&chainParams)
	require.NoError(t, err)

	blocks := make([]*bsvutil.Block, 0, n)
	prev := bsvutil.NewBlock(chainParams.GenesisBlock)

	for i := 0; i < n; i++ {
		block, err := CreateBlock(prev, nil, 2, nullTime, address, []wire.TxOut{}, &chainParams)
		require.NoError(t, err)

		blocks = append(blocks, block)
		prev = block
	}

	return blocks
}

// newTestPark builds a park over a real file blob store rooted in a temp
// directory, and returns the park plus that directory. query is appended to the
// store URL so a test can prove the layout survives a sharded store.
func newTestPark(t *testing.T, query string) (*blockPark, string) {
	t.Helper()

	root := t.TempDir()

	storeURL, err := url.Parse("file://" + root + query)
	require.NoError(t, err)

	store, err := blob.NewStore(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.TempStore = storeURL

	park := mustNewBlockPark(t, ulogger.TestLogger{}, tSettings, store)
	require.NotNil(t, park, "the park must be built for a file store")

	return park, filepath.Join(root, parkSubDirectory)
}

// parkDirEntries lists the park directory, tolerating it not existing yet.
func parkDirEntries(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}

	require.NoError(t, err)

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}

	return names
}

// TestBlockPark_RoundTripsThroughAShardedStore proves the layout does not
// depend on the temp_store URL. A store built with hashPrefix would otherwise
// put the blobs in shard subdirectories, where the flat recovery scan finds
// nothing at all and every parked block leaks on every restart.
func TestBlockPark_RoundTripsThroughAShardedStore(t *testing.T) {
	park, dir := newTestPark(t, "?hashPrefix=2")

	prev := chainhash.Hash{0x0a}
	hash := parkedRecord(t, park, prev, 1)

	for _, name := range parkDirEntries(t, dir) {
		require.True(t, strings.HasPrefix(name, hash.String()),
			"the park layout must be flat whatever the store URL says, not %s", name)
	}

	got, err := park.ReadConverted(context.Background(), hash)
	require.NoError(t, err)
	require.True(t, got.Header.Hash().IsEqual(&hash))

	// And the recovery scan finds it.
	fresh, _ := newTestPark(t, "?hashPrefix=2")
	fresh.dir = dir
	fresh.store = park.store
	fresh.Recover(context.Background(), nil)

	require.Equal(t, 1, fresh.Len(), "a sharded store must not hide parked blocks from the restart scan")

	taken := fresh.TakeChildren(prev)
	require.Len(t, taken, 1)
	require.True(t, taken[0].hash.IsEqual(&hash))
	require.True(t, taken[0].prevBlock.IsEqual(&prev),
		"the parent must be read back out of the stored record")
}

// TestBlockPark_SizeAloneNeverRefuses pins what is left once both of the
// park's old bounds are gone: a byte budget, then an entry cap after it. Both
// were removed for the same shape of reason. The byte budget could only be
// evaluated after the block had been downloaded and decoded, so it never saved
// any bandwidth, only threw away a block already in hand. The entry cap could
// refuse the one block that would drain the park whenever there was a hole in
// the run, on every retry, with nothing evicting to make room. What bounds the
// disk now is upstream of both: the download walk's read-ahead depth, in
// blocks, checked before anything is fetched.
func TestBlockPark_SizeAloneNeverRefuses(t *testing.T) {
	park, dir := newTestPark(t, "")

	adoptRecord(t, park, chainhash.Hash{0x99}, 1)

	require.NotEmpty(t, parkDirEntries(t, dir), "the block must be written whatever its size")
	require.Positive(t, park.Bytes(),
		"the byte total is still tracked, for the gauge, it just no longer refuses")
}

// TestBlockPark_RecoversWhatAPreviousRunLeftBehind is the restart case. A crash
// mid-write leaves a dot-prefixed temp file; a sidecar can outlive its block; a
// file can be corrupt. None of those may stop the good blobs behind them from
// being adopted, and none may be left to leak.
func TestBlockPark_RecoversWhatAPreviousRunLeftBehind(t *testing.T) {
	park, dir := newTestPark(t, "")

	prevs := []chainhash.Hash{{0x0b}, {0x0c}}
	hashes := make([]chainhash.Hash, len(prevs))

	for i, prev := range prevs {
		hashes[i] = parkedRecord(t, park, prev, byte(i+1))
	}

	recordFile := func(h chainhash.Hash) string {
		return h.String() + "." + string(fileformat.FileTypeBlock)
	}

	// The wreckage a crash leaves.
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".abcdef.4711.tmp"), []byte("half a block"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "0000000000000000000000000000000000000000000000000000000000000009.msgBlock.sha256"), []byte("orphaned"), 0o600))

	// A record that reads back perfectly and is somebody else's block. That is
	// evidence about the file, so it must be deleted and the block asked for
	// again.
	wrongBlock := chainhash.Hash{0x11, 0x22}
	firstRecord, err := os.ReadFile(filepath.Join(dir, recordFile(hashes[0])))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, recordFile(wrongBlock)), firstRecord, 0o600))

	// A file the store itself will not open — a torn store header, an unreadable
	// disk. The store reports both of those the same way, and the park's error
	// policy reads that as "says nothing about the block", so this one is left
	// where it is rather than deleted. See parkReadFailure.
	unreadable := chainhash.Hash{0x33, 0x44}
	require.NoError(t, os.WriteFile(filepath.Join(dir, recordFile(unreadable)), make([]byte, 200), 0o600))

	fresh, _ := newTestPark(t, "")
	fresh.dir = dir
	fresh.store = park.store
	fresh.Recover(context.Background(), nil)

	require.Equal(t, 2, fresh.Len(), "both good records must be adopted, whatever else is in the directory")

	for i, prev := range prevs {
		taken := fresh.TakeChildren(prev)
		require.Len(t, taken, 1)
		require.True(t, taken[0].hash.IsEqual(&hashes[i]))
		require.Nil(t, taken[0].peer, "a recovered block has no delivering peer")
	}

	names := parkDirEntries(t, dir)
	require.NotContains(t, names, ".abcdef.4711.tmp", "a crash's half-written temp file must be swept")
	require.NotContains(t, names, "0000000000000000000000000000000000000000000000000000000000000009.msgBlock.sha256", "a sidecar whose block is gone must be swept")
	require.NotContains(t, names, recordFile(wrongBlock), "a file that is not the block its name claims must be deleted")
	require.Contains(t, names, recordFile(unreadable),
		"a record the store could not open says nothing about the block, so recovery must leave it for the next start")
}

// parkedRecord writes a converted record for a block whose parent is prev into the park's store,
// as the pipeline sink does, and returns the block's hash. seed keeps two records in one test apart.
func parkedRecord(t *testing.T, park *blockPark, prev chainhash.Hash, seed byte) chainhash.Hash {
	t.Helper()

	header := &model.BlockHeader{Version: 1, HashPrevBlock: &prev, HashMerkleRoot: &chainhash.Hash{seed}}

	blk, err := model.NewBlock(header, coinbaseTx(t), []*chainhash.Hash{{0x50, seed}}, 1, 0, 100, 0)
	require.NoError(t, err)

	hash := *blk.Header.Hash()
	require.NoError(t, park.WriteConvertedBlock(context.Background(), hash, blk))

	return hash
}

// adoptRecord writes a converted record the way parkedRecord does, and then adopts it into the
// SAME park's own index — the way AdoptWritten does for a streamed body — returning the hash.
// It stands in for the old Park()/Admit() one-call setup now that a record's bytes always land
// on disk before anything registers it.
func adoptRecord(t *testing.T, park *blockPark, prev chainhash.Hash, seed byte) chainhash.Hash {
	t.Helper()

	hash := parkedRecord(t, park, prev, seed)

	size, exists, err := park.convertedRecordSize(context.Background(), hash)
	require.NoError(t, err)
	require.True(t, exists)

	require.True(t, park.AdoptWritten(parkedBlock{hash: hash, prevBlock: prev, size: size}))

	return hash
}

// parkHasConvertedRecord answers whether a converted record exists under hash, the same fact
// blockPark.IsConverted used to answer before it was removed as unreachable from every commit
// path: everything parked is a converted record now, so the store's own Exists is asked directly
// here rather than restoring a production method nothing calls any more.
func parkHasConvertedRecord(t *testing.T, park *blockPark, hash chainhash.Hash) bool {
	t.Helper()

	exists, err := park.store.Exists(context.Background(), hash[:], fileformat.FileTypeBlock, parkOpts...)
	require.NoError(t, err)

	return exists
}

// TestBlockPark_IsOffWhenItCannotBeRecovered covers the two settings-only kill
// switches and the store it refuses to run on. A store whose contents cannot be
// listed would leak every parked blob on every restart, so the park declines
// rather than leaking.
func TestBlockPark_RefusesAStoreItCannotRecover(t *testing.T) {
	base := func(t *testing.T) *settings.Settings {
		t.Helper()

		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("file://" + t.TempDir())
		require.NoError(t, err)
		tSettings.Legacy.TempStore = storeURL

		return tSettings
	}

	t.Run("a store that cannot be scanned", func(t *testing.T) {
		tSettings := base(t)

		memURL, err := url.Parse("memory://")
		require.NoError(t, err)
		tSettings.Legacy.TempStore = memURL

		park, err := newBlockPark(ulogger.TestLogger{}, tSettings, blob_memory.New())
		require.Error(t, err, "a store the restart scan cannot enumerate must stop the node, not leak into the park")
		require.Nil(t, park)
	})

	t.Run("no temp store", func(t *testing.T) {
		park, err := newBlockPark(ulogger.TestLogger{}, base(t), nil)
		require.Error(t, err, "the park is not optional, so no temp store is a configuration error")
		require.Nil(t, park)
	})

	t.Run("a nil park behaves as no park", func(t *testing.T) {
		var park *blockPark

		require.False(t, park.Enabled())
		require.Error(t, park.WriteConvertedBlock(context.Background(), chainhash.Hash{}, nil),
			"a nil park must refuse rather than panic")
		require.Zero(t, park.Len())
		require.Zero(t, park.Bytes())
		require.Nil(t, park.TakeChildren(chainhash.Hash{}))
		require.Nil(t, park.StuckCandidates(time.Now(), 8))

		_, ok := park.Take(chainhash.Hash{})
		require.False(t, ok)

		require.NotPanics(t, func() {
			park.Restore(parkedBlock{})
			park.Delete(context.Background(), parkedBlock{})
			park.Recover(context.Background(), nil)
		})
	})
}

// TestBlockPark_ParkStoreDeadlineIsTheOneThatCounts pins the floor on the store
// deadline. A zero or negative deadline would fail every store operation
// instantly, so a misconfigured setting must not switch parking off by accident.
func TestBlockPark_ParkStoreDeadlineIsTheOneThatCounts(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	tSettings.Legacy.TempStore = storeURL
	tSettings.Legacy.ParkStoreTimeout = 0

	store, err := blob.NewStore(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	park := mustNewBlockPark(t, ulogger.TestLogger{}, tSettings, store)
	require.NotNil(t, park)
	require.Equal(t, parkMinStoreTimeout, park.storeTimeout)

	// parkedRecord's own WriteConvertedBlock call already requires no error; a
	// deadline floored at zero must still let a genuine write through.
	parkedRecord(t, park, chainhash.Hash{0x01}, 1)
}

// TestBlockPark_RecoverDiscardsAConvertedRecordInsteadOfOrphaningItForever is
// fix-round item 2's Recover fix. Before it, Recover recognised only names
// ending in the whole-block suffix; a name ending in ".block" instead fell
// through into "anything we do not recognise is left alone" and stayed on
// disk across every future restart, because nothing else in the park, or
// anywhere else, ever revisits it. Adopting a converted record as a park
// entry directly is a later task's job (task 5 in this plan); what this fixes
// is that Recover now recognises the suffix at all, and discards what it
// cannot yet adopt rather than orphaning it permanently.
func TestBlockPark_RecoverDiscardsAConvertedRecordInsteadOfOrphaningItForever(t *testing.T) {
	park, dir := newTestPark(t, "")

	hash := chainhash.Hash{0x55, 0x66, 0x77}

	// A minimal but genuinely valid converted record — model.NewBlock needs a
	// header, a coinbase and a subtree list, so this builds one the same way
	// checkMerkleRootAgainst (merkle_accumulator_test.go) does for the same
	// reason: a narrower stand-in would mean inventing a format neither
	// WriteConvertedBlock nor Recover was ever asked to handle.
	header := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	blk, err := model.NewBlock(header, coinbaseTx(t), nil, 0, 0, 0, 0)
	require.NoError(t, err)

	require.NoError(t, park.WriteConvertedBlock(context.Background(), hash, blk))

	names := parkDirEntries(t, dir)
	require.Contains(t, names, hash.String()+".block",
		"sanity: the record must actually be on disk before recovery can be tested against it")

	fresh, _ := newTestPark(t, "")
	fresh.dir = dir
	fresh.store = park.store
	fresh.Recover(context.Background(), nil)

	require.Zero(t, fresh.Len(),
		"a converted record is not adopted as a park entry by this task (that is a later task's job); it must not be silently skipped forever either")

	names = parkDirEntries(t, dir)
	require.NotContains(t, names, hash.String()+".block",
		"recovery must not leave a converted record on disk forever; discarding what it cannot yet adopt is the floor this fixes")
}

// TestBlockPark_DeleteAlsoRemovesAConvertedRecord is fix-round item 2's core
// claim. Before it, Delete removed only the whole-block file type, so commit
// and every other path that retires an entry through Delete
// (applyParkDisposition's own comment calls it the ONLY place that deletes a
// parked blob) left a converted record behind. This drives a real converted
// record onto disk, retires it through the ordinary path every retiring
// caller already goes through, and requires the record to be gone afterward.
func TestBlockPark_DeleteAlsoRemovesAConvertedRecord(t *testing.T) {
	ctx := context.Background()
	park, dir := newTestPark(t, "")

	hash := chainhash.Hash{0x88, 0x99}

	header := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	blk, err := model.NewBlock(header, coinbaseTx(t), nil, 0, 0, 0, 0)
	require.NoError(t, err)

	require.NoError(t, park.WriteConvertedBlock(ctx, hash, blk))

	names := parkDirEntries(t, dir)
	require.Contains(t, names, hash.String()+".block",
		"sanity: the record must be on disk before Delete can be tested against it")

	park.Delete(ctx, parkedBlock{hash: hash})

	names = parkDirEntries(t, dir)
	require.NotContains(t, names, hash.String()+".block",
		"Delete must remove the converted record too, or every path that retires an entry through it -- commit, eviction, discard -- leaks the record")
}

// mustNewBlockPark builds a park for a test, failing it if the settings or store cannot have one.
func mustNewBlockPark(t *testing.T, logger ulogger.Logger, tSettings *settings.Settings, store blob.Store) *blockPark {
	t.Helper()

	park, err := newBlockPark(logger, tSettings, store)
	require.NoError(t, err)

	return park
}

// parkTempStore gives a test's settings the file:// temp store the park needs, since New refuses to
// start without one, and returns the store to pass New.
func parkTempStore(t *testing.T, tSettings *settings.Settings) blob.Store {
	t.Helper()

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	tSettings.Legacy.TempStore = storeURL

	return blob_memory.New()
}
