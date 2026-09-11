package netsync

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
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

	park := newBlockPark(ulogger.TestLogger{}, tSettings, store)
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

// TestBlockPark_ParksABlockWhoseMerkleRootIsCheckedLater records a deliberate
// change of behaviour, and the reasoning matters more than the assertion.
//
// This test used to assert the opposite: that a peer pairing a genuine,
// real-work header with somebody else's transactions was refused before
// anything reached the disk. It called itself the check that mattered most, on
// the grounds that such a block would otherwise fail only on drain, "by which
// point it has been given up on", so one crafted message on a public port would
// stop sync.
//
// That last step is no longer true, and it is what the check rested on. A
// rejection on the drain path drops the parked bytes, puts the block back on the
// download walk and blames the peer, so the block is obtained again rather than
// given up on; TestParkRejectionLeavesTheBlockRequestable pins that, including
// the cascade mark not suppressing the block's own retry.
//
// What the check cost was a merkle rebuild over every transaction: 13.7 seconds
// for a 100,001-transaction mainnet block, a 19.9 second mean across the blocks
// big enough to log a warning, paid on 91% of blocks because that is how many
// arrive out of order. The same root is verified during normal block processing
// on subtrees built from the transactions the peer actually sent, so the
// fabrication is still caught, just later.
//
// So the park now takes this block, and the exposure is a file that exists until
// the later check refuses it. The proof-of-work check on the 80-byte header
// stays, and it is the one that matters for flooding: a peer cannot mint
// unlimited distinct blocks without real work.
func TestBlockPark_ParksABlockWhoseMerkleRootIsCheckedLater(t *testing.T) {
	park, dir := newTestPark(t, "")

	blocks := minedBlocks(t, 2)

	// A real header, real proof of work, and somebody else's transactions.
	tampered := &wire.MsgBlock{
		Header:       blocks[0].MsgBlock().Header,
		Transactions: blocks[1].MsgBlock().Transactions,
	}
	hash := tampered.BlockHash()

	result := park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: tampered.Header.PrevBlock}, tampered)

	require.Equal(t, parkAccepted, result,
		"the park no longer rebuilds the merkle tree; normal block processing verifies it")
	require.NotEmpty(t, parkDirEntries(t, dir))
	require.Equal(t, 1, park.Len())

	// And the cheap header checks still bite, so this is not a free-for-all: a
	// block that does not hash to the key we asked for is still refused before
	// anything is written.
	fresh, freshDir := newTestPark(t, "")
	wrongKey := chainhash.Hash{0xde, 0xad}

	require.Equal(t, parkRejected,
		fresh.Park(context.Background(), parkedBlock{hash: wrongKey}, blocks[0].MsgBlock()),
		"a block that is not the block we asked for is still refused")
	require.Empty(t, parkDirEntries(t, freshDir))
}

// TestBlockPark_RefusesABlockWithNoTransactions covers a remote panic, not just
// a refusal: the merkle builder sizes its array as nextPowerOfTwo(n)*2-1, and
// nextPowerOfTwo(0) is 0, so an empty transaction list asks for a slice of
// length -1. The wire decoder accepts a transaction count of zero, so a peer
// can send exactly this.
func TestBlockPark_RefusesABlockWithNoTransactions(t *testing.T) {
	park, dir := newTestPark(t, "")

	blocks := minedBlocks(t, 1)

	empty := &wire.MsgBlock{Header: blocks[0].MsgBlock().Header}
	hash := empty.BlockHash()

	require.NotPanics(t, func() {
		require.Equal(t, parkRejected, park.Park(context.Background(), parkedBlock{hash: hash}, empty))
	}, "a block with no transactions must be refused, not panic the block-queue goroutine")

	require.Empty(t, parkDirEntries(t, dir))
}

// TestBlockPark_RefusesABlockThatDoesNotMeetItsTarget pins the check that stops
// an attacker minting unlimited distinct blocks to fill the park with, and the
// key/hash disagreement check beside it.
func TestBlockPark_RefusesABlockThatDoesNotMeetItsTarget(t *testing.T) {
	blocks := minedBlocks(t, 1)

	t.Run("no proof of work", func(t *testing.T) {
		park, dir := newTestPark(t, "")

		original := blocks[0].MsgBlock()

		unmined := &wire.MsgBlock{Header: original.Header, Transactions: original.Transactions}
		// A target nothing can plausibly meet, so the block's own header fails.
		unmined.Header.Bits = 0x03000001

		hash := unmined.BlockHash()

		require.Equal(t, parkRejected, park.Park(context.Background(), parkedBlock{hash: hash}, unmined))
		require.Empty(t, parkDirEntries(t, dir))
	})

	t.Run("hash does not match the key", func(t *testing.T) {
		park, dir := newTestPark(t, "")

		require.Equal(t, parkRejected,
			park.Park(context.Background(), parkedBlock{hash: chainhash.Hash{0xde, 0xad}}, blocks[0].MsgBlock()))
		require.Empty(t, parkDirEntries(t, dir))
	})
}

// TestBlockPark_RoundTripsThroughAShardedStore proves the layout does not
// depend on the temp_store URL. A store built with hashPrefix would otherwise
// put the blobs in shard subdirectories, where the flat recovery scan finds
// nothing at all and every parked block leaks on every restart.
func TestBlockPark_RoundTripsThroughAShardedStore(t *testing.T) {
	park, dir := newTestPark(t, "?hashPrefix=2")

	blocks := minedBlocks(t, 1)
	msgBlock := blocks[0].MsgBlock()
	hash := msgBlock.BlockHash()

	require.Equal(t, parkAccepted,
		park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock))

	require.Equal(t, []string{hash.String() + "." + string(fileformat.FileTypeMsgBlock), hash.String() + "." + string(fileformat.FileTypeMsgBlock) + ".sha256"},
		parkDirEntries(t, dir), "the park layout must be flat whatever the store URL says")

	got, err := park.Read(context.Background(), hash)
	require.NoError(t, err)
	require.Equal(t, msgBlock.SerializeSize(), got.SerializeSize())
	gotHash := got.BlockHash()
	require.True(t, gotHash.IsEqual(&hash))

	// And the recovery scan finds it.
	fresh, _ := newTestPark(t, "?hashPrefix=2")
	fresh.dir = dir
	fresh.store = park.store
	fresh.Recover(context.Background())

	require.Equal(t, 1, fresh.Len(), "a sharded store must not hide parked blocks from the restart scan")

	taken := fresh.TakeChildren(msgBlock.Header.PrevBlock)
	require.Len(t, taken, 1)
	require.True(t, taken[0].hash.IsEqual(&hash))
	prev := msgBlock.Header.PrevBlock
	require.True(t, taken[0].prevBlock.IsEqual(&prev),
		"the parent must be read back out of the stored block header")
}

// TestBlockPark_AFailedWriteLeavesNoGoroutineBehind. The blob store never
// closes the reader it is handed, so on an error return the goroutine
// serializing the block into the pipe blocks forever on its next write — one
// leaked goroutine per failed park, each pinning a whole decoded block.
func TestBlockPark_AFailedWriteLeavesNoGoroutineBehind(t *testing.T) {
	park, _ := newTestPark(t, "")
	park.store = failingWriteStore{Store: park.store}

	blocks := minedBlocks(t, 1)
	msgBlock := blocks[0].MsgBlock()
	hash := msgBlock.BlockHash()

	before := runtime.NumGoroutine()

	done := make(chan parkResult, 1)

	go func() {
		done <- park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock)
	}()

	select {
	case result := <-done:
		require.Equal(t, parkUnavailable, result, "a store that cannot write is a local fault, so the block must be re-requested")
	case <-time.After(10 * time.Second):
		t.Fatal("Park never returned: the goroutine serializing the block is stuck writing into a pipe nobody closed")
	}

	require.Zero(t, park.Bytes(), "a failed write must not leave the budget charged")
	require.Zero(t, park.Len())

	require.True(t, WaitUntil(func() bool { return runtime.NumGoroutine() <= before }, 5*time.Second),
		"the goroutine serializing the block must terminate when the write fails")
}

// TestBlockPark_RefusesOverBudgetAndWritesNothing pins both bounds. The byte
// budget is what an operator sets; the entry cap is what stops the in-memory
// index growing without limit when blocks are tiny.
func TestBlockPark_RefusesOverBudgetAndWritesNothing(t *testing.T) {
	blocks := minedBlocks(t, 1)
	msgBlock := blocks[0].MsgBlock()
	hash := msgBlock.BlockHash()

	t.Run("size alone never refuses", func(t *testing.T) {
		// There used to be a byte budget here and it was removed, because it
		// could only be evaluated after the block had been downloaded and
		// decoded, so it never saved any bandwidth, and because a park filled
		// above a hole would refuse the very block that would drain it. The
		// bound is now the number of blocks held, and the disk is bounded
		// upstream by the download walk's read-ahead depth, in blocks.
		park, dir := newTestPark(t, "")

		require.Equal(t, parkAccepted,
			park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock))

		require.NotEmpty(t, parkDirEntries(t, dir), "the block must be written whatever its size")
		require.Equal(t, int64(msgBlock.SerializeSize()), park.Bytes(),
			"the byte total is still tracked, for the gauge, it just no longer refuses")
	})

	t.Run("entry cap", func(t *testing.T) {
		park, dir := newTestPark(t, "")

		// Fill the index without touching the disk.
		for i := 0; i < maxParkedEntries; i++ {
			h := chainhash.Hash{}
			h[0], h[1] = byte(i), byte(i>>8)
			park.entries[h] = &parkedBlock{hash: h}
		}

		require.Equal(t, parkUnavailable,
			park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock))

		require.Empty(t, parkDirEntries(t, dir))
	})
}

// TestBlockPark_RecoversWhatAPreviousRunLeftBehind is the restart case. A crash
// mid-write leaves a dot-prefixed temp file; a sidecar can outlive its block; a
// file can be corrupt. None of those may stop the good blobs behind them from
// being adopted, and none may be left to leak.
func TestBlockPark_RecoversWhatAPreviousRunLeftBehind(t *testing.T) {
	park, dir := newTestPark(t, "")

	blocks := minedBlocks(t, 2)

	for _, b := range blocks {
		msgBlock := b.MsgBlock()
		hash := msgBlock.BlockHash()

		require.Equal(t, parkAccepted,
			park.Park(context.Background(), parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock))
	}

	// The wreckage a crash leaves.
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".abcdef.4711.tmp"), []byte("half a block"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "0000000000000000000000000000000000000000000000000000000000000009.msgBlock.sha256"), []byte("orphaned"), 0o600))

	// A blob that reads back perfectly and is somebody else's block. That is
	// evidence about the file, so it must be deleted and the block asked for
	// again.
	wrongBlock := chainhash.Hash{0x11, 0x22}
	firstBlob, err := os.ReadFile(filepath.Join(dir, blocks[0].MsgBlock().BlockHash().String()+".msgBlock"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, wrongBlock.String()+".msgBlock"), firstBlob, 0o600))

	// A file the store itself will not open — a torn store header, an unreadable
	// disk. The store reports both of those the same way, and the park's error
	// policy reads that as "says nothing about the block", so this one is left
	// where it is rather than deleted. See parkReadFailure.
	unreadable := chainhash.Hash{0x33, 0x44}
	require.NoError(t, os.WriteFile(filepath.Join(dir, unreadable.String()+".msgBlock"), make([]byte, 200), 0o600))

	fresh, _ := newTestPark(t, "")
	fresh.dir = dir
	fresh.store = park.store
	fresh.Recover(context.Background())

	require.Equal(t, 2, fresh.Len(), "both good blocks must be adopted, whatever else is in the directory")

	for _, b := range blocks {
		msgBlock := b.MsgBlock()
		hash := msgBlock.BlockHash()

		taken := fresh.TakeChildren(msgBlock.Header.PrevBlock)
		require.Len(t, taken, 1)
		require.True(t, taken[0].hash.IsEqual(&hash))
		require.Nil(t, taken[0].peer, "a recovered block has no delivering peer")
		require.Zero(t, taken[0].height, "a recovered block has no reported height; the parent supplies it")
	}

	names := parkDirEntries(t, dir)
	require.NotContains(t, names, ".abcdef.4711.tmp", "a crash's half-written temp file must be swept")
	require.NotContains(t, names, "0000000000000000000000000000000000000000000000000000000000000009.msgBlock.sha256", "a sidecar whose block is gone must be swept")
	require.NotContains(t, names, wrongBlock.String()+".msgBlock", "a file that is not the block its name claims must be deleted")
	require.Contains(t, names, unreadable.String()+".msgBlock",
		"a blob the store could not open says nothing about the block, so recovery must leave it for the next start")
}

// TestBlockPark_RecoveryAdoptsEverythingAPreviousRunParked: recovery used to
// stop at a byte budget and delete whatever it could not afford, which threw
// away fully downloaded blocks on every restart for no gain. With the byte
// budget gone it adopts the lot, and the only thing that can still stop it is
// the cap on how many blocks the park holds.
func TestBlockPark_RecoveryAdoptsEverythingAPreviousRunParked(t *testing.T) {
	park, dir := newTestPark(t, "")

	blocks := minedBlocks(t, 3)

	for _, b := range blocks {
		msgBlock := b.MsgBlock()
		require.Equal(t, parkAccepted,
			park.Park(context.Background(), parkedBlock{hash: msgBlock.BlockHash(), prevBlock: msgBlock.Header.PrevBlock}, msgBlock))
	}

	require.Len(t, parkDirEntries(t, dir), 6, "three blocks and their checksum sidecars")

	fresh, _ := newTestPark(t, "")
	fresh.dir = dir
	fresh.store = park.store
	fresh.Recover(context.Background())

	require.Equal(t, 3, fresh.Len(),
		"every block a previous run parked is adopted; there is no byte budget to stop at")

	blobs := 0

	for _, name := range parkDirEntries(t, dir) {
		if filepath.Ext(name) == "."+string(fileformat.FileTypeMsgBlock) {
			blobs++
		}
	}

	require.Equal(t, 3, blobs,
		"a restart must not delete blocks it has already paid to download")
}

// TestBlockPark_TheBlockCountStillBounds proves the surviving bound actually
// bounds. Removing the byte budget left the number of blocks held as the park's
// only limit, so if that stopped refusing there would be nothing at all between
// an out-of-order flood and the disk.
func TestBlockPark_TheBlockCountStillBounds(t *testing.T) {
	park, _ := newTestPark(t, "")

	blocks := minedBlocks(t, 2)
	first := blocks[0].MsgBlock()
	second := blocks[1].MsgBlock()

	require.Equal(t, parkAccepted,
		park.Park(context.Background(), parkedBlock{hash: first.BlockHash(), prevBlock: first.Header.PrevBlock}, first))

	// Filled to the cap directly, because parking maxParkedEntries real blocks
	// would take minutes. The entries map is what the clause counts.
	park.mu.Lock()
	for i := 0; i < maxParkedEntries; i++ {
		h := chainhash.Hash{}
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		h[2] = byte(i >> 16)
		park.entries[h] = &parkedBlock{hash: h}
	}
	held := len(park.entries)
	park.mu.Unlock()

	require.GreaterOrEqual(t, held, maxParkedEntries, "precondition: the park is at its cap")

	require.Equal(t, parkUnavailable,
		park.Park(context.Background(), parkedBlock{hash: second.BlockHash(), prevBlock: second.Header.PrevBlock}, second),
		"at its block limit the park must refuse, or nothing bounds the disk")
}

// TestBlockPark_IsOffWhenItCannotBeRecovered covers the two settings-only kill
// switches and the store it refuses to run on. A store whose contents cannot be
// listed would leak every parked blob on every restart, so the park declines
// rather than leaking.
func TestBlockPark_IsOffWhenItCannotBeRecovered(t *testing.T) {
	base := func(t *testing.T) *settings.Settings {
		t.Helper()

		tSettings := test.CreateBaseTestSettings(t)
		storeURL, err := url.Parse("file://" + t.TempDir())
		require.NoError(t, err)
		tSettings.Legacy.TempStore = storeURL

		return tSettings
	}

	t.Run("legacy_parkOutOfOrderBlocks false", func(t *testing.T) {
		tSettings := base(t)
		tSettings.Legacy.ParkOutOfOrderBlocks = false

		require.Nil(t, newBlockPark(ulogger.TestLogger{}, tSettings, blob_memory.New()))
	})

	t.Run("a store that cannot be scanned", func(t *testing.T) {
		tSettings := base(t)

		memURL, err := url.Parse("memory://")
		require.NoError(t, err)
		tSettings.Legacy.TempStore = memURL

		require.Nil(t, newBlockPark(ulogger.TestLogger{}, tSettings, blob_memory.New()),
			"a store the restart scan cannot enumerate must disable the park, not leak into it")
	})

	t.Run("a nil park behaves as no park", func(t *testing.T) {
		var park *blockPark

		require.False(t, park.Enabled())
		require.Equal(t, parkDisabled, park.Park(context.Background(), parkedBlock{}, nil))
		require.Zero(t, park.Len())
		require.Zero(t, park.Bytes())
		require.Nil(t, park.TakeChildren(chainhash.Hash{}))
		require.Nil(t, park.EvictBelow(1, parkSweepExpiryBudget))
		require.Nil(t, park.StuckCandidates(time.Now(), 8))

		_, ok := park.Take(chainhash.Hash{})
		require.False(t, ok)

		require.NotPanics(t, func() {
			park.Restore(parkedBlock{})
			park.Delete(context.Background(), parkedBlock{})
			park.Recover(context.Background())
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

	park := newBlockPark(ulogger.TestLogger{}, tSettings, store)
	require.NotNil(t, park)
	require.Equal(t, parkMinStoreTimeout, park.storeTimeout)

	blocks := minedBlocks(t, 1)
	msgBlock := blocks[0].MsgBlock()

	require.Equal(t, parkAccepted,
		park.Park(context.Background(), parkedBlock{hash: msgBlock.BlockHash(), prevBlock: msgBlock.Header.PrevBlock}, msgBlock))
}

// failingWriteStore is a blob store whose writes always fail, so the park's
// error path can be driven without breaking the filesystem.
type failingWriteStore struct {
	blob.Store
}

func (s failingWriteStore) SetFromReader(_ context.Context, _ []byte, _ fileformat.FileType, _ io.ReadCloser, _ ...options.FileOption) error {
	return errors.NewStorageError("[test] the store is not taking writes")
}
