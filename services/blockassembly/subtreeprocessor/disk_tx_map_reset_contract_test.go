package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newSubtreeProcessorWithTxMapDirs builds a SubtreeProcessor whose currentTxMap
// is a DiskTxMap, which is what blockassembly_txMapDirs configures in production.
func newSubtreeProcessorWithTxMapDirs(t *testing.T, dirs []string) *SubtreeProcessor {
	t.Helper()

	ctx := context.Background()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	settings := test.CreateBaseTestSettings(t)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, settings, utxoStoreURL)
	require.NoError(t, err)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blob_memory.New(),
		&blockchain.Mock{}, utxoStore, make(chan NewSubtreeRequest, 10), WithTxMapDirs(dirs))
	require.NoError(t, err)

	t.Cleanup(func() {
		if stp.diskTxMap != nil {
			stp.diskTxMap.Close()
		}
	})

	require.NotNil(t, stp.diskTxMap, "WithTxMapDirs must install a DiskTxMap, or this test proves nothing")

	return stp
}

// moveForwardBlock captures currentTxMap, calls resetSubtreeState, and only then
// reads the captured map in processRemainderTxHashes:
//
//	originalCurrentTxMap := stp.currentTxMap
//	stp.resetSubtreeState(...)
//	stp.processRemainderTransactionsAndDequeue(..., CurrentTxMap: originalCurrentTxMap)
//
// The in-memory path satisfies that by double-buffering: resetSubtreeState swaps
// currentTxMap with currentTxMapShadow, so the captured pointer keeps the old
// contents until the commit point empties the shadow. The DiskTxMap path instead
// calls diskTxMap.Clear(), which empties the very object the caller captured, so
// every subsequent Get misses and moveForwardBlock fails with
//
//	[processRemainderTxHashes] error getting node txInpoints from currentTxMap for <hash>
//
// Blocks containing only a coinbase never expose this, because
// processRemainderTransactionsAndDequeue short-circuits on an empty
// TransactionMap and performs no lookups at all.
func TestResetSubtreeState_DiskTxMap_KeepsCapturedMapReadable(t *testing.T) {
	stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir(), t.TempDir()})

	hash := chainhash.Hash{0x01, 0x02, 0x03}
	inpoints := subtreepkg.NewTxInpoints()

	_, wasSet := stp.currentTxMap.SetIfNotExists(hash, &inpoints)
	require.True(t, wasSet, "precondition: the entry must be stored before the reset")

	// What moveForwardBlock captures before resetting.
	originalCurrentTxMap := stp.currentTxMap

	_, found := originalCurrentTxMap.Get(hash)
	require.True(t, found, "precondition: the captured map must read back before the reset")

	require.NoError(t, stp.resetSubtreeState(true))

	_, found = originalCurrentTxMap.Get(hash)
	require.True(t, found,
		"the map captured before resetSubtreeState must still be readable afterwards: "+
			"processRemainderTxHashes reads it and errors out when a lookup misses")
}

// The freshly-current map must be empty after the reset, which is the other half
// of the same contract: new transactions arriving during block movement go into
// the new buffer, not on top of the previous block's entries.
func TestResetSubtreeState_DiskTxMap_NewMapIsEmpty(t *testing.T) {
	stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir(), t.TempDir()})

	hash := chainhash.Hash{0x0a, 0x0b, 0x0c}
	inpoints := subtreepkg.NewTxInpoints()

	_, wasSet := stp.currentTxMap.SetIfNotExists(hash, &inpoints)
	require.True(t, wasSet)

	require.NoError(t, stp.resetSubtreeState(true))

	_, found := stp.currentTxMap.Get(hash)
	require.False(t, found, "the new current map must start empty")
	require.Equal(t, 0, stp.currentTxMap.Length(), "the new current map must start empty")
}

// reorgBlocks captures originalCurrentTxMap once and relies on it referencing
// unchanged pre-reorg data for the whole moveForward loop, which is why it sets
// disableCurrentTxMapPool. The disk branch of resetSubtreeState used to run
// before that flag was consulted, so a reorg cleared the very map rollback would
// restore.
func TestResetSubtreeState_DiskTxMap_ReorgKeepsCapturedMapReadable(t *testing.T) {
	stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir(), t.TempDir()})

	stp.disableCurrentTxMapPool = true
	defer func() { stp.disableCurrentTxMapPool = false }()

	hash := chainhash.Hash{0x11, 0x22, 0x33}
	inpoints := subtreepkg.NewTxInpoints()

	_, wasSet := stp.currentTxMap.SetIfNotExists(hash, &inpoints)
	require.True(t, wasSet)

	originalCurrentTxMap := stp.currentTxMap

	require.NoError(t, stp.resetSubtreeState(true))

	_, found := originalCurrentTxMap.Get(hash)
	require.True(t, found, "a reorg must leave the captured pre-reorg map intact for rollback")

	require.NotSame(t, originalCurrentTxMap, stp.currentTxMap,
		"the reorg path must allocate a fresh map rather than reuse the captured one")
	require.Equal(t, 0, stp.currentTxMap.Length(), "the fresh map must start empty")

	// The displaced map owns Badger directories, so it must be retired for the
	// reorg to close rather than silently dropped.
	require.NotEmpty(t, stp.diskTxMapRetired, "the displaced map must be retired for closing")

	stp.closeRetiredDiskTxMaps()
	require.Empty(t, stp.diskTxMapRetired)
}

// Two consecutive blocks: each reset must expose an empty map while keeping the
// previous block's entries readable, and the commit point must empty the half
// that was just retired so it is clean when it comes back round.
func TestResetSubtreeState_DiskTxMap_AlternatesAcrossBlocks(t *testing.T) {
	stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir(), t.TempDir()})

	first := chainhash.Hash{0xa1}
	second := chainhash.Hash{0xb2}
	inpoints := subtreepkg.NewTxInpoints()

	// Block 1
	_, wasSet := stp.currentTxMap.SetIfNotExists(first, &inpoints)
	require.True(t, wasSet)

	capturedFirst := stp.currentTxMap
	require.NoError(t, stp.resetSubtreeState(true))

	_, found := capturedFirst.Get(first)
	require.True(t, found, "block 1 entries must survive the reset that starts block 2")

	stp.clearCurrentTxMapShadow() // commit point of block 1

	// Block 2
	_, wasSet = stp.currentTxMap.SetIfNotExists(second, &inpoints)
	require.True(t, wasSet, "the second block's map must be empty enough to accept the entry")

	capturedSecond := stp.currentTxMap
	require.NoError(t, stp.resetSubtreeState(true))

	_, found = capturedSecond.Get(second)
	require.True(t, found, "block 2 entries must survive the reset that starts block 3")

	_, found = stp.currentTxMap.Get(first)
	require.False(t, found, "block 1 entries must not reappear when the half is reused")
	require.Equal(t, 0, stp.currentTxMap.Length(), "the reused half must come back empty")
}
