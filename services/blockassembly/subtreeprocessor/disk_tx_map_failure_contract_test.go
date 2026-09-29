package subtreeprocessor

import (
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// sealDir makes dir unwritable so tempstore.New's os.MkdirAll fails, which is
// how a full disk or an exhausted FD table presents to DiskTxMap.Clear.
func sealDir(t *testing.T, dir string) {
	t.Helper()

	if os.Geteuid() == 0 {
		t.Skip("running as root: mode bits do not stop directory creation")
	}

	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
}

// requireSameMap compares tx map identity by pointer. require.Same would do the
// same job but renders the whole 4096-shard DiskTxMap on failure, which is tens
// of kilobytes of CI log for a one-word answer.
func requireSameMap(t *testing.T, want, got any, msgAndArgs ...any) {
	t.Helper()

	require.Truef(t, want == got, "want map %p, got %p: %v", want, got, msgAndArgs)
}

// Clear rotates every disk shard onto a fresh Badger generation and resets the
// cuckoo filters. If a replacement generation cannot be created it must leave
// the map exactly as it was rather than resetting the filters over a store that
// still holds the old keys: Get reads straight from disk without consulting the
// filter, so a half-cleared map answers lookups that should miss with the
// previous block's inpoints, while Length reports zero.
func TestDiskTxMap_Clear_IsAllOrNothing(t *testing.T) {
	dir := t.TempDir()

	m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{dir}, Prefix: "clear-test", FilterCapacity: 1024})
	require.NoError(t, err)

	t.Cleanup(func() { _ = m.Close() })

	hash := chainhash.Hash{0xc1, 0xe4}
	inpoints := subtreepkg.NewTxInpoints()

	_, wasSet := m.SetIfNotExists(hash, &inpoints)
	require.True(t, wasSet)
	require.NoError(t, m.Flush())

	sealDir(t, dir)

	m.Clear()

	_, found := m.Get(hash)
	require.True(t, found,
		"precondition for this test: a failed Clear cannot remove the entry from the store")
	require.Equal(t, 1, m.Length(),
		"a Clear that could not rotate the store must leave the map populated and consistent, "+
			"not report an empty map that still answers Get with the old entries")
	require.True(t, m.Exists(hash),
		"the cuckoo filter must still agree with the store after a failed Clear")
}

// The commit point empties the retired half so it is clean when it comes back
// round as "current". Clear is best-effort, so resetSubtreeState must verify
// rather than assume: swapping a still-populated half in makes the supposedly
// fresh map answer Get with the previous block's inpoints.
func TestResetSubtreeState_DiskTxMap_RefusesNonEmptyIncomingHalf(t *testing.T) {
	dir := t.TempDir()
	stp := newSubtreeProcessorWithTxMapDirs(t, []string{dir})

	stale := chainhash.Hash{0x5a, 0x1e}
	inpoints := subtreepkg.NewTxInpoints()

	// What a failed clearCurrentTxMapShadow leaves behind.
	stp.diskTxMapShadow.Set(stale, &inpoints)
	require.NoError(t, stp.diskTxMapShadow.Flush())

	sealDir(t, dir)

	active := stp.currentTxMap

	err := stp.resetSubtreeState(true)
	require.Error(t, err,
		"resetSubtreeState must fail the block rather than install a half that still holds entries")
	requireSameMap(t, active, stp.currentTxMap,
		"a refused reset must leave the active map in place")
}

// Both halves are allocated at full capacity and both stay resident for the
// process lifetime, so reporting only the active half halves the filter-memory
// gauge operators size the pod from.
func TestDiskTxMapStats_CoversBothHalves(t *testing.T) {
	stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})

	active := stp.diskTxMap.Stats()
	shadow := stp.diskTxMapShadow.Stats()

	require.Positive(t, active.FilterMemBytes, "precondition: the active half reports its filter memory")
	require.Equal(t, active.FilterMemBytes, shadow.FilterMemBytes,
		"precondition: the shadow is allocated at the same capacity as the active half")

	combined := stp.diskTxMapStats()

	require.Equal(t, active.FilterMemBytes+shadow.FilterMemBytes, combined.FilterMemBytes,
		"the reported filter memory must cover the whole double buffer")
	require.Equal(t, active.Entries, combined.Entries,
		"only the active half holds entries, so the entry count must not be doubled")
}

// resetSubtreeState swaps a pair — currentTxMap with its shadow, or the two
// DiskTxMap halves. A rollback that restores only currentTxMap leaves the pair
// crossed, so the next block's reset swaps the half that still holds this
// block's entries back in as the "fresh" map.
func TestRestoreCurrentTxMap_UncrossesTheHalves(t *testing.T) {
	stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})

	hash := chainhash.Hash{0xd0, 0x0d}
	inpoints := subtreepkg.NewTxInpoints()

	_, wasSet := stp.currentTxMap.SetIfNotExists(hash, &inpoints)
	require.True(t, wasSet)

	beforeReset := stp.currentTxMap
	activeBefore, shadowBefore := stp.diskTxMap, stp.diskTxMapShadow

	require.NoError(t, stp.resetSubtreeState(true))
	require.NotSame(t, beforeReset, stp.currentTxMap, "precondition: the reset swapped the halves")

	stp.restoreCurrentTxMap(beforeReset)

	requireSameMap(t, beforeReset, stp.currentTxMap, "the pre-reset map must be active again")
	requireSameMap(t, activeBefore, stp.diskTxMap, "the halves must be uncrossed, not just currentTxMap restored")
	requireSameMap(t, shadowBefore, stp.diskTxMapShadow)

	_, found := stp.currentTxMap.Get(hash)
	require.True(t, found, "the restored map must still hold this block's entries")
}

// A panic can also land before resetSubtreeState ran — the dispatcher registers
// its rollback before the first deref of the incoming block. There is no swap to
// undo then, and swapping anyway would install the empty shadow over the live map.
func TestRestoreCurrentTxMap_NoSwapToUndoIsANoOp(t *testing.T) {
	stp := newSubtreeProcessorWithTxMapDirs(t, []string{t.TempDir()})

	hash := chainhash.Hash{0x6e, 0x0f}
	inpoints := subtreepkg.NewTxInpoints()

	_, wasSet := stp.currentTxMap.SetIfNotExists(hash, &inpoints)
	require.True(t, wasSet)

	live := stp.currentTxMap
	activeBefore, shadowBefore := stp.diskTxMap, stp.diskTxMapShadow

	stp.restoreCurrentTxMap(live)

	requireSameMap(t, live, stp.currentTxMap)
	requireSameMap(t, activeBefore, stp.diskTxMap)
	requireSameMap(t, shadowBefore, stp.diskTxMapShadow)

	_, found := stp.currentTxMap.Get(hash)
	require.True(t, found, "restoring a map that was never displaced must not empty it")
}
