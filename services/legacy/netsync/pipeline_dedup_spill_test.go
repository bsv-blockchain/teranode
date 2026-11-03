package netsync

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// spillTestHash returns a distinct hash for i.
func spillTestHash(i uint64) chainhash.Hash {
	var h chainhash.Hash
	binary.LittleEndian.PutUint64(h[:], i)
	binary.LittleEndian.PutUint64(h[8:], i*0x9E3779B97F4A7C15)

	return h
}

// newSpillTestMap returns a pipeline dedup map that moves to a disk table under a test directory
// once it holds maxInMemory entries, and a counter of the times it reported moving.
func newSpillTestMap(t *testing.T, maxInMemory int) (*pipelineDedupMap, *[]int) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.StreamDedupMaxInMemoryEntries = maxInMemory
	tSettings.DataFolder = t.TempDir()

	var spills []int

	m := newPipelineDedupMap(1_000, tSettings, func(entries int) { spills = append(spills, entries) })

	t.Cleanup(func() { require.NoError(t, m.Close()) })

	return m, &spills
}

// The transactions a peer sends all go into the duplicate map before the merkle root can be
// checked, so past the cap the map moves to disk instead of growing on the heap (issue 4945). It
// moves once, with everything it held, and keeps catching a duplicate on both sides of the move.
func TestPipelineDedupMap_MovesToDiskPastTheCap(t *testing.T) {
	const maxInMemory = 8

	m, spills := newSpillTestMap(t, maxInMemory)

	for i := uint64(0); i < maxInMemory; i++ {
		require.NoError(t, m.Put(spillTestHash(i), i))
	}

	require.Nil(t, m.disk, "a map holding exactly the cap is still in memory")
	require.Empty(t, *spills)

	for i := uint64(maxInMemory); i < 3*maxInMemory; i++ {
		require.NoError(t, m.Put(spillTestHash(i), i))
	}

	require.NotNil(t, m.disk, "the entry past the cap moves the map to disk")
	require.Equal(t, []int{maxInMemory}, *spills, "it moves once, reporting what it held")
	require.Nil(t, m.mem, "and the in-memory map is let go")

	for i := uint64(0); i < 3*maxInMemory; i++ {
		v, ok := m.disk.Get(spillTestHash(i))
		require.True(t, ok, "entry %d is in the disk table, moved or put there", i)
		require.Equal(t, i, v)
	}

	moved := m.Put(spillTestHash(1), 99)
	require.Error(t, moved, "a repeat of an entry that was moved is a duplicate")
	require.True(t, errors.Is(moved, errors.ErrTxExists), "with the error the builder reads as a duplicate: %v", moved)

	late := m.Put(spillTestHash(2*maxInMemory), 99)
	require.Error(t, late, "a repeat of an entry put after the move is a duplicate")
	require.True(t, errors.Is(late, errors.ErrTxExists), "with the error the builder reads as a duplicate: %v", late)
}

// A map below the cap never touches disk, and the in-memory duplicate error is one the builder
// reads as a duplicate. Below, not at: a map holding the cap moves on its next Put, whether or not
// that Put is a duplicate, which the disk table then catches.
func TestPipelineDedupMap_StaysInMemoryBelowTheCap(t *testing.T) {
	m, spills := newSpillTestMap(t, 8)

	for i := uint64(0); i < 7; i++ {
		require.NoError(t, m.Put(spillTestHash(i), i))
	}

	err := m.Put(spillTestHash(3), 3)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxExists) || errors.Is(err, txmap.ErrHashAlreadyExists), "the builder reads it as a duplicate: %v", err)
	require.Nil(t, m.disk)
	require.Empty(t, *spills)
}

// A duplicate after the move is still the CVE-2012-2459 refusal, the peer's fault and one of the
// three that may ban, not a local failure.
func TestPipelineDedupMap_ABuilderStillRefusesADuplicateAfterTheMove(t *testing.T) {
	m, spills := newSpillTestMap(t, 2)

	b, err := newBlockStreamBuilder(8, 8, coinbaseTx(t), noopEmit, m)
	require.NoError(t, err)

	tx1, hash1 := streamTx(t, 1)
	require.NoError(t, b.AddTx(tx1, hash1))

	for i := 2; i <= 4; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash))
	}

	require.Len(t, *spills, 1, "four transactions against a cap of two moved the map")

	err = b.AddTx(tx1, hash1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "the peer's fault: %v", err)
	require.True(t, errors.Is(err, errors.ErrBlockBodyMismatch), "the duplicate refusal, which may ban")
	require.False(t, isLocalSinkFault(err))
}

// A disk table that cannot be created is this node's fault: the block is drained and asked for
// again, and the peer is kept.
func TestPipelineDedupMap_AFailedMoveIsALocalFault(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notADir, nil, 0o600))

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.StreamDedupMaxInMemoryEntries = 1
	tSettings.DataFolder = notADir

	m := newPipelineDedupMap(10, tSettings, nil)

	require.NoError(t, m.Put(spillTestHash(0), 0))

	err := m.Put(spillTestHash(1), 1)
	require.Error(t, err)
	require.True(t, isLocalSinkFault(err), "a store this node cannot write is not the peer's: %v", err)
	require.False(t, errors.Is(err, errors.ErrTxExists), "and must never read as a duplicate")
	require.NoError(t, m.Close())
}

// Where the map goes: its own directory under dataFolder, not block_diskMapDirs, which may be a
// volume only block validation's pods mount. Without settings it keeps the default cap.
func TestNewPipelineDedupMap_DirectoryAndCap(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.StreamDedupMaxInMemoryEntries = 0
	tSettings.Block.DiskMapDirs = []string{"/mnt/a", "/mnt/b"}
	tSettings.DataFolder = "/var/teranode"

	m := newPipelineDedupMap(10, tSettings, nil)
	require.Equal(t, "/var/teranode/legacy-dedup", m.dir)
	require.Equal(t, int(dedupInitialCapacity), m.maxInMemory, "0 means the default")

	tSettings.Legacy.StreamDedupMaxInMemoryEntries = 5

	m = newPipelineDedupMap(10, tSettings, nil)
	require.Equal(t, 5, m.maxInMemory)

	m = newPipelineDedupMap(10, nil, nil)
	require.Equal(t, int(dedupInitialCapacity), m.maxInMemory)
	require.Empty(t, m.dir, "os.TempDir")
}

// A cap below the default bounds the map's first allocation too, not only what it grows to: a
// peer declaring a million transactions must not get the ~50 MB default-sized map when the
// operator capped the map at a thousand entries.
func TestNewPipelineDedupMap_TheCapBoundsTheFirstAllocation(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.StreamDedupMaxInMemoryEntries = 1_000

	heapAfter := func(f func() *pipelineDedupMap) uint64 {
		var before, after runtime.MemStats

		runtime.GC()
		runtime.ReadMemStats(&before)

		m := f()

		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(m)

		if after.HeapAlloc < before.HeapAlloc {
			return 0
		}

		return after.HeapAlloc - before.HeapAlloc
	}

	capped := heapAfter(func() *pipelineDedupMap { return newPipelineDedupMap(uint64(dedupInitialCapacity), tSettings, nil) })
	require.Less(t, capped, uint64(8<<20), "a map capped at 1,000 entries is not sized for a million")
}

// A spilled map checks the free disk before it goes on, and refuses as this node's fault when the
// free disk does not cover what every spilled map claims: a write to a sparse mmap file on a full
// disk kills the process, and a peer drives these writes before the merkle root is checked.
func TestPipelineDedupMap_RefusesToSpillOntoAFullDisk(t *testing.T) {
	saved := dedupDiskFree
	t.Cleanup(func() { dedupDiskFree = saved })

	dedupDiskFree = func(string) (uint64, error) { return dedupDiskSpare, nil }

	claimedBefore := dedupDiskClaimed.Load()

	m, spills := newSpillTestMap(t, 2)

	require.NoError(t, m.Put(spillTestHash(0), 0))
	require.NoError(t, m.Put(spillTestHash(1), 1))

	err := m.Put(spillTestHash(2), 2)
	require.Error(t, err)
	require.True(t, isLocalSinkFault(err), "a disk this node cannot fill is not the peer's: %v", err)
	require.False(t, errors.Is(err, errors.ErrTxExists), "and must never read as a duplicate")
	require.Contains(t, err.Error(), "bytes free")
	require.Empty(t, *spills)
	require.Nil(t, m.disk, "the table it could not use is released")
	require.Equal(t, claimedBefore, dedupDiskClaimed.Load(), "and so is its claim")

	v, ok := m.mem.Get(spillTestHash(1))
	require.True(t, ok, "the in-memory map is intact")
	require.Equal(t, uint64(1), v)
}

// The check repeats as a spilled map grows, and the disk the maps claim is given back on Close.
func TestPipelineDedupMap_ChecksTheDiskAsItGrowsAndGivesItBackOnClose(t *testing.T) {
	saved := dedupDiskFree
	t.Cleanup(func() { dedupDiskFree = saved })

	free := uint64(1 << 50)
	dedupDiskFree = func(string) (uint64, error) { return free, nil }

	claimedBefore := dedupDiskClaimed.Load()

	m, spills := newSpillTestMap(t, 2)

	for i := uint64(0); i < 3; i++ {
		require.NoError(t, m.Put(spillTestHash(i), i))
	}

	require.Len(t, *spills, 1)
	require.Positive(t, m.claimed)
	require.Equal(t, claimedBefore+m.claimed, dedupDiskClaimed.Load())

	free = 0

	var err error
	for i := uint64(3); i < 3+dedupDiskCheckEvery && err == nil; i++ {
		err = m.Put(spillTestHash(i), i)
	}

	require.Error(t, err, "within dedupDiskCheckEvery Puts the map sees the disk is full")
	require.True(t, isLocalSinkFault(err), "%v", err)

	require.NoError(t, m.Close())
	require.Equal(t, claimedBefore, dedupDiskClaimed.Load(), "Close gives the claim back")
	require.NoError(t, m.Close(), "and is safe to repeat")

	err = m.Put(spillTestHash(1<<40), 0)
	require.Error(t, err, "a closed map refuses a Put rather than starting over empty")
	require.False(t, errors.Is(err, errors.ErrTxExists))
}

// End to end: a block with more transactions than the in-memory cap still converts, and a
// duplicate in such a block is still refused. The same block with nowhere to move the map fails as
// this node's fault, which is what shows the sink took the disk path at all.
func TestPipelineSink_ABlockPastTheDedupCapStillConverts(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.settings.Legacy.StreamDedupMaxInMemoryEntries = 4
	sm.settings.DataFolder = t.TempDir()

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)
	body := blockBodyBytes(t, blk)

	saved := dedupDiskFree
	t.Cleanup(func() { dedupDiskFree = saved })

	dedupDiskFree = func(string) (uint64, error) { return 0, nil }

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), sinkPayloadLen(body))
	require.Error(t, err, "the block passes the cap, so the map must move")
	require.Contains(t, err.Error(), "disk-backed duplicate map")
	require.True(t, isLocalSinkFault(err), "and failing to is this node's fault: %v", err)
	require.False(t, converted)

	dedupDiskFree = saved

	converted, err = sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), sinkPayloadLen(body))
	require.NoError(t, err)
	require.True(t, converted)

	dup := wireBlockWithTxs(t, 8, false)
	pipelineHeaderFixture(t, sm, dup)
	dupBody := blockBodyWithADuplicate(t, dup)

	converted, err = sm.pipelineBlockSink(*dup.Hash(), &dup.MsgBlock().Header, bytes.NewReader(dupBody), sinkPayloadLen(dupBody))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
	require.False(t, converted)
}
