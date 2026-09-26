package subtreeprocessor

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// With DiskTxMap, AddNodesDirectly must leave every tx's stored SubtreeIndex
// pointing at the chained subtree that actually holds it (chainedIdx+1), as the
// per-subtree index update would. It covers a load that starts on a partly
// filled subtree and spans several subtrees.
func TestAddNodesDirectly_DiskTxMapSubtreeIndex(t *testing.T) {
	const itemsPerSubtree = 1024

	stp, cleanup := newAddNodesBenchProcessor(t, itemsPerSubtree, WithTxMapDirs([]string{t.TempDir(), t.TempDir()}))
	defer cleanup()

	// First call leaves the current subtree part-full, second spans several
	// subtrees from there.
	first := makeUnminedBatches(300, 300)[0]
	second := makeUnminedBatches(5*itemsPerSubtree+17, 5*itemsPerSubtree+17)[0]

	for i := range second {
		second[i].Node.Hash[31] = 0x77 // distinct from the first batch
	}

	require.NoError(t, stp.AddNodesDirectly(first, true))
	require.NoError(t, stp.AddNodesDirectly(second, true))

	dtm := stp.diskTxMap
	require.NotNil(t, dtm)

	checked := 0

	for chainedIdx, st := range stp.chainedSubtrees {
		for n, node := range st.Nodes {
			if chainedIdx == 0 && n == 0 {
				continue // coinbase placeholder
			}

			got, ok := dtm.Get(node.Hash)
			require.True(t, ok, "tx in chained subtree %d missing from the map", chainedIdx)
			require.Equal(t, int16(chainedIdx+1), got.SubtreeIndex, "subtree index of a tx in chained subtree %d", chainedIdx)

			checked++
		}
	}

	// A tx still in the current subtree carries either no index or the index
	// that subtree will get once it completes; both are safe hints.
	future := int16(len(stp.chainedSubtrees) + 1)

	for _, node := range stp.currentSubtree.Load().Nodes {
		got, ok := dtm.Get(node.Hash)
		require.True(t, ok)
		require.Contains(t, []int16{0, future}, got.SubtreeIndex)

		checked++
	}

	require.Equal(t, len(first)+len(second), checked)
}

// SetBatch must behave like one Set per entry.
func TestDiskTxMap_SetBatchMatchesSet(t *testing.T) {
	batched, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir(), t.TempDir()}})
	require.NoError(t, err)

	defer batched.Close()

	txs := makeUnminedBatches(3000, 3000)[0]
	for i, tx := range txs {
		tx.TxInpoints.SubtreeIndex = int16(i % 7)
	}

	batched.SetBatch(txs)

	require.Equal(t, len(txs), batched.Length())

	for i, tx := range txs {
		require.True(t, batched.Exists(tx.Hash))

		got, ok := batched.Get(tx.Hash)
		require.True(t, ok, "tx %d missing", i)
		require.Equal(t, int16(i%7), got.SubtreeIndex)
	}

	// Setting the same hashes again must not double count.
	batched.SetBatch(txs)
	require.Equal(t, len(txs), batched.Length())
}

// SetBatch is bounded by entries in flight, not by channel messages: batches
// far larger than the bound, from many goroutines at once, must neither
// deadlock nor lose writes.
func TestDiskTxMap_SetBatchBoundedInFlight(t *testing.T) {
	m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir(), t.TempDir()}, MaxPendingWrites: 64})
	require.NoError(t, err)

	defer m.Close()

	txs := makeUnminedBatches(20_000, 20_000)[0]

	var wg sync.WaitGroup

	for w := 0; w < 16; w++ {
		chunk := txs[w*1250 : (w+1)*1250]

		wg.Add(1)

		go func() {
			defer wg.Done()
			m.SetBatch(chunk)
		}()
	}

	wg.Wait()
	require.NoError(t, m.Flush())
	require.Equal(t, len(txs), m.Length())

	for i, tx := range txs {
		_, ok := m.Get(tx.Hash)
		require.True(t, ok, "tx %d not written", i)
	}
}

// SetBatch must wait for room when the disk's in-flight budget is used up,
// and proceed once the writer releases it.
func TestDiskTxMap_SetBatchWaitsForInFlightBudget(t *testing.T) {
	m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}, MaxPendingWrites: 8})
	require.NoError(t, err)

	defer m.Close()

	disk := &m.disks[0]
	require.True(t, disk.pending.TryAcquire(disk.pendingCap), "take the whole budget")

	done := make(chan struct{})
	go func() {
		m.SetBatch(makeUnminedBatches(4, 4)[0])
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("SetBatch did not wait for in-flight budget")
	case <-time.After(200 * time.Millisecond):
	}

	disk.pending.Release(disk.pendingCap)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SetBatch did not proceed once budget was released")
	}
}
