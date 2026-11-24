package subtreeprocessor

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The two block-movement paths that put the pool's remaining transactions back into subtrees,
// each on the in-memory and the disk tx map, at utxostore_laneMigration off and shadow:
// processRemainderTxHashes after a peer's block (the parallel copy and rebuild) and
// processOwnBlockSubtreeNodes after our own block. Every transaction carries a lane position in
// the half it is copied from.

const (
	laneMoveBenchTxs         = 1 << 20
	laneMoveBenchSubtreeSize = 1 << 16
)

// laneMoveBenchHashes are the benchmark's transactions, and the parents they spend.
func laneMoveBenchHashes() []chainhash.Hash {
	hashes := make([]chainhash.Hash, laneMoveBenchTxs)
	for i := range hashes {
		binary.LittleEndian.PutUint64(hashes[i][:], 0x9e3779b97f4a7c15*uint64(i+1))
		binary.LittleEndian.PutUint64(hashes[i][8:], uint64(i)+7)
	}

	return hashes
}

// laneMoveBenchSource is the pool half the transactions are copied from, holding each with a
// lane position, 17 lanes taking turns in runs of 1,024.
func laneMoveBenchSource(b *testing.B, disk bool, hashes []chainhash.Hash) TxInpointsMap {
	b.Helper()

	var src TxInpointsMap = NewSplitTxInpointsMap(splitMapBuckets)

	if disk {
		m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{b.TempDir()}})
		require.NoError(b, err)
		b.Cleanup(func() { _ = m.Close() })

		src = m
	}

	parent := chainhash.HashH([]byte("lane-move-bench-parent"))
	next := make([]uint64, 17)

	for i, h := range hashes {
		l := (i / 1024) % 17
		next[l]++

		in := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent}, []uint32{1, uint32(i)}) //nolint:gosec // i < 2^20
		src.SetAt(h, &in, lane.Pack(uint8(l), next[l]))                                            //nolint:gosec // l < 17
	}

	return src
}

// laneMoveBenchProcessor is a processor on the disk or in-memory map, at shadow or off, whose
// stored subtrees are acknowledged at once.
func laneMoveBenchProcessor(b *testing.B, disk, shadow bool) *SubtreeProcessor {
	b.Helper()

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = laneMoveBenchSubtreeSize
	tSettings.BlockAssembly.UseDynamicSubtreeSize = false

	if shadow {
		tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	}

	newSubtreeChan := make(chan NewSubtreeRequest, 1024)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	b.Cleanup(func() { close(done) })

	opts := laneMoveBenchOptions(shadow)
	if disk {
		opts = append(opts, WithTxMapDirs([]string{b.TempDir(), b.TempDir()}))
	}

	stp, err := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, tSettings, nil, nil, nil, newSubtreeChan, opts...)
	require.NoError(b, err)

	return stp
}

// heapPeak samples the live heap until stop is called, and returns the highest reading above the
// one taken when it started.
func heapPeak() (stop func() uint64) {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	read := func() uint64 {
		metrics.Read(sample)
		return sample[0].Value.Uint64()
	}

	base := read()

	var (
		peak atomic.Uint64
		wg   sync.WaitGroup
	)

	done := make(chan struct{})

	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			if v := read(); v > peak.Load() {
				peak.Store(v)
			}

			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	return func() uint64 {
		close(done)
		wg.Wait()

		if p := peak.Load(); p > base {
			return p - base
		}

		return 0
	}
}

// BenchmarkLaneRemainderRebuild runs processRemainderTxHashes over 1M pooled transactions in 16
// chained subtrees, half of them mined by the block, and reports the rebuild's peak live heap.
func BenchmarkLaneRemainderRebuild(b *testing.B) {
	if testing.Short() {
		b.Skip("heavy block-movement benchmark; skipped in -short")
	}

	hashes := laneMoveBenchHashes()

	chained := make([]*subtreepkg.Subtree, 0, laneMoveBenchTxs/laneMoveBenchSubtreeSize+1)

	st, err := subtreepkg.NewTreeByLeafCount(laneMoveBenchSubtreeSize)
	require.NoError(b, err)
	require.NoError(b, st.AddCoinbaseNode())

	for _, h := range hashes {
		if st.IsComplete() {
			chained = append(chained, st)

			st, err = subtreepkg.NewTreeByLeafCount(laneMoveBenchSubtreeSize)
			require.NoError(b, err)
		}

		require.NoError(b, st.AddSubtreeNode(subtreepkg.Node{Hash: h, Fee: 100, SizeInBytes: 250}))
	}

	chained = append(chained, st)

	mined := NewSortedHashSet(laneMoveBenchTxs / 2)
	for i := 0; i < laneMoveBenchTxs; i += 2 {
		require.NoError(b, mined.Put(hashes[i]))
	}

	require.NoError(b, mined.Freeze(runtime.GOMAXPROCS(0)))

	for _, disk := range []bool{false, true} {
		for _, shadow := range []bool{false, true} {
			b.Run(fmt.Sprintf("disk=%t/shadow=%t", disk, shadow), func(b *testing.B) {
				src := laneMoveBenchSource(b, disk, hashes)

				var peakMB float64

				for i := 0; i < b.N; i++ {
					b.StopTimer()

					stp := laneMoveBenchProcessor(b, disk, shadow)

					runtime.GC()

					stop := heapPeak()

					b.StartTimer()

					require.NoError(b, stp.processRemainderTxHashes(context.Background(), chained, mined, nil, nil, src, true))

					b.StopTimer()

					peakMB = max(peakMB, float64(stop())/(1<<20))

					b.StartTimer()
				}

				b.ReportMetric(peakMB, "peak-heap-MB")
			})
		}
	}
}

// BenchmarkLaneOwnBlockSubtreeNodes runs processOwnBlockSubtreeNodes over 1M transactions left in
// the open subtree after our own block, on its parallel path.
func BenchmarkLaneOwnBlockSubtreeNodes(b *testing.B) {
	if testing.Short() {
		b.Skip("heavy block-movement benchmark; skipped in -short")
	}

	hashes := laneMoveBenchHashes()

	nodes := make([]subtreepkg.Node, len(hashes))
	for i, h := range hashes {
		nodes[i] = subtreepkg.Node{Hash: h, Fee: 100, SizeInBytes: 250}
	}

	block := &model.Block{Header: &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}}

	for _, disk := range []bool{false, true} {
		for _, shadow := range []bool{false, true} {
			b.Run(fmt.Sprintf("disk=%t/shadow=%t", disk, shadow), func(b *testing.B) {
				src := laneMoveBenchSource(b, disk, hashes)

				for i := 0; i < b.N; i++ {
					b.StopTimer()

					stp := laneMoveBenchProcessor(b, disk, shadow)

					b.StartTimer()

					require.NoError(b, stp.processOwnBlockSubtreeNodes(block, nodes, src, 0, nil, true))
				}
			})
		}
	}
}
