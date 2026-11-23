package subtreeprocessor

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// BenchmarkDequeueThroughput measures steady-state block assembly: batches
// enqueued as the ingest handlers do, drained by the processor's dequeue loop
// into subtrees. Subtree storage is acknowledged immediately, so this isolates
// the processor and its tx map.
//
// Run with -benchtime=1x; each iteration pushes total transactions.
func BenchmarkDequeueThroughput(b *testing.B) {
	const (
		batchSize       = 1024 // blockassembly_sendBatchSize on the scaling cluster
		itemsPerSubtree = 1 << 20
	)

	variants := []struct {
		name  string
		disks int
	}{
		{name: "memTxMap"},
		{name: "diskTxMap_1dir", disks: 1},
		{name: "diskTxMap_2dir", disks: 2},
	}

	for _, total := range []int{8 << 20} {
		for _, v := range variants {
			b.Run(fmt.Sprintf("%s/%dM", v.name, total>>20), func(b *testing.B) {
				nodes, inpoints := makeDequeueBenchBatches(total, batchSize)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					b.StopTimer()

					var opts []Options

					if v.disks > 0 {
						dirs := make([]string, v.disks)
						for d := range dirs {
							dirs[d] = b.TempDir()
						}

						opts = append(opts, WithTxMapDirs(dirs))
					}

					stp, cleanup := newAddNodesBenchProcessor(b, itemsPerSubtree, opts...)
					stp.Start(context.Background())

					// the coinbase placeholder is counted too
					want := stp.TxCount() + uint64(total)

					b.StartTimer()

					for j := range nodes {
						stp.AddBatch(nodes[j], inpoints[j])
					}

					for stp.TxCount() < want {
						time.Sleep(time.Millisecond)
					}

					b.StopTimer()
					cleanup()
					b.StartTimer()
				}

				b.ReportMetric(float64(b.N*total)/b.Elapsed().Seconds(), "txs/s")
			})
		}
	}
}

// makeDequeueBenchBatches builds the batches once so the (reused) inpoints are
// not part of the measurement. Each tx has one parent, like a typical blaster tx.
func makeDequeueBenchBatches(total, batchSize int) ([][]subtreepkg.Node, [][]*subtreepkg.TxInpoints) {
	var (
		nodes    [][]subtreepkg.Node
		inpoints [][]*subtreepkg.TxInpoints
	)

	for start := 0; start < total; start += batchSize {
		n := min(batchSize, total-start)
		bn := make([]subtreepkg.Node, n)
		bi := make([]*subtreepkg.TxInpoints, n)

		for j := 0; j < n; j++ {
			var h, parent chainhash.Hash

			binary.LittleEndian.PutUint64(h[:], 0x9e3779b97f4a7c15*uint64(start+j+1))
			binary.LittleEndian.PutUint64(h[8:], uint64(start+j)+1)
			binary.LittleEndian.PutUint64(parent[:], ^uint64(start+j))

			bn[j] = subtreepkg.Node{Hash: h, Fee: 200, SizeInBytes: 250}
			in := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent}, []uint32{1, 0})
			bi[j] = &in
		}

		nodes = append(nodes, bn)
		inpoints = append(inpoints, bi)
	}

	return nodes, inpoints
}
