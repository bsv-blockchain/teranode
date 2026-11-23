package subtreeprocessor

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
)

// BenchmarkAddNodesDirectly measures the last phase of the unmined reload:
// sorted batches handed to AddNodesDirectly, which inserts into the tx map in
// parallel and appends to subtrees on one goroutine. Subtree storage is
// acknowledged immediately, so this isolates the processor itself.
//
// Run with -benchtime=1x; each iteration loads total transactions.
func BenchmarkAddNodesDirectly(b *testing.B) {
	const (
		batchSize       = 1 << 20
		itemsPerSubtree = 1 << 20
	)

	variants := []struct {
		name    string
		diskMap bool
		mmap    bool
	}{
		{name: "memTxMap"},
		{name: "diskTxMap", diskMap: true},
		{name: "diskTxMap_mmapNodes", diskMap: true, mmap: true},
	}

	for _, total := range []int{4 << 20, 16 << 20} {
		for _, v := range variants {
			b.Run(fmt.Sprintf("%s/%dM", v.name, total>>20), func(b *testing.B) {
				batches := makeUnminedBatches(total, batchSize)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					b.StopTimer()

					var opts []Options
					if v.diskMap {
						opts = append(opts, WithTxMapDirs([]string{b.TempDir(), b.TempDir()}))
					}

					if v.mmap {
						opts = append(opts, WithMmapDir(b.TempDir()))
					}

					stp, cleanup := newAddNodesBenchProcessor(b, itemsPerSubtree, opts...)

					b.StartTimer()

					for _, batch := range batches {
						if err := stp.AddNodesDirectly(batch, true); err != nil {
							b.Fatal(err)
						}
					}

					// Count the disk writes still queued in the DiskTxMap writers.
					if dm, ok := stp.currentTxMap.(*DiskTxMap); ok {
						if err := dm.Flush(); err != nil {
							b.Fatal(err)
						}
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

func makeUnminedBatches(total, batchSize int) [][]*utxo.UnminedTransaction {
	batches := make([][]*utxo.UnminedTransaction, 0, (total+batchSize-1)/batchSize)

	for start := 0; start < total; start += batchSize {
		n := min(batchSize, total-start)
		batch := make([]*utxo.UnminedTransaction, n)
		nodes := make([]subtreepkg.Node, n)
		inpoints := make([]subtreepkg.TxInpoints, n)
		txs := make([]utxo.UnminedTransaction, n)

		for j := 0; j < n; j++ {
			var h chainhash.Hash
			binary.LittleEndian.PutUint64(h[:], uint64(start+j)+1)
			binary.LittleEndian.PutUint64(h[8:], 0x9e3779b97f4a7c15*uint64(start+j+1))

			nodes[j] = subtreepkg.Node{Hash: h, Fee: 200, SizeInBytes: 250}
			txs[j] = utxo.UnminedTransaction{Node: &nodes[j], TxInpoints: &inpoints[j], CreatedAt: start + j}
			batch[j] = &txs[j]
		}

		batches = append(batches, batch)
	}

	return batches
}

func newAddNodesBenchProcessor(b testing.TB, itemsPerSubtree int, opts ...Options) (*SubtreeProcessor, func()) {
	b.Helper()

	settings := test.CreateBaseTestSettings(b)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = itemsPerSubtree
	settings.BlockAssembly.UseDynamicSubtreeSize = false

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

	ctx := context.Background()

	utxoStoreURL, err := url.Parse("sqlitememory:///addnodesbench")
	if err != nil {
		b.Fatal(err)
	}

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, settings, utxoStoreURL)
	if err != nil {
		b.Fatal(err)
	}

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blob_memory.New(), &blockchain.Mock{}, utxoStore, newSubtreeChan, opts...)
	if err != nil {
		b.Fatal(err)
	}

	// With a block header set, every completed subtree also refreshes the
	// precomputed mining data, as it does during a real reload.
	stp.currentBlockHeader.Store(model.GenesisBlockHeader)

	return stp, func() {
		stp.Stop(context.Background())
		close(done)
	}
}
