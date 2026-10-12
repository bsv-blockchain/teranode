package subtreeprocessor

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// BenchmarkDrainQueueAfterBlock compares the sequential drain
// (dequeueDuringBlockMovement) with drainQueueAfterBlock on a queue of total
// txs, none of them in the block, into 64k-leaf subtrees. Run with
// -benchtime=1x; each iteration drains total txs into a fresh processor.
func BenchmarkDrainQueueAfterBlock(b *testing.B) {
	const total = 2 << 20

	for _, disk := range []bool{false, true} {
		for _, parallel := range []bool{false, true} {
			name := fmt.Sprintf("memTxMap/parallel=%t", parallel)
			if disk {
				name = fmt.Sprintf("diskTxMap/parallel=%t", parallel)
			}

			b.Run(name, func(b *testing.B) {
				hashes := genBenchHashes(total, 7)
				inpoints := make([]*subtreepkg.TxInpoints, total)

				for i := range inpoints {
					// One parent, vout 0: inpoints the disk map can serialize.
					ip := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{hashes[(i+1)%total]}, []uint32{1, 0})
					inpoints[i] = &ip
				}

				transactionMap := NewSplitSwissMap(1024, 1)
				transactionMap.Freeze()

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					b.StopTimer()

					stp := newDrainBenchProcessor(b, disk)

					for start := 0; start < total; start += 1024 {
						nodes := make([]subtreepkg.Node, 0, 1024)
						for _, h := range hashes[start:min(start+1024, total)] {
							nodes = append(nodes, subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 250})
						}

						stp.AddBatch(nodes, inpoints[start:min(start+1024, total)])
					}

					time.Sleep(5 * time.Millisecond)
					b.StartTimer()

					if parallel {
						require.NoError(b, stp.drainQueueAfterBlock(context.Background(), &deferredBlockDrain{drainQueue: true, transactionMap: transactionMap}))
					} else {
						require.NoError(b, stp.dequeueDuringBlockMovement(transactionMap, nil, nil, false))
					}

					stp.flushDiskTxMapWriters()

					b.StopTimer()
					require.NoError(b, stp.diskTxMapErr(), "the drain must not take a storage error path")
					b.StartTimer()
				}

				b.ReportMetric(float64(total)*float64(b.N)/b.Elapsed().Seconds(), "tx/s")
			})
		}
	}
}

func newDrainBenchProcessor(b *testing.B, disk bool) *SubtreeProcessor {
	b.Helper()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.DoubleSpendWindow = 0
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 1 << 16
	tSettings.BlockAssembly.TxMapDirs = nil
	tSettings.BlockAssembly.SubtreeMmapDir = ""

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(b, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(b, err)

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	newSubtreeChan := make(chan NewSubtreeRequest, 64)

	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	b.Cleanup(func() { close(newSubtreeChan) })

	subtreeStore, _ := null.New(logger)

	var opts []Options
	if disk {
		opts = append(opts, WithTxMapDirs([]string{b.TempDir()}))
	}

	stp, err := NewSubtreeProcessor(ctx, logger, tSettings, subtreeStore, blockchainClient, utxoStore, newSubtreeChan, opts...)
	require.NoError(b, err)

	b.Cleanup(func() {
		for _, m := range []*DiskTxMap{stp.diskTxMap, stp.diskTxMapShadow} {
			if m != nil {
				_ = m.Close()
			}
		}
	})

	stp.currentBlockHeader.Store(model.GenesisBlockHeader)

	return stp
}
