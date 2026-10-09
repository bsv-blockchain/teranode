package blockpersister

import (
	"encoding/binary"
	"net/url"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Large-subtree fixture: every tx carries one big previous locking script (extended
// input) and one big output, the shape that made testnet block 1681787's single
// subtree 1.5x the size of the block.
const (
	largeSubtreeTxCount    = 32
	largeSubtreeScriptSize = 1 << 20 // 1 MiB per script, 2 MiB of scripts per tx
)

// largeExtendedTx returns a 1-in-1-out transaction whose input carries a previous
// locking script of scriptSize bytes and whose output has a locking script of
// scriptSize bytes. seed makes every transaction distinct.
func largeExtendedTx(t *testing.T, seed uint32, scriptSize int) *bt.Tx {
	t.Helper()

	prevScript := make([]byte, scriptSize)
	lockScript := make([]byte, scriptSize)

	for i := range prevScript {
		prevScript[i] = byte(i) ^ byte(seed)
		lockScript[i] = byte(i>>3) ^ byte(seed>>1)
	}

	// OP_1 first, so the output is never mistaken for an OP_FALSE OP_RETURN data output.
	lockScript[0] = bscript.Op1
	binary.LittleEndian.PutUint32(lockScript[1:5], seed)

	var prevHashBytes [32]byte
	binary.LittleEndian.PutUint32(prevHashBytes[:4], seed+1)
	prevHash := chainhash.Hash(prevHashBytes)

	input := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 2000,
		PreviousTxScript:   bscript.NewFromBytes(prevScript),
		UnlockingScript:    bscript.NewFromBytes([]byte{bscript.Op1}),
		SequenceNumber:     0xffffffff,
	}
	require.NoError(t, input.PreviousTxIDAdd(&prevHash))

	tx := bt.NewTx()
	tx.Inputs = append(tx.Inputs, input)
	tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: bscript.NewFromBytes(lockScript)})

	return tx
}

// storeLargeSubtree writes a subtree of txCount large extended transactions, and its
// subtreeData file, to a file-backed blob store. A file store keeps the data off the
// heap, so a heap measurement taken while the persister reads it shows only what the
// persister itself holds.
func storeLargeSubtree(t *testing.T, txCount, scriptSize int) (*file.File, *subtreepkg.Subtree, int) {
	t.Helper()

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	// The existing-file path calls SetDAH on both subtree files, which the file store refuses
	// without a deletion scheduler.
	subtreeStore, err := file.New(ulogger.TestLogger{}, storeURL, options.WithBlobDeletionScheduler(&mockBlobDeletionScheduler{}))
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(txCount)
	require.NoError(t, err)

	data := make([]byte, 0, txCount*(2*scriptSize+256))

	for i := 0; i < txCount; i++ {
		tx := largeExtendedTx(t, uint32(i), scriptSize)
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size())))
		data = append(data, tx.ExtendedBytes()...)
	}

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))
	require.NoError(t, subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, data))

	return subtreeStore, subtree, len(data)
}

// peakHeapGrowth runs fn and returns the highest HeapAlloc seen above the
// pre-call baseline. GOGC is lowered for the duration so uncollected garbage
// stays small and the figure tracks what fn keeps live.
func peakHeapGrowth(t *testing.T, fn func()) uint64 {
	t.Helper()

	defer debug.SetGCPercent(debug.SetGCPercent(10))

	runtime.GC()

	var ms runtime.MemStats

	runtime.ReadMemStats(&ms)
	baseline := ms.HeapAlloc

	var peak atomic.Uint64

	sample := func() {
		var m runtime.MemStats

		runtime.ReadMemStats(&m)

		if m.HeapAlloc > peak.Load() {
			peak.Store(m.HeapAlloc)
		}
	}

	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		for {
			sample()

			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()

	func() {
		defer func() {
			close(stop)
			<-done
		}()

		fn()
		sample()
	}()

	if peak.Load() <= baseline {
		return 0
	}

	return peak.Load() - baseline
}

// TestProcessSubtreeUTXOStreaming_MemoryDoesNotScaleWithSubtree reproduces cause 1 of
// issue 1914: the decode arena is never reset inside the per-tx loop, so every script
// of every tx in the subtree accumulates in one doubling slab.
func TestProcessSubtreeUTXOStreaming_MemoryDoesNotScaleWithSubtree(t *testing.T) {
	ctx := t.Context()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	subtreeStore, subtree, dataSize := storeLargeSubtree(t, largeSubtreeTxCount, largeSubtreeScriptSize)

	blockStoreURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	blockStore, err := file.New(logger, blockStoreURL)
	require.NoError(t, err)

	persister := New(ctx, logger, tSettings, blockStore, subtreeStore, nil, nil)

	blockHash := chainhash.DoubleHashH([]byte("issue-1914-phase-2"))
	utxoDiff, err := utxopersister.NewUTXOSet(ctx, logger, tSettings, blockStore, &blockHash, 2000)
	require.NoError(t, err)

	defer func() { _ = utxoDiff.Close() }()

	growth := peakHeapGrowth(t, func() {
		err = persister.ProcessSubtreeUTXOStreaming(ctx, *subtree.RootHash(), utxoDiff)
	})
	require.NoError(t, err)

	t.Logf("subtreeData %d MiB, peak heap growth %d MiB", dataSize>>20, growth>>20)

	require.Less(t, growth, uint64(dataSize/2),
		"peak heap growth must not scale with the subtreeData size (%d bytes)", dataSize)
}

// TestCreateSubtreeDataFileStreaming_ExistingFileMemoryDoesNotScaleWithSubtree
// reproduces cause 2 of issue 1914: validating an existing subtreeData file decodes
// the whole subtree into memory just to check that it parses.
func TestCreateSubtreeDataFileStreaming_ExistingFileMemoryDoesNotScaleWithSubtree(t *testing.T) {
	ctx := t.Context()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	subtreeStore, subtree, dataSize := storeLargeSubtree(t, largeSubtreeTxCount, largeSubtreeScriptSize)

	persister := New(ctx, logger, tSettings, nil, subtreeStore, nil, nil)

	// The existing-file path only uses the block for logging; it never reads its coinbase.
	block := &model.Block{
		Header: &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
		},
		Subtrees: []*chainhash.Hash{subtree.RootHash()},
	}

	var err error

	growth := peakHeapGrowth(t, func() {
		err = persister.CreateSubtreeDataFileStreaming(ctx, *subtree.RootHash(), block, 1)
	})
	require.NoError(t, err)

	// The existing file was accepted, not recreated: a recreated file would be rebuilt from the
	// UTXO store (nil here) and written without the extended input data, so its size would differ.
	kept, err := subtreeStore.Get(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.Len(t, kept, dataSize)

	t.Logf("subtreeData %d MiB, peak heap growth %d MiB", dataSize>>20, growth>>20)

	require.Less(t, growth, uint64(dataSize/2),
		"peak heap growth must not scale with the subtreeData size (%d bytes)", dataSize)
}
