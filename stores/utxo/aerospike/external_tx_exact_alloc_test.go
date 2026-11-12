package aerospike

import (
	"bufio"
	"context"
	"net/url"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// dataCarrierPayloadSize is the size of the OP_FALSE OP_RETURN payload in the
// issue-2176 fixtures: tens of MB, like the testnet transactions that drove the
// subtreevalidation OOM kills.
const dataCarrierPayloadSize = 32 * 1024 * 1024

// buildDataCarrierTx returns a transaction shaped like the issue-2176 parents:
// a large OP_FALSE OP_RETURN output at vout 0 and a P2PKH change output at vout 1.
func buildDataCarrierTx(t *testing.T) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	in := &bt.Input{
		PreviousTxOutIndex: 1,
		PreviousTxSatoshis: 2000,
		PreviousTxScript:   bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x01, 0x88, 0xac}),
		UnlockingScript:    bscript.NewFromBytes([]byte{0x01, 0x02, 0x03}),
		SequenceNumber:     0xffffffff,
	}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{7}))
	tx.Inputs = append(tx.Inputs, in)

	payload := make([]byte, dataCarrierPayloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	dataScript := &bscript.Script{}
	require.NoError(t, dataScript.AppendOpcodes(bscript.OpFALSE, bscript.OpRETURN))
	require.NoError(t, dataScript.AppendPushData(payload))

	tx.Outputs = append(tx.Outputs,
		&bt.Output{Satoshis: 0, LockingScript: dataScript},
		&bt.Output{Satoshis: 1000, LockingScript: bscript.NewFromBytes(append([]byte{0x76, 0xa9, 0x14}, append(make([]byte, 20), 0x88, 0xac)...))},
	)

	return tx
}

// newFileExternalStore returns a Store whose external store is a file blob store,
// built the way New builds it, so the reader it hands back can seek.
func newFileExternalStore(t *testing.T) *Store {
	t.Helper()

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	externalStore, err := blob.NewStore(ulogger.TestLogger{}, storeURL, options.WithDisableDAH(true))
	require.NoError(t, err)

	tSettings := settings.NewSettings()

	return &Store{logger: ulogger.TestLogger{}, externalStore: externalStore, settings: tSettings}
}

// allocatedBy returns the bytes the Go heap allocated while fn ran.
func allocatedBy(fn func()) uint64 {
	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	fn()

	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc
}

// TestGetExternalTransactionAllocatesBlobSizeOnce pins issue 2176: decoding the
// node's own external blob must allocate its scripts once, not grow each one in
// 64KiB steps the way go-bt's incremental anti-DoS read does (about 5x the
// script size for a large data carrier).
func TestGetExternalTransactionAllocatesBlobSizeOnce(t *testing.T) {
	ctx := context.Background()

	parent := buildDataCarrierTx(t)
	parentHash := *parent.TxIDChainHash()
	blobBytes := parent.ExtendedBytes()
	blobSize := uint64(len(blobBytes))

	s := newFileExternalStore(t)
	require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, blobBytes))

	t.Run("decode allocates about the blob size", func(t *testing.T) {
		var (
			tx        *bt.Tx
			decodeErr error
		)

		reader, err := s.externalStore.GetIoReader(ctx, parentHash[:], fileformat.FileTypeTx)
		require.NoError(t, err)

		defer reader.Close()

		blobLen, known, err := remainingBlobBytes(reader)
		require.NoError(t, err)
		require.True(t, known, "the file store's reader must report its length")
		require.Equal(t, int(blobSize), blobLen)

		allocated := allocatedBy(func() {
			tx = &bt.Tx{}
			decodeErr = decodeOwnTxBlob(tx, bufio.NewReader(reader), blobLen, known)
		})
		require.NoError(t, decodeErr)
		require.Equal(t, parentHash, *tx.TxIDChainHash())

		t.Logf("blob %d bytes, decode allocated %d bytes (%.2fx)", blobSize, allocated, float64(allocated)/float64(blobSize))
		require.Less(t, allocated, blobSize*5/4, "decode must allocate the blob about once")
	})

	t.Run("getExternalTransaction allocates the decode plus one re-hash serialisation", func(t *testing.T) {
		var (
			tx     *bt.Tx
			getErr error
		)

		allocated := allocatedBy(func() {
			tx, getErr = s.getExternalTransaction(ctx, parentHash)
		})
		require.NoError(t, getErr)
		require.Equal(t, parentHash, *tx.TxIDChainHash())
		require.Equal(t, parent.Bytes(), tx.Bytes())

		// The decode (1x) plus tx.Bytes() for the txid re-hash (1x). With the
		// incremental read the decode alone is about 5x.
		t.Logf("blob %d bytes, getExternalTransaction allocated %d bytes (%.2fx)", blobSize, allocated, float64(allocated)/float64(blobSize))
		require.Less(t, allocated, blobSize*9/4, "getExternalTransaction must allocate about twice the blob")
	})
}

// TestGetExternalOutpointsDoesNotPinDroppedOutputs guards the one own-blob path
// that keeps the per-script decode. getExternalOutpoints drops the inputs and
// the unspendable outputs and the result is cached; if its scripts shared one
// slab, the surviving 25-byte P2PKH script would keep the dropped data-carrier
// payload alive with it.
func TestGetExternalOutpointsDoesNotPinDroppedOutputs(t *testing.T) {
	ctx := context.Background()

	parent := buildDataCarrierTx(t)
	parentHash := *parent.TxIDChainHash()

	s := newFileExternalStore(t)
	require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, parent.ExtendedBytes()))

	parent = nil

	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	// A post-Genesis height, so the OP_FALSE OP_RETURN output is dropped.
	tx, active, err := s.getExternalOutpoints(ctx, parentHash, s.settings.ChainCfgParams.GenesisActivationHeight+1)
	require.NoError(t, err)
	require.Equal(t, 1, active)
	require.Nil(t, tx.Outputs[0])
	require.Len(t, *tx.Outputs[1].LockingScript, 25)

	runtime.GC()
	runtime.ReadMemStats(&after)

	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("outpoint reconstruction retains %d bytes after GC", retained)
	require.Less(t, retained, int64(dataCarrierPayloadSize/4), "the dropped payload must be collectable")

	runtime.KeepAlive(tx)
}

// TestGetExternalTransactionTruncatedBlobOnFileStore checks the sized decode
// keeps the storage-fault classification for a torn blob: the arena path fails
// on the short read just as the incremental one does.
func TestGetExternalTransactionTruncatedBlobOnFileStore(t *testing.T) {
	ctx := context.Background()

	parent := buildDataCarrierTx(t)
	parentHash := *parent.TxIDChainHash()
	blobBytes := parent.ExtendedBytes()

	s := newFileExternalStore(t)
	require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, blobBytes[:len(blobBytes)/2]))

	_, err := s.getExternalTransaction(ctx, parentHash)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "a torn blob is a storage fault")
	require.False(t, errors.Is(err, errors.ErrTxInvalid))
}

// TestGetExternalTransactionSmallOutputDoesNotPinBlob covers how a child holds
// a parent: extending an input points PreviousTxScript at the parent's output
// script. A child spending the 25-byte P2PKH change of a data-carrier parent
// must not keep the parent's whole blob alive once the parent itself is gone.
func TestGetExternalTransactionSmallOutputDoesNotPinBlob(t *testing.T) {
	ctx := context.Background()

	parent := buildDataCarrierTx(t)
	parentHash := *parent.TxIDChainHash()

	s := newFileExternalStore(t)
	require.NoError(t, s.externalStore.Set(ctx, parentHash[:], fileformat.FileTypeTx, parent.ExtendedBytes()))

	parent = nil

	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	tx, err := s.getExternalTransaction(ctx, parentHash)
	require.NoError(t, err)

	// What extendInputFromPrefetchedParent keeps of the parent.
	changeScript := tx.Outputs[1].LockingScript
	require.Len(t, *changeScript, 25)

	tx = nil

	runtime.GC()
	runtime.ReadMemStats(&after)

	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("a held change script retains %d bytes after GC", retained)
	require.Less(t, retained, int64(dataCarrierPayloadSize/4), "the parent's data-carrier payload must be collectable")

	runtime.KeepAlive(changeScript)
}
