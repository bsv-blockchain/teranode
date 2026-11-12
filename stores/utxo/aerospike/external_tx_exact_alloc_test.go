package aerospike

import (
	"bufio"
	"bytes"
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
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
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

// retainedBy returns how much more live heap there is, after GC, while the
// value fn returns is still held.
func retainedBy(fn func() any) int64 {
	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	held := fn()

	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(held)

	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}

const mib = 1024 * 1024

// perScriptRetainBound is what holding a keptLen script read by go-bt's per-script
// read may retain: the incremental read leaves up to about a quarter of spare
// capacity on a large script. Half of droppedLen on top tells "kept script only"
// apart from "kept script plus the dropped one".
func perScriptRetainBound(keptLen, droppedLen int) int {
	return keptLen*5/4 + droppedLen/2
}

// buildTwoLargeOutputsTx returns a transaction with an OP_FALSE OP_RETURN output
// of droppedLen payload bytes at vout 0 and a spendable output at vout 1 whose
// locking script, <keptLen bytes> OP_DROP OP_1, is just over keptLen bytes.
func buildTwoLargeOutputsTx(t *testing.T, droppedLen, keptLen int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	in := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x51})}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{9}))
	tx.Inputs = append(tx.Inputs, in)

	dropped := &bscript.Script{}
	require.NoError(t, dropped.AppendOpcodes(bscript.OpFALSE, bscript.OpRETURN))
	require.NoError(t, dropped.AppendPushData(make([]byte, droppedLen)))

	kept := &bscript.Script{}
	require.NoError(t, kept.AppendPushData(make([]byte, keptLen)))
	require.NoError(t, kept.AppendOpcodes(bscript.OpDROP, bscript.Op1))

	tx.Outputs = append(tx.Outputs,
		&bt.Output{Satoshis: 0, LockingScript: dropped},
		&bt.Output{Satoshis: 1000, LockingScript: kept},
	)

	return tx
}

// storeTx writes tx's extended bytes under its txid as a FileTypeTx blob.
func storeTx(t *testing.T, s *Store, tx *bt.Tx) chainhash.Hash {
	t.Helper()

	hash := *tx.TxIDChainHash()
	require.NoError(t, s.externalStore.Set(context.Background(), hash[:], fileformat.FileTypeTx, tx.ExtendedBytes()))

	return hash
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
// the unspendable outputs and the result is cached. The output it keeps here is
// more than half the blob, so the sized decode would leave it in the arena slab,
// where it would keep the dropped data-carrier payload alive with it.
func TestGetExternalOutpointsDoesNotPinDroppedOutputs(t *testing.T) {
	const (
		droppedLen = 24 * mib
		keptLen    = 28 * mib // over half the blob, so a sized decode would leave it in the slab
	)

	s := newFileExternalStore(t)
	parentHash := storeTx(t, s, buildTwoLargeOutputsTx(t, droppedLen, keptLen))

	retained := retainedBy(func() any {
		// A post-Genesis height, so the OP_FALSE OP_RETURN output is dropped.
		tx, active, err := s.getExternalOutpoints(context.Background(), parentHash, s.settings.ChainCfgParams.GenesisActivationHeight+1)
		require.NoError(t, err)
		require.Equal(t, 1, active)
		require.Nil(t, tx.Outputs[0])
		require.Greater(t, len(*tx.Outputs[1].LockingScript), keptLen)

		return tx
	})

	t.Logf("outpoint reconstruction retains %d bytes after GC", retained)
	require.Less(t, retained, int64(perScriptRetainBound(keptLen, droppedLen)), "the dropped payload must be collectable")
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

// TestGetExternalTransactionHeldOutputDoesNotPinBlob covers how a child holds a
// parent: extending an input points PreviousTxScript at the parent's output
// script. A child holding a spendable output of a data-carrier parent must not
// keep the parent's whole blob alive once the parent itself is gone, whether
// that output is a 25-byte P2PKH change or a script well past 64KiB.
func TestGetExternalTransactionHeldOutputDoesNotPinBlob(t *testing.T) {
	tests := []struct {
		name    string
		parent  func(t *testing.T) *bt.Tx
		heldLen int
	}{
		{name: "P2PKH change", parent: buildDataCarrierTx, heldLen: 25},
		{name: "1MiB spendable script", parent: func(t *testing.T) *bt.Tx {
			return buildTwoLargeOutputsTx(t, dataCarrierPayloadSize, 1*mib)
		}, heldLen: 1*mib + 7}, // OP_PUSHDATA4 <len> <data> OP_DROP OP_1
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newFileExternalStore(t)
			parentHash := storeTx(t, s, tt.parent(t))

			retained := retainedBy(func() any {
				tx, err := s.getExternalTransaction(context.Background(), parentHash)
				require.NoError(t, err)

				// What extendInputFromPrefetchedParent keeps of the parent.
				held := tx.Outputs[1].LockingScript
				require.Equal(t, tt.heldLen, len(*held))

				return held
			})

			t.Logf("a held vout 1 script retains %d bytes after GC", retained)
			require.Less(t, retained, int64(tt.heldLen+dataCarrierPayloadSize/4), "the parent's data-carrier payload must be collectable")
		})
	}
}

// TestGetExternalTransactionUnknownLengthKeepsPerScriptRead covers a store whose
// reader cannot report the blob length (the memory store here; a file store's
// remote long-term fallback in production). It must keep go-bt's per-script
// read: an arena grown from nothing would put both large outputs in one slab, so
// holding the spendable one would pin the dropped one too.
func TestGetExternalTransactionUnknownLengthKeepsPerScriptRead(t *testing.T) {
	const (
		droppedLen = 24 * mib
		keptLen    = 28 * mib // over half the blob, so a sized decode would leave it in the slab
	)

	s := &Store{logger: ulogger.TestLogger{}, externalStore: memory.New(), settings: settings.NewSettings()}
	parentHash := storeTx(t, s, buildTwoLargeOutputsTx(t, droppedLen, keptLen))

	reader, err := s.externalStore.GetIoReader(context.Background(), parentHash[:], fileformat.FileTypeTx)
	require.NoError(t, err)

	_, known, err := remainingBlobBytes(reader)
	require.NoError(t, err)
	require.False(t, known, "the fixture needs a reader that cannot report its length")
	require.NoError(t, reader.Close())

	retained := retainedBy(func() any {
		tx, err := s.getExternalTransaction(context.Background(), parentHash)
		require.NoError(t, err)
		require.Equal(t, parentHash, *tx.TxIDChainHash())

		return tx.Outputs[1].LockingScript
	})

	t.Logf("a held vout 1 script retains %d bytes after GC", retained)
	require.Less(t, retained, int64(perScriptRetainBound(keptLen, droppedLen)), "the dropped payload must be collectable")
}

// TestDecodeOwnTxBlobRefusesArenaAboveMaxArenaAlloc covers a reported length
// above MaxArenaAlloc (appended junk, a misreporting store, or a valid
// transaction whose extended form passes 1GiB): the decode must not preallocate
// that length.
func TestDecodeOwnTxBlobRefusesArenaAboveMaxArenaAlloc(t *testing.T) {
	small := buildParentTx(t, 1000, 0xaa)

	var (
		tx        *bt.Tx
		decodeErr error
	)

	allocated := allocatedBy(func() {
		tx = &bt.Tx{}
		decodeErr = decodeOwnTxBlob(tx, bytes.NewReader(small.ExtendedBytes()), bt.MaxArenaAlloc+1, true)
	})
	require.NoError(t, decodeErr)
	require.Equal(t, *small.TxIDChainHash(), *tx.TxIDChainHash())
	require.Less(t, allocated, uint64(mib), "a reported length above MaxArenaAlloc must not be preallocated")
}

// buildSpreadOutputsTx returns a transaction with one input and n outputs whose
// locking scripts are scriptLen bytes each, so no script is anywhere near half
// the blob. The input carries a P2PKH previous script and a typical unlocking
// script, as an extended parent does.
func buildSpreadOutputsTx(t *testing.T, n, scriptLen int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	in := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: uint64(n) * 1000,
		PreviousTxScript:   bscript.NewFromBytes(append([]byte{0x76, 0xa9, 0x14}, append(make([]byte, 20), 0x88, 0xac)...)),
		UnlockingScript:    bscript.NewFromBytes(make([]byte, 107)),
		SequenceNumber:     0xffffffff,
	}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{11}))
	tx.Inputs = append(tx.Inputs, in)

	tx.Outputs = make([]*bt.Output, n)
	for i := range tx.Outputs {
		tx.Outputs[i] = &bt.Output{Satoshis: 1000, LockingScript: bscript.NewFromBytes(make([]byte, scriptLen))}
	}

	return tx
}

// TestGetExternalTransactionHeldParentRetainsNoMoreThanPerScriptRead covers a
// parent held whole, as externalTxCache and Data.Tx hold it, when no script in
// it is half the blob. Every output is then copied out of the arena, so if any
// input script stayed in the slab the parent would keep the whole blob alive on
// top of the copies. It must retain no more than go-bt's per-script read of the
// same blob retains.
func TestGetExternalTransactionHeldParentRetainsNoMoreThanPerScriptRead(t *testing.T) {
	tests := []struct {
		name      string
		outputs   int
		scriptLen int
	}{
		{name: "fan-out of 100,000 P2PKH outputs", outputs: 100_000, scriptLen: 25},
		{name: "five mid-sized outputs", outputs: 5, scriptLen: 4 * mib},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Built in a closure so the fixture tx is garbage before either measurement.
			blobBytes, parentHash := func() ([]byte, chainhash.Hash) {
				parent := buildSpreadOutputsTx(t, tt.outputs, tt.scriptLen)
				return parent.ExtendedBytes(), *parent.TxIDChainHash()
			}()
			blobLen := len(blobBytes)

			s := newFileExternalStore(t)
			require.NoError(t, s.externalStore.Set(context.Background(), parentHash[:], fileformat.FileTypeTx, blobBytes))

			perScript := retainedBy(func() any {
				tx := &bt.Tx{}
				_, err := tx.ReadFrom(bytes.NewReader(blobBytes))
				require.NoError(t, err)

				return tx
			})

			sized := retainedBy(func() any {
				tx, err := s.getExternalTransaction(context.Background(), parentHash)
				require.NoError(t, err)
				require.Len(t, tx.Outputs, tt.outputs)

				return tx
			})

			// blobBytes is last used by the per-script read; keep it live past both
			// measurements so its own release is not netted off either of them.
			runtime.KeepAlive(blobBytes)

			t.Logf("blob %d bytes; held parent retains %d bytes after GC, per-script read %d", blobLen, sized, perScript)
			require.Less(t, sized, perScript+int64(blobLen/2), "the blob must be collectable once every script is copied out")
		})
	}
}
