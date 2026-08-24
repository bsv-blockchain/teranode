package seeder

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// writeCompleteSnapshotFile assembles a well-formed .utxo-set file byte for
// byte in the layout CreateUTXOSet writes (services/utxopersister/UTXOSet.go):
// fileformat header, then blockHash(32)+height(4)+previousBlockHash(32), then
// one UTXOWrapper.Bytes() record per wrapper, then the trailing 16-byte
// footer (txCount + utxoCount, little-endian) that GetFooter reads back. It
// reuses the same per-record encoding (UTXOWrapper.Bytes()) that the real
// persister writes, so the file the seeder reads back is byte-identical to
// what production would have produced for these records.
func writeCompleteSnapshotFile(t *testing.T, wrappers []*utxopersister.UTXOWrapper) string {
	t.Helper()

	var blockHash chainhash.Hash
	blockHash[0] = 0xcc

	var prevHash chainhash.Hash
	prevHash[0] = 0xdd

	header := fileformat.NewHeader(fileformat.FileTypeUtxoSet)

	data := make([]byte, 0)
	data = append(data, header.Bytes()...)
	data = append(data, blockHash[:]...)

	heightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(heightBytes, 200)
	data = append(data, heightBytes...)

	data = append(data, prevHash[:]...)

	var txCount, utxoCount uint64

	for _, w := range wrappers {
		data = append(data, w.Bytes()...)
		txCount++
		utxoCount += uint64(len(w.UTXOs))
	}

	footer := make([]byte, 16)
	binary.LittleEndian.PutUint64(footer[0:8], txCount)
	binary.LittleEndian.PutUint64(footer[8:16], utxoCount)
	data = append(data, footer...)

	path := filepath.Join(t.TempDir(), "roundtrip.utxo-set")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

// importSnapshotFile drives the file through the same functions Seeder's
// processUTXOs uses: readUTXOWrapperFile feeds a channel that processUTXO
// drains into store, exactly as worker() does, but without the extra worker
// pool machinery that's orthogonal to file-format correctness.
func importSnapshotFile(t *testing.T, path string, store *utxosql.Store) error {
	t.Helper()

	f, reader := openForReading(t, path)

	utxoWrapperCh := make(chan *utxopersister.UTXOWrapper, 10)

	readErrCh := make(chan error, 1)

	go func() {
		readErrCh <- readUTXOWrapperFile(context.Background(), ulogger.TestLogger{}, f, reader, utxoWrapperCh)
	}()

	var processErr error

	for w := range utxoWrapperCh {
		if err := processUTXO(context.Background(), store, w, nil); err != nil && processErr == nil {
			processErr = err
		}
	}

	readErr := <-readErrCh

	if readErr != nil {
		return readErr
	}

	return processErr
}

// TestSeederImport_RoundTripsRealSnapshotFile writes a real .utxo-set file
// using the persister's own UTXOWrapper record encoding, feeds it through the
// seeder's import path (readUTXOWrapperFile -> processUTXO), and checks the
// exact record content - not just counts - survives into the UTXO store.
// This is the round-trip half of the two tests the audit recommended
// alongside the truncated-file regression test.
func TestSeederImport_RoundTripsRealSnapshotFile(t *testing.T) {
	ctx := context.Background()
	store := newTestUtxoStore(ctx, t)

	txA := chainhash.HashH([]byte("roundtrip-tx-a"))
	txB := chainhash.HashH([]byte("roundtrip-tx-b"))

	wrappers := []*utxopersister.UTXOWrapper{
		{
			TxID:   txA,
			Height: 200,
			UTXOs: []*utxopersister.UTXO{
				{Index: 0, Value: 1234, Script: []byte{0x76, 0xa9, 0x14}},
				{Index: 1, Value: 5678, Script: []byte{0x51}},
			},
		},
		{
			TxID:   txB,
			Height: 200,
			UTXOs: []*utxopersister.UTXO{
				{Index: 2, Value: 999999, Script: []byte{0x6a, 0x01, 0x02, 0x03}},
			},
		},
	}

	path := writeCompleteSnapshotFile(t, wrappers)

	require.NoError(t, importSnapshotFile(t, path, store))

	gotA, err := store.Get(ctx, &txA)
	require.NoError(t, err)
	require.NotNil(t, gotA.Tx)
	require.Len(t, gotA.Tx.Outputs, 2, "both outputs must be present; txA has no padded holes")
	require.Equal(t, uint64(1234), gotA.Tx.Outputs[0].Satoshis)
	require.Equal(t, []byte{0x76, 0xa9, 0x14}, gotA.Tx.Outputs[0].LockingScript.Bytes())
	require.Equal(t, uint64(5678), gotA.Tx.Outputs[1].Satoshis)
	require.Equal(t, []byte{0x51}, gotA.Tx.Outputs[1].LockingScript.Bytes())

	gotB, err := store.Get(ctx, &txB)
	require.NoError(t, err)
	require.NotNil(t, gotB.Tx)
	// txB's only UTXO is at index 2; the store keeps only real (non-nil)
	// outputs, so the padded holes at indices 0 and 1 (PadUTXOsWithNil) are
	// not themselves stored - only the surviving output's value and script
	// round-trip. Content alone isn't enough to prove this: the store's
	// compacted read view would return the same slice whether the UTXO was
	// written at index 2 (correct) or index 0 (an index-computation bug), so
	// assert against the actual on-disk index directly via GetSpend, which
	// queries by (txid, vout) rather than by position in a compacted slice.
	require.Len(t, gotB.Tx.Outputs, 1)
	require.Equal(t, uint64(999999), gotB.Tx.Outputs[0].Satoshis)
	require.Equal(t, []byte{0x6a, 0x01, 0x02, 0x03}, gotB.Tx.Outputs[0].LockingScript.Bytes())

	spendAtCorrectIndex, err := store.GetSpend(ctx, &utxo.Spend{TxID: &txB, Vout: 2})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), spendAtCorrectIndex.Status,
		"the UTXO must be recorded at vout 2, its real index")

	spendAtWrongIndex, err := store.GetSpend(ctx, &utxo.Spend{TxID: &txB, Vout: 0})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_NOT_FOUND), spendAtWrongIndex.Status,
		"the UTXO must not have been mis-indexed to vout 0, the first padded hole")
}

// TestSeederImport_CompleteFile_FinishesCleanly feeds the seeder a
// well-formed, complete snapshot file (footer counts match the records
// actually written) and asserts the whole import finishes without error,
// every record lands in the UTXO store, and the footer counts line up with
// what was processed - the positive-path counterpart to #14's
// truncated-file regression test.
func TestSeederImport_CompleteFile_FinishesCleanly(t *testing.T) {
	ctx := context.Background()
	store := newTestUtxoStore(ctx, t)

	metadata, wrapperBytes, txCount, utxoCount := buildSnapshotWrappers(t)
	require.Len(t, wrapperBytes, 2)

	header := fileformat.NewHeader(fileformat.FileTypeUtxoSet)

	data := make([]byte, 0)
	data = append(data, header.Bytes()...)
	data = append(data, metadata...)

	for _, wb := range wrapperBytes {
		data = append(data, wb...)
	}

	footer := make([]byte, 16)
	binary.LittleEndian.PutUint64(footer[0:8], txCount)
	binary.LittleEndian.PutUint64(footer[8:16], utxoCount)
	data = append(data, footer...)

	path := filepath.Join(t.TempDir(), "complete.utxo-set")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	require.NoError(t, importSnapshotFile(t, path, store))

	// The two wrappers built by buildSnapshotWrappers use TxID[0] = 0x01/0x02.
	var txid1, txid2 chainhash.Hash
	txid1[0] = 0x01
	txid2[0] = 0x02

	got1, err := store.Get(ctx, &txid1)
	require.NoError(t, err)
	require.NotNil(t, got1.Tx)
	require.Len(t, got1.Tx.Outputs, 1)
	require.Equal(t, uint64(5000000000), got1.Tx.Outputs[0].Satoshis)

	got2, err := store.Get(ctx, &txid2)
	require.NoError(t, err)
	require.NotNil(t, got2.Tx)
	require.Len(t, got2.Tx.Outputs, 2)
	require.Equal(t, uint64(1234), got2.Tx.Outputs[0].Satoshis)
	require.Equal(t, uint64(5678), got2.Tx.Outputs[1].Satoshis)

	// The footer itself must reflect exactly what was written, confirming the
	// writer and reader agree on the counts as well as the content.
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	gotTxCount, gotUTXOCount, err := utxopersister.GetFooter(f)
	require.NoError(t, err)
	require.Equal(t, txCount, gotTxCount)
	require.Equal(t, utxoCount, gotUTXOCount)
}
