package utxovalidator

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateUTXOFile(t *testing.T) {
	t.Skip("Skipping long-running test that requires large UTXO file on disk")

	file := "../../746044/00000000000000000c60d122906015547c6ba4c46ed29f62a6a30a73819ae960.utxo-set"

	tSettings := test.CreateBaseTestSettings(t)

	result, err := ValidateUTXOFile(t.Context(), file, ulogger.TestLogger{}, tSettings, false)
	require.NoError(t, err)

	t.Logf("Block Height: %d", result.BlockHeight)
	t.Logf("Block Hash: %s", result.BlockHash.String())
	t.Logf("Previous Hash: %s", result.PreviousHash.String())
	t.Logf("Actual Satoshis: %d", result.ActualSatoshis)
	t.Logf("Expected Satoshis: %d", result.ExpectedSatoshis)
	t.Logf("Is Valid: %t", result.IsValid)
	t.Logf("UTXO Count: %d", result.UTXOCount)
}

// TestValidateUTXOFile_LocalDoesNotDoubleReadMagic pins that the
// local-file path through ValidateUTXOFile consumes the fileformat
// magic exactly once. Previously, getLocalFileReader returned a raw
// os.File and validateUTXOSet called fileformat.ReadHeader on it
// — which worked for local files but mirrored a bug in
// utxopersister where the same call was made on a reader the blob
// store had already advanced past. The fix moved the
// read-and-validate-magic step into getLocalFileReader so both
// reader sources (local file, blob store) hand validateUTXOSet a
// body-only reader; the function no longer calls ReadHeader.
//
// Without that fix, removing ReadHeader from validateUTXOSet would
// have silently misread the first 8 bytes of the block hash field
// as a magic and rejected every local file as "unknown magic"; with
// the fix, both source paths converge on a single body-reading
// contract.
func TestValidateUTXOFile_LocalDoesNotDoubleReadMagic(t *testing.T) {
	ctx := context.Background()

	blockHash := chainhash.HashH([]byte("utxovalidator-double-read-current-block"))
	prevHash := chainhash.HashH([]byte("utxovalidator-double-read-previous-block"))

	// Build a minimal UTXO set file: 8-byte fileformat magic + 32-byte
	// current block hash + 4-byte height + 32-byte previous block hash
	// + zero UTXO wrappers. The OUTER wrapper loop in validateUTXOSet
	// breaks on either io.EOF or "failed to read txid", so a body that
	// ends after the metadata exercises the read path cleanly.
	header := fileformat.NewHeader(fileformat.FileTypeUtxoSet)
	body := make([]byte, 0, 8+32+4+32)
	body = append(body, header.Bytes()...)
	body = append(body, blockHash[:]...)
	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], 42)
	body = append(body, heightBuf[:]...)
	body = append(body, prevHash[:]...)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.utxo-set")
	require.NoError(t, os.WriteFile(path, body, 0o600))

	tSettings := test.CreateBaseTestSettings(t)
	result, err := ValidateUTXOFile(ctx, path, ulogger.TestLogger{}, tSettings, false)
	require.NoError(t, err, "ValidateUTXOFile must not double-read the fileformat magic; a regression here would surface as \"unknown magic: [...]\"")
	assert.Equal(t, blockHash.String(), result.BlockHash.String(), "BlockHash parsed from body must match what we wrote")
	assert.Equal(t, uint32(42), result.BlockHeight)
	assert.Equal(t, prevHash.String(), result.PreviousHash.String(), "PreviousHash parsed from body must match what we wrote")
}

// TestValidateUTXOFile_LocalRejectsWrongFileType pins that the
// magic + FileType validation moved into getLocalFileReader still
// rejects files of the wrong type (the check used to live in
// validateUTXOSet). A subtree file with subtree magic should fail
// with a clear "not a UTXO set file" error.
func TestValidateUTXOFile_LocalRejectsWrongFileType(t *testing.T) {
	ctx := context.Background()

	// Build a body with subtree magic — a recognised file type, but
	// not FileTypeUtxoSet, so getLocalFileReader should reject it.
	header := fileformat.NewHeader(fileformat.FileTypeSubtree)
	body := append([]byte(nil), header.Bytes()...)
	body = append(body, []byte("some-arbitrary-content")...)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.subtree")
	require.NoError(t, os.WriteFile(path, body, 0o600))

	tSettings := test.CreateBaseTestSettings(t)
	_, err := ValidateUTXOFile(ctx, path, ulogger.TestLogger{}, tSettings, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a UTXO set file")
}
