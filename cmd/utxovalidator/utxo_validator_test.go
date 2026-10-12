package utxovalidator

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
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
	// + zero UTXO wrappers + the 16-byte zero-count footer every real
	// writer appends. The OUTER wrapper loop in validateUTXOSet only
	// accepts the record stream as complete once it reaches that
	// footer and its counts agree with what was processed (0/0 here).
	header := fileformat.NewHeader(fileformat.FileTypeUtxoSet)
	body := make([]byte, 0, 8+32+4+32+16)
	body = append(body, header.Bytes()...)
	body = append(body, blockHash[:]...)
	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], 42)
	body = append(body, heightBuf[:]...)
	body = append(body, prevHash[:]...)
	body = append(body, make([]byte, 16)...) // txCount=0, utxoCount=0

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

// buildValidatorBody builds the per-file metadata (current block hash,
// height, previous block hash) plus the given wrappers, in the layout
// validateUTXOSet reads (i.e. positioned past the 8-byte fileformat magic).
func buildValidatorBody(blockHash, prevHash chainhash.Hash, height uint32, wrappers []*utxopersister.UTXOWrapper) []byte {
	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], height)

	body := make([]byte, 0)
	body = append(body, blockHash[:]...)
	body = append(body, heightBuf[:]...)
	body = append(body, prevHash[:]...)

	for _, w := range wrappers {
		body = append(body, w.Bytes()...)
	}

	return body
}

// TestValidateUTXOSet_TruncatedMidTxID_ReturnsError is the regression test
// for the silent-truncation-laundering bug in the validator's own read loop:
// a utxo-set file cut off partway through a second record's 32-byte TxID -
// with no footer at all - must fail validation loudly instead of being
// reported as a complete, successful read.
func TestValidateUTXOSet_TruncatedMidTxID_ReturnsError(t *testing.T) {
	blockHash := chainhash.HashH([]byte("validator-midtxid-block"))
	prevHash := chainhash.HashH([]byte("validator-midtxid-prev"))

	wrapper := &utxopersister.UTXOWrapper{
		TxID:   chainhash.HashH([]byte("validator-midtxid-wrapper-a")),
		Height: 100,
		UTXOs:  []*utxopersister.UTXO{{Index: 0, Value: 5000000000, Script: []byte{0x51}}},
	}
	secondTxID := chainhash.HashH([]byte("validator-midtxid-wrapper-b"))

	body := buildValidatorBody(blockHash, prevHash, 100, []*utxopersister.UTXOWrapper{wrapper})
	// Cut 10 bytes into the second record's 32-byte TxID - a genuine
	// mid-record truncation, with no footer appended at all.
	body = append(body, secondTxID[:10]...)

	_, err := validateUTXOSet(context.Background(), bytes.NewReader(body), false)
	require.Error(t, err, "a utxo-set truncated mid-TxID must not be silently accepted as a complete record stream")
}

// TestValidateUTXOSet_MissingFooter_ReturnsError covers the second traced
// laundering path: a utxo-set file cut exactly after a complete wrapper
// record, with the trailing 16-byte footer missing entirely. Every writer
// appends the footer unconditionally, so this shape can only happen from
// truncation and must always be rejected.
func TestValidateUTXOSet_MissingFooter_ReturnsError(t *testing.T) {
	blockHash := chainhash.HashH([]byte("validator-missing-footer-block"))
	prevHash := chainhash.HashH([]byte("validator-missing-footer-prev"))

	wrapper := &utxopersister.UTXOWrapper{
		TxID:   chainhash.HashH([]byte("validator-missing-footer-wrapper-a")),
		Height: 100,
		UTXOs:  []*utxopersister.UTXO{{Index: 0, Value: 5000000000, Script: []byte{0x51}}},
	}

	// No footer at all: the body ends exactly on a wrapper boundary.
	body := buildValidatorBody(blockHash, prevHash, 100, []*utxopersister.UTXOWrapper{wrapper})

	_, err := validateUTXOSet(context.Background(), bytes.NewReader(body), false)
	require.Error(t, err, "a utxo-set with a missing footer must not be silently accepted as complete")
}

// TestValidateUTXOSet_FooterMismatch_ReturnsError pins the counts check
// itself: a footer that decodes cleanly but whose counts don't match what
// the loop actually read must still be rejected.
func TestValidateUTXOSet_FooterMismatch_ReturnsError(t *testing.T) {
	blockHash := chainhash.HashH([]byte("validator-footer-mismatch-block"))
	prevHash := chainhash.HashH([]byte("validator-footer-mismatch-prev"))

	wrapper := &utxopersister.UTXOWrapper{
		TxID:   chainhash.HashH([]byte("validator-footer-mismatch-wrapper-a")),
		Height: 100,
		UTXOs:  []*utxopersister.UTXO{{Index: 0, Value: 5000000000, Script: []byte{0x51}}},
	}

	body := buildValidatorBody(blockHash, prevHash, 100, []*utxopersister.UTXOWrapper{wrapper})

	// Footer claims 2 transactions/2 utxos, but only 1 wrapper (1 utxo) is
	// actually present in the body.
	var footer [16]byte
	binary.LittleEndian.PutUint64(footer[0:8], 2)
	binary.LittleEndian.PutUint64(footer[8:16], 2)
	body = append(body, footer[:]...)

	_, err := validateUTXOSet(context.Background(), bytes.NewReader(body), false)
	require.Error(t, err, "a footer whose counts disagree with what was actually read must be rejected")
}
