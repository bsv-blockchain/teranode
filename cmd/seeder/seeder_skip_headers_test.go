package seeder

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// writeEmptyUTXOSetFile writes a minimal, valid, zero-UTXO ".utxo-set" file:
// just the header, tip hash, height and previous-block-hash fields that
// processUTXOs reads before it starts iterating UTXOWrapper records.
func writeEmptyUTXOSetFile(t *testing.T, path string) {
	t.Helper()

	var buf bytes.Buffer

	header := fileformat.NewHeader(fileformat.FileTypeUtxoSet)
	require.NoError(t, header.Write(&buf))

	var tipHash chainhash.Hash
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, tipHash))

	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint32(1)))

	var prevHash [32]byte
	buf.Write(prevHash[:])

	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o644))
}

// TestSeeder_SkipHeaders_CorruptedHeaderFileFailsVerification guards against
// the headers file being consumed unverified when -skipHeaders is set.
//
// headerFile is not only read by the header-import pass: processUTXOs reads
// it back unconditionally (via loadCoinbaseTxs) to recover coinbase inputs.
// So the documented "-skipHeaders" flow (headers already imported, now do the
// UTXOs) must still verify the headers file whenever it is present, even
// though the header-import pass itself is skipped.
//
// A checksum sidecar that doesn't match the header file's actual content
// must cause Seeder to fail fast, before any store is constructed -- not be
// silently consumed by processUTXOs.
func TestSeeder_SkipHeaders_CorruptedHeaderFileFailsVerification(t *testing.T) {
	dir := t.TempDir()
	hash := "abc123"

	headerFile := filepath.Join(dir, hash+".utxo-headers")
	original := []byte("this is the utxo-headers content")

	require.NoError(t, os.WriteFile(headerFile, original, 0o644))

	// Sidecar reflects the ORIGINAL content.
	sum := sha256.Sum256(original)
	sidecar := hex.EncodeToString(sum[:]) + "  " + filepath.Base(headerFile) + "\n"
	require.NoError(t, os.WriteFile(headerFile+checksumSidecarExtension, []byte(sidecar), 0o644))

	// Corrupt the header file in place after the sidecar was written, leaving
	// the sidecar stale -- exactly the corruption this check exists to catch.
	corrupted := append([]byte(nil), original...)
	corrupted[0] ^= 0xFF
	require.NoError(t, os.WriteFile(headerFile, corrupted, 0o644))

	utxoFile := filepath.Join(dir, hash+".utxo-set")
	writeEmptyUTXOSetFile(t, utxoFile)

	tSettings := test.CreateBaseTestSettings(t)

	blockchainStoreURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings.BlockChain.StoreURL = blockchainStoreURL

	utxoStoreURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = utxoStoreURL

	err = Seeder(ulogger.TestLogger{}, tSettings, dir, hash, true /* skipHeaders */, false /* skipUTXOs */, false /* force */)
	require.Error(t, err, "a corrupted headers file must not be silently consumed when -skipHeaders is set")
	require.Contains(t, err.Error(), "checksum verification failed for headers file")
}
