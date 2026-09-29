package utxopersister

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestCreateUTXOSet_WritesChecksumSidecar confirms that when the UTXO
// Persister writes a .utxo-set snapshot through the blob file store, a
// "<file>.sha256" sidecar is written alongside it, and that the sidecar's
// hash matches the actual bytes of the written file. This is exactly the
// sidecar the seeder's checksum verification (cmd/seeder) relies on when a
// snapshot produced this way is copied into its input directory.
func TestCreateUTXOSet_WritesChecksumSidecar(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	dir := t.TempDir()

	storeURL, err := url.Parse("file://" + dir)
	require.NoError(t, err)

	blockStore, err := file.New(logger, storeURL)
	require.NoError(t, err)

	currentBlockHash := chainhash.HashH([]byte("current-block-hash-for-sidecar-test"))

	// firstPreviousBlockHash = genesis so CreateUTXOSet skips reading a
	// previous UTXO set and just writes the new file.
	c := NewConsolidator(logger, tSettings, nil, nil, blockStore, tSettings.ChainCfgParams.GenesisHash)
	c.lastBlockHash = &currentBlockHash
	c.lastBlockHeight = 1
	c.previousBlockHash = tSettings.ChainCfgParams.GenesisHash

	us, err := GetUTXOSet(ctx, logger, tSettings, blockStore, &currentBlockHash)
	require.NoError(t, err)

	require.NoError(t, us.CreateUTXOSet(ctx, c))

	// The file store names blobs "<reversed-hex-key>.<filetype>" with no
	// hash-prefix subdirectory by default - the same layout the seeder
	// expects in its -inputDir.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var (
		snapshotPath string
		sidecarPath  string
	)

	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".utxo-set") {
			snapshotPath = filepath.Join(dir, name)
		} else if strings.HasSuffix(name, ".utxo-set.sha256") {
			sidecarPath = filepath.Join(dir, name)
		}
	}

	require.NotEmpty(t, snapshotPath, "expected a .utxo-set file to have been written")
	require.NotEmpty(t, sidecarPath, "expected a .utxo-set.sha256 checksum sidecar to have been written alongside it")

	content, err := os.ReadFile(snapshotPath)
	require.NoError(t, err)

	sidecar, err := os.ReadFile(sidecarPath)
	require.NoError(t, err)

	sum := sha256.Sum256(content)
	expectedHex := hex.EncodeToString(sum[:])

	fields := strings.Fields(string(sidecar))
	require.Len(t, fields, 2, "sidecar must be '<hex-sha256>  <filename>'")
	require.Equal(t, expectedHex, strings.ToLower(fields[0]), "sidecar checksum must match the actual file content")
	require.Equal(t, filepath.Base(snapshotPath), fields[1], "sidecar must name the snapshot it belongs to")
}
