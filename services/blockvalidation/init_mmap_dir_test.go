package blockvalidation

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// newInitTestServer builds the minimal Server needed to exercise Init without
// contacting real dependencies (mirrors the setup in malicious_peer_handling_test.go).
func newInitTestServer(t *testing.T, mmapDir string) (*Server, context.Context) {
	t.Helper()

	ctx := t.Context()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.SubtreeMmapDir = mmapDir

	mockBlockchainStore := blockchain_store.NewMockStore()
	mockBlockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
	require.NoError(t, err)

	mockValidator := &validator.MockValidator{}
	subtreeStore := memory.New()
	txStore := memory.New()
	mockUtxoStore := &utxo.MockUtxostore{}

	bv := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, subtreeStore, txStore, mockUtxoStore, mockValidator, nil)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    mockBlockchainClient,
		blockValidation:     bv,
		blockFoundCh:        make(chan processBlockFound, 10),
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		blockClassifier:     NewBlockClassifier(logger, 10, mockBlockchainClient),
		forkManager:         NewForkManager(logger, tSettings),
		catchupCh:           make(chan processBlockCatchup, 10),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		stats:               gocore.NewStat("test"),
	}

	return server, ctx
}

// TestServerInit_SubtreeMmapDir_NonExistent pins the fail-fast contract: an
// operator-configured blockvalidation_subtreeMmapDir that does not exist must
// stop the service at startup rather than silently degrading to heap
// allocation on every subsequent subtree load (get_blocks.go's
// fetchAndStoreSubtree runs the mmap path on every call once configured).
func TestServerInit_SubtreeMmapDir_NonExistent(t *testing.T) {
	server, ctx := newInitTestServer(t, filepath.Join(t.TempDir(), "does-not-exist"))

	err := server.Init(ctx)
	require.Error(t, err)
}

// TestServerInit_SubtreeMmapDir_NotWritable pins the same fail-fast contract
// for a directory that exists but rejects writes.
func TestServerInit_SubtreeMmapDir_NotWritable(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root, permission checks do not apply")
	}

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o500))

	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o700)
	})

	server, ctx := newInitTestServer(t, dir)

	err := server.Init(ctx)
	require.Error(t, err)
}

// TestServerInit_SubtreeMmapDir_Valid confirms a valid, writable directory
// does not block startup.
func TestServerInit_SubtreeMmapDir_Valid(t *testing.T) {
	initPrometheusMetrics()

	server, ctx := newInitTestServer(t, t.TempDir())

	err := server.Init(ctx)
	require.NoError(t, err)
}

// TestServerInit_SubtreeMmapDir_Empty confirms the default (feature off) does
// not block startup, i.e. this validation is opt-in only.
func TestServerInit_SubtreeMmapDir_Empty(t *testing.T) {
	initPrometheusMetrics()

	server, ctx := newInitTestServer(t, "")

	err := server.Init(ctx)
	require.NoError(t, err)
}
