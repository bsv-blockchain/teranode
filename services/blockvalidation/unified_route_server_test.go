package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// These tests pin processBlockFound on the unified below-checkpoint route now that legacy
// blocks are applied one at a time: a block whose parent is stored is quick-validated and
// committed inline, and a legacy block whose parent is not stored comes back as a local fault
// rather than a silent nil from the catch-up divert.
//
// The blocks are coinbase-only, which needs no subtree files on disk and reaches every branch
// these tests are about.

// recordingBlockchainClient is the real blockchain client (LocalClient over a sqlitememory
// store) that also records every block reaching AddBlock.
type recordingBlockchainClient struct {
	blockchain.ClientI

	mu    sync.Mutex
	added []chainhash.Hash
}

func (c *recordingBlockchainClient) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	c.mu.Lock()
	c.added = append(c.added, *block.Hash())
	c.mu.Unlock()

	return c.ClientI.AddBlock(ctx, block, peerID, opts...)
}

func (c *recordingBlockchainClient) addedBlocks() []chainhash.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]chainhash.Hash(nil), c.added...)
}

type unifiedRouteServer struct {
	s       *Server
	client  *recordingBlockchainClient
	genesis chainhash.Hash
	bits    model.NBit
}

// newUnifiedRouteServer builds a Server wired for the unified below-checkpoint route: real
// sqlitememory blockchain and UTXO stores, a checkpoint above every test height, no
// block-assembly client so the gate is skipped, and no p2p client.
func newUnifiedRouteServer(t *testing.T, name string) *unifiedRouteServer {
	t.Helper()

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	genesisHash := tSettings.ChainCfgParams.GenesisHash
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1000, Hash: genesisHash}}

	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.QuickValidateSkipUtxoLock = true

	utxoStoreURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	localClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, utxoStore)
	require.NoError(t, err)

	client := &recordingBlockchainClient{ClientI: localClient}

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	s := New(logger, tSettings, subtreeStore, txStore, utxoStore, nil, client, nil, nil, nil)
	s.blockValidation = NewBlockValidation(ctx, logger, tSettings, client, subtreeStore, txStore, utxoStore, nil, nil)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	return &unifiedRouteServer{s: s, client: client, genesis: *genesisHash, bits: *bits}
}

// unifiedRouteCoinbaseOnlyBlock builds a block with a coinbase and no subtrees on prev, with
// height written into block.Height the way the legacy client writes the request height.
func unifiedRouteCoinbaseOnlyBlock(t *testing.T, us *unifiedRouteServer, prev chainhash.Hash, height uint32) *model.Block {
	t.Helper()

	_, publicKey := bec.PrivateKeyFromBytes([]byte("unified-route-server"))

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(height, "/unified-route-server/"),
		transactions.WithP2PKHOutputs(1, 5_000_000_000, publicKey),
	)

	prevHash := prev

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: coinbase.TxIDChainHash(),
			Timestamp:      1_700_000_000 + height,
			Bits:           us.bits,
			Nonce:          height,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
		Height:           height,
	}
}

// A legacy block whose parent is stored is quick-validated and committed by this call, inline,
// with nothing else in flight to wait for.
func TestProcessBlockFound_UnifiedRouteBlockWithAStoredParentIsCommitted(t *testing.T) {
	us := newUnifiedRouteServer(t, "unified_route_committed")

	block := unifiedRouteCoinbaseOnlyBlock(t, us, us.genesis, 1)

	require.NoError(t, us.s.processBlockFound(context.Background(), block.Hash(), "peer-1", "legacy", block))

	exists, err := us.s.blockchainClient.GetBlockExists(context.Background(), block.Hash())
	require.NoError(t, err)
	require.True(t, exists, "the block is stored when processBlockFound returns")
	require.Equal(t, []chainhash.Hash{*block.Hash()}, us.client.addedBlocks())
}

// A legacy block on the unified route whose parent is not stored must never come back nil:
// the catch-up divert returns nil, and legacy sync would record the block as accepted with
// nothing stored. Legacy hands a block over only once its parent has committed, so this is a
// local fault, returned as a transient local error.
func TestProcessBlockFound_UnifiedRouteBlockWithNoStoredParentIsALocalFault(t *testing.T) {
	us := newUnifiedRouteServer(t, "unified_route_unknown_parent")

	orphan := unifiedRouteCoinbaseOnlyBlock(t, us, chainhash.Hash{0x01, 0x02, 0x03}, 2)

	err := us.s.processBlockFound(context.Background(), orphan.Hash(), "peer-1", "legacy", orphan)
	require.Error(t, err, "a legacy block with no stored parent must never come back nil")
	require.True(t, errors.IsTransientLocalError(err), "the failure is ours, not the peer's: %v", err)
	require.Contains(t, err.Error(), "has no stored parent")

	require.Empty(t, us.client.addedBlocks(), "nothing may be stored for a block whose parent is unknown")
	require.Empty(t, us.s.catchupCh, "a legacy block never takes the catch-up divert")
}

// Only a legacy block is refused. Any other block with a missing parent still goes to
// catch-up, which is how the native path resolves an orphan, and still returns nil.
func TestProcessBlockFound_NonLegacyBlockKeepsTheCatchupDivert(t *testing.T) {
	us := newUnifiedRouteServer(t, "unified_route_native")

	orphan := unifiedRouteCoinbaseOnlyBlock(t, us, chainhash.Hash{0x06, 0x05, 0x04}, 2)

	err := us.s.processBlockFound(context.Background(), orphan.Hash(), "peer-1", "http://peer:8000", orphan)
	require.NoError(t, err, "a non-legacy block with a missing parent still takes the catch-up divert")

	require.Eventually(t, func() bool { return len(us.s.catchupCh) == 1 }, 5*time.Second, 10*time.Millisecond,
		"the block must have been handed to catch-up")
	require.Empty(t, us.client.addedBlocks())
}
