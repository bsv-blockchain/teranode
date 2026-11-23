package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

// newCorruptDeliveryServer builds a Server whose only announced block is fetched CORRUPT: the served
// body carries a valid header (so the fetch and parent checks pass) but a 1-byte coinbase scriptSig,
// which the outer coinbase-length check classifies ERR_BLOCK_CORRUPT (bitcoin-sv/teranode#4692). It
// returns the server, the block hash, and the corrupt bytes (already registered for baseURL below via
// the caller). Parents are stored so processBlockFound reaches validation rather than catch-up.
func newCorruptDeliveryServer(t *testing.T) (*Server, *chainhash.Hash, []byte) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	mockBlockchainStore := blockchain_store.NewMockStore()
	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
	require.NoError(t, err)

	subtreeStore := memory.New()
	txStore := memory.New()
	mockUtxoStore := &utxo.MockUtxostore{}
	mockValidator := &validator.MockValidator{}

	bv := NewBlockValidation(ctx, logger, tSettings, blockchainClient, subtreeStore, txStore, mockUtxoStore, mockValidator, nil)

	blocks := testhelpers.CreateTestBlockChain(t, 4)
	target := blocks[3]
	for i := 0; i < 3; i++ {
		require.NoError(t, blockchainClient.AddBlock(ctx, blocks[i], "test-peer"))
	}

	// Hash is the header hash (unchanged by the body tamper below).
	h := target.Hash()

	// Corrupt the BODY: a 1-byte coinbase scriptSig fails the outer coinbase-length check (< 2) and
	// yields ERR_BLOCK_CORRUPT. The header (and thus the block hash) is unchanged.
	target.CoinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
	corruptBytes, err := target.Bytes()
	require.NoError(t, err)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    blockchainClient,
		blockValidation:     bv,
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		forkManager:         NewForkManager(logger, tSettings),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	return server, h, corruptBytes
}

// TestProcessBlockWithPriority_CorruptBody_DoesNotWalkAlternatives is the bitcoin-sv/teranode#4692 fix at
// the processBlockWithPriority site (bitcoin-sv/teranode#4692). IsBlockCorrupt is now tested BEFORE the
// network/malicious pair, so a corrupt body returns immediately and does NOT walk alternative
// sources (which exist for network/malicious FETCH failures). If the ordering were reverted, a
// corrupt error would match IsMaliciousResponseError (substring "corrupt") and the alternative would
// be consumed — so this test fails on revert.
func TestProcessBlockWithPriority_CorruptBody_DoesNotWalkAlternatives(t *testing.T) {
	initPrometheusMetrics()

	server, h, corruptBytes := newCorruptDeliveryServer(t)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://corrupt-peer/block/%s", h), httpmock.NewBytesResponder(200, corruptBytes))
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://alt-peer/block/%s", h), httpmock.NewBytesResponder(200, corruptBytes))

	primary := processBlockFound{hash: h, baseURL: "http://corrupt-peer", peerID: "bad"}
	alternative := processBlockFound{hash: h, baseURL: "http://alt-peer", peerID: "alt"}

	// The second Add for the same hash is registered as an alternative source.
	server.blockPriorityQueue.Add(primary, PriorityChainExtending, 3)
	server.blockPriorityQueue.Add(alternative, PriorityChainExtending, 3)

	err := server.processBlockWithPriority(context.Background(), primary)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "a corrupt body must propagate as corrupt, got: %v", err)

	// The alternative must still be present — proving processBlockWithPriority did NOT walk it.
	_, hasAlt := server.blockPriorityQueue.GetAlternativeSource(h)
	require.True(t, hasAlt, "a corrupt body must NOT walk alternative sources (that path is for network/malicious fetch failures)")
}

// TestBlockProcessingWorker_CorruptBody_ClearsProcessBlockNotify is the bitcoin-sv/teranode#4692 fix at the
// blockProcessingWorker site (bitcoin-sv/teranode#4692). For a corrupt body the worker CLEARS the
// in-flight processBlockNotify marker (so an honest peer's re-announcement re-enters validation),
// instead of taking the network/malicious retry branch (which re-queues after 5s and never clears
// the marker). If the ordering were reverted, the corrupt error would match the malicious branch and
// the marker would stay set — so this test fails on revert.
func TestBlockProcessingWorker_CorruptBody_ClearsProcessBlockNotify(t *testing.T) {
	initPrometheusMetrics()

	server, h, corruptBytes := newCorruptDeliveryServer(t)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://corrupt-peer/block/%s", h), httpmock.NewBytesResponder(200, corruptBytes))

	// The in-flight marker set when the block was queued; the corrupt branch must clear it.
	server.processBlockNotify.Set(*h, true, ttlcache.DefaultTTL)
	server.blockPriorityQueue.Add(processBlockFound{hash: h, baseURL: "http://corrupt-peer", peerID: "bad"}, PriorityChainExtending, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.blockProcessingWorker(ctx, 0)

	require.Eventually(t, func() bool {
		return server.processBlockNotify.Get(*h) == nil
	}, 3*time.Second, 10*time.Millisecond,
		"the worker must clear processBlockNotify for a corrupt body (not take the malicious retry branch)")
}

// existsErrorClient is a real blockchain client whose GetBlockExists fails with err for one hash,
// so processBlockFound returns that error before it fetches anything.
type existsErrorClient struct {
	blockchain.ClientI
	hash chainhash.Hash
	err  error
}

func (c *existsErrorClient) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	if *hash == c.hash {
		return false, c.err
	}

	return c.ClientI.GetBlockExists(ctx, hash)
}

// TestProcessBlockWithPriority_LocalCorruptBlob_DoesNotWalkAlternatives pins the call site in
// processBlockWithPriority: a block that failed on a corrupt blob in this node's own store does
// not walk alternative sources, because each one would read the same local file. The control
// shows a network failure from the same place still does.
func TestProcessBlockWithPriority_LocalCorruptBlob_DoesNotWalkAlternatives(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name      string
		err       func(t *testing.T) error
		isClass   func(error) bool
		walksAlts bool
	}{
		{"corrupt local blob", func(t *testing.T) error { return localCorruptSubtreeError(t, corruptSubtreeContents[0].contents) }, errors.IsBlobCorrupt, false},
		{"network failure", func(*testing.T) error { return errors.NewNetworkError("connection reset") }, func(err error) bool { return errors.Is(err, errors.ErrNetworkError) }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)

			blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, blockchainStore.Close(ctx)) })

			realClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
			require.NoError(t, err)

			h := &chainhash.Hash{0xc0}
			injected := tt.err(t)
			client := &existsErrorClient{ClientI: realClient, hash: *h, err: injected}

			bv := NewBlockValidation(ctx, logger, tSettings, client, memory.New(), memory.New(), &utxo.MockUtxostore{}, &validator.MockValidator{}, nil)

			server := &Server{
				logger:              logger,
				settings:            tSettings,
				blockchainClient:    client,
				blockValidation:     bv,
				blockPriorityQueue:  NewBlockPriorityQueue(logger),
				forkManager:         NewForkManager(logger, tSettings),
				processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
				catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
			}

			primary := processBlockFound{hash: h, baseURL: "http://primary-peer", peerID: "primary"}
			alternative := processBlockFound{hash: h, baseURL: "http://alt-peer", peerID: "alt"}
			server.blockPriorityQueue.Add(primary, PriorityChainExtending, 3)
			server.blockPriorityQueue.Add(alternative, PriorityChainExtending, 3)

			err = server.processBlockWithPriority(ctx, primary)
			require.Error(t, err)
			require.True(t, tt.isClass(err), "the injected failure's class must come back, got: %v", err)

			_, hasAlt := server.blockPriorityQueue.GetAlternativeSource(h)
			require.Equal(t, !tt.walksAlts, hasAlt, "alternative left in the queue")
		})
	}
}
