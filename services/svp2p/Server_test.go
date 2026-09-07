package svp2p

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer_api"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// warnRecordingLogger wraps ulogger.TestLogger, recording Warnf calls so a
// test can assert the header index sync produced no orphan warnings.
type warnRecordingLogger struct {
	ulogger.TestLogger

	mu       sync.Mutex
	warnings []string
}

func (l *warnRecordingLogger) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *warnRecordingLogger) Warnings() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.warnings...)
}

func newTestServer(t *testing.T) (*Server, string) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ListenAddresses = []string{"127.0.0.1:0"}
	tSettings.Legacy.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.Legacy.WorkingDir = t.TempDir()
	tSettings.GRPCAdminAPIKey = "test-admin-key"

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	return New(ulogger.TestLogger{}, tSettings, blockchainClient), tSettings.Legacy.GRPCListenAddress
}

func startServer(t *testing.T, srv *Server) context.CancelFunc {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, srv.Init(ctx))

	readyCh := make(chan struct{})

	go func() { _ = srv.Start(ctx, readyCh) }()

	select {
	case <-readyCh:
	case <-time.After(10 * time.Second):
		t.Fatal("server did not become ready")
	}

	return cancel
}

func dialPeerAPI(t *testing.T, addr string) peer_api.PeerServiceClient {
	t.Helper()

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() { _ = cc.Close() })

	return peer_api.NewPeerServiceClient(cc)
}

func TestServerStartServesGRPC(t *testing.T) {
	srv, grpcAddr := newTestServer(t)

	cancel := startServer(t, srv)
	defer cancel()

	defer func() { require.NoError(t, srv.Stop(context.Background())) }()

	client := dialPeerAPI(t, grpcAddr)

	resp, err := client.GetPeerCount(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, int32(0), resp.Count)

	peers, err := client.GetPeers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Empty(t, peers.Peers)
}

func TestServerBanUnbanRoundTrip(t *testing.T) {
	srv, _ := newTestServer(t)

	cancel := startServer(t, srv)
	defer cancel()

	defer func() { require.NoError(t, srv.Stop(context.Background())) }()

	ctx := context.Background()

	_, err := srv.BanPeer(ctx, &peer_api.BanPeerRequest{Addr: "1.2.3.4", Until: time.Now().Add(time.Hour).Unix()})
	require.NoError(t, err)

	banned, err := srv.IsBanned(ctx, &peer_api.IsBannedRequest{IpOrSubnet: "1.2.3.4"})
	require.NoError(t, err)
	require.True(t, banned.IsBanned)

	list, err := srv.ListBanned(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Contains(t, list.Banned, "1.2.3.4")

	_, err = srv.UnbanPeer(ctx, &peer_api.UnbanPeerRequest{Addr: "1.2.3.4"})
	require.NoError(t, err)

	banned, err = srv.IsBanned(ctx, &peer_api.IsBannedRequest{IpOrSubnet: "1.2.3.4"})
	require.NoError(t, err)
	require.False(t, banned.IsBanned)
}

func TestServerClearBanned(t *testing.T) {
	srv, _ := newTestServer(t)

	cancel := startServer(t, srv)
	defer cancel()

	defer func() { require.NoError(t, srv.Stop(context.Background())) }()

	ctx := context.Background()

	_, err := srv.BanPeer(ctx, &peer_api.BanPeerRequest{Addr: "10.0.0.0/8", Until: time.Now().Add(time.Hour).Unix()})
	require.NoError(t, err)

	_, err = srv.ClearBanned(ctx, &emptypb.Empty{})
	require.NoError(t, err)

	list, err := srv.ListBanned(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Empty(t, list.Banned)
}

func TestServerStopIsClean(t *testing.T) {
	srv, grpcAddr := newTestServer(t)

	cancel := startServer(t, srv)
	defer cancel()

	require.NoError(t, srv.Stop(context.Background()))
	cancel()

	require.Eventually(t, func() bool {
		_, err := net.DialTimeout("tcp", grpcAddr, time.Second)
		return err != nil
	}, 10*time.Second, 100*time.Millisecond, "gRPC port still open after Stop")
}

// testHeaderBlock builds a minimal block extending previousHash. It mirrors
// the header-chain stub used by the blockchain store's own tests: the store
// only validates prev-hash linkage, not proof-of-work or the merkle root
// against transactions.
func testHeaderBlock(t *testing.T, nonce uint32, previousHash *chainhash.Hash) *model.Block {
	t.Helper()

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("1d00ffff")
	require.NoError(t, err)

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      uint32(time.Now().Unix()), //nolint:gosec // test data, no overflow risk
			Nonce:          nonce,
			Bits:           *bits,
			HashPrevBlock:  previousHash,
			HashMerkleRoot: &chainhash.Hash{},
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      80,
	}
}

func TestServerHydratesHeaderIndexFromBlockchain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ListenAddresses = []string{"127.0.0.1:0"}
	tSettings.Legacy.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.Legacy.WorkingDir = t.TempDir()
	tSettings.GRPCAdminAPIKey = "test-admin-key"

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()

	genesisHeaders, _, err := blockchainClient.GetBlockHeadersFromHeight(ctx, 0, 1)
	require.NoError(t, err)
	require.Len(t, genesisHeaders, 1)

	prevHash := genesisHeaders[0].Hash()

	const chainLen = 3
	for i := 0; i < chainLen; i++ {
		block := testHeaderBlock(t, uint32(i+1), prevHash)
		require.NoError(t, blockchainClient.AddBlock(ctx, block, ""))
		prevHash = block.Header.Hash()
	}

	bestHeader, bestMeta, err := blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	srv := New(ulogger.TestLogger{}, tSettings, blockchainClient)
	cancel := startServer(t, srv)
	defer cancel()
	defer func() { require.NoError(t, srv.Stop(context.Background())) }()

	require.Eventually(t, func() bool {
		idx := srv.HeaderIndex()
		if idx == nil {
			return false
		}

		hash, height := idx.Tip()

		return hash == *bestHeader.Hash() && height == int32(bestMeta.Height) //nolint:gosec // test-bounded height
	}, 10*time.Second, 100*time.Millisecond, "header index did not hydrate to the store's best header")

	newBlock := testHeaderBlock(t, uint32(chainLen+1), prevHash)
	require.NoError(t, blockchainClient.AddBlock(ctx, newBlock, ""))

	wantHeight := int32(bestMeta.Height) + 1 //nolint:gosec // test-bounded height
	wantHash := *newBlock.Header.Hash()

	require.Eventually(t, func() bool {
		idx := srv.HeaderIndex()
		if idx == nil {
			return false
		}

		hash, height := idx.Tip()

		return hash == wantHash && height == wantHeight
	}, 10*time.Second, 100*time.Millisecond, "header index did not advance via the blockchain subscription")
}

// TestServerHeaderIndexFollowsReorgAcrossPreviouslySyncedHeights covers the
// case a linear forward walk cannot: a reorg whose new-best branch forks off
// below the index's already-synced tip. Branch A (height 3) hydrates first;
// branch C then forks from A1 (height 1, already walked past) and overtakes
// A at height 5, so C2's parent sits at a height the forward walk will never
// revisit. Only the reconciliation walk-back in reconcileHeaderIndex can
// pick that branch up.
func TestServerHeaderIndexFollowsReorgAcrossPreviouslySyncedHeights(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ListenAddresses = []string{"127.0.0.1:0"}
	tSettings.Legacy.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.Legacy.WorkingDir = t.TempDir()
	tSettings.GRPCAdminAPIKey = "test-admin-key"

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()

	genesisHeaders, _, err := blockchainClient.GetBlockHeadersFromHeight(ctx, 0, 1)
	require.NoError(t, err)
	require.Len(t, genesisHeaders, 1)

	genesisHash := genesisHeaders[0].Hash()

	// Branch A: A1 -> A2 -> A3.
	a1 := testHeaderBlock(t, 1, genesisHash)
	require.NoError(t, blockchainClient.AddBlock(ctx, a1, ""))
	a2 := testHeaderBlock(t, 2, a1.Header.Hash())
	require.NoError(t, blockchainClient.AddBlock(ctx, a2, ""))
	a3 := testHeaderBlock(t, 3, a2.Header.Hash())
	require.NoError(t, blockchainClient.AddBlock(ctx, a3, ""))

	srv := New(ulogger.TestLogger{}, tSettings, blockchainClient)
	cancel := startServer(t, srv)
	defer cancel()
	defer func() { require.NoError(t, srv.Stop(context.Background())) }()

	wantAHash := *a3.Header.Hash()

	require.Eventually(t, func() bool {
		idx := srv.HeaderIndex()
		if idx == nil {
			return false
		}

		hash, height := idx.Tip()

		return hash == wantAHash && height == 3
	}, 10*time.Second, 100*time.Millisecond, "header index did not hydrate to branch A's tip")

	// Branch C forks from A1 and overtakes A: C2 -> C3 -> C4 -> C5, height 5
	// with equal per-block difficulty beats A's height 3 on cumulative
	// chainwork, so the store's best switches to C once C5 lands.
	c2 := testHeaderBlock(t, 102, a1.Header.Hash())
	require.NoError(t, blockchainClient.AddBlock(ctx, c2, ""))
	c3 := testHeaderBlock(t, 103, c2.Header.Hash())
	require.NoError(t, blockchainClient.AddBlock(ctx, c3, ""))
	c4 := testHeaderBlock(t, 104, c3.Header.Hash())
	require.NoError(t, blockchainClient.AddBlock(ctx, c4, ""))
	c5 := testHeaderBlock(t, 105, c4.Header.Hash())
	require.NoError(t, blockchainClient.AddBlock(ctx, c5, ""))

	bestHeader, bestMeta, err := blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, *c5.Header.Hash(), *bestHeader.Hash(), "test setup: branch C should be the new best chain")
	require.Equal(t, uint32(5), bestMeta.Height, "test setup: branch C should be at height 5")

	wantCHash := *c5.Header.Hash()

	require.Eventually(t, func() bool {
		idx := srv.HeaderIndex()
		if idx == nil {
			return false
		}

		hash, height := idx.Tip()

		return hash == wantCHash && height == 5
	}, 10*time.Second, 100*time.Millisecond, "header index did not follow the reorg to branch C's tip")
}

// TestServerHydratesHeaderIndexAcrossMultipleBatches forces the forward walk
// in syncHeaderIndex to span several fetches (batch size 2 over 5 blocks: a
// [1,2], [3,4], [5,5] height split) and asserts every height connects with
// no orphan warnings, covering the batch-seam stride fix in
// forwardWalkHeaderIndex (advancing by the requested height range, not by
// how many rows a batch happened to return).
func TestServerHydratesHeaderIndexAcrossMultipleBatches(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ListenAddresses = []string{"127.0.0.1:0"}
	tSettings.Legacy.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.Legacy.WorkingDir = t.TempDir()
	tSettings.GRPCAdminAPIKey = "test-admin-key"

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()

	genesisHeaders, _, err := blockchainClient.GetBlockHeadersFromHeight(ctx, 0, 1)
	require.NoError(t, err)
	require.Len(t, genesisHeaders, 1)

	prevHash := genesisHeaders[0].Hash()

	const chainLen = 5

	hashes := make([]chainhash.Hash, 0, chainLen)

	for i := 0; i < chainLen; i++ {
		block := testHeaderBlock(t, uint32(i+1), prevHash)
		require.NoError(t, blockchainClient.AddBlock(ctx, block, ""))
		prevHash = block.Header.Hash()
		hashes = append(hashes, *prevHash)
	}

	bestHeader, bestMeta, err := blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	warnLogger := &warnRecordingLogger{}

	srv := New(warnLogger, tSettings, blockchainClient)
	srv.headerBatchSize = 2
	cancel := startServer(t, srv)
	defer cancel()
	defer func() { require.NoError(t, srv.Stop(context.Background())) }()

	require.Eventually(t, func() bool {
		idx := srv.HeaderIndex()
		if idx == nil {
			return false
		}

		hash, height := idx.Tip()

		return hash == *bestHeader.Hash() && height == int32(bestMeta.Height) //nolint:gosec // test-bounded height
	}, 10*time.Second, 100*time.Millisecond, "header index did not hydrate across multiple batches")

	idx := srv.HeaderIndex()
	require.NotNil(t, idx)

	for height, hash := range hashes {
		node, ok := idx.Lookup(hash)
		require.True(t, ok, "height %d not indexed", height+1)
		require.Equal(t, int32(height+1), node.Height) //nolint:gosec // test-bounded height
	}

	// Asserted as a SET, not filtered: a linear multi-batch hydration must
	// produce no orphan warning, and nothing else must start warning either.
	// The one expected line says block sync is off until the ingestion
	// dependencies are injected.
	require.Equal(t, []string{
		"[svp2p] block sync disabled: the block ingestion dependencies are not injected",
	}, warnLogger.Warnings(), "unexpected warnings during a linear multi-batch hydration")
}

func TestServerHealth(t *testing.T) {
	srv, _ := newTestServer(t)

	code, msg, err := srv.Health(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, 200, code)
	require.Equal(t, "OK", msg)
}

// divergentBestHeaderClient wraps a REAL sqlitememory-backed blockchain
// client (the project's no-mocked-store rule stands: genesis, height ranges
// and every other call still come from that store) and injects exactly one
// divergence — the shape a badly diverged or untrusted store can present to
// hydration. GetBestBlockHeader answers with a header the store never linked
// to genesis, and GetBlockHeader then hands back a fresh unknown parent on
// every hop, so the backward reconciliation walk never reaches an indexed
// ancestor. Without a depth cap that walk does not terminate.
type divergentBestHeaderClient struct {
	blockchain.ClientI

	best *model.BlockHeader

	mu    sync.Mutex
	calls int
}

func (c *divergentBestHeaderClient) GetBestBlockHeader(_ context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	// Height 0 keeps the forward walk a no-op, so the test measures the
	// backward reconciliation walk alone.
	return c.best, &model.BlockHeaderMeta{Height: 0}, nil
}

func (c *divergentBestHeaderClient) GetBlockHeader(_ context.Context, _ *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls++

	unknownParent := chainhash.Hash{}
	unknownParent[0] = byte(c.calls)
	unknownParent[1] = byte(c.calls >> 8)

	bits, err := model.NewNBitFromString("1d00ffff")
	if err != nil {
		return nil, nil, err
	}

	return &model.BlockHeader{
		Version:        1,
		Timestamp:      1,
		Nonce:          uint32(c.calls), //nolint:gosec // test-bounded counter
		Bits:           *bits,
		HashPrevBlock:  &unknownParent,
		HashMerkleRoot: &chainhash.Hash{},
	}, &model.BlockHeaderMeta{Height: 0}, nil
}

func (c *divergentBestHeaderClient) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

// TestServerReconcileWalkStopsAtDepthCap asserts the bound on
// reconcileHeaderIndex. A store whose best header recedes forever must fail
// hydration AT the cap, not walk without end. The cap is 10 x
// headerBatchSize, so a batch size of 4 caps the walk at 40 hops and the
// call count is asserted exactly: an error alone would not prove the walk
// stopped where it was told to.
func TestServerReconcileWalkStopsAtDepthCap(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ListenAddresses = []string{"127.0.0.1:0"}
	tSettings.Legacy.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.Legacy.WorkingDir = t.TempDir()
	tSettings.GRPCAdminAPIKey = "test-admin-key"

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	realClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()

	// A best header that is genuinely absent from the index: its parent is a
	// hash the store never held.
	orphanParent := chainhash.Hash{0xff}
	divergentBest := testHeaderBlock(t, 9999, &orphanParent).Header

	client := &divergentBestHeaderClient{ClientI: realClient, best: divergentBest}

	srv := New(ulogger.TestLogger{}, tSettings, client)

	const batchSize = uint32(4)

	srv.headerBatchSize = batchSize

	wantCap := int(reconcileDepthCapMultiplier * batchSize)

	require.NoError(t, srv.hydrateHeaderIndex(ctx))

	err = srv.syncHeaderIndex(ctx)
	require.Error(t, err, "hydration must fail when the store's best header cannot be reconciled")
	require.ErrorContains(t, err, divergentBest.Hash().String(), "the error must name the diverged best header")
	require.ErrorContains(t, err, "diverged", "the error must name the divergence")
	require.True(t, errors.Is(err, errors.ErrServiceError), "the reconcile depth cap must fail as a service error, got %v", err)

	require.Equal(t, wantCap, client.Calls(), "the reconciliation walk must stop at exactly the depth cap")

	// The index must be left as hydration built it — genesis only, nothing
	// from the unreconcilable branch linked in.
	idx := srv.HeaderIndex()
	require.NotNil(t, idx)

	_, tipHeight := idx.Tip()
	require.Equal(t, int32(0), tipHeight, "a failed reconciliation must not advance the index tip")
}

// go-wire's `ebs` bounds every buffered block and tx message it reads or
// writes, so Init must feed it the node's own excessiveblocksize. A literal
// there caps the node below its configured limit and, once the literal is
// under 4,294,967,295, refuses every block large enough to need an extended
// header.
func TestServerInitFeedsWireLimitsFromExcessiveBlockSize(t *testing.T) {
	const excessive = 6_000_000_000

	// wire's limits are process-wide, so put back what the rest of the
	// package expects.
	original := wire.MaxBlockPayload()
	t.Cleanup(func() { wire.SetLimits(original) })

	srv, _ := newTestServer(t)
	srv.settings.Policy.ExcessiveBlockSize = excessive

	require.NoError(t, srv.Init(context.Background()))

	require.Equal(t, uint64(excessive), wire.MaxBlockPayload())
}
