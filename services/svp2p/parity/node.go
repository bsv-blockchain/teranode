// Package parity is the svp2p parity harness: it drives the legacy service and
// the svp2p service from the same scripted peer and compares what each did.
// Design: docs/superpowers/specs/2026-08-26-svp2p-parity-harness-design.md.
package parity

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/svp2p"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Impl names the service under test.
type Impl string

const (
	// Legacy is services/legacy, the oracle.
	Legacy Impl = "legacy"
	// Svp2p is services/svp2p, the port.
	Svp2p Impl = "svp2p"
)

// service is the part of both servers the harness drives.
type service interface {
	Init(ctx context.Context) error
	Start(ctx context.Context, readyCh chan<- struct{}) error
	Stop(ctx context.Context) error
}

// nodeUnderTest is one running node: the implementation named by Impl on top
// of the real Teranode pipeline (block assembly, subtree validation, block
// validation, validator) over in-memory stores.
type nodeUnderTest struct {
	Impl     Impl
	Logger   *svp2ptest.RecordingLogger
	Settings *settings.Settings
	// PeerListen is the node's P2P listen address, for inbound scripted peers.
	PeerListen string

	blockchainStore blockchain_store.Store
	svc             service
	cancel          context.CancelFunc
	// txMeta is the validator's txmeta Kafka producer, held so Stop can close
	// it: it owns a goroutine that ctx cancellation alone does not join.
	txMeta *kafka.KafkaAsyncProducer

	// scores is filled by a scenario's Drive from a scoreSampler, for Observe.
	scores map[string]int
	// notes is filled by a scenario's Drive for Observe.
	notes map[string]string
}

var nodeCounter atomic.Uint64

// Clocks the scenarios lean on. Both services share the legacy_* settings by
// design, so the settings tweaks below reach both; the svp2p tick and rotation
// window are instance fields and are narrowed through SetSyncClocks.
const (
	syncTick         = 100 * time.Millisecond
	maxLastBlockTime = 3 * time.Second
)

// newNode builds and starts a node of the given implementation that dials
// connectPeers. Every dependency is the real code over in-memory stores; only
// the implementation flag differs between the two legs of a scenario.
func newNode(t *testing.T, impl Impl, connectPeers []string, tweaks ...func(*settings.Settings)) *nodeUnderTest {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	logger := &svp2ptest.RecordingLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Context = fmt.Sprintf("%s-parity-%s-%d", tSettings.Context, impl, nodeCounter.Add(1))
	// A fixed loopback port rather than :0, so scripted peers can DIAL the node
	// (inbound peers are what addr relay and self-advertisement act on).
	peerListen := svp2ptest.FreePort(t)
	tSettings.Legacy.ListenAddresses = []string{peerListen}
	tSettings.Legacy.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.Legacy.WorkingDir = t.TempDir()
	tSettings.Legacy.ConnectPeers = connectPeers
	tSettings.GRPCAdminAPIKey = "test-admin-key"
	tSettings.BlockAssembly.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.BlockAssembly.GRPCAddress = tSettings.BlockAssembly.GRPCListenAddress
	tSettings.SubtreeValidation.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.SubtreeValidation.GRPCAddress = tSettings.SubtreeValidation.GRPCListenAddress
	tSettings.BlockValidation.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.BlockValidation.GRPCAddress = tSettings.BlockValidation.GRPCListenAddress
	tSettings.BlockValidation.PeriodicProcessingInterval = 200 * time.Millisecond

	// Fixture blocks are ~190 bytes, so no peer can ever meet a bytes-per-second
	// sync floor; both floors are zeroed or the sync peer is rotated (svp2p) or
	// disconnected (legacy, 2026-08-26 diagnostic: "stalled due to network
	// speed violation") in the middle of every scenario. legacy reads its floor
	// from gocore under legacy_config_, which an environment variable of the
	// same name overrides (gocore config.go LookupEnv).
	tSettings.Legacy.MinSyncPeerNetworkSpeed = 0
	t.Setenv("legacy_config_MinSyncPeerNetworkSpeed", "0")

	// legacy Server.Init refuses to start without an asset address; these
	// scenarios never serve a block, so the address only has to parse.
	if tSettings.Asset.HTTPAddress == "" {
		tSettings.Asset.HTTPAddress = "http://127.0.0.1:1/api/v1"
	}

	for _, tweak := range tweaks {
		tweak(tSettings)
	}

	isolateTxMetaTopic(t, tSettings)

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///parity")
	require.NoError(t, err)

	tSettings.UtxoStore.UtxoStore = utxoStoreURL

	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	startUtxoBlockState(ctx, logger, blockchainClient, utxoStore)

	subtreeStore := blobmemory.New()
	tempStore := blobmemory.New()
	txStore := blobmemory.New()

	blockAssemblyClient := svp2ptest.StartBlockAssembly(ctx, t, logger, tSettings, txStore, subtreeStore, utxoStore, blockchainClient)

	txMeta := txMetaProducer(ctx, t, logger, tSettings)

	validatorClient, err := validator.New(ctx, logger, tSettings, utxoStore, txMeta, nil, nil,
		blockAssemblyClient, blockchainClient)
	require.NoError(t, err)

	subtreeValidationClient := svp2ptest.StartSubtreeValidation(ctx, t, tSettings.Context, logger, tSettings, subtreeStore,
		txStore, utxoStore, validatorClient, blockchainClient)

	blockValidationClient := svp2ptest.StartBlockValidation(ctx, t, tSettings.Context, logger, tSettings, subtreeStore, txStore,
		utxoStore, validatorClient, blockchainClient, blockAssemblyClient)

	var svc service

	switch impl {
	case Legacy:
		// legacy newServer takes the process-wide p2p ban list, which is built
		// once from settings and backed by the blockchain store URL; every
		// legacy node in this process gets a fresh in-memory one.
		p2p.ResetBanListSingleton()

		banStoreURL, urlErr := url.Parse("sqlitememory://")
		require.NoError(t, urlErr)

		tSettings.BlockChain.StoreURL = banStoreURL

		svc = legacy.New(logger, tSettings, blockchainClient, validatorClient, subtreeStore, tempStore, utxoStore,
			subtreeValidationClient, blockValidationClient, blockAssemblyClient)
	case Svp2p:
		srv := svp2p.NewWithDeps(logger, tSettings, blockchainClient, svp2p.Deps{
			ValidationClient:  validatorClient,
			SubtreeStore:      subtreeStore,
			TempStore:         tempStore,
			UtxoStore:         utxoStore,
			SubtreeValidation: subtreeValidationClient,
			BlockValidation:   blockValidationClient,
			BlockAssembly:     blockAssemblyClient,
		})
		srv.SetSyncClocks(syncTick, maxLastBlockTime)
		svc = srv
	default:
		t.Fatalf("unknown impl %q", impl)
	}

	require.NoError(t, svc.Init(ctx))

	readyCh := make(chan struct{})

	go func() { _ = svc.Start(ctx, readyCh) }()

	select {
	case <-readyCh:
	case <-time.After(30 * time.Second):
		logger.Dump(t)
		t.Fatalf("%s did not become ready", impl)
	}

	n := &nodeUnderTest{Impl: impl, Logger: logger, Settings: tSettings, PeerListen: peerListen,
		blockchainStore: blockchainStore, svc: svc, cancel: cancel, txMeta: txMeta}

	t.Cleanup(n.Stop)

	return n
}

// utxoBlockStateInterval is how often the harness republishes the chain tip
// into the UTXO store. It is a poll rather than a subscription only because
// blockchain.LocalClient is the client this harness runs on; the effect is the
// factory's.
const utxoBlockStateInterval = 100 * time.Millisecond

// startUtxoBlockState keeps the UTXO store's block state (tip height and median
// time) following the chain, which is what the store factory does in production
// through its blockchain subscription (stores/utxo/factory/utxo.go:139-190).
// The harness builds the store directly — the factory's own client is a gRPC
// one, and every service here runs in process — so the same two calls are made
// here instead.
//
// Without it the store's median time stays zero and the validator refuses every
// policy-checked transaction with "utxo store not ready", which is the state
// every parity scenario before Task 10 ran in unnoticed: none of them offered
// the node a loose transaction.
func startUtxoBlockState(ctx context.Context, logger ulogger.Logger, blockchainClient blockchain.ClientI, utxoStore utxo.Store) {
	go func() {
		ticker := time.NewTicker(utxoBlockStateInterval)
		defer ticker.Stop()

		for {
			height, medianTime, err := blockchainClient.GetBestHeightAndTime(ctx)
			if err == nil && height > 0 {
				if err = utxoStore.SetBlockState(height, medianTime); err != nil {
					logger.Errorf("[parity] error setting utxo store block state: %v", err)
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// isolateTxMetaTopic gives this leg its own txmeta topic.
//
// settings.conf always resolves kafka_txmetaConfig to a non-empty URL, and the
// test context resolves its scheme to memory, so EVERY leg of EVERY scenario
// publishes and consumes on the process-wide in-memory broker
// (in_memory_kafka.GetSharedBroker). Left at the configured default they would
// all share one topic named "txmeta", and one leg would read what another left
// behind — which matters the moment a scenario asserts on what the
// recent-transaction index holds.
//
// tSettings.Context already carries the implementation and a process-wide
// counter (newNode), so it is the name that makes the topic unique. Applied
// AFTER the scenario's tweaks, deliberately: isolation is not a scenario's to
// waive.
func isolateTxMetaTopic(t *testing.T, tSettings *settings.Settings) {
	t.Helper()

	require.NotNil(t, tSettings.Kafka.TxMetaConfig, "kafka_txmetaConfig is required; validator.New refuses a nil one")

	isolated := *tSettings.Kafka.TxMetaConfig
	// "txmeta-parity-", not "txmeta-": svp2ptest.StartSubtreeValidation already
	// runs a consumer on "txmeta-"+Context (services.go), and a scenario waits
	// on GetSharedBroker().HasConsumer to know the BRIDGE's consumer has
	// joined. Sharing the name would let that wait be satisfied by the wrong
	// consumer, and the relayed transaction would then be produced into a topic
	// the index is not yet reading.
	isolated.Path = "/txmeta-parity-" + tSettings.Context

	tSettings.Kafka.TxMetaConfig = &isolated
}

// txMetaTopic is the topic name isolateTxMetaTopic settled on, which is what a
// scenario waits on before it relays a transaction it expects to be indexed.
func (n *nodeUnderTest) txMetaTopic() string {
	return strings.TrimPrefix(n.Settings.Kafka.TxMetaConfig.Path, "/")
}

// txMetaProducer is the validator's txmeta Kafka producer. It is the PRODUCE
// half of the path that puts an accepted transaction into the svp2p bridge's
// recent-transaction index: the validator publishes an ADD entry,
// bridge.StartTxMetaConsumer reads it back off the same topic and calls
// RecentTxIndex.Add. Compact block reconstruction has nothing to match short IDs
// against until that round trip has run.
//
// The harness passed nil here until Task 10, which is why nothing was ever
// published on the topic. It is built for EVERY leg, not only the compact ones:
// the producer is what the daemon wires in production, and a leg-local topic
// (isolateTxMetaTopic) is what keeps that from reaching another leg.
//
// The returned producer's lifecycle is the node's ctx, like every other
// background dependency the harness builds; Stop closes it explicitly as well,
// because it is the one piece of new state whose goroutine outlives the
// service under test.
func txMetaProducer(ctx context.Context, t *testing.T, logger ulogger.Logger, tSettings *settings.Settings) *kafka.KafkaAsyncProducer {
	t.Helper()

	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, tSettings.Kafka.TxMetaConfig, &tSettings.Kafka)
	require.NoError(t, err)

	return producer
}

// Stop tears the node down. Safe to call twice.
func (n *nodeUnderTest) Stop() {
	if n.cancel == nil {
		return
	}

	n.cancel()
	n.cancel = nil

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStop()

	_ = n.svc.Stop(stopCtx)

	if n.txMeta != nil {
		kafka.StopProducerCtx(stopCtx, n.Logger, "parity txmeta", n.txMeta)
		n.txMeta = nil
	}
}

// BestHeight is the node's active chain height.
func (n *nodeUnderTest) BestHeight(t *testing.T) uint32 {
	t.Helper()

	_, meta, err := n.blockchainStore.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	return meta.Height
}

// WaitForHeight polls until the active chain reaches h, dumping the node's
// log on timeout.
func (n *nodeUnderTest) WaitForHeight(t *testing.T, h uint32, timeout time.Duration) {
	t.Helper()

	n.WaitFor(t, func() bool { return n.BestHeight(t) == h }, timeout,
		fmt.Sprintf("%s never reached height %d", n.Impl, h))
}

// WaitFor polls cond until it holds or timeout passes.
func (n *nodeUnderTest) WaitFor(t *testing.T, cond func() bool, timeout time.Duration, what string) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	if what == "" {
		return // a bounded wait, not an assertion
	}

	n.Logger.Dump(t)
	t.Fatalf("%s (height now %d)", what, n.BestHeight(t))
}

// RecentTxIndexLen is how many transaction hashes the node's compact-block
// transaction index holds. Zero on the legacy leg, which has no such index.
func (n *nodeUnderTest) RecentTxIndexLen() int {
	if srv, ok := n.svc.(*svp2p.Server); ok {
		return srv.RecentTxIndexLen()
	}

	return 0
}

// HeaderKnown reports whether the node's header index holds hash, and at what
// height. It is how a scenario distinguishes "the announcement was processed and
// the block declined" from "the announcement was never processed": the compact
// dispatcher accepts the header UNCONDITIONALLY, ahead of every guard, so the
// header is in the index even when the block is refused.
//
// Legacy has no such index and always answers false.
func (n *nodeUnderTest) HeaderKnown(hash chainhash.Hash) (int32, bool) {
	srv, ok := n.svc.(*svp2p.Server)
	if !ok {
		return 0, false
	}

	idx := srv.HeaderIndex()
	if idx == nil {
		return 0, false
	}

	node, known := idx.Lookup(hash)
	if !known {
		return 0, false
	}

	return node.Height, true
}

// ConnectedCount is how many peers the node currently holds.
func (n *nodeUnderTest) ConnectedCount(t *testing.T) int {
	t.Helper()

	switch svc := n.svc.(type) {
	case *svp2p.Server:
		return svc.ConnectedCount()
	case *legacy.Server:
		resp, err := svc.GetPeerCount(context.Background(), &emptypb.Empty{})
		require.NoError(t, err)

		return int(resp.Count)
	}

	return 0
}

// ---------------------------------------------------------------------------
// Asset service stub
// ---------------------------------------------------------------------------

// assetBody is one block the stub will serve: the real 80 byte header followed
// by filler out to Length. The filler is never materialised — Length is a
// number, and the handler streams a reused zero chunk until it has written that
// many bytes — so a scenario can declare a body far larger than this process
// could hold.
type assetBody struct {
	Header []byte
	Length uint64
}

// assetStub stands in for the asset service's block_legacy?wire=1 route, the
// one route BOTH implementations read a block body from: svp2p through
// bridge.FetchBlock (services/svp2p/bridge/fetch.go) and legacy through
// pushBlockMsg (services/legacy/peer_server.go:2277). A hash it was not given
// is answered 404, which is the status FetchBlock folds into
// errors.ErrBlockNotFound.
//
// One stub serves BOTH legs of a scenario, and the per-hash maps are never
// cleared between them. That is safe only because runLeg builds a FRESH
// FixtureChain per leg from a fresh random key (svp2ptest.BuildFixtureChainPadded),
// so the two legs register and count under different block hashes. A scenario
// that ever gives both legs the same tip hash must reset these maps between
// legs, or Completed will report the first leg's fetch to the second.
type assetStub struct {
	srv *httptest.Server

	mu        sync.Mutex
	bodies    map[chainhash.Hash]assetBody
	completed map[chainhash.Hash]int
}

// assetChunkBytes is the filler chunk the handler reuses. One mebibyte keeps
// the write count on a 4 GiB body near four thousand rather than half a
// million, without holding anything worth counting.
const assetChunkBytes = 1 << 20

// startAssetStub starts the stub on loopback and stops it with the test.
func startAssetStub(t *testing.T) *assetStub {
	t.Helper()

	a := &assetStub{
		bodies:    make(map[chainhash.Hash]assetBody),
		completed: make(map[chainhash.Hash]int),
	}

	a.srv = httptest.NewServer(http.HandlerFunc(a.handle))

	t.Cleanup(a.srv.Close)

	return a
}

// URL is what Asset.HTTPAddress must be set to.
func (a *assetStub) URL() string { return a.srv.URL }

// Register makes the stub serve length bytes for hash: header first, filler
// after. Registering the same hash twice is idempotent, which is what lets a
// scenario call it from a Tweaks hook that runs once per leg.
func (a *assetStub) Register(hash chainhash.Hash, header []byte, length uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.bodies[hash] = assetBody{Header: header, Length: length}
}

// Completed is how many times the stub has written a whole body for hash. It is
// the signal a scenario waits on when the node under test answers a fat block
// with silence: the fetch finishing is observable, the silence that follows is
// not.
func (a *assetStub) Completed(hash chainhash.Hash) int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.completed[hash]
}

func (a *assetStub) handle(w http.ResponseWriter, r *http.Request) {
	// Matched as a suffix rather than a whole path: Asset.HTTPAddress carries
	// a base path in production ("…/api/v1"), and this stub is given whatever
	// the scenario set, so the route may sit under a prefix.
	const route = "/block_legacy/"

	idx := strings.LastIndex(r.URL.Path, route)
	if idx < 0 || r.URL.Query().Get("wire") != "1" {
		http.NotFound(w, r)

		return
	}

	hash, err := chainhash.NewHashFromStr(r.URL.Path[idx+len(route):])
	if err != nil {
		http.NotFound(w, r)

		return
	}

	a.mu.Lock()
	body, known := a.bodies[*hash]
	a.mu.Unlock()

	if !known {
		http.NotFound(w, r)

		return
	}

	// A declared length below the header it is supposed to start with is a
	// broken registration, not a body: served, it would underflow the
	// remaining count below into a near-infinite write loop. Answered 404, so
	// the scenario fails on the node's notfound rather than on a hung stub.
	if body.Length < uint64(len(body.Header)) {
		http.NotFound(w, r)

		return
	}

	// No Content-Length: block_legacy is one of the asset service's streaming
	// routes and does not carry one either (services/asset/httpimpl/stream.go:83),
	// which is why FetchBlock reads the declared length from the blockchain
	// store instead of from the response.
	w.WriteHeader(http.StatusOK)

	written, err := w.Write(body.Header)
	if err != nil {
		return
	}

	remaining := body.Length - uint64(written) //nolint:gosec // the handler is only ever given a length above the header
	chunk := make([]byte, assetChunkBytes)

	for remaining > 0 {
		n := uint64(len(chunk))
		if n > remaining {
			n = remaining
		}

		if _, err := w.Write(chunk[:n]); err != nil {
			return
		}

		remaining -= n
	}

	a.mu.Lock()
	a.completed[*hash]++
	a.mu.Unlock()
}
