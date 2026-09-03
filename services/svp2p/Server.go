// Package svp2p is the rewrite of the legacy P2P bridge service, modeled on
// the bitcoin-sv reference implementation (net.cpp / net_processing.cpp)
// per docs/superpowers/specs/2026-08-18-svp2p-rewrite-design.md.
// Phase 1 scope: transport, handshake, peer management, and the peer_api
// gRPC surface. Block/tx ingestion (bridge) arrives in Phase 2.
package svp2p

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/peer_api"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/svp2p/bridge"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// defaultHeaderBatchSize bounds each forward-walk fetch during header index
// hydration and resync to the wire protocol's own headers-per-message cap.
const defaultHeaderBatchSize = uint32(wire.MaxBlockHeadersPerMsg)

// reconcileDepthCapMultiplier bounds the backward reconciliation walk to this
// many headerBatchSize-sized batches. See reconcileHeaderIndex for the
// argument behind the number and for why the walk needs a bound at all.
const reconcileDepthCapMultiplier = uint32(10)

// Deps carries the Teranode service dependencies the block-ingestion bridge
// needs, in the same set legacy.New takes (daemon/daemon_services.go). They
// are optional on this Server: the daemon always injects them via
// NewWithDeps, but a caller that constructs the Server through New (tests,
// or any depless caller) still gets a service that runs without block sync
// rather than with a faked pipeline behind it.
type Deps struct {
	ValidationClient  validator.Interface
	SubtreeStore      blob.Store
	TempStore         blob.Store
	UtxoStore         utxo.Store
	SubtreeValidation subtreevalidation.Interface
	BlockValidation   blockvalidation.Interface
	BlockAssembly     *blockassembly.Client
}

// complete reports whether every dependency the bridge needs is present. A
// partial set is not usable: the ingestion pipeline calls all of them.
func (d Deps) complete() bool {
	return d.ValidationClient != nil &&
		d.SubtreeStore != nil &&
		d.TempStore != nil &&
		d.UtxoStore != nil &&
		d.SubtreeValidation != nil &&
		d.BlockValidation != nil &&
		d.BlockAssembly != nil
}

// Server is the Teranode service shell for svp2p. It matches the service
// manager contract of services/legacy/Server.go so the daemon can host
// either service behind the same lifecycle.
type Server struct {
	peer_api.UnsafePeerServiceServer

	logger           ulogger.Logger
	settings         *settings.Settings
	blockchainClient blockchain.ClientI
	deps             Deps

	listenAddresses []string
	banList         *protocol.BanList
	manager         *protocol.PeerManager

	// addrMan is the CAddrMan port (protocol/addrman.go), handed to the peer
	// manager in Init and given its persistence goroutine in Start. It is
	// always constructed; legacy_savePeers is what decides whether it has a
	// peers.json path, and an empty path is the disabled state (no file read,
	// none written, no goroutine).
	addrMan   *protocol.AddrMan
	admission *bridge.Admission

	// retained is the block ingestor's orphan spool, held only so Stop can
	// join the replay goroutines it starts. A replay reports its outcome into
	// the peer manager, so it must be joined before the manager's own
	// resources are released. nil when retention is off.
	retained *orphanBlocks

	// stoppableBridge is the concrete bridge newBlockIngestor built, held
	// only so Stop can release its background goroutines (orphanPool's TTL
	// ticker and eviction worker — orphans.go, fix round 1 Issues I1/I4).
	// bridge.Bridge itself carries no Stop method — spec §4.4 keeps that
	// interface to what protocol needs, and protocol never stops a bridge —
	// so this is typed as the narrow local interface below rather than
	// widening bridge.Bridge for one caller. nil when newBlockIngestor found
	// no injected dependencies (a depless caller), matching every other
	// nil-means-"not built" field in this struct.
	stoppableBridge stoppableBridge

	// recentTxIndex is the bridge's recent-transaction hash ring
	// (bridge/recenttx.go), held here because two things outside the bridge
	// feed or read it: the txmeta Kafka consumer Adds to it, and the peer
	// manager matches compact-block short IDs against it. nil when the
	// block ingestion dependencies are not injected (a depless caller) —
	// bridge.RecentTxIndex's own methods are nil-safe for exactly that
	// case.
	recentTxIndex *bridge.RecentTxIndex

	// txIndex is the same index seen as the seam the peer manager takes
	// (protocol.TxIndex). Held as the interface rather than derived from
	// recentTxIndex at the call site because a typed nil in an interface is
	// not nil, and a non-nil TxIndex is what tells the manager compact
	// blocks are available.
	txIndex protocol.TxIndex

	// blocksFinalConsumer is the block announcement relay's Kafka leg
	// (bridge/kafka.go). nil when settings.Kafka.BlocksFinalConfig is unset,
	// which is not an error: block announcements just do not go out this way.
	// Its lifecycle is Start/Stop's, like admission and manager: started in
	// Start bound to the Start ctx, closed explicitly in Stop rather than
	// left to ctx cancellation alone. That ordering exists so no new
	// RelayBlock call starts against a peer registry the manager is already
	// tearing down (see Stop's own comment) — NOT as a claim that Close
	// proves the underlying consumer goroutine has exited. Against
	// util/kafka/in_memory_kafka's own fake it provably has not (fix round 1,
	// review finding I3): Close's wg.Wait() is a no-op nothing ever Add()s
	// to, and nothing inside its idle message loop ever checks ctx. A real
	// Kafka client's Close is unaffected by that fake's limitation.
	blocksFinalConsumer kafka.KafkaConsumerGroupI

	// txAnnouncer is the tx announcement relay's batching leg (txrelay.go):
	// bridge.StartTxMetaConsumer's decoded, non-coinbase, not-in-block ADD
	// entries land here via txAnnouncer.put, and its own flush calls
	// s.manager.RelayTxs. Unlike blocksFinalConsumer, this is constructed
	// unconditionally in Start regardless of whether
	// settings.Kafka.TxMetaConfig is set: it is cheap (no I/O of its own
	// until something Puts into it), and constructing it unconditionally
	// means Stop always has exactly one thing to close rather than a
	// conditional mirroring bridge.StartTxMetaConsumer's own nil check. nil
	// only before Start runs (Init/New alone).
	txAnnouncer *txAnnouncer

	// legacyInvProducer is Task 16's tx-inv round trip's PRODUCE leg
	// (bridge/kafka.go LegacyInvProducer): built before startSync, so
	// startSync's SyncConfig has it ready as protocol.SyncConfig.
	// TxInvProducer, the same ordering reason txAnnouncer is built before
	// startSync. nil when settings.Kafka.LegacyInvConfig is unset — tx invs
	// from the wire are then simply not produced to Kafka (Produce/Stop are
	// both nil-receiver safe, so Stop can still call it unconditionally).
	legacyInvProducer *bridge.LegacyInvProducer

	// legacyInvConsumer is Task 16's tx-inv round trip's CONSUME leg
	// (bridge.StartLegacyInvConsumer). A PLAIN listener, like
	// blocksFinalConsumer — not controlled, see that function's own doc
	// comment for why. nil when settings.Kafka.LegacyInvConfig is unset.
	// Closed from Stop's own defer (fix round 2, Important 1), not inline
	// like blocksFinalConsumer: see that defer's doc comment for why.
	legacyInvConsumer kafka.KafkaConsumerGroupI

	headerIndexMu sync.RWMutex
	headerIndex   *protocol.HeaderIndex

	// headerBatchSize is an instance field, not a package const, purely so
	// tests can shrink it on their own *Server to force a multi-batch
	// forward walk without touching shared state.
	headerBatchSize uint32

	// syncTick and maxLastBlockTime are instance fields for the same reason:
	// the end-to-end sync test drives the periodic send pass and the sync-peer
	// rotation on its own *Server without waiting out the production windows.
	// Zero on both means "keep the protocol package's own default".
	syncTick         time.Duration
	maxLastBlockTime time.Duration
}

func New(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI) *Server {
	return &Server{
		logger:           logger,
		settings:         tSettings,
		blockchainClient: blockchainClient,
		headerBatchSize:  defaultHeaderBatchSize,
	}
}

// NewWithDeps is New plus the ingestion dependencies, which is what enables
// block sync. The daemon uses this constructor exclusively.
func NewWithDeps(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, deps Deps) *Server {
	srv := New(logger, tSettings, blockchainClient)
	srv.deps = deps

	return srv
}

// SetSyncClocks narrows the sync tick and the sync-peer rotation window. It is
// test support for the parity harness and the integration tests, which need
// the 180 second rotation and the one second tick brought down to what a test
// can wait for; the daemon never calls it. Zero keeps the default for either.
func (s *Server) SetSyncClocks(tick, maxLastBlockTime time.Duration) {
	s.syncTick = tick
	s.maxLastBlockTime = maxLastBlockTime
}

// ConnectedCount is the number of peers connected to this server; 0 before
// Start. Test support for the parity harness.
func (s *Server) ConnectedCount() int {
	if s.manager == nil {
		return 0
	}

	return int(s.manager.ConnectedCount())
}

// PeerScores is PeerManager.PeerScores for the peers this server holds; nil
// before Start. Test support for the parity harness.
func (s *Server) PeerScores() map[string]int {
	if s.manager == nil {
		return nil
	}

	return s.manager.PeerScores()
}

// RecentTxIndexLen is how many transaction hashes the bridge's
// recent-transaction index holds (bridge/recenttx.go); 0 before Start, for a
// depless caller, and whenever compact blocks are off. Test support for the
// parity harness: the index is filled asynchronously, off the txmeta Kafka
// topic, and a scenario that announces a compact block has to know the
// transactions it expects the node to already hold have arrived. Nothing
// outside the node reports that — the node's own inv for a transaction is
// raised by the peer-sourced path as well, so it does not stand for this.
func (s *Server) RecentTxIndexLen() int {
	return s.recentTxIndex.Len()
}

func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		return http.StatusOK, "OK", nil
	}

	checks := make([]health.Check, 0, 2)

	if s.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: s.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(s.blockchainClient)})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (s *Server) Init(_ context.Context) error {
	if s.settings.Policy.ExcessiveBlockSize <= 0 {
		s.logger.Warnf("[svp2p] excessiveblocksize is %d; the wire layer cannot size messages from 0 and uses %d bytes", s.settings.Policy.ExcessiveBlockSize, protocol.BlockPayloadLimit(s.settings))
	}

	wire.SetLimits(protocol.BlockPayloadLimit(s.settings))

	s.listenAddresses = s.settings.Legacy.ListenAddresses
	if len(s.listenAddresses) == 0 {
		ip, err := outboundIP()
		if err != nil {
			return err
		}

		s.listenAddresses = []string{net.JoinHostPort(ip.String(), s.settings.ChainCfgParams.DefaultPort)}
	}

	banListPath := ""
	if s.settings.Legacy.WorkingDir != "" {
		banListPath = filepath.Join(s.settings.Legacy.WorkingDir, "banlist.json")
	}

	banList, err := protocol.NewBanList(banListPath)
	if err != nil {
		return err
	}

	s.banList = banList
	s.manager = protocol.NewPeerManager(s.logger, s.settings, banList)

	// The address table. Persistence is behind legacy_savePeers, which
	// defaults to false ("by default we do not save the peers",
	// settings/settings.go:674); an empty path is that disabled state
	// (protocol/addrman_persist.go). No new settings key is introduced here —
	// legacy_savePeers and legacy WorkingDir both already exist, and the
	// filename matches the banlist.json convention immediately above.
	peersPath := ""
	if s.settings.Legacy.SavePeers && s.settings.Legacy.WorkingDir != "" {
		peersPath = filepath.Join(s.settings.Legacy.WorkingDir, "peers.json")
	}

	s.addrMan = protocol.NewAddrMan(s.logger, protocol.AddrManOptions{Path: peersPath})

	// A snapshot that cannot be read is not fatal: Load has already logged
	// what to do about it and has latched the table into never-overwrite mode,
	// so the operator's file survives for inspection while the node cold
	// starts (protocol/addrman_persist.go Load).
	if err := s.addrMan.Load(); err != nil {
		s.logger.Warnf("[svp2p] starting with an empty address table: %v", err)
	}

	s.manager.SetAddrMan(s.addrMan)

	return nil
}

func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var readyClosed bool

	closeReady := func() {
		if !readyClosed {
			readyClosed = true

			close(readyCh)
		}
	}
	defer closeReady()

	if err := s.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx); err != nil {
		if errors.IsContextError(err) {
			s.logger.Infof("[svp2p] shutting down during FSM wait")
			return err
		}

		s.logger.Errorf("[svp2p] failed to wait for FSM transition from IDLE state: %s", err)

		return err
	}

	// txAnnouncer is built before startSync, not just before the Kafka
	// consumer below: startSync wires the tx ingestor (Task 14) into the
	// manager, and that ingestor's announce seam is txAnnouncer.put. It only
	// needs s.manager.RelayTxs, which Init already constructed, so building
	// it here costs nothing extra and guarantees the seam always has
	// somewhere to land — the same reasoning this file already documents for
	// the Kafka consumer's callback below.
	//
	// The canRelay closure is spec §7's FSM RUNNING gate (canRelayTx,
	// txrelay.go), applied at put — the choke point BOTH tx-announce
	// producers share, this one and the Kafka-sourced one below — not just
	// at that Kafka listener's own control channel, which covers only
	// itself. See txAnnouncer.canRelay's doc comment (review round 1,
	// Important 1) for why both gates stay.
	s.txAnnouncer = newTxAnnouncer(s.logger, s.manager.RelayTxs, func() bool {
		return canRelayTx(ctx, s.blockchainClient, s.logger)
	})

	// legacyInvProducer is built here for the identical reason: startSync's
	// SyncConfig.TxInvProducer needs it ready, and it costs nothing extra to
	// build unconditionally — Produce/Stop are both nil-receiver safe when
	// the topic is unconfigured.
	legacyInvProducer, err := bridge.StartLegacyInvProducer(ctx, s.logger, s.settings)
	if err != nil {
		return errors.New(errors.ERR_SERVICE_NOT_STARTED, "svp2p: failed to start legacy inv Kafka producer", err)
	}

	s.legacyInvProducer = legacyInvProducer

	if err := s.startSync(ctx); err != nil {
		if errors.IsContextError(err) {
			s.logger.Infof("[svp2p] shutting down during header index hydration")
			return err
		}

		s.logger.Errorf("[svp2p] failed to start block sync: %s", err)

		return err
	}

	// Hydrating before subscribing cannot miss a block that lands in between:
	// both Client and LocalClient replay their last/current best-block
	// notification to a subscriber immediately upon Subscribe, so the first
	// notification received here always re-confirms (rather than skips)
	// whatever the store's best header was at subscribe time.
	headerNotifications, err := s.blockchainClient.Subscribe(ctx, blockchain.SubscriberSVP2P)
	if err != nil {
		s.logger.Errorf("[svp2p] failed to subscribe to blockchain notifications: %s", err)
		return err
	}

	go s.runHeaderIndexSubscription(ctx, headerNotifications)

	// Started before the manager, so the periodic snapshot is running by the
	// time the first peer can add an address. It is a no-op when persistence
	// is disabled.
	if s.addrMan != nil {
		s.addrMan.StartPersistence()
	}

	if err := s.manager.Start(ctx, s.listenAddresses); err != nil {
		return err
	}

	s.logger.Infof("[svp2p] peer manager started, listening on %v", s.manager.ListenAddrs())

	// The block announcement relay's Kafka leg. Started after the manager so
	// RelayBlock always has a live peer registry to read; a message that
	// arrives in the gap before the manager is up simply finds no peers
	// connected yet, which is the same "nobody to tell" case as a call with
	// zero peers at any other time.
	consumer, err := bridge.StartBlocksFinalConsumer(ctx, s.logger, s.settings, s.manager.RelayBlock)
	if err != nil {
		return errors.New(errors.ERR_SERVICE_NOT_STARTED, "svp2p: failed to start blocks-final Kafka consumer", err)
	}

	s.blocksFinalConsumer = consumer

	// The tx announcement relay's Kafka leg. Started the same way as
	// blocks-final and after the same manager readiness point, for the same
	// "nobody to tell yet" reasoning — except this one is CONTROLLED (E1):
	// bridge.StartTxMetaConsumer only actually consumes while the FSM is
	// RUNNING, polling that state itself. s.txAnnouncer was already built
	// above, before startSync, so both this consumer's callback AND the
	// peer-sourced tx ingestor (Task 14) share the one announcer instance.
	// The same consumer is the recent-transaction index's main feed
	// (bridge/recenttx.go): every entry it relays is also a transaction a
	// compact block may name. s.recentTxIndex is nil for a depless caller,
	// which Add treats as "keep nothing".
	bridge.StartTxMetaConsumer(ctx, s.logger, s.settings, s.blockchainClient, s.recentTxIndex, s.txAnnouncer.put)

	// The tx-inv round trip's CONSUME leg (Task 16), started the same way
	// and after the same manager readiness point as blocks-final above:
	// PeerManager.InvFromKafka needs a live peer registry to answer into.
	// A PLAIN listener, like blocks-final, not controlled (see
	// bridge.StartLegacyInvConsumer's own doc comment for why): the RUNNING
	// gate is applied per-message inside it, not by pausing the consumer.
	// s.legacyInvProducer above is the PRODUCE leg this consumer's own
	// round trip closes.
	legacyInvConsumer, err := bridge.StartLegacyInvConsumer(ctx, s.logger, s.settings, s.blockchainClient, s.manager.InvFromKafka)
	if err != nil {
		return errors.New(errors.ERR_SERVICE_NOT_STARTED, "svp2p: failed to start legacy inv Kafka consumer", err)
	}

	s.legacyInvConsumer = legacyInvConsumer

	apiKey := s.settings.GRPCAdminAPIKey
	if apiKey == "" {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return errors.New(errors.ERR_SERVICE_NOT_STARTED, "svp2p: failed to generate API key", err)
		}

		apiKey = hex.EncodeToString(key)
		s.logger.Infof("[svp2p] generated admin API key: %s", apiKey)
	}

	authOptions := &util.AuthOptions{
		APIKey: apiKey,
		ProtectedMethods: map[string]bool{
			"/peer_api.PeerService/BanPeer":   true,
			"/peer_api.PeerService/UnbanPeer": true,
		},
	}

	// This blocks until the gRPC server stops.
	if err := util.StartGRPCServer(ctx, s.logger, s.settings, "svp2p", s.settings.Legacy.GRPCListenAddress, func(server *grpc.Server) {
		peer_api.RegisterPeerServiceServer(server, s)
		closeReady()
	}, authOptions); err != nil {
		return errors.New(errors.ERR_SERVICE_NOT_STARTED, "svp2p: cannot start gRPC server", err)
	}

	return nil
}

// Stop's ctx is discarded rather than threaded through to the teardowns
// below: ServiceManager calls Stop synchronously and relies on the callee
// to honour whatever deadline it was given, but nothing here currently
// does. This matters most for stoppableBridge.Stop, which can block on an
// in-flight orphan-pool validate call with no bound of its own in the
// production-default batched validator path — see that method's own doc
// comment (orphans.go's orphanPool.stop) for the traced reason. A future
// fix would thread ctx into these teardowns and give stoppableBridge.Stop
// a ctx parameter to respect; this task does not build that, only names
// where a reader of the shutdown path would look for it.
func (s *Server) Stop(_ context.Context) error {
	// admission, stoppableBridge, the legacy-inv consumer and the legacy-inv
	// producer are ALL released via defer, not inline below, so an early
	// return from blocksFinalConsumer.Close or manager.Stop still releases
	// every one of them rather than leaking (fix round 2, Minor 3, extended
	// by fix round 2's Important 1): each owns something that only its own
	// Stop/Close call releases (admission's failure-map eviction loop;
	// stoppableBridge's orphan-pool TTL ticker and eviction worker; the
	// legacy-inv consumer's Kafka subscription; the legacy-inv producer's
	// DC11 flush), and leaking — or, for the DC11 flush, silently dropping
	// announcements — on an error path is strictly worse than the small
	// ordering risk of releasing them without every peer goroutine having
	// joined first. That risk is real only on this same abnormal,
	// something-already-failed path: on the ordinary success path this
	// defer still runs after manager.Stop() below has already completed,
	// preserving every ordering constraint exactly (no new RelayBlock/
	// InvFromKafka call starts against a torn-down registry; the DC11 flush
	// runs after every Produce caller has joined). The risk that DOES
	// remain on the abnormal path — Produce racing the flush — is recovered
	// in bridge.LegacyInvProducer.Produce itself (fix round 2, Important 1);
	// see that method's own doc comment.
	defer func() {
		if s.admission != nil {
			s.admission.Stop()
			s.admission = nil
		}

		if s.stoppableBridge != nil {
			s.stoppableBridge.Stop()
			s.stoppableBridge = nil
		}

		// The producer is flushed BEFORE the consumer is closed, deliberately.
		// This ordering only buys anything on the ABNORMAL path — an early
		// return above, where manager.Stop() never ran: there the peer
		// registry is still live, so a tx inv still sitting in the producer's
		// channel can be flushed, consumed, and answered with a getdata.
		// On the ordinary path this defer runs AFTER manager.Stop() has
		// returned — a defer fires at function exit, so it follows every
		// statement in the body — and the registry is therefore ALREADY torn
		// down: a flushed inv lands on the departed-peer path, which is
		// harmless and is what txinv_shutdown_test.go asserts. Reversing the
		// order would flush into a topic nothing is listening to on either
		// path.
		if err := s.legacyInvProducer.Stop(); err != nil {
			s.logger.Errorf("[svp2p] failed to stop legacy inv Kafka producer during shutdown: %v", err)
		}

		s.legacyInvProducer = nil

		if s.legacyInvConsumer != nil {
			if err := s.legacyInvConsumer.Close(); err != nil {
				s.logger.Errorf("[svp2p] failed to close legacy inv Kafka consumer during shutdown: %v", err)
			}

			s.legacyInvConsumer = nil
		}
	}()

	// The header index subscription goroutine (runHeaderIndexSubscription)
	// is ctx-owned: it exits on the Start ctx's cancellation, which the
	// daemon triggers before calling Stop. Nothing to join here.
	//
	// The blocks-final consumer is closed FIRST, ahead of the manager: this
	// is the intended ordering so no new RelayBlock call starts while the
	// peer registry is being torn down under it. Against a real Kafka client
	// Close stops the fetch loop before returning, so the ordering is
	// actually enforced there. It is NOT enforced against
	// util/kafka/in_memory_kafka's own consumer-group fake, whose Close does
	// not wait for its message loop to exit (see blocksFinalConsumer's field
	// comment) — fix round 1, review finding I3 — so a test built on that
	// fake can show Close/Stop returning promptly, but cannot itself prove
	// this ordering held.
	if s.blocksFinalConsumer != nil {
		if err := s.blocksFinalConsumer.Close(); err != nil {
			return errors.New(errors.ERR_SERVICE_ERROR, "svp2p: failed to close blocks-final Kafka consumer", err)
		}

		s.blocksFinalConsumer = nil
	}

	// The tx announcer is closed here too, ahead of the manager, for the
	// same reason as blocksFinalConsumer above: RelayTxs (its flush target)
	// must not be called against a peer registry that is already being torn
	// down. Unlike blocksFinalConsumer this has no separate consumer handle
	// to Close — bridge.StartTxMetaConsumer's controlled listener is
	// entirely ctx-owned (E1's own doc comment) and already stops consuming
	// once the Start ctx is cancelled, which happens before Stop runs. What
	// txAnnouncer.close does is mark future/in-flight Puts into the batcher
	// no-ops and drain whatever is already queued, under a bounded timeout
	// (util.DrainBatcher) — the same close-before-teardown shape legacy's
	// own closeTxAnnounceBatcher uses in Stop (netsync/manager.go:3078).
	if s.txAnnouncer != nil {
		s.txAnnouncer.close(s.logger)
		s.txAnnouncer = nil
	}

	// Peers are joined next: PeerManager.Stop waits for every peer goroutine,
	// and a peer may still be running an ingest that goes through Admission
	// or the bridge's orphan pool, or a produce call into the legacy-inv
	// topic. Releasing any of those background resources before their
	// callers are gone would pull the failure map, the pool, the consumer's
	// subscription, or the producer's flush out from under a live caller —
	// which is why all four releases live in the defer above rather than
	// here: on the ordinary (no error) path the defer still runs after this
	// call returns, preserving that ordering exactly; it only fires ahead of
	// a joined manager on the abnormal path where manager.Stop itself never
	// got the chance to run.
	if s.manager != nil {
		if err := s.manager.Stop(); err != nil {
			return err
		}
	}

	// Replays are joined AFTER the manager, and this is the only correct
	// order. A replay runs on its own goroutine, reaches the bridge and the
	// admission gate, and reports its outcome into the manager when it
	// finishes; joining before the manager would prove nothing, because a peer
	// still running could start another one. The manager's join is what stops
	// new replays; this one waits out the replays already in flight, so no
	// goroutine is left to touch manager state or the four resources the defer
	// above releases.
	if s.retained != nil {
		s.retained.Wait()
	}

	// Stopped AFTER the manager, which is the reverse of the start order and
	// the only correct one: Stop joins the snapshot goroutine and then writes
	// the final peers.json, so it must run once no peer can still be adding
	// an address. It is idempotent and a no-op when persistence is disabled.
	if s.addrMan != nil {
		if err := s.addrMan.Stop(); err != nil {
			return err
		}
	}

	return nil
}

// HeaderIndex returns the server's header index, or nil before Start has
// hydrated it. Safe for concurrent use.
func (s *Server) HeaderIndex() *protocol.HeaderIndex {
	s.headerIndexMu.RLock()
	defer s.headerIndexMu.RUnlock()

	return s.headerIndex
}

func (s *Server) setHeaderIndex(idx *protocol.HeaderIndex) {
	s.headerIndexMu.Lock()
	s.headerIndex = idx
	s.headerIndexMu.Unlock()
}

// startSync builds the header index, hands it and the block-ingestion path to
// the peer manager, and hydrates the index. The order matters: the manager
// must own the index before the first header is written to it, because every
// write goes through the manager's shared sync-state mutex.
func (s *Server) startSync(ctx context.Context) error {
	if err := s.hydrateHeaderIndex(ctx); err != nil {
		return err
	}

	ing, err := s.newBlockIngestor()
	if err != nil {
		return err
	}

	// All three adapters come from the same bridge instance, so the getdata
	// answerer and the tx ingestor read/write through the same clients the
	// block-ingest path does — in particular, so IngestTx's rejectedTxns and
	// IngestBlock's clear of it (ingest_tx.go, ingest.go) are the same set.
	// Assigned from a nil check rather than passed straight through, because
	// a typed nil in an interface is not nil, and ConfigureSync switches on
	// Ingestor == nil.
	var (
		ingestor    protocol.BlockIngestor
		fetcher     protocol.BlockTxFetcher
		txIngestorI protocol.TxIngestor
	)

	if ing != nil {
		ingestor = ing
		fetcher = ing.bridge
		txIngestorI = &txIngestor{bridge: ing.bridge, announce: s.txAnnouncer.put}
	}

	// Compact-block reconstruction reads the bridge's recent-transaction
	// index (spec §7). Set only when legacy_compactBlocks is on and a real
	// bridge was built: a nil TxIndex is what leaves compact blocks off
	// inside the manager, so the flag has exactly one place it is read
	// here. Set before manager.Start, as SetTxIndex requires.
	if s.settings.Legacy.CompactBlocks && s.txIndex != nil {
		s.manager.SetTxIndex(s.txIndex)
	}

	// Task 16's tx-inv round trip PRODUCE seam: s.legacyInvProducer is
	// *bridge.LegacyInvProducer, built (possibly nil, an unconfigured topic)
	// before this method ran (Start's own doc comment). Nil-checked the same
	// way as ingestor/fetcher/txIngestorI above — a typed nil in an
	// interface is not nil.
	var txInvProducer protocol.TxInvProducer
	if s.legacyInvProducer != nil {
		txInvProducer = s.legacyInvProducer
	}

	minSyncPeerNetworkSpeed := s.settings.Legacy.MinSyncPeerNetworkSpeed

	if err := s.manager.ConfigureSync(protocol.SyncConfig{
		Index:                            s.HeaderIndex(),
		Ingestor:                         ingestor,
		Fetcher:                          fetcher,
		TxIngestor:                       txIngestorI,
		TxInvProducer:                    txInvProducer,
		AllowSyncCandidateFromLocalPeers: s.settings.Legacy.AllowSyncCandidateFromLocalPeers,
		TickInterval:                     s.syncTick,
		MaxLastBlockTime:                 s.maxLastBlockTime,

		BlockDownloadTimeoutBasePercent:    s.settings.Legacy.BlockDownloadTimeoutBasePercent,
		BlockDownloadTimeoutBaseIBDPercent: s.settings.Legacy.BlockDownloadTimeoutBaseIBDPercent,
		BlockDownloadTimeoutPerPeerPercent: s.settings.Legacy.BlockDownloadTimeoutPerPeerPercent,

		BlockDownloadSlowFetchTimeout: s.settings.Legacy.BlockDownloadSlowFetchTimeout,
		BlockDownloadMaxParallelFetch: s.settings.Legacy.BlockDownloadMaxParallelFetch,

		// Passed by address, unlike the fields above: 0 is an operator value
		// here (it disables the rotation rate floor, as legacy's
		// -minsyncpeernetworkspeed=0 does), so "unset" cannot be spelled with
		// a zero. Copied into a local first rather than pointing into the
		// settings struct, so the downloader cannot observe a later write to
		// it.
		MinSyncPeerNetworkSpeed: &minSyncPeerNetworkSpeed,
	}); err != nil {
		return err
	}

	return s.syncHeaderIndex(ctx)
}

// newBlockIngestor constructs the real bridge and its admission gate from the
// injected dependencies, or returns nil when they are not injected. A nil
// ingestor leaves the manager with the header index and no block sync: the
// service keeps serving the peer API and following the chain, and asks no peer
// for a block it has nothing to ingest with. The daemon always injects the
// dependencies; only a depless caller (New instead of NewWithDeps) hits this.
func (s *Server) newBlockIngestor() (*blockIngestor, error) {
	if !s.deps.complete() {
		s.logger.Warnf("[svp2p] block sync disabled: the block ingestion dependencies are not injected")
		return nil, nil
	}

	s.admission = bridge.NewAdmission(s.logger, s.settings)

	br := bridge.New(
		s.logger,
		s.settings,
		s.blockchainClient,
		s.deps.ValidationClient,
		s.deps.SubtreeStore,
		s.deps.TempStore,
		s.deps.UtxoStore,
		s.deps.SubtreeValidation,
		s.deps.BlockValidation,
		s.deps.BlockAssembly,
	)

	// br's concrete type (bridge.New's *svp2pBridge) has a Stop method
	// (bridge.go) even though bridge.Bridge itself does not declare one;
	// this compiles only because that stays true, so a future bridge.New
	// that stops satisfying stoppableBridge fails the build here rather
	// than silently degrading Stop into a no-op.
	s.stoppableBridge = br
	s.recentTxIndex = br.RecentTxIndex()
	s.txIndex = br.TxIndex()

	ing := &blockIngestor{
		logger:    s.logger,
		bridge:    br,
		admission: s.admission,
	}

	if s.admission.Enabled() {
		ing.retained = newOrphanBlocks(s.logger, s.deps.TempStore, s.admission.BudgetBytes())

		// A replay has no delivering peer: the one that sent the bytes was
		// released the moment the block was retained. BlockDone takes a nil
		// sync peer for exactly this, and without the report a replay that
		// fails on a local fault leaves the block recorded as held, so the
		// download walk never offers it again — the mainnet stall of
		// 2026-08-30.
		ing.retained.report = func(hash chainhash.Hash, outcome protocol.IngestOutcome) {
			_, _ = s.manager.BlockDone(nil, hash, outcome)
		}

		s.retained = ing.retained
	}

	return ing, nil
}

// stoppableBridge is the narrow slice of *svp2pBridge Server.Stop needs:
// releasing the orphan pool's background goroutines. bridge.Bridge (the
// interface protocol is shaped against, spec §4.4) deliberately does not
// declare Stop — protocol never stops a bridge — so this local interface is
// how Server reaches it without widening that one.
type stoppableBridge interface {
	Stop()
}

// hydrateHeaderIndex builds a fresh, genesis-rooted header index from the
// blockchain service. The blockchain service stays the authoritative header
// store (spec §11): this index is discarded and rebuilt on every startup.
func (s *Server) hydrateHeaderIndex(ctx context.Context) error {
	genesisHeaders, _, err := s.blockchainClient.GetBlockHeadersFromHeight(ctx, 0, 1)
	if err != nil {
		return errors.NewServiceError("svp2p: failed to fetch genesis header", err)
	}

	if len(genesisHeaders) == 0 {
		return errors.NewServiceError("svp2p: blockchain store returned no genesis header")
	}

	idx, err := protocol.NewHeaderIndex(genesisHeaders[0].ToWireBlockHeader())
	if err != nil {
		return err
	}

	s.setHeaderIndex(idx)

	return nil
}

// syncHeaderIndex walks the header index forward from its current tip to the
// blockchain service's best header. A bounded-batch forward walk covers
// straightforward chain growth cheaply, including a full genesis replay on
// first hydration. That walk alone cannot follow a reorg, though: the
// blockchain service stays authoritative and can switch its best chain to
// one whose ancestors sit at heights the forward walk already passed (a
// competing branch that only recently became taller), leaving those headers
// unfetched and the new best unconnected. reconcileHeaderIndex covers that
// case afterward. Both passes are called once during hydration and again on
// every subscription notification, so both are no-ops once the index has
// caught up.
func (s *Server) syncHeaderIndex(ctx context.Context) error {
	idx := s.HeaderIndex()
	if idx == nil {
		return errors.NewServiceError("svp2p: header index sync called before hydration")
	}

	best, bestMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewServiceError("svp2p: failed to get best block header", err)
	}

	if err := s.forwardWalkHeaderIndex(ctx, idx, bestMeta.Height); err != nil {
		return err
	}

	if err := s.reconcileHeaderIndex(ctx, idx, best); err != nil {
		return err
	}

	// The download scheduler walks against our own chain tip (the chainActive
	// counterpart), and it must be a header the index holds — which the two
	// passes above have just guaranteed for this one.
	if !s.manager.SetActiveTip(*best.Hash()) {
		s.logger.Warnf("[svp2p] best header %s is not in the header index; block download keeps the previous tip", best.Hash())
	}

	return nil
}

// addHeaders links headers into the index through the peer manager, which
// holds the shared sync-state mutex while it writes. Nothing may call
// HeaderIndex.AddHeader directly: the headers-first machine reads the index
// and then writes to it under that mutex, so a write outside it makes the
// machine act on stale state.
func (s *Server) addHeaders(headers []*wire.BlockHeader, pass string) error {
	orphans, err := s.manager.AddHeaders(headers)

	for _, orphan := range orphans {
		hash := orphan.BlockHash()
		s.logger.Warnf("[svp2p] orphan header %s while syncing header index (%s)", hash, pass)
	}

	return err
}

// forwardWalkHeaderIndex fetches headers in bounded, height-ordered batches
// from the index's current tip up to targetHeight.
//
// GetBlockHeadersFromHeight ranges over height, not row count — its query is
// `WHERE height >= $1 AND height < $1+limit` (see
// stores/blockchain/sql/GetBlockHeadersFromHeight.go), and it returns ALL
// headers at each height, including competing-fork headers, preallocating
// for up to 2*limit rows. So the next fetch must start at height+limit
// regardless of how many rows came back, and each batch is returned in
// descending height order, so it's walked oldest-first here so a header's
// parent is always already indexed when AddHeader sees it.
//
// MEASURED, mainnet-scale replay (900000-header sqlitememory store, cold
// cache, batch size 2000): 451 range reads, 1.85s, 1498 MiB allocated
// through the walk, 16 GC cycles, 509 MiB retained afterwards — 281 MiB of
// that is the header index itself (unavoidable, it is the product) and
// 220 MiB is the store's response cache holding all 450 batches at ~501 KiB
// each. GetBlockHeadersFromHeight caches every result unconditionally
// (stores/blockchain/sql/GetBlockHeadersFromHeight.go, cacheTTL 2 minutes,
// ttlcache with no capacity bound), and the walk never re-reads a height
// range, so those 220 MiB are pure startup waste held for the TTL window.
//
// Tuning the batch size does NOT reduce it, and this was measured rather
// than assumed: the same store walked at batch size 250 makes 3600 cache
// entries instead of 450 and retains 221 MiB — the same 900000 headers,
// merely re-partitioned.
//
// The 450 batches only ever co-exist in ONE scenario, also measured: a cold
// walk over a store that is already populated, which is a restart of a
// synced node. Nothing stores a block during that walk, so nothing clears
// the cache. When blocks arrive WHILE the walk runs — live IBD or catch-up,
// driven per notification through syncHeaderIndex — StoreBlock's
// ResetResponseCache (stores/blockchain/sql/StoreBlock.go:171) wipes the
// cache faster than the walk fills it: the same 900000-header replay,
// interleaved, retains 1734 KiB instead of 220 MiB. Each notification-driven
// walk also starts at idx.Tip()+1, so it fetches one batch rather than 450.
// So the exposure is one TTL window on restart, and it is absent during IBD.
//
// The only real fix is a read that bypasses the store's response cache,
// which needs a store-side change: every header-range reader in
// stores/blockchain/sql caches unconditionally, and neither
// blockchain.ClientI nor stores/blockchain.Store exposes a bypass or the
// concrete *SQL.ResetResponseCache. Left as an external dependency rather
// than reached into from here.
func (s *Server) forwardWalkHeaderIndex(ctx context.Context, idx *protocol.HeaderIndex, targetHeight uint32) error {
	_, tipHeight := idx.Tip()

	for height := uint32(tipHeight) + 1; height <= targetHeight; { //nolint:gosec // header heights are non-negative
		limit := targetHeight - height + 1
		if limit > s.headerBatchSize {
			limit = s.headerBatchSize
		}

		headers, _, err := s.blockchainClient.GetBlockHeadersFromHeight(ctx, height, limit)
		if err != nil {
			return errors.NewServiceError("svp2p: failed to fetch headers from height %d", height, err)
		}

		batch := make([]*wire.BlockHeader, 0, len(headers))
		for i := len(headers) - 1; i >= 0; i-- {
			batch = append(batch, headers[i].ToWireBlockHeader())
		}

		if err := s.addHeaders(batch, fmt.Sprintf("forward walk from height %d", height)); err != nil {
			return err
		}

		height += limit
	}

	return nil
}

// reconcileHeaderIndex recovers from a reorg the forward walk could not
// follow: if best is not yet indexed, its ancestors were left behind on a
// branch that stopped being best before the forward walk reached their
// heights. It walks backward one header at a time via GetBlockHeader,
// starting at best, until it reaches a header already in idx, then adds the
// recovered segment forward so every ancestor connects.
//
// The walk is bounded. On a healthy store it terminates on its own, because
// genesis is present from hydration and every chain reaches it. That
// guarantee belongs to the store, not to us: a store that is badly diverged,
// corrupt, or untrusted can answer with a chain of unknown parents that
// never meets our index, and an unbounded walk then loops issuing one
// GetBlockHeader per hop forever. So the walk stops after
// reconcileDepthCapMultiplier x headerBatchSize hops and fails hydration.
//
// The cap has NO SVNode counterpart, and none is implied: SVNode loads its
// own block index from its own datadir and never reconciles against a
// separate authoritative header store. This is a Teranode-only hydration
// path (spec §11: the blockchain service is authoritative, this index is
// rebuilt every startup), so the number is justified on our own terms:
//   - It must exceed any reorg depth we intend to survive. The forward walk
//     already covers straightforward growth in headerBatchSize-sized steps
//     (2000 headers, the wire headers-per-message cap), so ten of those —
//     20000 headers, roughly five months of mainnet blocks — is far deeper
//     than any reorg a running node should reconcile across, and it scales
//     with the batch size rather than being a second independent constant.
//   - It must be small enough that the failure is prompt: 20000 sequential
//     GetBlockHeader calls is a bounded, observable cost, not a hang.
//
// Past the cap the answer is a service error naming the diverged best
// header, not a retry and not a warning. ErrServiceError (not
// ERR_THRESHOLD_EXCEEDED, which carries retry semantics and maps to gRPC
// ResourceExhausted) is the right class because retrying the same walk
// against the same store yields the same answer: the store's best chain and
// our index disagree, hydration cannot produce a valid index, and the
// service must fail to start rather than run on a knowingly broken one. No
// peer is involved on this path, so no peer-scoring class applies either.
//
// Locking contract (Phase 2 carry, unchanged here): the sync-state machines
// this feeds are caller-locked under PeerManager.syncMu, the lock order is
// peer lock then manager lock, and nothing blocking runs under syncMu. This
// function holds neither lock — it only issues store reads — and hands its
// recovered headers to addHeaders, which takes syncMu inside
// PeerManager.AddHeaders. The store reads therefore stay outside syncMu,
// which is what keeps a slow or diverged store from blocking under it.
func (s *Server) reconcileHeaderIndex(ctx context.Context, idx *protocol.HeaderIndex, best *model.BlockHeader) error {
	if _, ok := idx.Lookup(*best.Hash()); ok {
		return nil
	}

	maxDepth := reconcileDepthCapMultiplier * s.headerBatchSize

	missing := []*model.BlockHeader{best}
	current := best

	for depth := uint32(0); current.HashPrevBlock != nil; depth++ {
		if depth >= maxDepth {
			return errors.NewServiceError(
				"svp2p: reorg reconciliation from best header %s reached the %d-header depth cap without meeting an indexed ancestor: the blockchain store's best chain has diverged from the header index",
				best.Hash(), maxDepth,
			)
		}

		parent, _, err := s.blockchainClient.GetBlockHeader(ctx, current.HashPrevBlock)
		if err != nil {
			return errors.NewServiceError("svp2p: failed to fetch header %s while reconciling a reorg", current.HashPrevBlock, err)
		}

		if _, ok := idx.Lookup(*parent.Hash()); ok {
			break
		}

		missing = append(missing, parent)
		current = parent
	}

	recovered := make([]*wire.BlockHeader, 0, len(missing))
	for i := len(missing) - 1; i >= 0; i-- {
		recovered = append(recovered, missing[i].ToWireBlockHeader())
	}

	return s.addHeaders(recovered, "reorg reconciliation")
}

// runHeaderIndexSubscription keeps the header index current from the
// blockchain service's own notifications. It exits when ctx is cancelled,
// the same shutdown signal Start passes to the rest of the service.
func (s *Server) runHeaderIndexSubscription(ctx context.Context, notifications <-chan *blockchain.Notification) {
	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[svp2p] header index subscription shutting down")
			return
		case notification, ok := <-notifications:
			if !ok {
				// Channel closed, exit the listener.
				return
			}

			if notification == nil || notification.Type != model.NotificationType_Block {
				continue
			}

			if err := s.syncHeaderIndex(ctx); err != nil {
				if errors.IsContextError(err) {
					return
				}

				s.logger.Errorf("[svp2p] failed to sync header index: %s", err)
			}
		}
	}
}

func (s *Server) GetPeerCount(_ context.Context, _ *emptypb.Empty) (*peer_api.GetPeerCountResponse, error) {
	return &peer_api.GetPeerCountResponse{Count: s.manager.ConnectedCount()}, nil
}

func (s *Server) GetPeers(_ context.Context, _ *emptypb.Empty) (*peer_api.GetPeersResponse, error) {
	resp := &peer_api.GetPeersResponse{}

	for _, snap := range s.manager.Snapshots() {
		resp.Peers = append(resp.Peers, &peer_api.Peer{
			Addr:           snap.Addr,
			Inbound:        snap.Inbound,
			SubVer:         snap.UserAgent,
			Version:        snap.ProtocolVersion,
			StartingHeight: snap.StartingHeight,
			BytesSent:      snap.BytesSent,
			BytesReceived:  snap.BytesReceived,
			ConnTime:       snap.ConnectedAt.Unix(),
			LastRecv:       snap.LastRecv.Unix(),
			Banscore:       int32(snap.MisbehaviorScore), //nolint:gosec // bounded by ban threshold
		})
	}

	return resp, nil
}

func (s *Server) BanPeer(_ context.Context, req *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error) {
	host := banHost(req.Addr)

	if err := s.banList.Add(host, time.Unix(req.Until, 0)); err != nil {
		return nil, err
	}

	s.manager.DisconnectHost(host)
	s.logger.Infof("[svp2p] banned %s until %d", host, req.Until)

	return &peer_api.BanPeerResponse{Ok: true}, nil
}

func (s *Server) UnbanPeer(_ context.Context, req *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error) {
	if err := s.banList.Remove(banHost(req.Addr)); err != nil {
		return nil, err
	}

	return &peer_api.UnbanPeerResponse{Ok: true}, nil
}

func (s *Server) IsBanned(_ context.Context, req *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error) {
	target := banHost(req.IpOrSubnet)

	if _, _, err := net.ParseCIDR(target); err == nil {
		for _, entry := range s.banList.List() {
			if entry.Host == target {
				return &peer_api.IsBannedResponse{IsBanned: true}, nil
			}
		}

		return &peer_api.IsBannedResponse{IsBanned: false}, nil
	}

	return &peer_api.IsBannedResponse{IsBanned: s.banList.IsBanned(target)}, nil
}

func (s *Server) ListBanned(_ context.Context, _ *emptypb.Empty) (*peer_api.ListBannedResponse, error) {
	entries := s.banList.List()

	banned := make([]string, 0, len(entries))
	for _, e := range entries {
		banned = append(banned, e.Host)
	}

	return &peer_api.ListBannedResponse{Banned: banned}, nil
}

func (s *Server) ClearBanned(_ context.Context, _ *emptypb.Empty) (*peer_api.ClearBannedResponse, error) {
	if err := s.banList.Clear(); err != nil {
		return nil, err
	}

	return &peer_api.ClearBannedResponse{Ok: true}, nil
}

// banHost strips a port from ip:port input; ban entries are host-level.
func banHost(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}

	return addr
}

// outboundIP mirrors legacy GetOutboundIP: the default listen address when
// none is configured.
func outboundIP() (net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "svp2p: failed to get outbound IP", err)
	}

	defer func() { _ = conn.Close() }()

	localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return nil, errors.New(errors.ERR_SERVICE_ERROR, "svp2p: unexpected local address type")
	}

	return localAddr.IP, nil
}
