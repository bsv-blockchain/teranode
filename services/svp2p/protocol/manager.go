package protocol

import (
	"context"
	"crypto/rand"
	"net"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/transport"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	// UserAgent identifies this node on the wire.
	UserAgent = "/teranode-svp2p:0.1.0/"

	// pingInterval mirrors SVNode PING_INTERVAL (net.h: 2 * 60).
	pingInterval = 2 * time.Minute

	// banScoreThreshold mirrors SVNode DEFAULT_BANSCORE_THRESHOLD (100),
	// validation.h:191.
	banScoreThreshold = 100

	// scoreInvalidBlock is what a block the pipeline rejected costs the peer
	// that delivered it. BlockDownloadTracker::BlockChecked
	// (net/block_download_tracker.cpp:113-127) scores the DoS level the
	// validation state carries — Misbehaving(node, nDoS,
	// state.GetRejectReason()) — and every block-invalidity site in SVNode's
	// validation.cpp sets that level to 100: state.DoS(100, ...) at :537
	// (bad-txns-oversize), :579 (bad-cb-missing), :589 (bad-cb-length), :3714
	// (blk-bad-inputs), and so on throughout CheckBlock and ConnectBlock.
	//
	// 100 equals banScoreThreshold, so one invalid block reaches the threshold
	// on its own. That is SVNode's own arithmetic, and it is why the reject
	// disconnects immediately as well as scoring: the score is what survives
	// the reconnect.
	//
	// SVNode gates that Misbehaving call TWICE, and this port satisfies both
	// gates rather than skipping them.
	//
	// The first gate is the reject-code window at block_download_tracker.cpp
	// :117 — see isPeerAttributableReject (services/svp2p/ingest.go), which is
	// this port's analogue of it and carries the citation.
	//
	// The second is the per-block `punish` flag at :124 (`nDoS > 0 &&
	// it->second.punish`), recorded by MarkBlockAsReceived(blockSource, punish,
	// state) at :68-73 and declared at block_download_tracker.h:97-101. Its
	// three call sites split, and the split is entirely about BIP 152:
	//
	//   - punish=TRUE at net_processing.cpp:4058, in ProcessBlockMessage
	//     (:4027) — the ordinary solicited `block` message path.
	//   - punish=FALSE at net_processing.cpp:3683, in ProcessBlockTxnMessage
	//     (:3613), whose own comment gives the reason: "BIP 152 permits peers
	//     to relay compact blocks after validating the header only; we should
	//     not punish peers if the block turns out to be invalid."
	//   - punish=FALSE at net_processing.cpp:3984, in
	//     ProcessCompactBlockMessage (:3731), the fBlockReconstructed branch —
	//     a compact block optimistically reconstructed while it is in flight
	//     from a different peer.
	//
	// Both false sites are compact-block paths, and compact blocks are out of
	// scope for this phase (spec §10 item 5). This port has no BLOCKTXN and no
	// cmpctblock path at all, so ProcessBlockMessage is the only analogue any
	// svp2p ingest entry has, every reachable case is punish=true, and scoring
	// every PeerFault unconditionally is faithful rather than approximate.
	//
	// FORWARD TRIGGER: whoever adds compact blocks MUST add the punish flag in
	// the same change. Without it this port will ban peers for compact blocks
	// they relayed innocently on a header-only check — precisely the outcome
	// BIP 152 and the comment at :3681-3683 exist to prevent.
	scoreInvalidBlock = 100

	// dialRetryBase and dialRetryMax bound the outbound reconnect backoff,
	// matching the retry semantics the bsvd connmgr gave the old service
	// (DefaultRetryDuration 5s, capped).
	dialRetryBase = 5 * time.Second
	dialRetryMax  = 5 * time.Minute

	// dialTimeout is the connect timeout every outbound dial goes through,
	// unchanged from the value dialLoop carried inline before the addrman-fed
	// dialer shared the seam with it.
	dialTimeout = 30 * time.Second

	// maxSentNonces bounds the self-connection nonce registry, mirroring
	// bsvd's sentNonces mruNonceMap size (50).
	maxSentNonces = 50

	sendBudgetBytes = 10 * 1024 * 1024
	recvQueueLen    = 128
	writeTimeout    = 30 * time.Second

	// defaultSyncTick is how often the manager runs the send pass the
	// machines are clockless for: the net_processing.cpp SendMessages block
	// pass (SendGetDataBlocks) and the DetectStalling check. SVNode runs
	// SendMessages on its 100 ms message loop; one second is enough here,
	// because a peer holds up to MAX_BLOCKS_IN_TRANSIT_PER_PEER blocks and
	// both stall windows are measured in tens of seconds.
	defaultSyncTick = time.Second
)

// SyncConfig wires the block-sync machines into the manager. Index is
// mandatory: without it the manager cannot answer a version message with a
// real starting height. Ingestor is optional — without it the manager keeps
// the index and runs the Phase 1 shape (handshake and ping only), because
// requesting blocks nothing can ingest would only burn bandwidth.
type SyncConfig struct {
	// Index is the shared header index. The manager takes every WRITE to it
	// under its own sync-state mutex; see the mutex's note.
	Index *HeaderIndex

	// Ingestor is the Teranode-side block ingestion path, or nil when the
	// bridge dependencies are not injected yet.
	Ingestor BlockIngestor

	// Fetcher is the Teranode-side read path a getdata is answered from, or
	// nil when the bridge dependencies are not injected yet. Unlike Ingestor
	// it gates nothing else: a manager with no fetcher still syncs, it just
	// answers no getdata.
	Fetcher BlockTxFetcher

	// TxIngestor is the Teranode-side transaction ingestion path, or nil
	// when the bridge dependencies are not injected yet. Unlike Ingestor it
	// gates nothing else here either: tx ingestion has no dependency on the
	// header-sync/block-download machines this method builds, so it is
	// stored unconditionally below rather than inside the
	// "cfg.Ingestor == nil" early return.
	TxIngestor TxIngestor

	// TxInvProducer is the tx-inv round trip's Kafka producer (Task 16), or
	// nil when settings.Kafka.LegacyInvConfig is not set. Like TxIngestor it
	// gates nothing else here: a manager with no producer still syncs, tx
	// invs from the wire are simply never produced to Kafka (OnInv's
	// collected hashes have nowhere to go, so PeerManager.Inv drops them).
	TxInvProducer TxInvProducer

	// DisableCheckpoints mirrors legacy netsync Config.DisableCheckpoints.
	DisableCheckpoints bool

	// AllowSyncCandidateFromLocalPeers mirrors
	// settings.Legacy.AllowSyncCandidateFromLocalPeers.
	AllowSyncCandidateFromLocalPeers bool

	// TickInterval overrides defaultSyncTick.
	TickInterval time.Duration

	// MaxLastBlockTime narrows the sync-peer rotation window from the
	// MaxLastBlockTime constant. Zero keeps the constant. It exists so an
	// integration test can observe a rotation in seconds instead of the three
	// real minutes the production window costs.
	MaxLastBlockTime time.Duration

	// BlockDownloadTimeoutBasePercent, BlockDownloadTimeoutBaseIBDPercent and
	// BlockDownloadTimeoutPerPeerPercent carry the three DetectStalling
	// percentages from settings.Legacy. Anything at or below zero keeps the
	// SVNode default the downloader is built with, so a caller that leaves them
	// unset gets SVNode's behavior rather than a zero-length timeout.
	BlockDownloadTimeoutBasePercent    int64
	BlockDownloadTimeoutBaseIBDPercent int64
	BlockDownloadTimeoutPerPeerPercent int64

	// BlockDownloadSlowFetchTimeout and BlockDownloadMaxParallelFetch carry the
	// parallel-fetch fuse and cap from settings.Legacy. Zero or less keeps the
	// SVNode default, as above. A cap of 1 is the way to turn racing off, since
	// one stalled holder already reaches it.
	BlockDownloadSlowFetchTimeout time.Duration
	BlockDownloadMaxParallelFetch int

	// MinSyncPeerNetworkSpeed carries settings.Legacy.MinSyncPeerNetworkSpeed:
	// the download rate, in bytes per second, below which a sync peer that has
	// completed no block inside the rotation window loses the sync slot.
	//
	// It is a POINTER, alone among the numeric fields here, because 0 is a
	// meaningful operator value rather than an absent one — it disables the
	// floor, which is legacy's own semantics (netsync/manager.go:266 compares
	// unsigned, so nothing is below a floor of 0). The "zero keeps the
	// default" convention the fields above use would make that value
	// unreachable. nil keeps MinBlockDownloadBytesPerSec, so a caller that
	// sets nothing — every test that builds a bare SyncConfig — keeps legacy's
	// floor rather than silently losing it.
	MinSyncPeerNetworkSpeed *uint64
}

// PeerManager owns listeners, the outbound dialer, the peer registry, and the
// node-wide sync state. It is the net.cpp CConnman counterpart, and the owner
// of this package's port of cs_main.
type PeerManager struct {
	logger    ulogger.Logger
	tSettings *settings.Settings
	banList   *BanList

	// maxBlockPayload is the node's excessiveblocksize, read once at
	// construction and given to every stream this manager builds. Server.Init
	// gives the same number to wire.SetLimits, so the buffered and the
	// streaming block paths share one ceiling.
	maxBlockPayload uint64

	// mu guards the connection registry only: peers, listeners, nonces and
	// the started flag.
	mu        sync.Mutex
	peers     map[*Peer]*SyncPeer
	listeners []net.Listener
	nonces    []uint64
	started   bool

	// associations indexes every established peer's association by its ID, so
	// a createstream naming one can find it (net.cpp:3192-3202 walks vNodes
	// for the same lookup). Guarded by mu, keyed by string(id). An entry
	// appears when the peer's handshake completes and goes when its runPeer
	// returns.
	associations map[string]*transport.Association

	// firstMessageTimeout bounds how long a fresh inbound connection may stay
	// silent before it names itself. A field rather than the constant so a
	// test can shorten it.
	firstMessageTimeout time.Duration

	// newStreams is CConnman::mPendingStreams (net.h:357): the queue of
	// further streams an outbound peer's policy asked for, filled by
	// queueNewStream and drained by openNewStreamsLoop.
	newStreams chan pendingStream

	// protoconfWait bounds how long the policy choice waits for an
	// established peer's protoconf. A field for the same reason
	// firstMessageTimeout is one.
	protoconfWait time.Duration

	// syncMu is this package's cs_main: ONE mutex over the whole sync-state
	// graph — HeaderSync, BlockDownloader, every peerSyncState, activeTip,
	// and every WRITE to headerIndex. Those machines are all caller-locked by
	// design (see their locking notes), and headersync's Lookup-before-
	// AddHeader reads stale state if any writer skips it, which is why
	// Server's blockchain-subscription goroutine reaches the index through
	// AddHeaders below rather than calling HeaderIndex.AddHeader itself.
	// Index READS need only the index's own lock.
	//
	// Lock order in this package is peer lock, then manager lock: nothing may
	// call a Peer method (Info, Disconnect) or Conn.Send while holding a
	// manager lock, or a future refactor deadlocks. mu and syncMu are never
	// held at the same time — every path that needs both snapshots the
	// registry under mu first, then works under syncMu.
	syncMu          sync.Mutex
	headerIndex     *HeaderIndex
	headerSync      *HeaderSync
	blockDownloader *BlockDownloader
	serving         *Serving
	ingestor        BlockIngestor
	fetcher         BlockTxFetcher
	txIngestor      TxIngestor
	txInvProducer   TxInvProducer
	// activeTip is our own best chain tip (the chainActive counterpart),
	// fed from the blockchain service and always a header present in the
	// index — the download scheduler cannot place a tip it cannot look up.
	activeTip HeaderNode
	syncTick  time.Duration

	// addrMan is the CAddrMan port (addrman.go), or nil when address handling
	// is not wired — in which case getaddr is never answered and never sent,
	// and an inbound addr is processed for its DoS rules but stored nowhere.
	// It is guarded by mu (the registry lock), not syncMu: AddrMan is
	// self-synchronising and deliberately outside the sync-state graph (see
	// addrman.go's own LOCKING note), and every one of its methods is a
	// blocking call that must not run under syncMu.
	addrMan *AddrMan

	// txIdx is bridge's recent-tx index (txindex.go), read by compact-block
	// reconstruction, or nil when compact blocks are off regardless of
	// legacy_compactBlocks: Server only calls SetTxIndex when the flag is on.
	// It is guarded by mu (the registry lock), not syncMu, for the same
	// reason addrMan is: Match hashes millions of entries and Open fetches
	// from the store, so both TxIndex methods are blocking calls that must
	// not run under syncMu.
	txIdx TxIndex

	// addrRelaySeed is CConnman's nSeed0/nSeed1 as used by
	// GetDeterministicRandomizer(RANDOMIZER_ID_ADDRESS_RELAY) (net.cpp:3429):
	// the node-global secret that makes the addr forwarding pick unpredictable
	// to anyone who does not know it. Generated once at construction and never
	// written again, so it needs no lock.
	addrRelaySeed [32]byte

	// outboundDials is the set of addresses the addrman-driven dialer holds a
	// dial for, keyed by ip:port and guarded by mu. It is the stand-in for
	// SVNode's semOutbound grant, which ThreadOpenConnections takes before
	// ConnectNode and moves into the CNode it created (net.cpp:1852,
	// :2178-2180) — see outbound.go's outboundSlots.connected.
	outboundDials map[string]struct{}

	// outboundTick is the addrman-driven dialer's pass period, SVNode's own
	// 500ms (net.cpp:1846). A field for the same reason syncTick is one: the
	// tests drive the loop faster than a real node does.
	outboundTick time.Duration

	// dialTCP opens one outbound TCP connection. It is the single dial seam
	// both dialLoop (legacy_connect_peers) and the addrman-driven dialer go
	// through, so the network — the one dependency neither can provide for
	// itself — can be stood in for without either loop's own logic being
	// bypassed.
	dialTCP func(addr string) (net.Conn, error)

	// dialTCPContext is dialTCP for a caller that must be able to abandon the
	// dial. The stream dialer needs it because it drains its queue on ONE
	// goroutine: a dial it cannot give up on holds Stop and every DATA1
	// request behind it. The default routes through dialTCP, so a test that
	// replaces the seam above still governs both.
	dialTCPContext func(ctx context.Context, addr string) (net.Conn, error)

	// dnsLookup, dnsSeedDelay, fixedSeedGrace and fixedSeeds are the seams of
	// seeds.go: the resolver, ThreadDNSAddressSeed's opening sleep, the
	// fixed-seed grace period and the fixed-seed table. Fields so tests can
	// drive the bootstrap without the network or a minute of wall clock.
	dnsLookup       dnsLookupFunc
	dnsSeedDelay    time.Duration
	fixedSeedGrace  time.Duration
	fixedSeeds      func() []Address
	fixedSeedsAdded bool

	quit chan struct{}
	wg   sync.WaitGroup
}

func NewPeerManager(logger ulogger.Logger, tSettings *settings.Settings, banList *BanList) *PeerManager {
	m := &PeerManager{
		logger:          logger,
		tSettings:       tSettings,
		banList:         banList,
		maxBlockPayload: BlockPayloadLimit(tSettings),
		peers:           make(map[*Peer]*SyncPeer),
		associations:    make(map[string]*transport.Association),
		outboundDials:   make(map[string]struct{}),
		quit:            make(chan struct{}),
		syncTick:        defaultSyncTick,
		outboundTick:    defaultOpenConnectionsTick,
		dnsLookup:       defaultDNSLookup,
		dnsSeedDelay:    defaultDNSSeedDelay,
		fixedSeedGrace:  defaultFixedSeedGrace,

		firstMessageTimeout: defaultFirstMessageTimeout,
		protoconfWait:       defaultProtoconfWait,
		newStreams:          make(chan pendingStream, newStreamQueueLen),
	}

	m.fixedSeeds = m.defaultFixedSeeds

	m.dialTCP = func(addr string) (net.Conn, error) {
		return net.DialTimeout("tcp", addr, dialTimeout)
	}

	m.dialTCPContext = func(ctx context.Context, addr string) (net.Conn, error) {
		return m.dialWithContext(ctx, addr)
	}

	// net.cpp CConnman::Start seeds nSeed0/nSeed1 from GetRand; a failed read
	// leaves the zero seed, which still gives a stable daily pick and only
	// costs the unpredictability, so it is logged rather than fatal.
	if _, err := rand.Read(m.addrRelaySeed[:]); err != nil {
		logger.Warnf("[svp2p] addr relay seed could not be randomized: %v", err)
	}

	return m
}

// SetAddrMan gives the manager the address table getaddr and addr work from.
// It must be called before Start; a nil AddrMan leaves address handling off.
func (m *PeerManager) SetAddrMan(addrMan *AddrMan) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addrMan = addrMan
}

// addrManager reads the address table under mu. Never called with syncMu held:
// every AddrMan method is a blocking call.
func (m *PeerManager) addrManager() *AddrMan {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.addrMan
}

// SetTxIndex gives the manager the transaction index compact-block
// reconstruction reads from. It must be called before Start; a nil TxIndex
// leaves compact blocks off regardless of legacy_compactBlocks.
func (m *PeerManager) SetTxIndex(idx TxIndex) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.txIdx = idx
}

// txIndex reads the transaction index under mu. Never called with syncMu
// held: Match hashes millions of entries and Open fetches from the store, so
// both TxIndex methods are blocking calls that must run with no manager lock
// held, exactly the AddrMan contract above.
func (m *PeerManager) txIndex() TxIndex {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.txIdx
}

// ConfigureSync gives the manager the shared header index and, when an
// ingestor is supplied, builds the headers-first and block-download machines
// that drive block sync. It must be called before Start.
func (m *PeerManager) ConfigureSync(cfg SyncConfig) error {
	if cfg.Index == nil {
		return errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: sync config carries no header index")
	}

	m.mu.Lock()
	started := m.started
	m.mu.Unlock()

	if started {
		return errors.New(errors.ERR_SERVICE_ERROR, "svp2p: sync must be configured before the peer manager starts")
	}

	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	m.headerIndex = cfg.Index

	// Stored unconditionally, ahead of the cfg.Ingestor nil check below: tx
	// ingestion has no dependency on the header-sync/block-download
	// machines that check gates.
	m.txIngestor = cfg.TxIngestor
	m.txInvProducer = cfg.TxInvProducer

	if cfg.TickInterval > 0 {
		m.syncTick = cfg.TickInterval
	}

	if cfg.Ingestor == nil {
		return nil
	}

	headerSync, err := NewHeaderSync(HeaderSyncConfig{
		Index:                            cfg.Index,
		Params:                           m.tSettings.ChainCfgParams,
		DisableCheckpoints:               cfg.DisableCheckpoints,
		AllowSyncCandidateFromLocalPeers: cfg.AllowSyncCandidateFromLocalPeers,
	})
	if err != nil {
		return err
	}

	blockDownloader, err := NewBlockDownloader(cfg.Index, headerSync)
	if err != nil {
		return err
	}

	if cfg.MaxLastBlockTime > 0 {
		blockDownloader.maxLastBlockTime = cfg.MaxLastBlockTime
	}

	// A zero here means "not configured", not "no timeout": a zero base would
	// disconnect every peer holding a block on the first check.
	if cfg.BlockDownloadTimeoutBasePercent > 0 {
		blockDownloader.timeoutBasePercent = cfg.BlockDownloadTimeoutBasePercent
	}

	if cfg.BlockDownloadTimeoutBaseIBDPercent > 0 {
		blockDownloader.timeoutBaseIBDPercent = cfg.BlockDownloadTimeoutBaseIBDPercent
	}

	// Zero means unset for this one too, so the per-peer compensation cannot be
	// turned off from settings the way SVNode's own flag allows. That is the
	// price of the zero-means-unset convention this struct already uses for
	// TickInterval and MaxLastBlockTime, and turning the compensation off is not
	// something an operator has a reason to want: it only ever widens a window,
	// and only for peers whose blocks we asked for.
	if cfg.BlockDownloadTimeoutPerPeerPercent > 0 {
		blockDownloader.timeoutPerPeerPercent = cfg.BlockDownloadTimeoutPerPeerPercent
	}

	if cfg.BlockDownloadSlowFetchTimeout > 0 {
		blockDownloader.slowFetchTimeout = cfg.BlockDownloadSlowFetchTimeout
	}

	if cfg.BlockDownloadMaxParallelFetch > 0 {
		blockDownloader.maxParallelFetch = cfg.BlockDownloadMaxParallelFetch
	}

	// Nil, not zero, is what means unset here — see the field's own note. A
	// configured 0 is carried through as 0 and disables the rotation rate
	// floor, the way legacy's -minsyncpeernetworkspeed=0 does.
	if cfg.MinSyncPeerNetworkSpeed != nil {
		blockDownloader.minDownloadBytesPerSec = *cfg.MinSyncPeerNetworkSpeed
	}

	serving, err := NewServing(cfg.Index, headerSync)
	if err != nil {
		return err
	}

	m.headerSync = headerSync
	m.blockDownloader = blockDownloader
	m.serving = serving
	m.ingestor = cfg.Ingestor
	m.fetcher = cfg.Fetcher

	// Until the blockchain service reports a tip, our own chain is whatever
	// hydration put in the index.
	tipHash, _ := cfg.Index.Tip()
	if tip, ok := cfg.Index.Lookup(tipHash); ok {
		m.activeTip = tip
	}

	return nil
}

// SyncEnabled reports whether block sync is wired.
func (m *PeerManager) SyncEnabled() bool {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	return m.headerSync != nil
}

// AddHeaders links headers into the shared header index under the sync-state
// mutex, and returns those whose parent was not known for the caller to log.
// It is how the blockchain-subscription goroutine writes to the index without
// racing the headers-first machine's Lookup-before-AddHeader sequence. Pass
// headers oldest first, so each header's parent is already indexed.
//
// THIS PATH RUNS NO HEADER VALIDATION, deliberately. It is the trusted half of
// the two ways a header reaches the index: Server hydration and the blockchain
// subscription feed it headers Teranode's own blockchain service already
// validated and stored, so re-checking them here would be checking our own
// chain against itself. A peer's headers take the other half,
// HeaderSync.OnHeaders, which applies CheckBlockHeader, the checkpoint fence,
// the contextual difficulty rule and the timestamp cap before anything is
// inserted (see headersync.go acceptHeader).
//
// The distinction is load-bearing for the contextual difficulty check, which
// needs the parent's 147-block window. Hydration walks the store forward from
// the index tip and would otherwise have to satisfy that window while it is
// still building it. It matters for the timestamp cap too: a header our own
// store already holds must not be refused because THIS node's clock is skewed,
// which is exactly what the too-far-in-the-future rule would do to it. It also
// means a header this method inserts is NOT evidence that a peer could have got
// the same header past OnHeaders.
//
// Anything else that grows the index must go through OnHeaders, or the gates
// there stop bounding what a peer can make us store.
func (m *PeerManager) AddHeaders(headers []*wire.BlockHeader) ([]*wire.BlockHeader, error) {
	// Snapshotted before syncMu is taken: the two mutexes are never held
	// together (see the note on syncMu).
	handles := m.peerHandles()

	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	// Deferred after the unlock, so it runs BEFORE it — including on the
	// mid-batch error return, where the headers accepted so far stay indexed
	// and may already have resolved somebody's parked hash. It is a no-op when
	// the index is not configured.
	defer m.promoteBlockAvailabilityLocked(handles)

	if m.headerIndex == nil {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: header index is not configured")
	}

	var orphans []*wire.BlockHeader

	for _, header := range headers {
		connected, err := m.headerIndex.AddHeader(header)
		if err != nil {
			return orphans, err
		}

		if !connected {
			orphans = append(orphans, header)
		}
	}

	return orphans, nil
}

// promoteBlockAvailabilityLocked resolves every peer's parked
// hashLastUnknownBlock against the header index, and is called on both paths
// that grow the index.
//
// net_processing.cpp runs ProcessBlockAvailability (net_processing.cpp:355) on
// every peer on every SendMessages pass, so a parked hash is resolved by the
// next pass whatever put the header in mapBlockIndex. THE DIVERGENCE IS THAT
// THIS PORT MAKES IT EVENT-DRIVEN INSTEAD OF PER-PASS: the sweep runs when the
// index grows rather than on a timer.
//
// The port needs it because it cannot inherit the C++ safety net. Phase 2 Task
// 5's N2 fix made a non-sync peer availability-only during a headers-first
// round (see the divergence note in headersync.go OnHeaders), so its
// announcement parks. The only other caller of processBlockAvailability is
// FindNextBlocksToDownload, which the download walk reaches on none of its
// early returns — a peer with no pindexBestKnownBlock is exactly a peer that
// walk gives up on. So a parked hash could otherwise sit unresolved while the
// round indexed the very chain it named, and the peer stayed unschedulable.
// Running it on index growth resolves the hash at the earliest moment it CAN
// resolve, which is strictly sooner than any pass-driven sweep.
//
// Cost is one HeaderIndex.Lookup per PARKED peer per batch — not per header:
// both callers sweep once, after the whole batch is indexed.
// processBlockAvailability returns on its zero-hash test for every peer with
// nothing parked, so an idle peer costs one comparison. Nothing here blocks,
// which is what lets it run under syncMu.
//
// Requires syncMu. handles must be snapshotted by the caller BEFORE it takes
// syncMu, because mu and syncMu are never held together.
func (m *PeerManager) promoteBlockAvailabilityLocked(handles []peerHandle) {
	if m.headerIndex == nil {
		return
	}

	for _, h := range handles {
		if h.sync == nil || h.sync.State == nil {
			continue
		}

		h.sync.State.processBlockAvailability(m.headerIndex)
	}
}

// SetActiveTip records our own best chain tip, the chainActive counterpart the
// download scheduler walks against. It reports false when the hash is not in
// the index, in which case the previous tip stands: FindNextBlocksToDownload
// refuses to schedule against a tip it cannot place, so a wrong answer here
// would stall download rather than misdirect it.
func (m *PeerManager) SetActiveTip(hash chainhash.Hash) bool {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.headerIndex == nil {
		return false
	}

	tip, ok := m.headerIndex.Lookup(hash)
	if !ok {
		return false
	}

	m.activeTip = tip

	return true
}

// startingHeight is net_processing.cpp PushNodeVersion's nNodeStartingHeight:
// the height we advertise in our own version message.
func (m *PeerManager) startingHeight() int32 {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.headerIndex == nil {
		return 0
	}

	_, height := m.headerIndex.Tip()

	return height
}

func (m *PeerManager) Start(ctx context.Context, listenAddresses []string) error {
	m.mu.Lock()

	if m.started {
		m.mu.Unlock()
		return errors.New(errors.ERR_SERVICE_ERROR, "svp2p: peer manager already started")
	}

	m.started = true

	for _, addr := range listenAddresses {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			m.mu.Unlock()
			_ = m.Stop()

			return errors.New(errors.ERR_SERVICE_ERROR, "svp2p: cannot listen on %s", addr, err)
		}

		m.listeners = append(m.listeners, ln)
	}

	listeners := append([]net.Listener(nil), m.listeners...)
	m.mu.Unlock()

	for _, ln := range listeners {
		m.wg.Add(1)

		go func(ln net.Listener) {
			defer m.wg.Done()
			m.acceptLoop(ctx, ln)
		}(ln)
	}

	// net.cpp:2645-2651 CConnman::Start: threadOpenNewStreamConnections runs
	// for the whole life of the node, and only a node that allows further
	// streams ever queues one.
	if m.tSettings.Legacy.AllowBlockPriority {
		m.wg.Add(1)

		go func() {
			defer m.wg.Done()
			m.openNewStreamsLoop(ctx)
		}()
	}

	for _, addr := range m.tSettings.Legacy.ConnectPeers {
		m.wg.Add(1)

		go func(addr string) {
			defer m.wg.Done()
			m.dialLoop(ctx, addr)
		}(addr)
	}

	// net.cpp ThreadOpenConnections' `-connect` branch (net.cpp:1817-1836)
	// returns without ever reaching the addrman-fed loop below it, which is
	// what makes legacy_connect_peers dominant when it is set. A zero target
	// and a missing address table are the other two states that leave the loop
	// off; SetAddrMan must be called before Start, so reading it once here is
	// enough.
	if addrMan := m.addrManager(); addrMan != nil &&
		len(m.tSettings.Legacy.ConnectPeers) == 0 &&
		m.tSettings.Legacy.TargetOutboundPeers > 0 {
		target := m.tSettings.Legacy.TargetOutboundPeers

		m.wg.Add(1)

		go func() {
			defer m.wg.Done()
			m.openConnectionsLoop(ctx, addrMan, target)
		}()

		// net.cpp CConnman::Start: threadDNSAddressSeed unless -dnsseed=0;
		// init.cpp soft-sets -dnsseed=0 when -connect is given, which the
		// enclosing ConnectPeers test already reproduces.
		if !m.tSettings.Legacy.DisableDNSSeed {
			m.wg.Add(1)

			go func() {
				defer m.wg.Done()
				m.runDNSSeeding(ctx, addrMan)
			}()
		}
	}

	if m.SyncEnabled() {
		m.wg.Add(1)

		go func() {
			defer m.wg.Done()
			m.syncLoop(ctx)
		}()
	}

	return nil
}

// dialWithContext runs the dial seam on its own goroutine so the caller can
// give up on it. The abandoned dial still finishes on its own dialTimeout and
// closes whatever it produced; it is detached from m.wg on purpose, which is
// what keeps Stop from waiting on a socket nobody wants any more.
func (m *PeerManager) dialWithContext(ctx context.Context, addr string) (net.Conn, error) {
	type dialed struct {
		nc  net.Conn
		err error
	}

	done := make(chan dialed, 1)

	go func() {
		nc, err := m.dialTCP(addr)
		done <- dialed{nc: nc, err: err}
	}()

	select {
	case d := <-done:
		return d.nc, d.err

	case <-ctx.Done():
		go func() {
			if d := <-done; d.nc != nil {
				_ = d.nc.Close()
			}
		}()

		return nil, errors.New(errors.ERR_NETWORK_ERROR, "svp2p: dial to %s abandoned", addr, ctx.Err())

	case <-m.quit:
		go func() {
			if d := <-done; d.nc != nil {
				_ = d.nc.Close()
			}
		}()

		return nil, errors.New(errors.ERR_SERVICE_ERROR, "svp2p: dial to %s abandoned, the node is stopping", addr)
	}
}

func (m *PeerManager) acceptLoop(ctx context.Context, ln net.Listener) {
	for {
		nc, err := ln.Accept()
		if err != nil {
			select {
			case <-m.quit:
			case <-ctx.Done():
			default:
				m.logger.Errorf("[svp2p] accept failed on %s: %v", ln.Addr(), err)
			}

			return
		}

		// net.cpp CConnman::AcceptConnection: drop banned peers before any
		// protocol traffic.
		if m.banList.IsBanned(nc.RemoteAddr().String()) {
			m.logger.Infof("[svp2p] rejected banned inbound peer %s", nc.RemoteAddr())

			_ = nc.Close()

			continue
		}

		m.wg.Add(1)

		go func() {
			defer m.wg.Done()

			// net_processing.cpp:4708-4715: the first message decides whether
			// this is a peer or a further stream of one we already have.
			m.classifyInbound(ctx, nc)
		}()
	}
}

func (m *PeerManager) dialLoop(ctx context.Context, addr string) {
	delay := dialRetryBase

	for {
		if m.banList.IsBanned(addr) {
			m.logger.Infof("[svp2p] not dialing banned peer %s", addr)
			return
		}

		nc, err := m.dialTCP(addr)
		if err == nil {
			runErr := m.runPeer(ctx, nc, false, nil)

			// net.cpp: never redial a peer that proved to be ourselves.
			if errors.Is(runErr, ErrSelfConnection) {
				m.logger.Warnf("[svp2p] %s is ourselves, not redialing", addr)
				return
			}

			// bsvd connmgr semantics: a completed connection resets backoff.
			delay = dialRetryBase
		} else {
			m.logger.Debugf("[svp2p] dial %s failed: %v", addr, err)
		}

		select {
		case <-time.After(delay):
		case <-m.quit:
			return
		case <-ctx.Done():
			return
		}

		delay *= 2
		if delay > dialRetryMax {
			delay = dialRetryMax
		}
	}
}

// connectionDirection names which side dialled, for the connect and disconnect
// log lines.
func connectionDirection(inbound bool) string {
	if inbound {
		return "inbound"
	}

	return "outbound"
}

// runPeer drives one connection's whole life. first carries the bytes the
// inbound classifier already took off the socket, which the general stream
// replays before it reads any further (transport.Config.Prefix); it is nil for
// an outbound connection, which nothing has read from yet.
func (m *PeerManager) runPeer(ctx context.Context, nc net.Conn, inbound bool, first []byte) error {
	m.logger.Infof("[svp2p] peer %s connected (%s)", nc.RemoteAddr(), connectionDirection(inbound))

	general := transport.New(nc, transport.Config{
		Net:             m.tSettings.ChainCfgParams.Net,
		ProtocolVersion: wire.ProtocolVersion,
		SendBudgetBytes: sendBudgetBytes,
		RecvQueueLen:    recvQueueLen,
		WriteTimeout:    writeTimeout,
		MaxBlockPayload: m.maxBlockPayload,
		Logger:          m.logger,
		StreamType:      wire.StreamTypeGeneral,
		Prefix:          first,
	})

	// net_processing.cpp:210 CreateAssociationID: the side that dials names
	// the association and advertises the name in its version message. An
	// inbound peer names its own, which the handshake learns from that
	// message (net_processing.cpp:1775) and the watcher below registers.
	var ourAssociationID []byte

	// net_processing.cpp:209-211: the ID is created only when multistreams
	// are enabled, so a node with them off never names an association and can
	// never be given a second stream.
	if !inbound && m.tSettings.Legacy.AllowBlockPriority {
		id, err := generateAssociationID()
		if err != nil {
			m.logger.Warnf("[svp2p] running %s without multistreams: %v", nc.RemoteAddr(), err)
		} else {
			ourAssociationID = id
		}
	}

	assoc := transport.NewAssociation(general, ourAssociationID)

	syncPeer := NewSyncPeer(nc.RemoteAddr().String(), 0, newPeerSyncState())

	var (
		dispatch syncDispatcher
		ingestor BlockIngestor
		fetcher  BlockTxFetcher
	)

	if m.SyncEnabled() {
		dispatch = m

		m.syncMu.Lock()
		ingestor = m.ingestor
		fetcher = m.fetcher
		m.syncMu.Unlock()
	}

	// Read independently of SyncEnabled(): tx ingestion has no dependency on
	// the header-sync/block-download machines that gates, so a TxIngestor
	// set via ConfigureSync must reach every peer regardless of whether
	// block sync itself is enabled.
	m.syncMu.Lock()
	txIngestor := m.txIngestor
	m.syncMu.Unlock()

	// Read independently of SyncEnabled() for the same reason txIngestor is:
	// address handling has no dependency on the block-sync machines. A nil
	// AddrMan leaves PeerConfig.Addrs nil, which is what turns getaddr and
	// addr handling off (peer.go dispatchAddr).
	var addrs addrDispatcher

	if m.addrManager() != nil {
		addrs = m
	}

	peer := NewPeer(PeerConfig{
		Handshake: HandshakeConfig{
			Inbound:   inbound,
			Nonce:     m.newNonce(),
			UserAgent: UserAgent,
			// net_processing.cpp PushNodeVersion: nNodeStartingHeight, which
			// is the height of the header index tip.
			StartingHeight:       m.startingHeight(),
			MaxRecvPayloadLength: wire.DefaultMaxRecvPayloadLength,
			AllowBlockPriority:   m.tSettings.Legacy.AllowBlockPriority,
			AssociationID:        ourAssociationID,
			LocalAddr:            netAddressOf(nc.LocalAddr()),
			RemoteAddr:           netAddressOf(nc.RemoteAddr()),
			CheckIncomingNonce:   m.hasSentNonce,
		},
		Conn:         assoc,
		Logger:       m.logger,
		IdleTimeout:  m.tSettings.Legacy.PeerIdleTimeout,
		PingInterval: effectivePingInterval(m.tSettings.Legacy.PeerIdleTimeout, pingInterval),
		BanThreshold: banScoreThreshold,
		// The legacy `--nobanning` switch, read from the same setting the
		// legacy service reads it from (see PeerConfig.DisableBanning).
		DisableBanning: m.tSettings.Legacy.DisableBanning,
		Sync:           dispatch,
		Addrs:          addrs,
		Compact:        m,
		SyncPeer:       syncPeer,
		Ingestor:       ingestor,
		Fetcher:        fetcher,
		TxIngestor:     txIngestor,
		// A bound method value, not manager state snapshotted under a lock:
		// markTxKnown takes syncMu itself on each call, so there is nothing
		// to read here ahead of time the way ingestor/fetcher/txIngestor are.
		MarkTxKnown: m.markTxKnown,
	})

	m.mu.Lock()
	m.peers[peer] = syncPeer
	m.mu.Unlock()

	// The association only becomes reachable by ID once the handshake is
	// done, because an inbound peer's ID arrives in its version message. This
	// watcher leaves on either outcome, so an association that dies before it
	// establishes leaks nothing.
	var watch sync.WaitGroup

	watch.Add(1)

	go func() {
		defer watch.Done()

		select {
		case <-peer.Established():
		case <-assoc.Done():
			return
		}

		// net_processing.cpp ProcessVerAckMessage (:1942-1953): once per
		// peer, right after the handshake, independent of which side dialled
		// — sent before the direction-specific branching below because
		// SVNode sends it to every peer, inbound and outbound alike. Gated
		// on the same two conditions Task 4's TxIndex seam already
		// established: legacy_compactBlocks on, and a TxIndex actually wired
		// (SetTxIndex's own doc comment).
		//
		// Queued here from the established-watch goroutine, not from the
		// peer loop itself, so its position relative to the peer loop's own
		// post-verack sends (ping, getaddr) is not fixed the way SVNode's
		// synchronous PushMessage call is. Harmless: every one of them goes
		// through the same Conn.Send, which only orders what it is actually
		// given, and nothing here depends on sendcmpct arriving before or
		// after any of the others.
		if m.tSettings.Legacy.CompactBlocks && m.txIndex() != nil {
			peer.send([]wire.Message{wire.NewMsgSendcmpct(false)})
		}

		id := ourAssociationID

		if inbound {
			// net_processing.cpp:209-211 creates an association ID only when
			// multistreams are enabled. With them off there is nothing this
			// node ever asked for, so the ID an inbound peer echoes is
			// unvalidated peer bytes and must not become a registry key.
			if !m.tSettings.Legacy.AllowBlockPriority {
				return
			}

			id = peer.Info().AssociationID
		}

		if len(id) > 0 {
			assoc.SetID(id)
			m.registerAssociation(assoc)
		}

		// association.cpp:113-115: only the side that dialled opens further
		// streams. The side that accepted keeps the Default policy until the
		// peer's own createstream names one (net.cpp:3233-3236 MoveStream).
		if inbound {
			return
		}

		m.setupStreams(ctx, peer, assoc, nc.RemoteAddr().String())
	}()

	err := peer.Run(ctx)

	// Peer.Run always ends through disconnect, which closes the association;
	// this is the belt for the brace, and it is what releases the watcher
	// above when the handshake never completed.
	_ = assoc.Close()

	watch.Wait()

	m.unregisterAssociation(assoc)

	m.mu.Lock()
	delete(m.peers, peer)
	m.mu.Unlock()

	// net_processing.cpp FinalizeNode, both halves: the sync slot and every
	// block this peer was downloading go back on offer. This is the single
	// release point for a peer that goes away, however it went away —
	// a stall disconnect, a machine's disconnect error, or a dead socket.
	m.peerGone(syncPeer)

	m.logger.Infof("[svp2p] peer %s done: %v", nc.RemoteAddr(), err)

	return err
}

// peerGone is net_processing.cpp FinalizeNode. A rotation must NOT come here:
// CheckStall's rotate branch has already released the slot and the peer's
// downloads, and the peer stays connected.
//
// This is also the ONLY correct owner of syncPeer.State.requestedTxns'
// TTL-eviction goroutine (fix round 2, Important 3/4): every connection gets
// one (runPeer's own NewSyncPeer call, unconditional on whether sync is
// configured at all), so it must be released on every real disconnect,
// unconditional on m.headerSync too — checked and stopped BEFORE the
// headerSync-nil early return below, not after, which is exactly the bug
// an earlier version of this method had: with no block ingestor injected
// (a depless server, or the bridge dependencies simply not wired yet),
// m.headerSync is nil, every peerGone call returned before reaching
// anything that stopped this goroutine, and every connect/disconnect cycle
// leaked one. clearPeer is NOT the right place either — see that method's
// own doc comment for why (it also runs on a rotation, where the peer
// stays connected).
func (m *PeerManager) peerGone(syncPeer *SyncPeer) {
	m.syncMu.Lock()

	if syncPeer != nil && syncPeer.State != nil && syncPeer.State.requestedTxns != nil {
		syncPeer.State.requestedTxns.Stop()
	}

	if m.headerSync == nil {
		m.syncMu.Unlock()
		return
	}

	m.blockDownloader.PeerDisconnected(syncPeer)
	m.headerSync.PeerDisconnected(syncPeer)
	m.syncMu.Unlock()

	// The slot this peer held is free; hand it to another candidate rather
	// than waiting for one to connect.
	m.electSyncPeer(syncPeer)
}

// peerHandle pairs a connected peer with its CNodeState entry. Everything
// that needs both the registry and the sync state snapshots handles under mu
// first, so the two mutexes are never held together.
type peerHandle struct {
	peer *Peer
	sync *SyncPeer

	// established is net_processing.cpp's fSuccessfullyConnected, which
	// SendMessages tests before it does anything at all for a peer
	// (net_processing.cpp:5835-5837, "Don't send anything until the version
	// handshake is complete"). runPeer puts a peer in the registry BEFORE it runs
	// the handshake, so a pass can reach one that is not ready.
	//
	// It is sampled here because everything a pass needs about a peer is sampled
	// before syncMu is taken. The read is a non-blocking select on a channel the
	// peer loop closes exactly once, so it takes no peer lock and cannot block.
	established bool
}

func (m *PeerManager) peerHandles() []peerHandle {
	m.mu.Lock()
	defer m.mu.Unlock()

	handles := make([]peerHandle, 0, len(m.peers))
	for peer, syncPeer := range m.peers {
		handles = append(handles, peerHandle{peer: peer, sync: syncPeer, established: handshakeComplete(peer)})
	}

	return handles
}

// handshakeComplete reports whether peer finished its version/verack exchange,
// without waiting for it.
func handshakeComplete(peer *Peer) bool {
	select {
	case <-peer.Established():
		return true
	default:
		return false
	}
}

// outgoing is one peer's share of a send pass, collected under the sync-state
// mutex and sent after it is released: Conn.Send can block on a backed-up
// writer, and no lock may be held across it.
type outgoing struct {
	peer *Peer
	msgs []wire.Message
}

func sendAll(out []outgoing) {
	for _, o := range out {
		o.peer.send(o.msgs)
	}
}

// Established is the syncDispatcher half of net_processing.cpp SendMessages'
// initial getheaders: a peer finished its handshake, so consider starting
// header sync with it.
func (m *PeerManager) Established(syncPeer *SyncPeer, services wire.ServiceFlag) []wire.Message {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.headerSync == nil || syncPeer == nil {
		return nil
	}

	// The sync-candidate rules read the services the peer advertised, which
	// are only known once its version message has arrived.
	syncPeer.Services = services

	msgs := m.headerSync.PeerEstablished(syncPeer)
	markGetHeadersOutstanding(syncPeer, msgs)

	return msgs
}

// GetAddrRequest is the outbound half of ProcessVersionMessage's
// `if(!pfrom->fInbound)` block: "Get recent addresses" plus the
// MarkAddressGood that closes it (net_processing.cpp:1867-1872). The OUTBOUND
// side asks its peer for addresses, arms fGetAddr, and marks the peer's own
// address Good in the table; an inbound connection gets none of it, the same
// asymmetry ProcessGetAddrMessage enforces in the other direction.
//
// The Good call is here, and not at the dial site, because this IS that C++
// block: SVNode marks the address good only once the peer has proved it speaks
// the protocol, which is what reaching this method means. Its position after
// the getaddr keeps SVNode's decision order.
//
// Two differences from C++, both forced and both harmless. It is sent at
// verack rather than at version, because this port has no version-time send
// hook (the handshake machine owns that exchange). And SVNode's own condition,
// `fOneShot || nVersion >= CADDR_TIME_VERSION || GetAddressCount() < 1000`,
// collapses to "always": this port has no one-shot connections, and
// MinPeerProtoVersion (31800) already exceeds CADDR_TIME_VERSION (31402), so
// the middle test holds for every peer that completed a handshake. The
// addrman gate replaces it, and it is applied at the wiring point (runPeer
// leaves PeerConfig.Addrs nil when there is no address table), so reaching
// this method at all means there is somewhere to put the reply.
//
// NOT carried from the same C++ block: the self-advertisement immediately
// above it (net_processing.cpp:1847-1864, GetLocalAddress + PushAddress,
// gated on fListen && !IsInitialBlockDownload()). This port advertises its
// local address only in a getaddr REPLY (selectGetAddrResponse's bestLocal,
// which is legacy's own shape). Unsolicited self-advertisement to every
// outbound peer is a separate behavior and is deliberately left out rather
// than half-built.
func (m *PeerManager) GetAddrRequest(syncPeer *SyncPeer, inbound bool, remoteAddr *wire.NetAddress) []wire.Message {
	if inbound || syncPeer == nil {
		return nil
	}

	m.syncMu.Lock()
	syncPeer.fGetAddr = true
	m.syncMu.Unlock()

	// AddrMan.Good takes AddrMan's own lock, so it runs with syncMu released:
	// every AddrMan method is a blocking call (see the addrMan field's note).
	if addrMan := m.addrManager(); addrMan != nil && remoteAddr != nil {
		addrMan.Good(addrFromNetAddr(remoteAddr), time.Now().Unix())
	}

	return []wire.Message{wire.NewMsgGetAddr()}
}

// GetAddr is ProcessGetAddrMessage (net_processing.cpp:4096-4129) — the
// decision itself lives in selectGetAddrResponse (addrrelay.go); this is the
// state and I/O around it. localAddr is the connection's own local address,
// which is what stands in for legacy's addrmgr GetBestLocalAddress
// (services/legacy/peer_server.go:1770): on an inbound connection it is the
// address this peer actually reached us on, which is the best possible answer
// to "how do others find us" for that particular peer.
func (m *PeerManager) GetAddr(syncPeer *SyncPeer, inbound bool, localAddr *wire.NetAddress) []wire.Message {
	if syncPeer == nil {
		return nil
	}

	addrMan := m.addrManager()
	if addrMan == nil {
		return nil
	}

	// AddrMan.GetAddr takes its own lock and must not run under syncMu.
	cached := addrMan.GetAddr()

	now := time.Now().Unix()
	bestLocal := bestLocalAddress(localAddr, wire.SFNodeNetwork, now)

	m.syncMu.Lock()

	send := selectGetAddrResponse(inbound, syncPeer.fSentAddr, cached, bestLocal, syncPeer.addrKnown)

	// fSentAddr is set for a request we ANSWERED, not for one we refused:
	// ProcessGetAddrMessage returns before `pfrom->fSentAddr = true` on both
	// of its refusal paths (net_processing.cpp:4109, :4118), so an ignored
	// getaddr does not consume the connection's one answer.
	if inbound && !syncPeer.fSentAddr {
		syncPeer.fSentAddr = true

		// CNode::PushAddress marks nothing, but SendMessages "will filter it
		// again for knowns that were added after addresses were pushed"
		// (net.h:1242-1244) against the same set; legacy marks explicitly at
		// its own send site (pushAddrMsg's addKnownAddresses,
		// peer_server.go:531). Marking here is that explicit form.
		for _, a := range send {
			syncPeer.addrKnown.mark(a)
		}
	}

	m.syncMu.Unlock()

	msg := addrMessageFor(send)
	if msg == nil {
		return nil
	}

	return []wire.Message{msg}
}

// Addr is ProcessAddrMessage (net_processing.cpp:2270-2368) plus RelayAddress
// (:998-1041) — the decisions live in processAddrEntries and
// selectAddrRelayTargets (addrrelay.go); this is the state, the addrman write
// and the forwarding sends around them.
//
// Ordering matters in one place and is not incidental: the source peer's
// addrKnown is marked BEFORE relay targets are chosen, exactly as C++ marks at
// :2350 and relays at :2355. RelayAddress considers every inbound peer,
// including the sender, and it is CNode::PushAddress's addrKnown test
// (net.h:1245) that keeps the sender from being handed back the address it
// just gave us.
func (m *PeerManager) Addr(syncPeer *SyncPeer, msg *wire.MsgAddr, inbound bool, remoteAddr *wire.NetAddress) (int, error) {
	if syncPeer == nil || msg == nil {
		return 0, nil
	}

	addrMan := m.addrManager()

	// Snapshotted before syncMu, like every other pass that needs both the
	// registry and the sync state (see peerHandles).
	handles := m.peerHandles()

	peerAddr := addrFromNetAddr(remoteAddr)
	now := time.Now().Unix()

	m.syncMu.Lock()

	// `pfrom->fGetAddr.exchange(false)` (net_processing.cpp:2290-2291): read
	// and clear in one step, so a second addr message on the same connection
	// is unsolicited even if the first was not.
	requestedAddr := syncPeer.fGetAddr
	syncPeer.fGetAddr = false

	m.syncMu.Unlock()

	result := processAddrEntries(msg.AddrList, peerAddr, inbound, requestedAddr, now)

	if result.err != nil {
		return result.score, result.err
	}

	// Built BEFORE syncMu is taken, for the same reason RelayBlock snapshots
	// Peer.WantsHeaders first: the package lock order is peer lock then
	// manager lock, so no Peer method may be called while a manager lock is
	// held. SyncPeer.Addr is written once at construction (runPeer) and never
	// again, so reading it here needs no lock either.
	candidates := make([]addrRelayCandidate, 0, len(handles))

	for _, h := range handles {
		if !h.established || h.sync == nil {
			continue
		}

		candidates = append(candidates, addrRelayCandidate{
			peer:    h.peer,
			sync:    h.sync,
			addr:    h.sync.Addr,
			inbound: h.peer.Inbound(),
		})
	}

	m.syncMu.Lock()

	for _, a := range result.known {
		syncPeer.addrKnown.mark(a)
	}

	// Chosen and marked under one syncMu section, so two concurrent addr
	// messages carrying the same address cannot both decide to forward it to
	// the same peer.
	//
	// This does put selectAddrRelayTargets' hashing under syncMu, which is
	// worth pricing rather than glossing: it is one double-SHA256 per
	// candidate per forwarded address, and both factors are bounded — the
	// forwarded list is at most addrForwardBatchMax (10) entries, and an addr
	// message is a rare event compared with the inv and headers traffic that
	// shares this lock. C++ holds its own locks across RelayAddress for the
	// same reason: the pick and the addrKnown marks have to agree.
	type forward struct {
		peer  *Peer
		addrs []Address
	}

	pending := make(map[*Peer]*forward, len(candidates))

	for _, a := range result.forward {
		reachable := newNetAddr(a.IP()).isRoutable()

		for _, tgt := range selectAddrRelayTargets(candidates, a, reachable, now, m.addrRelaySeed) {
			if tgt.sync.addrKnown.has(a) {
				continue
			}

			tgt.sync.addrKnown.mark(a)

			f, ok := pending[tgt.peer]
			if !ok {
				f = &forward{peer: tgt.peer}
				pending[tgt.peer] = f
			}

			f.addrs = append(f.addrs, a)
		}
	}

	m.syncMu.Unlock()

	// Sent with no lock held, like every other send site in this package.
	for _, f := range pending {
		if out := addrMessageFor(f.addrs); out != nil {
			f.peer.send([]wire.Message{out})
		}
	}

	// AddrMan.Add takes its own lock, so it runs outside syncMu. The two-hour
	// penalty is AddNewAddresses' own nTimePenalty (net_processing.cpp:2362).
	if addrMan != nil && len(result.store) > 0 {
		addrMan.AddMany(result.store, sourceIPOf(peerAddr), addrTimePenaltySource)
	}

	return result.score, nil
}

// markGetHeadersOutstanding records, on syncPeer, that a getheaders just left
// for it — Task 11's per-peer solicitation tracking (see the doc comment on
// SyncPeer.getHeadersOutstanding). Established, electLocked, the sync-pass
// sweep, Headers and Inv are the only five places a sync machine's output
// reaches the wire (peer.go dispatchSync's contract that the machines
// themselves perform no I/O, plus the two election/sweep sites that hand a
// grant's getheaders straight to their own outgoing/out slice), which is what
// makes checking their return value here sufficient to cover every
// getheaders-emitting site inside HeaderSync and BlockDownloader without
// either machine having to know about solicitation tracking itself.
//
// It counts every *wire.MsgGetHeaders in msgs rather than stopping at the
// first: BlockDownloader.OnInv (blockdownload.go) can return more than one in
// a single call, one per distinct unknown block hash in the inv, and that
// count is exactly how many replies must be read as solicited.
//
// Requires syncMu, like every other write to sync state; every caller already
// holds it for their whole call.
func markGetHeadersOutstanding(syncPeer *SyncPeer, msgs []wire.Message) {
	if syncPeer == nil {
		return
	}

	for _, out := range msgs {
		if _, ok := out.(*wire.MsgGetHeaders); ok {
			syncPeer.getHeadersOutstanding++
		}
	}
}

// Headers dispatches the NetMsgType::HEADERS event.
func (m *PeerManager) Headers(syncPeer *SyncPeer, msg *wire.MsgHeaders) ([]wire.Message, int, error) {
	// Snapshotted before syncMu is taken: the two mutexes are never held
	// together (see the note on syncMu).
	handles := m.peerHandles()

	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.headerSync == nil {
		return nil, 0, nil
	}

	msgs, score, err := m.headerSync.OnHeaders(syncPeer, msg)
	markGetHeadersOutstanding(syncPeer, msgs)

	// This batch may have grown the index, which is what can resolve another
	// peer's parked announcement. It runs on the error path too: OnHeaders
	// keeps the headers it accepted before the refusal that stopped the batch,
	// exactly as validation.cpp does, so the index may have grown even when
	// this peer is about to be disconnected.
	m.promoteBlockAvailabilityLocked(handles)

	return msgs, score, err
}

// Inv dispatches the NetMsgType::INV event. Its tx half is the PRODUCE side
// of Task 16's tx-inv round trip: BlockDownloader.OnInv collects the tx
// hashes under syncMu, and this method hands them to the Kafka producer
// (TxInvProducer) only after releasing it — a Kafka produce can block, and
// no lock in this package may be held across one (spec §4.3, the same
// collect-under-lock/send-after-unlock shape RelayBlock and RelayTxs use).
func (m *PeerManager) Inv(syncPeer *SyncPeer, msg *wire.MsgInv) ([]wire.Message, error) {
	m.syncMu.Lock()

	// Mark every announced block known to this peer before anything else runs
	// — legacy netsync processInvMsg's unconditional peer.AddKnownInventory
	// (manager.go:2380), ahead of the headers-first check and the
	// haveInventory lookup that follow it there. This is the block
	// announcement relay's (relay.go) "originating peer" signal: whichever
	// peer told us about a hash first is marked here, before RelayBlock ever
	// runs for it, so the relay never re-announces a block back to the peer
	// that announced it. Unconditional on m.blockDownloader below, so it
	// still runs the one time this dispatcher is reachable with sync
	// unconfigured (defensive; in practice Inv is only ever called through
	// syncDispatcher, which peer.go only wires up when SyncEnabled).
	if syncPeer != nil && syncPeer.State != nil {
		for _, inv := range msg.InvList {
			if inv != nil && inv.Type == wire.InvTypeBlock {
				syncPeer.State.knownBlocks.mark(inv.Hash)
			}
		}
	}

	if m.blockDownloader == nil {
		m.syncMu.Unlock()
		return nil, nil
	}

	msgs, txHashes, err := m.blockDownloader.OnInv(syncPeer, msg)
	markGetHeadersOutstanding(syncPeer, msgs)

	producer := m.txInvProducer

	m.syncMu.Unlock()

	if err == nil && producer != nil && syncPeer != nil && len(txHashes) > 0 {
		producer.Produce(syncPeer.Addr, txHashes)
	}

	return msgs, err
}

// InvFromKafka is the CONSUME side of Task 16's tx-inv round trip: legacy's
// kafkaINVListener (netsync/manager.go:3417-3441) plus handleInvMsg/
// processInvMsg's tx branch, entered here once bridge/kafka.go has already
// decoded the Kafka message and applied the RUNNING gate (processInvs,
// manager.go:2270-2280) — that gate needs the blockchain client's FSM state,
// which this package never imports (spec §4.4), so it is checked by the
// caller before this method is ever reached. Headers-first suppression is
// checked here instead (BlockDownloader.RequestTxs), because it is this
// package's own state.
//
// peerAddr identifies the peer the original inv came from, exactly as
// carried on the wire in KafkaInvTopicMessage.PeerAddress. A peerAddr that
// matches no currently-connected peer is a departed peer — dropped with a
// log, never treated as an error, mirroring legacy's own
// newInvFromKafkaMessage "peer could not be found in peer list" path
// (netsync/inv_msg.go:129-131), which kafkaINVListener logs and discards
// rather than retrying (netsync/manager.go:3428-3431).
func (m *PeerManager) InvFromKafka(peerAddr string, hashes []chainhash.Hash) {
	if len(hashes) == 0 {
		return
	}

	// Snapshotted OUTSIDE syncMu, matching RelayBlock/RelayTxs's own
	// collect-under-mu-first shape (peerHandles takes only mu, never syncMu).
	handles := m.peerHandles()

	var target *peerHandle

	for i := range handles {
		if handles[i].sync != nil && handles[i].sync.Addr == peerAddr {
			target = &handles[i]
			break
		}
	}

	if target == nil {
		m.logger.Debugf("[svp2p] dropping inv-from-kafka for departed peer %s", peerAddr)
		return
	}

	m.syncMu.Lock()

	if m.blockDownloader == nil {
		m.syncMu.Unlock()
		return
	}

	var rejected func(chainhash.Hash) bool
	if m.txIngestor != nil {
		rejected = m.txIngestor.Rejected
	}

	gdmsg := m.blockDownloader.RequestTxs(target.sync, hashes, rejected)

	m.syncMu.Unlock()

	if gdmsg != nil {
		target.peer.send([]wire.Message{gdmsg})
	}
}

// markTxKnown records hash as known to syncPeer — the tx-side counterpart
// of Inv's unconditional knownBlocks.mark above (manager.go:800-806,
// itself citing legacy's processInvMsg AddKnownInventory): whichever peer
// delivers a tx to us must never be re-offered it by RelayTxs
// (relay.go selectTxRelayTargets reads knownTxs). Legacy's own equivalent,
// OnTx's AddKnownInventory call, runs BEFORE QueueTx
// (services/legacy/peer_server.go:906-908) — this is called from
// Peer.dispatchTx before queueTx for the same reason (review round 1,
// Important 2).
//
// Unlike Inv's marking, this needs no headerSync/blockDownloader state —
// RelayTxs itself has none either (it iterates m.peers directly) — so it
// takes syncMu on its own rather than being folded into the syncDispatcher
// interface, which exists only where cfg.Sync is configured. This must
// work whether or not it is: a tx-ingestion-only deployment (TxIngestor
// set, Ingestor/Sync not) still runs RelayTxs for every connected peer.
func (m *PeerManager) markTxKnown(syncPeer *SyncPeer, hash chainhash.Hash) {
	if syncPeer == nil || syncPeer.State == nil {
		return
	}

	m.syncMu.Lock()
	syncPeer.State.knownTxs.mark(hash)
	m.syncMu.Unlock()
}

// GetHeaders dispatches the NetMsgType::GETHEADERS event. The refusal is
// logged here rather than in the machine, which holds no logger, and the log
// line is the one the legacy service wrote (services/legacy/peer_server.go:1585).
func (m *PeerManager) GetHeaders(syncPeer *SyncPeer, msg *wire.MsgGetHeaders) []wire.Message {
	m.syncMu.Lock()

	if m.serving == nil || syncPeer == nil {
		m.syncMu.Unlock()
		return nil
	}

	out, refused := m.serving.OnGetHeaders(syncPeer, m.activeTip, msg)

	m.syncMu.Unlock()

	// Logged with no lock held: the package lock order forbids reaching a peer
	// while a manager lock is taken, and a logger sink is outside this package.
	if refused {
		m.logger.Debugf("[svp2p] ignoring getheaders from %s: node is syncing in headers-first mode", syncPeer.Addr)
	}

	return out
}

// GetBlocks dispatches the NetMsgType::GETBLOCKS event.
func (m *PeerManager) GetBlocks(syncPeer *SyncPeer, msg *wire.MsgGetBlocks) []wire.Message {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.serving == nil || syncPeer == nil {
		return nil
	}

	return m.serving.OnGetBlocks(syncPeer, m.activeTip, msg)
}

// GetData dispatches the NetMsgType::GETDATA event. It only classifies the
// request; the fetches and the sends run on the peer's serve goroutine, with
// no lock held (getdata.go serveGetData).
func (m *PeerManager) GetData(syncPeer *SyncPeer, msg *wire.MsgGetData) []getDataItem {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.serving == nil || syncPeer == nil {
		return nil
	}

	return m.serving.OnGetData(msg)
}

// ContinueInv dispatches the getdata continuation, after a block has gone out.
func (m *PeerManager) ContinueInv(syncPeer *SyncPeer, hash chainhash.Hash) []wire.Message {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.serving == nil || syncPeer == nil {
		return nil
	}

	return m.serving.ContinueInv(syncPeer, m.activeTip, hash)
}

// BlockExpected reports whether hash is in flight from this peer, which is
// what makes an inbound block solicited.
func (m *PeerManager) BlockExpected(syncPeer *SyncPeer, hash chainhash.Hash) bool {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.blockDownloader == nil {
		return false
	}

	return m.blockDownloader.IsInFlightFrom(syncPeer, hash)
}

// BlockDone reports one block's ingest outcome to the download scheduler:
// in-flight is cleared, the peer's progress clock is fed, and a pre-admission
// timeout rotates the sync peer. A non-nil error return means the peer must be
// disconnected, which the caller does with no lock held; the int is the
// misbehavior delta the outcome earned, which the caller applies FIRST (see
// Peer.Run's ingest-report case).
//
// syncPeer may be nil, which is how an ingest no peer answers for reports its
// outcome: the retained-block spool replays a block long after the peer that
// delivered it was released, and that replay's failure still has to reach the
// scheduler or the block stays recorded as held and is never fetched again.
func (m *PeerManager) BlockDone(syncPeer *SyncPeer, hash chainhash.Hash, outcome IngestOutcome) (int, error) {
	now := time.Now().UnixMicro()

	var (
		rotate      bool
		delta       int
		disconnect  error
		streamFault bool
	)

	m.syncMu.Lock()

	// This peer delivered the block bytes, whatever the ingest outcome —
	// legacy peer_server.go OnBlock marks the sending peer's known inventory
	// before validation even runs, unconditionally (peer_server.go:997-1001:
	// "Add the block to the known inventory for the peer"). The relay
	// (relay.go, via Inv's comment above) never re-announces this hash to
	// whoever just gave it to us.
	if syncPeer != nil && syncPeer.State != nil {
		syncPeer.State.knownBlocks.mark(hash)
	}

	// The partial block this ingest came from, released here because this is
	// the single completion path for both success and failure. compactSeen is
	// false for every ordinary block, which is almost all of them.
	compactStatus, compactSeen := takeCompactStatus(syncPeer, hash)

	if m.blockDownloader == nil {
		m.syncMu.Unlock()
		return 0, nil
	}

	switch {
	case outcome.Duplicate:
		// Another copy of this hash won the admission race and owns the
		// have-data record, so BlockFailed must not be used here — it would
		// delete a record that copy may already have written. What must go is
		// THIS peer's own in-flight claim: without it a block whose holder
		// changed under a rotation stays in flight for ever and is never
		// re-offered. BlockNotDelivered is guarded, so it does nothing when
		// the holder is someone else.
		m.blockDownloader.BlockNotDelivered(syncPeer, hash, now)

		m.logger.Debugf("[svp2p] block %s was already in flight: %v", hash, outcome.Err)

	case outcome.Err == nil:
		m.blockDownloader.BlockReceived(syncPeer, hash, now)

	case outcome.Retained:
		// The bytes are spooled by the ingestor (orphanBlocks); for the
		// scheduler this download is complete and must not be re-requested.
		m.blockDownloader.BlockReceived(syncPeer, hash, now)

		m.logger.Debugf("[svp2p] block %s retained until its parent lands", hash)

	case outcome.ParentMissing:
		// The block is wanted and the peer delivered it; our own chain is just
		// not ready for it. Release the claim, hold the block back from the next
		// walk, and refresh the peer's stall clock as any other local fault
		// does. Deliberately NOT the default branch below: nothing here is a
		// rotation, a disconnect or a warning.
		m.blockDownloader.BlockParentMissing(syncPeer, hash, now)

		if syncPeer != nil && syncPeer.State != nil {
			syncPeer.State.nLastProgressTime = now
		}

		// Debug, not Warn. This is an expected consequence of asking several
		// peers for blocks at once, and at Warn it buried the ingest failures
		// that are not expected: 476 of these against 36 real ingests on the
		// two-peer integration leg.
		m.logger.Debugf("[svp2p] block %s is waiting for its parent: %v", hash, outcome.Err)

	default:
		// The compact override below applies to THIS branch only: see the note
		// where it runs.
		streamFault = true

		if outcome.RetryAfter > 0 {
			// Our own fault, with a backoff window: keep the block off the wire
			// for that long rather than letting the next tick re-request the
			// same bytes from the same peer.
			m.blockDownloader.BlockDeferred(syncPeer, hash, now, now+micros64(outcome.RetryAfter))
		} else {
			// The block is back on offer to any peer, including this one.
			m.blockDownloader.BlockFailed(syncPeer, hash, now)
		}

		if outcome.TransientLocal && syncPeer != nil && syncPeer.State != nil {
			// Admission.SkipForBackoff's caller contract: our own store
			// faulted, and the peer did deliver, so refresh its stall clock
			// instead of rotating it. Legacy netsync manager.go:1668-1670
			// refreshed lastBlockTime on exactly this path.
			syncPeer.State.nLastProgressTime = now
		}

		// A rotation acts on the peer that stranded the block: it releases that
		// peer's slot and its downloads, and elects a replacement excluding it.
		// A report with no peer has none of that to do — PeerDisconnected and
		// SyncPeerTimedOut are nil-guarded no-ops, and electSyncPeer(nil) would
		// re-run an election excluding nobody while logging a rotation that did
		// not happen.
		rotate = outcome.Rotate && syncPeer != nil

		// services/legacy/peer_server.go shouldDisconnectOnBlockErr: a block
		// the pipeline rejected is the peer's fault and the peer goes, so the
		// sync peer actually rotates instead of the same bad block being
		// re-offered to the same peer for ever.
		//
		// Read from PeerFault alone, never from the absence of
		// TransientLocal. A rotation is a LOCAL fault that has already
		// released the slot and this peer's downloads; disconnecting it as
		// well would drive that release a second time through peerGone, which
		// is exactly the double-finalize the rotation contract forbids. An
		// unclassified failure disconnects nobody either — the block goes back
		// on offer and the stall rules deal with a peer that keeps failing.
		if outcome.PeerFault {
			// The reject is recorded against the peer as well as acted on.
			// SVNode does both from one place: BlockDownloadTracker::BlockChecked
			// (net/block_download_tracker.cpp:113-127) reads the DoS level out
			// of the validation state and calls Misbehaving(node, nDoS,
			// state.GetRejectReason()) for the node that sourced the block.
			// Without the score a peer that reconnects arrives with a clean
			// counter, so a peer feeding invalid blocks pays one reconnect per
			// block instead of accumulating towards a ban.
			delta = scoreInvalidBlock
			disconnect = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS,
				"svp2p: block %s was rejected", hash, outcome.Err)
		}

		m.logger.Warnf("[svp2p] block %s ingest failed: %v", hash, outcome.Err)
	}

	// A compact block's reconstruction can only fail WHILE the assembled
	// stream is read, because that is where the short IDs of the gap
	// transactions are checked and where the index is opened (compactblock.go
	// fill's own doc comment). The ingestor therefore reports it as a stream
	// error, which the default branch above has already treated as nobody's
	// fault; the partial block's own status is what separates the two cases
	// the branch cannot tell apart.
	//
	// It is gated on streamFault — the switch's own default branch — rather
	// than on outcome.Err alone. Duplicate, Retained and ParentMissing all
	// carry an error too, and every one of them is decided BEFORE the stream is
	// read, so a compact status cannot be their cause; overriding them here
	// would disconnect a peer from a branch whose own comment says "nothing
	// here is a rotation, a disconnect or a warning".
	//
	// The override REPLACES the default branch's verdict on the peer; it does
	// not merely add to it. The default branch reads outcome.PeerFault, which
	// is the pipeline's verdict on the BYTES it was handed, and on a compact
	// ingest those bytes were assembled by this node — from its own index,
	// against its own short IDs — so PeerFault cannot stand as a verdict on the
	// peer without the partial block's status to interpret it.
	if compactSeen && streamFault && outcome.Err != nil {
		switch compactStatus {
		case readInvalid:
			// The peer supplied a transaction that is not the one the slot
			// asked for, or bytes that do not decode to the count it declared.
			// net_processing.cpp:3610-3616 scores exactly this 100.
			delta = scoreInvalidBlock
			disconnect = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS,
				"svp2p: compact block %s was filled with invalid transactions", hash, outcome.Err)

		case readFailed:
			// net_processing.cpp:3618-3623, "Might have collided, fall back to
			// getdata now" — a short ID is 48 bits, so an honest peer's
			// transaction can hash onto the slot we asked about, and the index
			// entry we matched may be bytes we no longer hold. SVNode has no
			// Misbehaving call on this path at all.
			//
			// delta and disconnect are CLEARED, not left alone: the default
			// branch above has already set both from outcome.PeerFault, and
			// without this a collision costs an honest peer the same 100 and
			// the same disconnect as a malicious fill — which also strands the
			// block, since the peer that announced it is the only one known to
			// hold it. BlockFailed has already put the block back on offer.
			//
			// :3618-3623 pushes a getdata to pfrom right here. This port
			// releases the block instead and lets the next syncPass walk
			// re-offer it, the same choice CompactBlock's own readFailed
			// branch makes and for the same reason: the scheduler owns
			// getdata. Same end state one TickInterval later, and any peer
			// known to hold the block may serve it, not only the one whose
			// reply collided.
			delta = 0
			disconnect = nil

			m.logger.Debugf("[svp2p] compact block for %s unreconstructable, falling back to getdata", hash)

		case readOK:
		}
	}

	if rotate {
		// A wedged local round-trip stranded a requested block. This is the
		// same release pair CheckStall's rotate branch performs: the peer
		// stays connected, only the slot and its downloads go.
		m.blockDownloader.PeerDisconnected(syncPeer)
		m.headerSync.SyncPeerTimedOut(syncPeer)
	}

	m.syncMu.Unlock()

	if rotate {
		m.logger.Warnf("[svp2p] rotating the sync peer after a pre-admission timeout on block %s", hash)
		m.electSyncPeer(syncPeer)
	}

	return delta, disconnect
}

// RelayBlock is the block announcement relay's entry point: hash just
// reached finality on our own chain (bridge/kafka.go's blocks-final
// consumer), so tell every peer about it, by headers or by plain inv per
// selectRelayTargets (relay.go), unless a peer already knows.
//
// header carries the block's own fields for the headers branch; hash is
// passed alongside it rather than recomputed, because the caller already
// has it from the Kafka message key and BlockHash() is not free.
//
// Unlike RelayBlock's SVNode ancestor (SendBlockHeaders, called from every
// SendMessages pass over every peer) this runs once per finality event
// rather than on a timer, because this port has no per-peer send loop to
// piggyback on (spec §4.3 divergence, same shape as
// promoteBlockAvailabilityLocked's).
func (m *PeerManager) RelayBlock(hash chainhash.Hash, header *wire.BlockHeader) {
	if header == nil {
		return
	}

	// Snapshotted and read OUTSIDE every lock this package holds:
	// Peer.WantsHeaders takes the peer lock, and the package's lock order
	// forbids taking a peer lock while a manager lock is held (see the note
	// on syncMu). peerHandles itself only ever holds mu, never syncMu, so
	// this call is safe before syncMu is taken below.
	handles := m.peerHandles()

	type target struct {
		peer         *Peer
		sync         *SyncPeer
		wantsHeaders bool
	}

	targets := make([]target, 0, len(handles))

	for _, h := range handles {
		if !h.established || h.sync == nil || h.sync.State == nil {
			continue
		}

		targets = append(targets, target{peer: h.peer, sync: h.sync, wantsHeaders: h.peer.WantsHeaders()})
	}

	m.syncMu.Lock()

	// hasParent is the same test as hasBlock, run against the block's PARENT
	// instead of the block itself — SendBlockHeaders' per-hash connectivity
	// check (net_processing.cpp:5301-5307): a peer missing the parent cannot
	// place the header we would send. The zero PrevBlock is genesis, which
	// SVNode's own `pindex->IsGenesis() ||` short-circuits the same way.
	genesisParent := header.PrevBlock == (chainhash.Hash{})

	candidates := make([]relayCandidate, len(targets))
	for i, tgt := range targets {
		state := tgt.sync.State

		candidates[i] = relayCandidate{
			peer:         tgt.peer,
			wantsHeaders: tgt.wantsHeaders,
			hasBlock:     state.knownBlocks.has(hash) || peerHasHeader(m.headerIndex, state, hash),
			hasParent:    genesisParent || peerHasHeader(m.headerIndex, state, header.PrevBlock),
		}
	}

	decisions := selectRelayTargets(candidates, hash, header)

	// Marked while syncMu is still held, in the same pass that decided to
	// send: a peer that gets an announcement here must never get the same
	// one again from a later RelayBlock call for the same hash (a Kafka
	// replay, or a fast reorg back onto a block already relayed).
	//
	// A headers decision also resets pindexBestHeaderSent to this block,
	// net_processing.cpp SendBlockHeaders' own write right after it pushes
	// the plain HEADERS message (net_processing.cpp:5372-5373 — the write at
	// :5357 belongs to the sibling compact-block branch this port does not
	// take, Phase 4; verified by grepping every pindexBestHeaderSent site in
	// the file rather than trusting one remembered line number, fix round 2,
	// review Minor 1): a relay announcement is exactly as much "telling the
	// peer about a header" as a getheaders reply is (peerHasHeader's other
	// reader, Serving.OnGetHeaders, writes the same field at its own site,
	// net_processing.cpp:3044).
	//
	// This write is a THIRD defect, not part of I1/I2's root cause (fix
	// round 2, review Minor 2 correction) — I1 was the dropped
	// pindexBestKnownBlock branch and I2 was the absent parent test; both
	// are read-side. Before this write existed, pindexBestHeaderSent was
	// only ever READ to suppress a headers send (peerHasHeader), so its
	// never advancing just meant "keep sending headers" — never a defect on
	// its own. It only becomes one once I2's parent test needs it to see a
	// PRIOR relay's headers send when deciding the NEXT block's hasParent;
	// this write is what I2's fix depends on to have any effect across
	// repeated calls, not what I1/I2 were caused by.
	// Guarded the same way every other syncMu-held m.headerIndex reader in
	// this file is (manager.go AddHeaders, SetActiveTip, startingHeight):
	// latent today (ConfigureSync rejects a nil index and runs before the
	// consumer starts, so RelayBlock cannot be reached with one), but this
	// file does not rely on that elsewhere and should not start here (fix
	// round 2, review Minor 5).
	var (
		node   HeaderNode
		nodeOK bool
	)

	if m.headerIndex != nil {
		node, nodeOK = m.headerIndex.Lookup(hash)
	}

	// nodeOK can be false here in a real, if narrow, race: the blockchain
	// service's blocks-final Kafka message and its SendNotification travel
	// by different transports with no ordering guarantee between them
	// (services/blockchain/Server.go:1097 vs :1102), so a blocks-final event
	// can in principle reach RelayBlock before this node's own header index
	// subscription has indexed the hash the service just finalized. When
	// that happens the write below is silently skipped rather than merely
	// deferred: pindexBestHeaderSent does not advance for this block, so
	// hasParent for the NEXT block falls back to false and that peer drops
	// to inv until something else (a getheaders round trip) catches it back
	// up. Logged at Debug rather than left silent so the degradation is
	// observable; fixing the cross-service ordering itself is a residual
	// outside this task (fix round 2, review Minor 3).
	if !nodeOK {
		m.logger.Debugf("[svp2p] relay: block %s not yet in the header index; pindexBestHeaderSent will not advance for it", hash)
	}

	for _, tgt := range targets {
		for _, d := range decisions {
			if d.peer != tgt.peer {
				continue
			}

			tgt.sync.State.knownBlocks.mark(hash)

			if nodeOK {
				if _, isHeaders := d.msg.(*wire.MsgHeaders); isHeaders {
					tgt.sync.State.pindexBestHeaderSent = &node
				}
			}

			break
		}
	}

	m.syncMu.Unlock()

	// Sent with no lock held, matching every other send site in this package
	// (sendAll, electLocked's callers): Conn.Send can block on a backed-up
	// writer.
	for _, d := range decisions {
		d.peer.send([]wire.Message{d.msg})
	}
}

// RelayTxs is the tx announcement relay's entry point: Task 13's batcher
// (services/svp2p/txrelay.go) flushes here once per batch window, and every
// entry gets the per-peer relayDisabled/feefilter/dedup gate
// (relay.go selectTxRelayTargets) before anything is sent.
//
// Unlike RelayBlock, which relays ONE hash per call, this relays a BATCH —
// net_processing.cpp SendTxnInventory's own shape (:5464, "batched tx invs
// per peer"): every tx in txs that a given peer should receive is
// accumulated into that peer's own single wire.MsgInv, so a peer with many
// newly-relayed txs in one flush window gets one INV message carrying all
// of them (bounded at wire.MaxInvPerMsg — see the AddInvVect error handling
// below), not one INV per tx.
//
// Same shape as RelayBlock (spec §4.3, and Task 12's own review finding on
// RelayBlock: collect under syncMu, send after unlocking) for the same
// reason: Conn.Send can block on a backed-up writer, and no lock in this
// package may be held across a blocking call.
func (m *PeerManager) RelayTxs(txs []TxHashAndFee) {
	if len(txs) == 0 {
		return
	}

	// Snapshotted OUTSIDE every lock this package holds, exactly like
	// RelayBlock's own targets pass: Peer.RelayTxDisabled and Peer.FeeFilter
	// both take the peer lock, and the package's lock order forbids taking a
	// peer lock while a manager lock is held (see the note on syncMu).
	handles := m.peerHandles()

	type target struct {
		peer          *Peer
		sync          *SyncPeer
		relayDisabled bool
		feeFilter     int64
	}

	targets := make([]target, 0, len(handles))
	// byPeer mirrors targets, keyed for O(1) lookup: selectTxRelayTargets
	// returns the chosen *Peer values directly, and this is what turns
	// "which target did this decision come from" into a map lookup instead
	// of an O(len(targets)) scan repeated for every decision of every tx in
	// the batch — a batch can hold up to wire.MaxInvPerMsg (50,000) txs, so
	// an O(targets) scan per decision would make this whole loop
	// O(txs*targets^2) rather than O(txs*targets).
	byPeer := make(map[*Peer]target, len(handles))

	for _, h := range handles {
		if !h.established || h.sync == nil || h.sync.State == nil {
			continue
		}

		tgt := target{
			peer:          h.peer,
			sync:          h.sync,
			relayDisabled: h.peer.RelayTxDisabled(),
			feeFilter:     h.peer.FeeFilter(),
		}

		targets = append(targets, tgt)
		byPeer[h.peer] = tgt
	}

	invs := make(map[*Peer]*wire.MsgInv, len(targets))

	// candidates is hoisted out of the tx loop and reused for every tx in
	// the batch, rather than a fresh slice per tx (review round 1, Important
	// 1): only the `known` field actually varies per tx, so every other
	// field is set once here and left alone. A flush can carry up to
	// wire.MaxInvPerMsg (50,000) txs; a fresh len(targets)-sized allocation
	// per tx was needless churn on a path that runs once per second.
	candidates := make([]txRelayCandidate, len(targets))
	for i, tgt := range targets {
		candidates[i] = txRelayCandidate{
			peer:          tgt.peer,
			relayDisabled: tgt.relayDisabled,
			feeFilter:     tgt.feeFilter,
		}
	}

	for _, tx := range txs {
		// syncMu is taken and released PER TX, not once for the whole batch
		// (review round 1, Important 1): the lock also serialises header
		// sync, block download scheduling, and RelayBlock, and a 50,000-tx
		// flush holding it continuously would park all of that for the
		// length of one flush. Releasing between txs lets any of those
		// interleave. Nothing here blocks (map/slice operations and one
		// AddInvVect call, all pure CPU), so per-tx lock/unlock overhead is
		// the only added cost, and it is negligible next to what it buys.
		m.syncMu.Lock()

		for i := range candidates {
			candidates[i].known = targets[i].sync.State.knownTxs.has(tx.TxHash)
		}

		decisions := selectTxRelayTargets(candidates, tx)

		for _, p := range decisions {
			tgt, ok := byPeer[p]
			if !ok {
				// Cannot happen: decisions is built from candidates, which
				// is built from targets, so every decision peer has a
				// byPeer entry. Guarded rather than indexed unconditionally
				// in case that invariant ever breaks.
				continue
			}

			msg, ok := invs[tgt.peer]
			if !ok {
				msg = wire.NewMsgInv()
				invs[tgt.peer] = msg
			}

			if err := msg.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &tx.TxHash)); err != nil {
				// Only reachable once a single peer's accumulated batch
				// already hit wire.MaxInvPerMsg (50,000) in one flush
				// window — the batcher's own size cap (maxRequestedTxns,
				// txrelay.go) already bounds txs itself to that same
				// number, so this cannot happen today; kept rather than
				// silently dropped in case that relationship changes. Not
				// marked known below: net_processing.cpp's own equivalent
				// flushes and continues rather than dropping
				// (SendTxnInventory, :5476-5480), so a tx that fails to
				// queue here must stay eligible for a later relay attempt,
				// not be permanently suppressed for this peer (review
				// round 1, Minor 4).
				m.logger.Warnf("[svp2p] relay: dropping tx %s for a peer already at the max inv batch size: %v", tx.TxHash, err)
				continue
			}

			// Marked only after AddInvVect actually succeeded, and still
			// under the same syncMu acquisition that read `known` for this
			// tx — the same reasoning as RelayBlock's own knownBlocks.mark
			// call: a peer that gets this tx here must never get it again
			// from a later RelayTxs call for the same hash (a Kafka replay,
			// or the same hash surviving past the batcher's own 1-minute
			// dedup window — see peerSyncState.knownTxs's doc comment).
			tgt.sync.State.knownTxs.mark(tx.TxHash)
		}

		m.syncMu.Unlock()
	}

	// Sent with no lock held, matching RelayBlock's own send pass.
	out := make([]outgoing, 0, len(invs))

	for peer, msg := range invs {
		if len(msg.InvList) == 0 {
			continue
		}

		out = append(out, outgoing{peer: peer, msgs: []wire.Message{msg}})
	}

	sendAll(out)
}

// electSyncPeer offers the free sync slot to the first eligible candidate,
// skipping exclude (the peer that just lost it). HeaderSync.PeerEstablished
// is the election: it returns the initial getheaders only for a peer that may
// take the slot. Map iteration makes "first" arbitrary, which matches
// net_processing.cpp taking the first eligible candidate that reaches
// SendMessages rather than ranking them.
func (m *PeerManager) electSyncPeer(exclude *SyncPeer) {
	handles := m.peerHandles()

	var out []outgoing

	m.syncMu.Lock()

	if m.headerSync != nil {
		out = m.electLocked(handles, exclude)
	}

	m.syncMu.Unlock()

	sendAll(out)
}

// electLocked runs the election under the sync-state mutex. It prefers a peer
// other than exclude, and falls back to exclude itself when no one else is
// eligible: legacy netsync updateSyncPeer re-runs startSync over every peer,
// the freed one included, so a single-peer node keeps syncing rather than
// stopping for ever. PeerEstablished leaves a peer it refuses untouched, so
// the second pass costs only the refusals of the first.
//
// It answers the two EVENT paths only — peerGone and BlockDone's rotation —
// which have no SVNode counterpart at all: SendMessages simply reaches the peers
// again on its next pass. Both fill the freed slot without waiting for a tick,
// so they are kept. syncPass does NOT use it: the per-pass eligibility sweep
// there is the port of SendBlockSync, and it offers the slot to every eligible
// peer rather than to the first one an election reaches.
func (m *PeerManager) electLocked(handles []peerHandle, exclude *SyncPeer) []outgoing {
	for _, allowExcluded := range []bool{false, true} {
		for _, h := range handles {
			// The same guard syncPass carries, and for the same reason: this
			// election reads the connection registry, which holds a peer from
			// before its handshake runs, and net_processing.cpp SendMessages does
			// nothing at all for such a peer (:5835-5837). Both event paths that
			// reach here — peerGone and BlockDone's rotation — could otherwise
			// give it the sync slot and a getheaders before its verack, wherever
			// isSyncCandidate does not refuse it on the services flag (see the
			// regtest branch of headersync.go isSyncCandidate).
			if !h.established {
				continue
			}

			if !allowExcluded && h.sync == exclude {
				continue
			}

			if msgs := m.headerSync.PeerEstablished(h.sync); len(msgs) > 0 {
				markGetHeadersOutstanding(h.sync, msgs)

				return []outgoing{{peer: h.peer, msgs: msgs}}
			}
		}

		if exclude == nil {
			break
		}
	}

	return nil
}

// syncLoop drives the two passes the machines are deliberately clockless for:
// net_processing.cpp SendMessages' block-download pass and its stall checks.
// Time enters the machines only here, as a parameter.
func (m *PeerManager) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(m.syncTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.quit:
			return
		case <-ticker.C:
			m.syncTickOnce()
		}
	}
}

// stallDisconnect is one peer a stall check condemned, carried with the action
// that condemned it. The action travels with the peer because the two
// DetectStalling clauses disconnect for DIFFERENT reasons — the download window
// held shut, versus one block owed too long — and an operator reading the logs
// has to be able to tell them apart. Until Task 25 both logged the single line
// "stalling block download". SVNode keeps them apart with two distinct log
// lines (net_processing.cpp:5620 and :5650); StallAction.DisconnectReason is
// the one place this port's two texts live.
type stallDisconnect struct {
	peer   *Peer
	action StallAction
}

func (m *PeerManager) syncTickOnce() {
	handles := m.peerHandles()
	now := time.Now().UnixMicro()

	// Sampled before the sync-state mutex is taken: IngestSnapshot takes the
	// PEER lock, and the package lock order forbids reaching for a peer lock
	// while holding a manager one.
	ingests := make([]IngestSnapshot, len(handles))
	for i, h := range handles {
		ingests[i] = h.peer.IngestSnapshot()
	}

	out, disconnect := m.syncPass(handles, ingests, now)

	for _, d := range disconnect {
		reason := d.action.DisconnectReason()

		m.logger.Warnf("[svp2p] disconnecting %s: %s", d.peer.Info().Addr, reason)
		d.peer.Disconnect(reason)
	}

	sendAll(out)
}

// syncPass is the locked half of one tick: the header-sync eligibility sweep,
// the stall check and the block-download pass, run for every peer in that order.
// It RETURNS the work instead of doing it, because neither Conn.Send nor
// Peer.Disconnect may run under syncMu — both can block on a peer. That is the
// collect-then-act shape every sync path here keeps; syncTickOnce acts on the
// two lists after the unlock.
//
// THE CALLER MUST NOT HOLD syncMu. This method takes it itself, unlike the
// *Locked helpers in this file, and syncMu is not reentrant. That cuts against
// the convention here, so any test or caller that seeds sync state under the
// lock must release it before calling this.
//
// ingests must be index-aligned with handles, and is sampled by the caller
// before syncMu is taken: IngestSnapshot takes the PEER lock, and the package
// lock order forbids reaching for a peer lock while holding a manager one.
func (m *PeerManager) syncPass(handles []peerHandle, ingests []IngestSnapshot, now int64) (out []outgoing, disconnect []stallDisconnect) {
	m.syncMu.Lock()

	if m.blockDownloader == nil {
		m.syncMu.Unlock()
		return nil, nil
	}

	activeTip := m.activeTip

	for i, h := range handles {
		// net_processing.cpp SendMessages does nothing at all for a peer whose
		// handshake has not finished (:5835-5837), and the sweep below is why that
		// guard has to be carried. Outside regtest a handshaking peer has
		// advertised no services yet, so isSyncCandidate refuses it on its own.
		// The regtest branch refuses far less: it reads the address alone, and
		// nothing whatsoever once AllowSyncCandidateFromLocalPeers is set. So
		// without this, a peer could take the sync slot — and be sent a
		// getheaders — before its verack. It is also what peer.go dispatchSync
		// states for the inbound direction: nothing pre-handshake may reach the
		// sync machines. electLocked carries the same guard for the event paths.
		if !h.established {
			continue
		}

		// THE ORDER OF THE THREE PASSES BELOW IS net_processing.cpp
		// SendMessages' own (net_processing.cpp:5829-5897), which runs them for
		// one peer at a time: SendBlockSync at :5865, DetectStalling at :5881,
		// and SendGetDataBlocks at :5888. The eligibility sweep therefore comes
		// FIRST, ahead of both the stall check and the getdata pass for the same
		// peer.
		//
		// That order is what makes the rotate branch below hold without a second
		// exclusion rule: a peer that rotates on this pass had its sweep before
		// the rotation, and nothing sweeps it again, so it cannot take the slot
		// back on the pass that took it away.
		//
		// The sweep is the per-pass half of net_processing.cpp SendBlockSync
		// (net_processing.cpp:5180-5222). PeerEstablished was previously reached
		// only from PeerManager.Established (the handshake) and from electLocked
		// (a rotation or a peerGone), so a peer that became eligible later never
		// started header sync — see the note at PeerEstablished for what that
		// cost past the final checkpoint. Its per-tick cost is one bool read for a
		// peer that already holds the slot; for any other peer one
		// isSyncCandidate, plus — while somebody else holds the slot outside a
		// headers-first round — one header-index tip read, one lookup and one
		// clock read, which is the near-tip test.
		//
		// WHAT THE SWEEP COSTS IN STEADY STATE, past the final checkpoint, because
		// it is not free and it is a divergence from SVNode. The 24 hour near-tip
		// relaxation admits every eligible peer there, headersFirstMode is off so
		// CheckStall's header-progress refresh is unreachable, and
		// nLastProgressTime moves only on a delivered block. Mainnet blocks are
		// about ten minutes apart against a 180 second rotation window, so most
		// windows deliver nothing and EVERY header peer rotates, then the next
		// tick's sweep re-admits all of them — for ever. Each cycle costs one
		// getheaders per peer, one clearPeer per peer, which nils
		// pindexLastCommonBlock and so buys a fresh lastCommonAncestor walk on the
		// next download pass, and one resetHeaderState. SVNode pays none of it: it
		// has no sync-peer rotation at all, and keeps a sync peer until it
		// disconnects or its blocks time out. The rotation is the licensed legacy
		// netsync deviation documented at CheckStall; the churn is what it costs
		// once every eligible peer is swept rather than one being elected.
		//
		// Nothing here blocks, which is what lets it run under syncMu: any
		// getheaders it produces joins out and is sent by syncTickOnce after the
		// unlock, the collect-then-act shape this file keeps throughout.
		msgs := m.headerSync.PeerEstablished(h.sync)
		markGetHeadersOutstanding(h.sync, msgs)

		switch action := m.blockDownloader.CheckStall(h.sync, ingests[i], now); action {
		case StallActionDisconnect, StallActionDisconnectTimeout:
			// The caller disconnects; runPeer then drives FinalizeNode, which is
			// what releases the slot and the peer's downloads. CheckStall itself
			// mutated nothing on this branch.
			//
			// THE SWEEP ABOVE MAY HAVE, THOUGH, and this is the hazard the source
			// pass order carries. A peer holding no slot but heading the download
			// window is granted the slot by the sweep and disconnected by this
			// clause on the SAME pass, so as the first sync peer it has just
			// seeded fSyncStarted, nSyncStarted, nextCheckpoint, headersFirstMode
			// and roundAnchorHeight — for a peer that is about to go. While it
			// holds that slot every peer after it on this pass is refused, because
			// PeerEstablished's single-slot guard reads the mode as it stands.
			//
			// The order is not ours to change: SVNode runs SendBlockSync (:5865)
			// before DetectStalling (:5881) and carries the same hazard.
			//
			// WHAT MAKES IT SAFE IS THE UNWIND, and the whole of it runs:
			// Peer.Disconnect closes the connection, so Peer.Run returns and
			// runPeer reaches peerGone unconditionally. peerGone calls
			// BlockDownloader.PeerDisconnected, which releases the peer's
			// downloads, and HeaderSync.PeerDisconnected, which reaches
			// releaseSyncPeer. That clears fSyncStarted, decrements nSyncStarted
			// and runs resetHeaderState, which clears headersFirstMode and
			// roundAnchorHeight and re-seeds nextCheckpoint from the current tip.
			// releaseSyncPeer's own !fSyncStarted early return cannot fire here:
			// the sweep has just set the flag and nothing between clears it.
			// TestSyncPass_ADisconnectUnwindsTheSlotTheSweepJustGranted walks that
			// path and asserts each piece.
			//
			// Whatever the sweep produced for this peer is dropped with it.
			// syncTickOnce disconnects before it sends, so the message would
			// reach a closed peer; C++ has already pushed its getheaders by this
			// point, but there PushMessage precedes the fDisconnect flag. It is
			// the same reasoning OnInv states for its own error path: messages
			// queued for a peer we are about to drop are not worth sending.
			// Both clauses land here because the ACTION the caller must take is
			// identical; only the reason differs, and it rides along so the log
			// and the peer's own disconnect reason can name the rule that fired.
			disconnect = append(disconnect, stallDisconnect{peer: h.peer, action: action})

			continue

		case StallActionRotateSyncPeer:
			// The slot and the peer's in-flight blocks are ALREADY released
			// and the peer stays connected, so it must not be disconnected or
			// finalized here — only left to be swept again on a later tick.
			m.logger.Warnf("[svp2p] rotating the sync peer %s: no sync progress", h.sync.Addr)

			// This branch runs no getdata pass, so the rest of this peer's tick is
			// skipped. Without the skip, SendGetDataBlocks hands the peer we have
			// just judged non-progressing another MaxBlocksInTransitPerPeer blocks
			// on THIS pass: clearPeer nils pindexLastCommonBlock but keeps
			// pindexBestKnownBlock, so FindNextBlocksToDownload bootstraps the
			// window from our own tip again and refills it.
			//
			// The seam is a composition of two models, and neither model's
			// safety net came across with it. SVNode runs a per-peer download
			// loop and relies on a per-block download timeout to take blocks
			// back from a peer that does not deliver. legacy netsync rotates
			// the sync peer alone and hands blocks to nobody else. Composed
			// here, the rotated peer keeps being re-handed blocks, while its
			// own rotation clause can no longer fire for it, because
			// SyncPeerTimedOut cleared fSyncStarted. What DOES still govern it
			// from here is stated at CheckStall.
			//
			// The skip costs this peer its own chance to NAME a staller on
			// this pass, since that clock is started from inside
			// SendGetDataBlocks. Every other peer on the pass still names one,
			// and the next tick gives this peer the chance back.
			//
			// THE SWEEP ABOVE CANNOT HAVE PRODUCED ANYTHING FOR THIS PEER, and
			// that coupling is what makes "the rotating peer is asked for nothing"
			// hold rather than the weaker "asked for no blocks". The rotation
			// clause only ever judges a peer with fSyncStarted set — CheckStall's
			// !fSyncStarted early return sits above it — and PeerEstablished
			// returns nil for exactly that peer. So msgs is empty here, and the
			// send list below gets no entry for this peer. Anything that ever
			// lets PeerEstablished answer an fSyncStarted peer breaks that, and
			// TestSyncPass_ASingleCandidateNodeTakesTheSlotBackOnTheNextTick is
			// what would catch it.
			//
			// There is no continue in this branch: the switch itself is the skip,
			// because only StallActionNone runs the getdata pass.

		case StallActionNone:
			msgs = append(msgs, m.blockDownloader.SendGetDataBlocks(h.sync, activeTip, now)...)
		}

		if len(msgs) > 0 {
			out = append(out, outgoing{peer: h.peer, msgs: msgs})
		}
	}

	m.syncMu.Unlock()

	return out, disconnect
}

func (m *PeerManager) newNonce() uint64 {
	nonce := randNonce()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.nonces = append(m.nonces, nonce)
	if len(m.nonces) > maxSentNonces {
		m.nonces = m.nonces[len(m.nonces)-maxSentNonces:]
	}

	return nonce
}

// hasSentNonce mirrors net.cpp CConnman::CheckIncomingNonce: true if this
// node itself generated the given nonce for one of its own connections
// (inbound or outbound), meaning an incoming VERSION carrying it proves a
// self-connect.
func (m *PeerManager) hasSentNonce(nonce uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, n := range m.nonces {
		if n == nonce {
			return true
		}
	}

	return false
}

func (m *PeerManager) ListenAddrs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	addrs := make([]string, 0, len(m.listeners))
	for _, ln := range m.listeners {
		addrs = append(addrs, ln.Addr().String())
	}

	return addrs
}

// PeerScores is the misbehaviour score of every peer that has completed its
// handshake, keyed by remote address. Test support for the parity harness; the
// daemon does not call it.
func (m *PeerManager) PeerScores() map[string]int {
	out := make(map[string]int)

	for _, h := range m.peerHandles() {
		if !h.established {
			continue
		}

		info := h.peer.Info()
		out[info.Addr] = info.MisbehaviorScore
	}

	return out
}

func (m *PeerManager) ConnectedCount() int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return int32(len(m.peers)) //nolint:gosec // peer count is small
}

// SyncRateFloor reports the sync-peer rotation rate floor the downloader is
// running with, in bytes per second: the value
// settings.Legacy.MinSyncPeerNetworkSpeed travelled to through ConfigureSync,
// or 0 when sync is not configured at all. Zero is also the operator value that
// disables the floor, so the two are not distinguishable here — a caller that
// needs that distinction should ask SyncEnabled first.
//
// It exists so the service-level wiring test can prove an operator's setting
// reaches the machine that reads it, and it is the only way to observe that
// from outside this package.
func (m *PeerManager) SyncRateFloor() uint64 {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.blockDownloader == nil {
		return 0
	}

	return m.blockDownloader.minDownloadBytesPerSec
}

func (m *PeerManager) Snapshots() []PeerSnapshot {
	m.mu.Lock()
	peers := make([]*Peer, 0, len(m.peers))
	syncPeers := make([]*SyncPeer, 0, len(m.peers))

	for p, sp := range m.peers {
		peers = append(peers, p)
		syncPeers = append(syncPeers, sp)
	}
	m.mu.Unlock()

	snaps := make([]PeerSnapshot, 0, len(peers))
	for _, p := range peers {
		snaps = append(snaps, p.Info())
	}

	// The sync flag lives on peerSyncState under syncMu, which is never held
	// together with mu or a peer lock (see syncMu's own doc comment), so it
	// is stamped in a separate pass after every Info call has completed.
	m.syncMu.Lock()
	for i, sp := range syncPeers {
		if sp != nil && sp.State != nil {
			snaps[i].SyncStarted = sp.State.fSyncStarted
		}
	}
	m.syncMu.Unlock()

	return snaps
}

func (m *PeerManager) DisconnectHost(host string) {
	m.mu.Lock()
	peers := make([]*Peer, 0, len(m.peers))

	for p := range m.peers {
		peers = append(peers, p)
	}
	m.mu.Unlock()

	for _, p := range peers {
		peerHost, _, err := net.SplitHostPort(p.Info().Addr)
		if err != nil {
			peerHost = p.Info().Addr
		}

		if peerHost == host {
			p.Disconnect("banned by operator")
		}
	}
}

func (m *PeerManager) Stop() error {
	m.mu.Lock()

	select {
	case <-m.quit:
	default:
		close(m.quit)
	}

	listeners := m.listeners
	m.listeners = nil

	peers := make([]*Peer, 0, len(m.peers))
	for p := range m.peers {
		peers = append(peers, p)
	}
	m.mu.Unlock()

	for _, ln := range listeners {
		_ = ln.Close()
	}

	for _, p := range peers {
		p.Disconnect("shutting down")
	}

	m.wg.Wait()

	// Released only after every peer goroutine has joined above: those
	// goroutines are the only callers of RequestTxs/OnInv that touch
	// blockDownloader.requestedTxns, so this ordering is the same
	// "join producers before releasing what they used" guarantee legacy's
	// own Stop documents for its DC11 producer flush (netsync/manager.go:
	// 3078-3086, "handlerDone above guarantees no more sends").
	m.syncMu.Lock()
	if m.blockDownloader != nil {
		m.blockDownloader.Stop()
	}
	m.syncMu.Unlock()

	return nil
}

func netAddressOf(addr net.Addr) *wire.NetAddress {
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return wire.NewNetAddressIPPort(nil, 0, 0)
	}

	return wire.NewNetAddress(tcpAddr, 0)
}

// effectivePingInterval fits the ping cadence to the idle window it keeps
// alive. SVNode's PING_INTERVAL (2 min) sits under a TIMEOUT_INTERVAL of 20
// min (net.h), a tenfold margin; svp2p's window is legacy_peerIdleTimeout,
// 125 s by default, which would leave a pong 5 s to arrive. When the window is
// shorter than two cadences the cadence is halved, so a pong always has at
// least half the window; otherwise SVNode's own cadence is kept.
func effectivePingInterval(idle, ping time.Duration) time.Duration {
	if idle > 0 && idle < 2*ping {
		return idle / 2
	}

	return ping
}
