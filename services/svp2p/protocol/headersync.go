package protocol

import (
	"encoding/binary"
	"math/big"
	"net"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// MaxHeadersResults mirrors net_processing.h MAX_HEADERS_RESULTS (2000): the
// number of headers a peer may put in one headers message.
const MaxHeadersResults = 2000

// MaxUnconnectingHeaders mirrors net_processing.cpp MAX_UNCONNECTING_HEADERS
// (10): how many headers messages with an unknown parent a peer may send
// before it scores misbehavior.
const MaxUnconnectingHeaders = 10

// MaxFutureBlockTime mirrors block_index.h MAX_FUTURE_BLOCK_TIME
// (block_index.h:31): 2 * 60 * 60 seconds. It is how far past the adjusted
// time a header's timestamp may sit before ContextualCheckBlockHeader refuses
// it as "time-too-new".
const MaxFutureBlockTime int64 = 2 * 60 * 60

// NearTipHeaderSyncWindow is the 24 * 60 * 60 seconds net_processing.cpp
// SendBlockSync (net_processing.cpp:5191-5196) measures pindexBestHeader
// against before it starts header sync with ADDITIONAL peers. The C++ spells
// the constant inline; it is named here because PeerEstablished reads it.
const NearTipHeaderSyncWindow int64 = 24 * 60 * 60

// MaxBlocksToAnnounce mirrors net_processing.cpp MAX_BLOCKS_TO_ANNOUNCE (8):
// the size limit that separates a short "we are announcing a block you have
// not seen" headers message, which earns the gap-filling getheaders, from a
// bulk batch, which does not.
const MaxBlocksToAnnounce = 8

// Misbehavior scores for headers processing, from net_processing.cpp.
const (
	scoreTooManyHeaders            = 20 // Misbehaving(pfrom, 20, "too-many-headers")
	scoreNonContinuousHeaders      = 20 // Misbehaving(pfrom, 20, "disconnected headers")
	scoreTooManyUnconnectedHeaders = 20 // Misbehaving(pfrom, 20, "too-many-unconnected-headers")

	// AcceptBlockHeader fails a header whose parent is not in mapBlockIndex
	// with state.DoS(10, ..., "prev-blk-not-found"), which net_processing.cpp
	// applies as Misbehaving(pfrom, nDoS, "invalid header received"). Only a
	// bulk batch reaches it: a batch shorter than MAX_BLOCKS_TO_ANNOUNCE takes
	// the gap-filling getheaders path instead.
	scorePrevBlkNotFound = 10

	// CheckBlockHeader fails a bad proof of work with
	// state.DoS(50, ..., "high-hash"); net_processing.cpp then applies that
	// nDoS as Misbehaving(pfrom, nDoS, "invalid header received").
	scoreInvalidHeader = 50

	// ContextualCheckBlockHeader fails a header whose nBits is not the value
	// GetNextWorkRequired demands with
	// state.DoS(100, ..., REJECT_INVALID, "bad-diffbits"), which
	// net_processing.cpp applies as Misbehaving(pfrom, nDoS, "invalid header
	// received"). One hundred is the ban threshold, so a single such header
	// ends the peer.
	scoreBadDiffBits = 100

	// scoreUnsolicitedBulkHeaders is Task 11's policy at the round-ignore
	// site below (see the comment on the `hs.headersFirstMode &&
	// !peer.State.fSyncStarted` branch in OnHeaders): a BULK headers batch
	// (len(headers) >= MaxBlocksToAnnounce) from a peer that neither holds
	// the sync slot nor answers a getheaders we sent it.
	//
	// THIS HAS NO SVNODE COUNTERPART. net_processing.cpp's HEADERS handler
	// runs no solicitation check at all — accepting unsolicited headers is
	// what makes block announcement work — so there is no C++ line to port.
	// The nearest relative is legacy netsync's mode-based disconnect,
	// services/legacy/netsync/manager.go handleHeadersMsg, which drops ANY
	// peer that sends headers while !headersFirstMode, unconditionally and
	// regardless of batch size. This constant is deliberately narrower:
	// scoped to bulk batches only (an announcement-size batch stays free,
	// exactly because SVNode's own accept-unsolicited-announcements behavior
	// must not break), and scored rather than disconnected outright.
	//
	// PARITY-HARNESS-DEPENDENT: the number itself is a POLICY CHOICE, not a
	// ported constant, and the parity harness (a separate plan, not yet
	// built — see OPEN QUESTION 1 in the Phase 3 plan) is the only thing that
	// can adjudicate whether it diverges from SVNode observably. The harness
	// may move this number; it must not move whether an unsolicited bulk
	// batch scores at all. Every place this dependency is recorded carries
	// the literal string PARITY-HARNESS-DEPENDENT, so grep for it.
	scoreUnsolicitedBulkHeaders = 20
)

// ErrCheckpointMismatch reports a header at a checkpoint height whose hash is
// not the checkpoint hash. Legacy netsync manager.go answers this with
// DisconnectWithWarning, so the caller must drop the peer rather than score
// it. It shares ERR_NETWORK_PEER_MALICIOUS with ErrProtocolViolation on
// purpose: the teranode errors package matches errors.Is by code, and both
// sentinels mean the same thing to a caller — disconnect this peer.
var ErrCheckpointMismatch = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: header does not match checkpoint")

// ErrCheckpointFork reports a header that would fork the chain below the last
// checkpoint the index holds — validation.cpp CheckIndexAgainstCheckpoint's
// "bad-fork-prior-to-checkpoint". SVNode scores that DoS(100), one point above
// the ban threshold, so the peer is gone either way; this port answers with a
// disconnect error instead, the legacy DisconnectWithWarning shape the
// checkpoint-mismatch path above already uses, so both checkpoint refusals
// reach the caller the same way.
//
// It shares ERR_NETWORK_PEER_MALICIOUS with ErrCheckpointMismatch for the
// reason documented there: the teranode errors package matches errors.Is by
// code, and both sentinels mean "disconnect this peer". A caller that must
// tell them apart reads the message.
var ErrCheckpointFork = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: header forks below the last checkpoint")

// ErrHeadersNoProgress reports a headers batch that did not carry the
// headers-first round past the point the last getheaders was issued from — a
// peer replaying headers we already have. Legacy netsync catches the same
// peer with its header-list anchor ("Received block header that does not
// properly connect to the chain") and answers with DisconnectWithWarning, so
// the caller must drop the peer. It carries ERR_NETWORK_INVALID_RESPONSE to
// stay distinguishable from ErrCheckpointMismatch, since errors.Is matches by
// code; it therefore aliases the handshake's ErrObsoleteVersion, which no
// caller ever compares against a headers error.
var ErrHeadersNoProgress = errors.New(errors.ERR_NETWORK_INVALID_RESPONSE, "svp2p: headers batch made no sync progress")

// HeaderSyncConfig carries the immutable inputs of the headers-first machine.
type HeaderSyncConfig struct {
	// Index is the mapBlockIndex counterpart the machine reads and extends.
	Index *HeaderIndex

	// Params is the active chain configuration. It supplies the checkpoints
	// (the same source legacy netsync uses: chainParams.Checkpoints), the
	// powLimit the header PoW range check needs, and the network magic that
	// selects the regtest branches.
	Params *chaincfg.Params

	// DisableCheckpoints mirrors legacy netsync Config.DisableCheckpoints
	// (the -nocheckpoints flag): no checkpoint is ever selected and the
	// machine never enters headers-first mode.
	DisableCheckpoints bool

	// AllowSyncCandidateFromLocalPeers mirrors
	// settings.Legacy.AllowSyncCandidateFromLocalPeers
	// (legacy_allowSyncCandidateFromLocalPeers): in regtest it lifts the
	// localhost restriction on sync candidates.
	AllowSyncCandidateFromLocalPeers bool

	// AdjustedTime supplies the nAdjustedTime argument
	// ContextualCheckBlockHeader takes, in Unix seconds. It is the only clock
	// this machine consults, and it is injected rather than read, which is
	// what keeps every decision path in this package testable without a wall
	// clock. NewHeaderSync fills it with the system clock when it is nil, so
	// the default is chosen at construction rather than inside a check.
	//
	// SVNode passes GetAdjustedTime(), which is the local clock plus the
	// median time offset of its peers (timedata.cpp, clamped to +/-70
	// minutes). This port has no peer time-offset machinery, so it passes the
	// local clock alone. The divergence only matters for a node whose own
	// clock is wrong by more than MAX_FUTURE_BLOCK_TIME minus the network's
	// real spread — two hours — and a clock that far out breaks far more of
	// this node than header acceptance.
	AdjustedTime func() int64
}

// SyncPeer is the per-peer handle the machine needs for one event: the
// address and services the sync-candidate rules read, the peer's CNodeState
// entry, and the CNodeState::nUnconnectingHeaders counter this machine owns.
// PeerManager keeps one SyncPeer per connected peer for the peer's lifetime.
type SyncPeer struct {
	// Addr is the peer's remote address in host:port form, read by the
	// regtest localhost rule in isSyncCandidate.
	Addr string

	// Services is the service flag set the peer advertised in its version
	// message.
	Services wire.ServiceFlag

	// State is the peer's CNodeState entry, shared with block download
	// scheduling.
	State *peerSyncState

	// nUnconnectingHeaders mirrors CNodeState::nUnconnectingHeaders: how many
	// headers messages this peer has sent whose first header has no known
	// parent.
	nUnconnectingHeaders int

	// hashContinue mirrors CNode::hashContinue (net.h) and the legacy
	// service's serverPeer.continueHash (services/legacy/peer_server.go:375):
	// the last hash of a getblocks inv that filled to MaxGetBlocksResults. When
	// the peer asks for that block we answer the body and then an inv of our
	// tip, which is what makes it send the next getblocks — the continue-inv
	// convention. Serving.OnGetBlocks writes it; the getdata path (Task 10)
	// reads and clears it, as pushBlockMsg does (peer_server.go:2121-2144).
	// The zero chainhash.Hash means no continuation is pending, matching the
	// C++ SetNull() sentinel.
	hashContinue chainhash.Hash

	// getHeadersOutstanding counts how many getheaders PeerManager has
	// decided to send this peer whose reply has not arrived yet — "decided
	// to send", not "sent": a getheaders the send budget refuses still
	// increments this count (see markGetHeadersOutstanding, manager.go),
	// because the decision is made and recorded before send is attempted, and
	// a peer.go dispatchSync/p.send failure only logs and drops (peer.go).
	// That direction is safe for what this field guards: it can only ever
	// leave the count too HIGH relative to what actually reached the wire,
	// and a too-high count only ever SUPPRESSES a score — it can never
	// manufacture one. Task 11's per-peer solicitation tracking: HAS NO
	// SVNODE COUNTERPART, since net_processing.cpp never needs to ask
	// whether a headers batch was solicited (see scoreUnsolicitedBulkHeaders
	// above).
	//
	// IT IS A COUNT, NOT A BOOL, because a single event can leave MORE THAN
	// ONE getheaders outstanding for this peer at once:
	// BlockDownloader.OnInv (blockdownload.go) sends one getheaders per
	// DISTINCT unknown block hash in a single inv message, and a peer that
	// pipelines a second inv before answering the first getheaders adds a
	// second one on top. A bool can only remember "at least one was sent",
	// so it reads the SECOND of two honest replies as unsolicited. Each
	// increment below stands for exactly one getheaders this machine has not
	// yet been answered for; each decrement below stands for one headers
	// message that answered one of them, whichever it was — there is no
	// hashStop correlation, so this cannot tell one outstanding request from
	// another, only how many are open.
	//
	// Written by PeerManager (manager.go Established/electLocked/the sync-pass
	// sweep/Headers/Inv — every place a sync machine's output reaches the
	// wire, see peer.go dispatchSync and the two syncPass call sites) once per
	// *wire.MsgGetHeaders any of those calls returns for this peer. Read and
	// decremented by OnHeaders below, once per call, before any other check:
	// that is "the next headers batch" the field answers for — it answers
	// for ANY one of the outstanding requests, not a specific one, since
	// nothing here correlates a reply to the getheaders that solicited it.
	// This is what lets an unsolicited bulk batch be told apart from one that
	// answers a getheaders we actually sent (a round-owner's continuation, an
	// inv-driven fetch, an election/sweep grant, or the announcement-gap
	// getheaders below).
	//
	// Not bounded: net_processing.cpp itself sends one getheaders per
	// unknown-block inv entry with no cap on how many may be outstanding for
	// one peer at once, and MAX_HEADERS_RESULTS already bounds what one reply
	// can cost us to process regardless of how many are open.
	//
	// Locking: guarded by PeerManager.syncMu, the same lock that already
	// covers the whole sync-state graph (see peerSyncState's doc comment).
	// Every write and the one read+decrement happen inside a method that
	// already holds syncMu for its whole call — manager.go's dispatch and
	// election/sweep call sites, and HeaderSync.OnHeaders is only ever
	// reached through the syncMu-held Headers wrapper. This field is
	// manager-lock-only: peer.go's own `mu` (guarding the handshake and
	// connection state) never touches it, so the peer-lock-then-manager-lock
	// order documented in peer.go handleMessage is not in play here.
	getHeadersOutstanding int

	// fSentAddr mirrors CNode::fSentAddr (net.h): this connection has already
	// been answered one getaddr. SVNode answers only one per connection,
	// "to reduce resource waste and discourage addr stamping of INV
	// announcements" (net_processing.cpp:4111-4113); the legacy service's own
	// sentAddrs does the same (services/legacy/peer_server.go:378, :1753).
	//
	// Locking: guarded by PeerManager.syncMu, like every other field on this
	// struct. PeerManager.GetAddr reads and writes it inside one syncMu-held
	// section, so the read-then-set cannot interleave with a second getaddr.
	fSentAddr bool

	// fGetAddr mirrors CNode::fGetAddr (net.h): WE sent this peer a getaddr
	// and its addr reply has not arrived yet. It is set when the outbound
	// handshake completes (PeerManager.Established,
	// net_processing.cpp:1867-1870) and cleared by the first addr message that
	// arrives — C++ does both halves atomically, `pfrom->fGetAddr.exchange(false)`
	// (net_processing.cpp:2290-2291), and PeerManager.Addr reproduces that
	// exchange under syncMu.
	//
	// It is what lifts the unsolicited-addr fence (addrrelay.go
	// processAddrEntries): an addr nobody asked for may only tell us about the
	// sender itself.
	fGetAddr bool

	// addrKnown mirrors CNode::addrKnown (net.h:968): the addresses already
	// exchanged with this peer in either direction, so neither side sends the
	// same one twice. See knownAddrSet (addrrelay.go) for the capacity choice
	// and why this is an exact set rather than SVNode's rolling bloom filter.
	//
	// Locking: guarded by PeerManager.syncMu, like knownBlocks and knownTxs on
	// peerSyncState.
	addrKnown *knownAddrSet
}

func NewSyncPeer(addr string, services wire.ServiceFlag, state *peerSyncState) *SyncPeer {
	return &SyncPeer{Addr: addr, Services: services, State: state, addrKnown: newKnownAddrSet(knownAddrCap)}
}

// HeaderSync is the headers-first sync state machine: the net_processing.cpp
// header path (SendMessages' initial getheaders and the HEADERS handler) with
// the checkpointed getheaders scheme legacy netsync manager.go carries from
// bsvd. It performs no I/O: every event returns the messages the caller must
// send, a misbehavior delta to apply to the peer, and an error only when the
// peer must be disconnected. It mutates neither Conn nor Peer.
//
// Locking: HeaderSync carries no lock of its own. Like peerSyncState, every
// method assumes the caller already holds PeerManager's shared sync-state
// mutex — this package's port of cs_main. Lock order in this package is
// peer lock, then manager lock.
type HeaderSync struct {
	cfg         HeaderSyncConfig
	checkpoints []chaincfg.Checkpoint

	// nextCheckpoint mirrors SyncManager.nextCheckpoint: the checkpoint the
	// current headers-first round runs up to, or nil past the final one.
	nextCheckpoint *chaincfg.Checkpoint

	// headersFirstMode mirrors SyncManager.headersFirstMode.
	headersFirstMode bool

	// roundAnchorHeight is the height of the header the current round's last
	// getheaders was issued from. It is this port's replacement for legacy's
	// header-list anchor: a batch must reach past it, or the round is not
	// advancing and the peer is replaying headers we already hold.
	roundAnchorHeight int32

	// nSyncStarted mirrors the net_processing.cpp file-scope nSyncStarted:
	// how many peers we have started headers synchronization with.
	nSyncStarted int
}

func NewHeaderSync(cfg HeaderSyncConfig) (*HeaderSync, error) {
	if cfg.Index == nil {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: header index is nil")
	}

	if cfg.Params == nil {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: chain params are nil")
	}

	// Without a powLimit the header PoW check would accept any target the peer
	// claims, so refuse to build a machine that cannot enforce it.
	if cfg.Params.PowLimit == nil {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: chain params carry no powLimit")
	}

	// Both findNextHeaderCheckpoint and lastCheckpointInIndex read the list as
	// ascending by height — the first walks it backwards to find the next
	// checkpoint above a height, the second to find the highest one we hold.
	// SVNode gets that ordering for free from std::map<int32_t, uint256>; a Go
	// slice does not, and lastCheckpointInIndex now gates a disconnect, so an
	// unordered list is refused here rather than silently fencing at the wrong
	// height. Every network go-chaincfg ships is already ascending.
	for i := 1; i < len(cfg.Params.Checkpoints); i++ {
		if cfg.Params.Checkpoints[i].Height <= cfg.Params.Checkpoints[i-1].Height {
			return nil, errors.New(errors.ERR_INVALID_ARGUMENT,
				"svp2p: chain params checkpoints are not ascending by height (%d after %d at index %d)",
				cfg.Params.Checkpoints[i].Height, cfg.Params.Checkpoints[i-1].Height, i)
		}
	}

	// The machine consults no wall clock of its own; the default is bound here
	// so every check downstream can assume a non-nil function.
	if cfg.AdjustedTime == nil {
		cfg.AdjustedTime = func() int64 { return time.Now().Unix() }
	}

	hs := &HeaderSync{cfg: cfg}

	// legacy netsync manager.go New: the checkpoint list comes from the chain
	// params, and stays empty when checkpoints are disabled.
	if !cfg.DisableCheckpoints {
		hs.checkpoints = cfg.Params.Checkpoints
	}

	// legacy netsync manager.go New: seed nextCheckpoint from our current
	// height via resetHeaderState, which leaves headers-first mode off until a
	// sync actually starts.
	hs.resetHeaderState()

	return hs, nil
}

// IsHeadersFirstMode reports whether the node is downloading headers up to a
// checkpoint. Phase 3 reads it to refuse serving getheaders during catch-up,
// the same use legacy netsync makes of it.
func (hs *HeaderSync) IsHeadersFirstMode() bool { return hs.headersFirstMode }

// PeerEstablished is the SendMessages event: a peer finished the handshake, so
// consider starting headers synchronization with it. It returns the initial
// getheaders, or nothing when this peer must not drive the sync.
//
// Peer choice follows net_processing.cpp, which starts with the first eligible
// candidate that reaches SendMessages. Legacy netsync startSync instead ranks
// the candidates and picks the one with the greatest advertised height. Ranking
// needs the whole peer set, which this per-peer machine does not see; if the
// parity harness shows the first-eligible rule picking poor sync peers,
// PeerManager can rank candidates before it calls this.
func (hs *HeaderSync) PeerEstablished(peer *SyncPeer) []wire.Message {
	if peer == nil || peer.State == nil {
		return nil
	}

	// net_processing.cpp SendMessages gates the initial getheaders on
	// !state.fSyncStarted and on fFetch, the "this peer can serve us blocks"
	// predicate. isSyncCandidate carries the legacy reading of fFetch.
	if peer.State.fSyncStarted || !hs.isSyncCandidate(peer) {
		return nil
	}

	// net_processing.cpp SendBlockSync (net_processing.cpp:5191-5196): "Only
	// actively request headers from a single peer, unless we're close to
	// today." The slot is single until the best header we hold is within 24
	// hours of the adjusted time, and from there header sync starts with every
	// further candidate too. Legacy netsync carries only the single-peer half.
	//
	// Phase 2 kept the single-peer rule alone because HeaderIndex exposed no
	// header timestamp; Phase 3 Task 2 put GetBlockTime() on HeaderNode, so the
	// relaxation lands here. It reads hs.cfg.AdjustedTime, the SAME injected
	// clock the time-too-new cap in acceptHeader reads — this machine has one
	// clock and only one.
	//
	// THIS CHECK IS RE-RUN FOR EVERY PEER ON EVERY SYNC TICK, which is what
	// SendBlockSync gets for free: SendMessages calls it per peer
	// (net_processing.cpp:5865), so a peer that becomes eligible later still
	// starts header sync. PeerManager.syncPass carries that sweep — its per-peer
	// loop states the source order the sweep runs in and what it costs per tick.
	// So this method now has three callers: PeerManager.Established (the
	// handshake), PeerManager.electLocked (a rotation or a peerGone), and the
	// sweep.
	//
	// WHAT THE SWEEP PAYS OFF, because the cost was real while this check was
	// event-driven only. Past the final checkpoint headersFirstMode is false, so
	// CheckStall's header-progress refresh (blockdownload.go, the
	// IsHeadersFirstMode branch) is unreachable and nLastProgressTime moves only
	// when a block is delivered — to the ONE peer that delivered it
	// (BlockReceived). With MaxLastBlockTime at 180 seconds and mainnet blocks
	// about ten minutes apart, most 180 second windows deliver no block at all, so
	// every fSyncStarted peer rotates in the same pass. That freed every slot,
	// while the single election that followed returned on the FIRST peer to yield
	// messages and refilled exactly one; CheckStall's !fSyncStarted early return
	// then stopped a rotated-out peer from ever rotating again, so it triggered no
	// further election of its own. A node that reached several header peers fell to
	// exactly ONE within about 180 seconds and stayed there until peers
	// reconnected. The sweep restarts header sync with every eligible peer on the
	// next tick instead, and
	// TestSyncPass_HeaderSyncBreadthRecoversAfterEveryPeerRotates pins that.
	//
	// IT WAS NEVER A THREAT TO ALL-PEER DOWNLOAD SCHEDULING, and is not one now.
	// What that needs from a peer is pindexBestKnownBlock, and Task 4's promotion
	// mechanism supplies it from index growth alone, independently of fSyncStarted
	// — a peer that never held the sync slot is still schedulable. The collapse
	// cost header-sync breadth, not download breadth.
	//
	// THE !headersFirstMode CONJUNCT HAS NO SVNODE COUNTERPART. It is a
	// Teranode-specific structural guard, and SendBlockSync has nothing like
	// it, for the simple reason that SVNode has no headers-first round to
	// protect: the checkpointed round is legacy netsync's scheme, which this
	// port inherited from the legacy service and net_processing.cpp never had.
	// Phase 2 Task 5 scoped that round to ONE peer — only the slot holder may
	// index headers while it runs — so admitting a second fSyncStarted peer
	// mid-round would let it re-seed nextCheckpoint, headersFirstMode and
	// roundAnchorHeight underneath the peer driving the round.
	//
	// WHY IT EXISTS, and the dependency to re-evaluate it against. The plan for
	// this task argued the guard was unnecessary, because a round only runs
	// below the final checkpoint, where the tip is old. That argument conflates
	// HEIGHT with TIME and does not hold TODAY FOR ONE REASON: acceptHeader
	// deliberately does not port ContextualCheckBlockHeader's time-too-old rule
	// (validation.cpp:5904-5907 — see the note at that site, which books it as
	// a Phase 3 follow-up). With no median-time-past bound, a header at ANY
	// height may claim ANY timestamp up to the future cap, and Tip() answers
	// the highest-WORK node, which during a round is whatever the slot holder
	// served last. So a peer feeding a fresh node cheap headers below its first
	// checkpoint can make the tip look recent while the round still runs.
	//
	// IF THE TIME-TOO-OLD RULE EVER LANDS, re-evaluate this conjunct rather
	// than carrying it forward unexamined: a monotonic timestamp sequence would
	// restore the plan's argument, and the guard could then go. Do not remove
	// it before that, and do not remove it without first making
	// roundAnchorHeight per-peer — see the invariant note below.
	//
	// IT ALSO KEEPS THE ROUND STATE SINGLE-SOURCED. roundAnchorHeight and the
	// no-progress terminator that reads it (acceptHeaders) live on this
	// machine, not on the peer, and both are gated on headersFirstMode. Because
	// this conjunct makes "headersFirstMode is on" and "more than one peer holds
	// fSyncStarted" mutually exclusive, two simultaneous header sources can
	// never share that anchor and so can never make each other look
	// non-progressing. Outside a round, nextCheckpoint, headersFirstMode and
	// roundAnchorHeight are all inert — nothing reads them — so parallel header
	// syncs contend on nothing.
	//
	// The guard costs nothing in intended operation: past the final checkpoint,
	// where a tip within 24 hours of now actually occurs, headersFirstMode is
	// off. A network with no checkpoints at all — regtest, tstn — never turns
	// it on, so there the 24 hour test is the only gate, which is exactly
	// net_processing.cpp.
	if hs.nSyncStarted > 0 && (hs.headersFirstMode || !hs.tipIsNearAdjustedTime()) {
		return nil
	}

	first := hs.nSyncStarted == 0

	peer.State.fSyncStarted = true
	hs.nSyncStarted++

	if !first {
		// An ADDITIONAL near-tip peer joins a header sync that is already
		// running; it does not own the round state. Only the first sync peer
		// seeds nextCheckpoint, headersFirstMode and the round anchor, so a
		// late joiner can never turn a round on underneath the peer already
		// syncing — the guard above only reads the mode as it stands now.
		// SendBlockSync gives every peer the same plain getheaders from
		// pindexBestHeader->pprev, which is what syncStartLocator builds, and
		// headersFirstMode is false here so the hashStop is the zero hash.
		locator, _ := hs.syncStartLocator()

		return []wire.Message{hs.getHeaders(locator)}
	}

	_, tipHeight := hs.cfg.Index.Tip()
	hs.nextCheckpoint = hs.findNextHeaderCheckpoint(tipHeight)

	// legacy netsync manager.go startSync: run headers-first only while a
	// checkpoint is still ahead of us, and never in regression test mode.
	hs.headersFirstMode = hs.nextCheckpoint != nil &&
		tipHeight < hs.nextCheckpoint.Height &&
		!hs.isRegtest()

	locator, anchor := hs.syncStartLocator()
	hs.roundAnchorHeight = anchor

	return []wire.Message{hs.getHeaders(locator)}
}

// tipIsNearAdjustedTime is the net_processing.cpp SendBlockSync
// (net_processing.cpp:5191-5196) test on pindexBestHeader:
// GetBlockTime() > GetAdjustedTime() - 24 * 60 * 60. The header index tip is
// this port's pindexBestHeader — HeaderIndex.Tip answers the highest-work node
// it holds, which is what pindexBestHeader tracks in the source.
//
// A tip the index cannot look up answers false, which keeps the single-slot
// rule. That case is unreachable — Tip returns a hash it holds — and refusing
// the relaxation is the conservative side of it either way.
func (hs *HeaderSync) tipIsNearAdjustedTime() bool {
	tipHash, _ := hs.cfg.Index.Tip()

	tip, ok := hs.cfg.Index.Lookup(tipHash)
	if !ok {
		return false
	}

	return tip.Time > hs.cfg.AdjustedTime()-NearTipHeaderSyncWindow
}

// PeerDisconnected mirrors net_processing.cpp FinalizeNode: a peer that was
// driving header sync releases the single sync slot when it goes away, and the
// header state resets so the next candidate starts a clean round.
func (hs *HeaderSync) PeerDisconnected(peer *SyncPeer) {
	hs.releaseSyncPeer(peer)
}

// SyncPeerTimedOut releases a sync peer that is still connected but has stopped
// answering, mirroring legacy netsync's sync-peer timeout, which calls
// resetHeaderState and lets startSync choose another peer. The machine reads no
// clock: the caller owns the timeout and calls this when it expires — see
// BlockDownloader.CheckStall, which is that caller. There is no SVNode rule
// behind it. Bitcoin Core times a headers round out against
// HEADERS_DOWNLOAD_TIMEOUT_BASE, but this SVNode fork never took that constant;
// it relies on the peer being disconnected or its in-flight blocks timing out,
// neither of which fires during a silent headers-first round. The timeout is
// therefore the Teranode rotation carried from legacy netsync. The peer stays
// connected; only the sync slot and the header state are released.
func (hs *HeaderSync) SyncPeerTimedOut(peer *SyncPeer) {
	hs.releaseSyncPeer(peer)
}

// releaseSyncPeer frees the single sync slot and resets the header state, the
// FinalizeNode plus resetHeaderState pair. Without the reset,
// IsHeadersFirstMode stays true with nobody syncing, and nextCheckpoint keeps
// pointing at the round that died.
func (hs *HeaderSync) releaseSyncPeer(peer *SyncPeer) {
	if peer == nil || peer.State == nil || !peer.State.fSyncStarted {
		return
	}

	peer.State.fSyncStarted = false

	if hs.nSyncStarted > 0 {
		hs.nSyncStarted--
	}

	hs.resetHeaderState()
}

// resetHeaderState is legacy netsync manager.go resetHeaderState: leave
// headers-first mode and re-seed the checkpoint from where our chain now
// stands. The legacy header list has no counterpart here — HeaderIndex holds
// the headers already accepted, and they stay valid.
func (hs *HeaderSync) resetHeaderState() {
	hs.headersFirstMode = false
	hs.roundAnchorHeight = 0

	_, tipHeight := hs.cfg.Index.Tip()
	hs.nextCheckpoint = hs.findNextHeaderCheckpoint(tipHeight)
}

// OnHeaders is the net_processing.cpp NetMsgType::HEADERS event. It returns
// the messages to send back, the misbehavior score to add to this peer, and an
// error only when the peer must be disconnected outright.
//
// Caller contract on that error, which is either ErrCheckpointMismatch or
// ErrHeadersNoProgress: the machine does not release the round itself. The
// offending peer still holds the sync slot and headers-first mode is still on
// when the error returns. The caller must disconnect the peer and then call
// PeerDisconnected, which frees the slot and resets the header state. This is
// the legacy shape, where DisconnectWithWarning leads to resetHeaderState and a
// fresh startSync; splitting it this way keeps the machine free of any
// knowledge of how a peer is torn down.
func (hs *HeaderSync) OnHeaders(peer *SyncPeer, msg *wire.MsgHeaders) ([]wire.Message, int, error) {
	if peer == nil || peer.State == nil || msg == nil {
		return nil, 0, nil
	}

	// Task 11: this batch answers "the next headers batch" the
	// getHeadersOutstanding doc comment promises — one of however many
	// getheaders are currently open for this peer, not a specific one, since
	// nothing here correlates a reply to the request that solicited it. It is
	// read and decremented here, once, before anything else runs — including
	// the too-many-headers and empty-batch returns below, neither of which is
	// a batch the round-ignore branch would ever see anyway. solicited feeds
	// the scoring decision at that branch further down.
	solicited := peer.getHeadersOutstanding > 0
	if solicited {
		peer.getHeadersOutstanding--
	}

	headers := msg.Headers

	// net_processing.cpp HEADERS: "headers message size = %u" →
	// Misbehaving(pfrom, 20, "too-many-headers"). The wire decoder already
	// refuses to decode more than MaxBlockHeadersPerMsg, so this only fires
	// for a batch built in-process; it is the rule, kept where the rule lives.
	if len(headers) > MaxHeadersResults {
		return nil, scoreTooManyHeaders, nil
	}

	// net_processing.cpp HEADERS: "Nothing interesting. Stop asking this
	// peer for more headers."
	if len(headers) == 0 {
		return nil, 0, nil
	}

	// Deliberate divergence from both parents, and stricter than either.
	// net_processing.cpp indexes headers from every peer at all times. Legacy
	// netsync manager.go handleHeadersMsg tests the MODE, not the peer — it
	// disconnects anyone who sends headers while headers-first is off — but the
	// round it protects is one it drives with a single sync peer. This port
	// keeps the round single-peer without borrowing the disconnect: while the
	// round runs, a peer that does not hold the sync slot has its batch
	// ignored, so it can neither race the round nor push the tip past the
	// checkpoint the round is working toward. Its announcement still counts for
	// block availability, so it stays usable for download afterwards. Outside
	// the round every peer's headers are indexed, as net_processing.cpp does.
	//
	// An announcement-size ignored batch is unscored: junk headers cost a peer
	// nothing but our wire decode here, and SVNode's own HEADERS handler has
	// no solicitation check at all — accepting unsolicited announcements is
	// what makes block announcement work, so scoring them here would be
	// stricter than SVNode for the traffic pattern SVNode relies on.
	//
	// Task 11's policy: a BULK batch (len(headers) >= MaxBlocksToAnnounce) is
	// different. It cannot be an honest announcement — MAX_BLOCKS_TO_ANNOUNCE
	// is exactly the boundary net_processing.cpp itself uses to tell an
	// announcement from a bulk reply — so an unsolicited one is free to send
	// (a 37 byte inv earns a getheaders, and nothing here correlates a reply
	// to the request's HashStop, so one inv buys one free bulk decode of up
	// to MAX_HEADERS_RESULTS headers regardless of this policy). What this
	// scores is the batch that answers NEITHER an inv NOR any other
	// getheaders we sent — the peer that skips even that one-inv cost and
	// pushes bulk headers at us unprompted. scoreUnsolicitedBulkHeaders (see
	// its doc comment) scores that per occurrence, UNLESS the batch answers
	// a getheaders PeerManager actually sent this peer (solicited, above) —
	// which covers a bulk reply to BlockDownloader.OnInv's getHeadersFor, or
	// a peer that held the sync slot until moments ago and is still draining
	// its last continuation. The mechanism raises the cost of pure spam above
	// zero; it does not close the inv-first path, and closing that is beyond
	// this brief. THIS POLICY HAS NO SVNODE COUNTERPART; see the constant's
	// doc comment for the nearest relative, legacy netsync's mode-based
	// disconnect.
	//
	// The peer loop's equivalent gate for unrequested BLOCKS predates this:
	// those consume the shared admission budget and starve the sync peer,
	// where an unrequested headers batch costs only a decode and was already
	// bounded by MAX_HEADERS_RESULTS regardless of this policy.
	//
	// The cost of parking the announcement here — a peer stays unschedulable
	// until something resolves the hash — is what Phase 3 Task 4 answered, on
	// the OTHER side of the seam: PeerManager sweeps every peer's parked hash
	// through processBlockAvailability whenever the index grows, so this peer
	// becomes a download candidate the moment the round indexes what it
	// announced, with no further traffic from it.
	//
	// Soliciting getheaders from non-sync peers DURING a round was considered
	// for Task 4 and DECLINED (ledger M7), for two reasons. It has no SVNode
	// source: net_processing.cpp starts header sync only from SendBlockSync. And
	// mid-round solicitation would put headers from unsolicited peers back on the
	// round this gate exists to protect.
	//
	// SendBlockSync's own near-tip relaxation is NOT a third reason, though it
	// reads like one. This port refuses that relaxation while a round runs (the
	// !headersFirstMode conjunct in PeerEstablished), so mid-round — the only
	// case M7 covers — it yields no extra header peers at all.
	if hs.headersFirstMode && !peer.State.fSyncStarted {
		peer.State.updateBlockAvailability(hs.cfg.Index, headers[len(headers)-1].BlockHash())

		score := 0
		if !solicited && len(headers) >= MaxBlocksToAnnounce {
			score = scoreUnsolicitedBulkHeaders
		}

		return nil, score, nil
	}

	// net_processing.cpp HEADERS: when the first header's parent is unknown and
	// the batch is shorter than MAX_BLOCKS_TO_ANNOUNCE, treat it as a block
	// announcement that outran us — ask for the headers that fill the gap
	// instead of accepting an orphan, and count the event. A bulk batch with an
	// unknown parent falls through to AcceptBlockHeader instead, which fails it
	// with prev-blk-not-found (see acceptHeaders).
	if _, ok := hs.cfg.Index.Lookup(headers[0].PrevBlock); !ok && len(headers) < MaxBlocksToAnnounce {
		peer.nUnconnectingHeaders++

		score := 0
		if peer.nUnconnectingHeaders%MaxUnconnectingHeaders == 0 {
			score = scoreTooManyUnconnectedHeaders
		}

		// net_processing.cpp HEADERS: UpdateBlockAvailability with the last
		// header of the batch even here, so the peer counts as having that
		// block once the gap fills.
		peer.State.updateBlockAvailability(hs.cfg.Index, headers[len(headers)-1].BlockHash())

		return []wire.Message{hs.getHeaders(hs.cfg.Index.Locator())}, score, nil
	}

	// net_processing.cpp HEADERS: the whole batch must be one continuous
	// sequence, checked before any header is accepted →
	// Misbehaving(pfrom, 20, "disconnected headers"),
	// error("non-continuous headers sequence").
	//
	// Phase-2 ledger, Task 5 minor: "hoist header.BlockHash() once per loop
	// iteration (4 hashes/header -> 3)". This scan already hashes every
	// header, so it keeps what it computed and hands it on: acceptHeaders, the
	// proof-of-work check and the post-insert lookup all read hashes[i]
	// instead of hashing again, which makes it one BlockHash() per header for
	// the whole handler. The slice is discarded unread on the
	// non-continuous return below.
	var (
		hashLastBlock chainhash.Hash
		hashes        = make([]chainhash.Hash, len(headers))
	)

	for i, header := range headers {
		if hashLastBlock != (chainhash.Hash{}) && header.PrevBlock != hashLastBlock {
			return nil, scoreNonContinuousHeaders, nil
		}

		hashLastBlock = header.BlockHash()
		hashes[i] = hashLastBlock
	}

	return hs.acceptHeaders(peer, headers, hashes)
}

// acceptHeaders is the ProcessNewBlockHeaders half of the HEADERS handler: it
// validates and inserts each header, enforces the checkpoint, and decides what
// to ask for next.
//
// hashes carries each header's BlockHash(), computed once by the continuity
// scan in OnHeaders; hashes[i] belongs to headers[i].
func (hs *HeaderSync) acceptHeaders(peer *SyncPeer, headers []*wire.BlockHeader, hashes []chainhash.Hash) ([]wire.Message, int, error) {
	var (
		lastAccepted       chainhash.Hash
		lastAcceptedHeight int32
		accepted           int
		sawNewHeader       bool
		reachedCheckpoint  bool
	)

	// The peer holding the sync slot is the one driving the headers-first
	// round; only it can trip the checkpoint or advance the node-global
	// checkpoint state. Outside a round, roundOwner is false for everyone and
	// this machine follows net_processing.cpp instead.
	roundOwner := hs.headersFirstMode && peer.State.fSyncStarted
	checkpointRound := roundOwner && hs.nextCheckpoint != nil

	for i, header := range headers {
		hash := hashes[i]

		// AcceptBlockHeader (validation.cpp:6104-6117) checks for a duplicate
		// FIRST and returns early: a header already in mapBlockIndex is
		// answered without running CheckBlockHeader, the checkpoint fence or
		// ContextualCheckBlockHeader. That order is load-bearing here, not an
		// optimisation. Our own history is re-served to us constantly — the
		// start-at-the-parent locator asks for it, and any peer may echo it —
		// and the fence below refuses every NEW header below the last
		// checkpoint. Re-checking a header we already hold would turn that
		// echo into a disconnect.
		//
		// Whether the header is new is also what the round's progress rule at
		// the bottom of this function needs, since AddHeader reports a header
		// already in the index as connected.
		node, known := hs.cfg.Index.Lookup(hash)

		if !known {
			inserted, score, ok, err := hs.acceptHeader(header, hash)
			if err != nil {
				return nil, 0, err
			}

			// A refused header stops the batch where AcceptBlockHeader fails
			// it. The headers already inserted stay, as they do in the source,
			// and the handler returns before the availability update and before
			// any continuation getheaders — net_processing.cpp's early return
			// on a failed ProcessNewBlockHeaders.
			//
			// The score is whatever the failed check declares. It is non-zero
			// for the DoS refusals, which net_processing.cpp applies as
			// Misbehaving(pfrom, nDoS, "invalid header received"), and zero for
			// a state.Invalid refusal such as time-too-new. Reading `ok` rather
			// than testing the score is what keeps those two apart.
			if !ok {
				return nil, score, nil
			}

			node = inserted
			sawNewHeader = true
		}

		lastAccepted = node.Hash
		lastAcceptedHeight = node.Height
		accepted++

		// legacy netsync manager.go handleHeadersMsg: "Verify the header at
		// the next checkpoint height matches", and disconnect the peer when it
		// does not. The loop stops at the checkpoint either way.
		if checkpointRound && node.Height == hs.nextCheckpoint.Height {
			if !node.Hash.IsEqual(hs.nextCheckpoint.Hash) {
				return nil, 0, errors.New(errors.ERR_NETWORK_PEER_MALICIOUS,
					"svp2p: block header at height %d/hash %s does not match expected checkpoint hash %s",
					node.Height, node.Hash, hs.nextCheckpoint.Hash, ErrCheckpointMismatch)
			}

			reachedCheckpoint = true

			break
		}
	}

	if accepted == 0 {
		return nil, 0, nil
	}

	// net_processing.cpp HEADERS: headers that connect clear the unconnecting
	// counter ("resetting nUnconnectingHeaders (%d -> 0)"), so a peer that was
	// briefly behind is not scored for it later.
	peer.nUnconnectingHeaders = 0

	// net_processing.cpp HEADERS: UpdateBlockAvailability with the last header
	// of the batch.
	peer.State.updateBlockAvailability(hs.cfg.Index, lastAccepted)

	if reachedCheckpoint {
		// legacy netsync manager.go: on reaching a checkpoint, run the next
		// round of headers up to the checkpoint after it; when none is left,
		// "Reached the final checkpoint -- switching to normal mode".
		hs.nextCheckpoint = hs.findNextHeaderCheckpoint(hs.nextCheckpoint.Height)
		if hs.nextCheckpoint == nil {
			hs.headersFirstMode = false
		}
	}

	if roundOwner && hs.headersFirstMode {
		// The round must move forward. AddHeader reports a header we already
		// hold as connected, so a peer replaying an old batch would otherwise
		// draw the same getheaders from us for ever, unscored. Legacy netsync
		// catches that peer on its header-list anchor — a replayed header does
		// not connect to the expected parent — and disconnects it.
		//
		// A batch counts as progress if it carried any header the index did not
		// already hold, or if it ended above the height the last request went
		// out from. Both clauses are needed. Height alone rejects an honest
		// fork reply: our tip may sit on a taller branch, so a peer serving its
		// real chain answers below that height with headers that are all new.
		// Novelty alone rejects the honest echo of our own tip that the
		// start-at-the-parent locator asks for. Only a replay — nothing new and
		// no higher — fails both.
		if !sawNewHeader && lastAcceptedHeight <= hs.roundAnchorHeight {
			return nil, 0, errors.New(errors.ERR_NETWORK_INVALID_RESPONSE,
				"svp2p: headers batch ended at height %d with nothing new, not past the height %d it was requested from",
				lastAcceptedHeight, hs.roundAnchorHeight, ErrHeadersNoProgress)
		}

		// legacy netsync manager.go handleHeadersMsg: while the headers-first
		// round runs and the checkpoint is not reached yet, "request the next
		// batch of headers starting from the latest known header and ending
		// with the next checkpoint" — unconditionally, with no batch-length
		// gate. Without this, a peer that answers with a short batch below the
		// checkpoint would leave the round with no outstanding request and no
		// way to resume.
		//
		// The anchor only ever rises. A batch that carried something new but
		// ended below the anchor — an honest fork reply, which the progress
		// test above deliberately admits — must not lower the bar the next
		// batch is measured against, or a peer could walk the anchor down one
		// novel-but-lower batch at a time and then replay everything above it.
		if lastAcceptedHeight > hs.roundAnchorHeight {
			hs.roundAnchorHeight = lastAcceptedHeight
		}

		return []wire.Message{hs.getHeaders(hs.locatorFrom(lastAccepted))}, 0, nil
	}

	// net_processing.cpp HEADERS: "Headers message had its maximum size; the
	// peer may have more headers" → getheaders from the last header received.
	// A shorter batch means the peer has nothing more, so we ask for nothing.
	if len(headers) == MaxHeadersResults {
		return []wire.Message{hs.getHeaders(hs.locatorFrom(lastAccepted))}, 0, nil
	}

	return nil, 0, nil
}

// acceptHeader is the body of validation.cpp AcceptBlockHeader
// (validation.cpp:6087-6154) for a header the index does not already hold:
// CheckBlockHeader, then the previous-block lookup, then the checkpoint fence,
// then ContextualCheckBlockHeader's difficulty rule, then its time-too-new
// timestamp cap, then the mapBlockIndex insert. The order is the source's, and
// it matters — the fence and the difficulty check both read the parent, and
// neither may run before the cheap proof-of-work check has made the header cost
// something to produce.
//
// accepted reports whether the header was inserted. When it is false the
// header is refused and the batch stops, and score carries the DoS value the
// failed check declares — which is legitimately ZERO for a state.Invalid
// refusal such as time-too-new, where SVNode refuses the header but neither
// scores nor disconnects the peer. err is set only for a refusal that must
// disconnect the peer.
//
// hash is header.BlockHash(), passed in rather than recomputed (Task 5 minor,
// see OnHeaders).
func (hs *HeaderSync) acceptHeader(header *wire.BlockHeader, hash chainhash.Hash) (node HeaderNode, score int, accepted bool, err error) {
	// AcceptBlockHeader calls CheckBlockHeader before the mapBlockIndex
	// insert; HeaderIndex.AddHeader deliberately carries only the insert, so
	// the PoW check belongs here.
	if !hs.checkBlockHeaderPoW(header, hash) {
		return HeaderNode{}, scoreInvalidHeader, false, nil
	}

	// FindPreviousBlockIndex. AcceptBlockHeader answers a missing parent with
	// "prev-blk-not-found" → DoS(10). Reachable only for the first header of a
	// bulk batch whose parent we do not have; the continuity scan in OnHeaders
	// guarantees the rest of the batch attaches.
	parent, ok := hs.cfg.Index.Lookup(header.PrevBlock)
	if !ok {
		return HeaderNode{}, scorePrevBlkNotFound, false, nil
	}

	// fCheckpointsEnabled && !CheckIndexAgainstCheckpoint(...).
	fence := hs.lastCheckpointInIndex()
	if !checkIndexAgainstCheckpoint(parent, fence) {
		return HeaderNode{}, 0, false, errors.New(errors.ERR_NETWORK_PEER_MALICIOUS,
			"svp2p: header at height %d forks below the last checkpoint we hold at height %d",
			parent.Height+1, fence.Height, ErrCheckpointFork)
	}

	// ContextualCheckBlockHeader (validation.cpp:5896-5901): "Check proof of
	// work" — the header's nBits must be exactly what GetNextWorkRequired
	// demands for this parent → state.DoS(100, ..., "bad-diffbits").
	//
	// validated is false where this port has no answer: a parent in a historic
	// difficulty era, or one whose DAA window is short. Both are documented on
	// GetNextWorkRequired, and both mean the header is accepted on its own
	// claimed target alone — see the scope note at the top of difficulty.go
	// before assuming otherwise.
	headerTime := header.Timestamp.Unix()

	if want, validated := GetNextWorkRequired(hs.cfg.Index, hs.cfg.Params, parent, headerTime); validated &&
		header.Bits != want {
		return HeaderNode{}, scoreBadDiffBits, false, nil
	}

	// ContextualCheckBlockHeader's "Check timestamp against prev" —
	// time-too-old, block.GetBlockTime() <= pindexPrev->GetMedianTimePast()
	// (validation.cpp:5904-5907) — SITS HERE IN THE SOURCE AND IS NOT PORTED.
	// It needs median-time-past over the header index, which is a piece of
	// machinery this index does not have (the 11-block median CBlockIndex
	// caches at insert). Booked as a named Phase 3 follow-up: "port
	// median-time-past to HeaderIndex and add the time-too-old rule". Until it
	// lands, a header may claim any timestamp at or before its parent's, so
	// the timestamp sequence of an indexed branch is not monotonic.
	//
	// ContextualCheckBlockHeader "Check timestamp"
	// (validation.cpp:5909-5913): a header more than MAX_FUTURE_BLOCK_TIME
	// past the adjusted time is refused with
	// state.Invalid(false, REJECT_INVALID, "time-too-new", ...).
	//
	// state.Invalid is DoS(0), and net_processing.cpp calls Misbehaving only
	// when nDoS > 0, so this refusal neither scores nor disconnects the peer —
	// deliberately, because it is the ONLY refusal in this function that a
	// well-behaved peer can trigger and that time alone will undo. The header
	// may be perfectly valid a few minutes from now, and the peer re-announces
	// it; scoring an honest peer for our own clock would be wrong.
	if headerTime > hs.cfg.AdjustedTime()+MaxFutureBlockTime {
		return HeaderNode{}, 0, false, nil
	}

	// AddToBlockIndex.
	connected, err := hs.cfg.Index.AddHeader(header)
	if err != nil {
		return HeaderNode{}, 0, false, err
	}

	// Defensive: the parent lookup above already proved the header attaches,
	// and nothing writes to the index between the two calls, so this cannot
	// fire. It scores the source's value rather than inserting nothing and
	// carrying on.
	if !connected {
		return HeaderNode{}, scorePrevBlkNotFound, false, nil
	}

	node, ok = hs.cfg.Index.Lookup(hash)
	if !ok {
		return HeaderNode{}, 0, false, errors.New(errors.ERR_SERVICE_ERROR,
			"svp2p: header %s vanished from the index directly after insert", hash)
	}

	return node, 0, true, nil
}

// checkIndexAgainstCheckpoint is validation.cpp CheckIndexAgainstCheckpoint
// (validation.cpp:5856-5884), second half: "Don't accept any forks from the
// main chain prior to last checkpoint." A new header whose own height falls
// below the last checkpoint we hold is refused, because our chain already
// reaches that checkpoint and anything branching in below it is a fork we can
// never adopt.
//
// The source's first half — Checkpoints::CheckBlock, the hash-must-match rule
// at a checkpoint height — is not repeated here. This port already carries it
// in acceptHeaders, from legacy netsync manager.go, where it is scoped to the
// headers-first round and answers with ErrCheckpointMismatch. Running it twice
// would give one rule two different refusal shapes.
//
// WHAT THAT COSTS. The two rules cover different sets, and the omission leaves
// a real gap rather than a redundancy. Take a checkpoint height ABOVE the
// highest checkpoint currently in the index — one the sync has not reached yet:
//
//   - this fence does not refuse a wrong-hash header there, because the header's
//     height is above the fence height, which is all this rule tests;
//   - the round's check does not see it either, unless the header arrives from
//     the peer holding the sync slot while headers-first mode is running.
//
// So a NON-SYNC peer's header claiming a wrong hash at an unreached checkpoint
// height is checked by neither rule, and is indexed if it satisfies the
// difficulty and timestamp gates. It cannot mislead the sync round, which
// re-checks the checkpoint when it gets there, and it cannot take the tip
// without out-working the honest chain. It does cost an index entry. Closing it
// means porting CheckBlock here and accepting two refusal shapes for one rule,
// or unifying both onto this one — a deliberate design change, not an edit.
//
// It runs for every new header, in a round or outside one, which is what
// bounds HeaderIndex growth below the final checkpoint: a peer cannot spend a
// map entry on a header at a height our chain has already settled.
//
// The source resolves the fence itself with Checkpoints::GetLastCheckpoint;
// here the caller resolves it through lastCheckpointInIndex and passes it in,
// so the rule stays a pure function of the two values it compares and the
// index is read once per header. A nil checkpoint is both "we have reached
// none of them" and the source's fCheckpointsEnabled being off, which
// DisableCheckpoints expresses by leaving hs.checkpoints empty.
func checkIndexAgainstCheckpoint(parent HeaderNode, checkpoint *chaincfg.Checkpoint) bool {
	if checkpoint == nil {
		return true
	}

	// int32_t nHeight = pindexPrev->GetHeight() + 1;
	return parent.Height+1 >= checkpoint.Height
}

// lastCheckpointInIndex is checkpoints.cpp Checkpoints::GetLastCheckpoint
// (checkpoints.cpp:24-36): the highest checkpoint whose hash the index holds,
// or nil when we have reached none of them. It walks the list in reverse, as
// the source does with boost::adaptors::reverse over the ordered map.
//
// The fence therefore tightens as the chain grows: it does nothing on a fresh
// node, and settles at the final checkpoint once we are past it. That is the
// source's behavior, and it is why the fence cannot lock out a node that is
// still syncing toward its first checkpoint.
//
// The reverse walk assumes hs.checkpoints is ASCENDING by height — the source
// gets that from std::map<int32_t, uint256>, a Go slice does not. NewHeaderSync
// refuses to build a machine from an unordered list, so the assumption is
// enforced at construction rather than trusted here; without it this function
// could answer a checkpoint that is not the highest one held, and fence at the
// wrong height on a rule that disconnects.
func (hs *HeaderSync) lastCheckpointInIndex() *chaincfg.Checkpoint {
	for i := len(hs.checkpoints) - 1; i >= 0; i-- {
		checkpoint := &hs.checkpoints[i]
		if checkpoint.Hash == nil {
			continue
		}

		if _, ok := hs.cfg.Index.Lookup(*checkpoint.Hash); ok {
			return checkpoint
		}
	}

	return nil
}

// checkBlockHeaderPoW is pow.cpp CheckProofOfWork, the cheap per-header check
// validation.cpp CheckBlockHeader runs: the target the header's own nBits
// claims must be in range, and the header hash must be at or below it. It
// needs no ancestor context.
//
// Phase 2 left the contextual difficulty check unported, which let a peer feed
// difficulty-1 headers above the final checkpoint at no cost and grow
// HeaderIndex by one unevictable map entry each. Phase 3 Task 2 narrowed that:
// acceptHeader now runs checkIndexAgainstCheckpoint, GetNextWorkRequired and
// the time-too-new cap on every new header from every peer, in a round or
// outside one.
//
// The honest scope of that, because "closed" would overstate it:
//
//   - mainnet past the final checkpoint (945000) — its steady state — is fully
//     bounded: every header there is post-DAA and costs real difficulty.
//   - mainnet and STN during IBD are bounded below the highest checkpoint the
//     index holds and above daaHeight, with a transient unpriced band between
//     the two that closes as the sync passes each checkpoint. A node holding no
//     checkpoint yet has no lower bound.
//   - testnet, teratestnet and tstn are NOT bounded by difficulty at all: their
//     min-difficulty rule makes cheap headers legitimate. time-too-new caps how
//     DEEP a cheap chain can run (five headers, floor(7200/1201)) but not how
//     WIDE, so a peer may still put many cheap siblings on one parent. SVNode
//     has the same exposure on the same networks.
//
// Still unported, deliberately: the historic difficulty eras (see difficulty.go)
// and the time-too-old rule (see acceptHeader).
//
// hash is header.BlockHash(). It is a parameter rather than a local because
// every caller already holds it (Task 5 minor, see OnHeaders).
func (hs *HeaderSync) checkBlockHeaderPoW(header *wire.BlockHeader, hash chainhash.Hash) bool {
	var bits model.NBit

	binary.LittleEndian.PutUint32(bits[:], header.Bits)

	// NBit.CalculateTarget already carries arith_uint256::SetCompact's
	// negative and overflow rejection, returning a zero target for both.
	target := bits.CalculateTarget()
	if target.Sign() <= 0 {
		return false
	}

	// pow.cpp: bnTarget > powLimit is out of range.
	if hs.cfg.Params.PowLimit != nil && target.Cmp(hs.cfg.Params.PowLimit) > 0 {
		return false
	}

	bigEndian := make([]byte, len(hash))
	for i := range hash {
		bigEndian[len(hash)-1-i] = hash[i]
	}

	return new(big.Int).SetBytes(bigEndian).Cmp(target) <= 0
}

// findNextHeaderCheckpoint returns the next checkpoint after height, or nil
// when height is already at or past the final one, or when there are no
// checkpoints. Ported from legacy netsync manager.go.
func (hs *HeaderSync) findNextHeaderCheckpoint(height int32) *chaincfg.Checkpoint {
	if len(hs.checkpoints) == 0 {
		return nil
	}

	finalCheckpoint := &hs.checkpoints[len(hs.checkpoints)-1]
	if height >= finalCheckpoint.Height {
		return nil
	}

	nextCheckpoint := finalCheckpoint

	for i := len(hs.checkpoints) - 2; i >= 0; i-- {
		if height >= hs.checkpoints[i].Height {
			break
		}

		nextCheckpoint = &hs.checkpoints[i]
	}

	return nextCheckpoint
}

// isSyncCandidate reports whether we may sync from this peer, ported from
// legacy netsync manager.go isSyncCandidate.
func (hs *HeaderSync) isSyncCandidate(peer *SyncPeer) bool {
	// Regression test is special: the regression tool is not a full node and
	// still needs to be a sync candidate, so the service flag is not required
	// and a localhost restriction takes its place.
	if hs.isRegtest() {
		if hs.cfg.AllowSyncCandidateFromLocalPeers {
			return true
		}

		host, _, err := net.SplitHostPort(peer.Addr)
		if err != nil {
			return false
		}

		return host == "127.0.0.1" || host == "localhost"
	}

	// Outside regtest, a peer that is not a full node cannot serve us blocks.
	return peer.Services&wire.SFNodeNetwork == wire.SFNodeNetwork
}

// isRegtest reports whether the active params are the regression network. The
// legacy port compares the params pointer with &chaincfg.RegressionNetParams;
// this machine compares the network magic instead, so a copied Params value is
// still recognized. The legacy pointer comparison is an artifact of its test
// harness, not a protocol rule.
func (hs *HeaderSync) isRegtest() bool {
	return hs.cfg.Params.Net == wire.RegTestNet
}

// syncStartLocator builds the locator for the initial getheaders, and returns
// the height it was built from for the round anchor.
// net_processing.cpp SendMessages: "If possible, start at the block preceding
// the currently best known header. This ensures that we always get a
// non-empty list of headers back as long as the peer is up-to-date." That
// non-empty list starts with our own tip, so the anchor is the parent's
// height: a peer at our height answers with one header we already have and is
// still making the only progress it can.
func (hs *HeaderSync) syncStartLocator() (locator []chainhash.Hash, anchorHeight int32) {
	tipHash, tipHeight := hs.cfg.Index.Tip()

	if node, ok := hs.cfg.Index.Lookup(tipHash); ok && node.ParentHash != (chainhash.Hash{}) {
		if locator := hs.cfg.Index.LocatorFrom(node.ParentHash); locator != nil {
			return locator, node.Height - 1
		}
	}

	return hs.cfg.Index.Locator(), tipHeight
}

// locatorFrom builds a locator for hash, falling back to the tip locator if
// the hash left the index between the accept and this call.
func (hs *HeaderSync) locatorFrom(hash chainhash.Hash) []chainhash.Hash {
	if locator := hs.cfg.Index.LocatorFrom(hash); locator != nil {
		return locator
	}

	return hs.cfg.Index.Locator()
}

// getHeaders builds the getheaders message for a locator. While headers-first
// mode runs, hashStop is the next checkpoint hash — the legacy netsync scheme
// that asks a peer for exactly the headers up to the next checkpoint.
// Otherwise hashStop is the zero hash net_processing.cpp always sends.
func (hs *HeaderSync) getHeaders(locator []chainhash.Hash) *wire.MsgGetHeaders {
	var hashStop chainhash.Hash
	if hs.headersFirstMode && hs.nextCheckpoint != nil {
		hashStop = *hs.nextCheckpoint.Hash
	}

	return newGetHeaders(locator, hashStop)
}

// newGetHeaders builds a getheaders message from a locator and a hashStop.
//
// Phase-2 ledger, Task 6 minor M6: "locator-copy duplication". HeaderSync's
// getHeaders above and BlockDownloader.getHeadersFor (blockdownload.go) each
// carried this same loop; it lives here once now. hashStop stays the caller's
// decision, because the two callers answer different questions: HeaderSync
// sends the next checkpoint while headers-first mode runs, and the inv
// answerer sends the hash the peer just announced.
//
// The loop copies each hash into its own variable on purpose.
// wire.MsgGetHeaders holds []*chainhash.Hash, so appending &locator[i] would
// publish pointers into the caller's slice and let a later write to it rewrite
// an already-built message.
//
// A locator holds one hash per height step with an exponential tail, so it
// stays far below MaxBlockLocatorsPerMsg (500) at any reachable height.
func newGetHeaders(locator []chainhash.Hash, hashStop chainhash.Hash) *wire.MsgGetHeaders {
	msg := wire.NewMsgGetHeaders()
	msg.ProtocolVersion = wire.ProtocolVersion
	msg.HashStop = hashStop

	for i := range locator {
		hash := locator[i]
		msg.BlockLocatorHashes = append(msg.BlockLocatorHashes, &hash)
	}

	return msg
}
