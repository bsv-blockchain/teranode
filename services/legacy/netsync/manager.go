// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package netsync provides network synchronization functionality for the legacy Bitcoin protocol.
// It handles peer coordination, block synchronization, and transaction relay operations.
package netsync

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	teranodeblockchain "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"
)

const (
	// defaultMaxInFlightBlocks is the default maximum number of blocks that
	// should be in the request queue for headers-first mode. This is the
	// starting value for small blocks, and will be dynamically adjusted down
	// based on observed block sizes to avoid memory issues with large blocks.
	defaultMaxInFlightBlocks = 20

	// minInFlightBlockWeight is the minimum prefetch-budget weight charged for an
	// admitted block, regardless of how small it serializes. Each in-flight block
	// costs a fixed overhead beyond its bytes — an awaitBlockResult goroutine
	// (stack), a reply channel, and the decoded block wrapper. Charging only the
	// serialized size would let a flood of minimal (e.g. ~81-byte, zero-tx) blocks
	// admit a huge number of concurrent goroutines within the byte budget; the
	// floor bounds the in-flight block count (≈ budget/minInFlightBlockWeight) and
	// therefore the goroutine count. It is well below any real small-block size,
	// so it never reduces prefetch depth for legitimate traffic.
	minInFlightBlockWeight = 64 * 1024

	// maxBlockQueueSlots caps the block-queue channel capacity so a misconfigured
	// (e.g. multi-TB) prefetch budget cannot size an enormous channel backing
	// array. 65536 slots covers budgets up to 4 GiB at the weight floor before the
	// clamp binds; the channel holds pointers, so this is ~512 KiB.
	maxBlockQueueSlots = 65536

	// defaultBlockProcessingStallTimeout bounds how long localReadBackpressured
	// keeps suppressing the sync-peer stall check while the block backlog is
	// non-empty but not advancing (see lastBacklogProgress). It is only the
	// fallback for settings.Legacy.PeerProcessingTimeout — the pre-prefetch
	// per-message watchdog whose coverage this progress-aware rule restores —
	// used when a SyncManager has no settings wired (unit tests) or the setting
	// is unset. Kept equal to that setting's own default so behaviour matches
	// production when it is configured.
	defaultBlockProcessingStallTimeout = 3 * time.Minute

	// maxNetworkViolations is the max number of network violations a
	// sync peer can have before a new sync peer is found.
	maxNetworkViolations = 3

	// maxRejectedTxns is the maximum number of rejected transactions
	// hashes to store in memory.
	maxRejectedTxns = 10_000

	// blockFailureBackoffMaxTracked bounds the per-block transient-failure
	// backoff map (#1187). Legacy sync only has a handful of failing block
	// hashes in flight, but capping the map guarantees a pathological stream of
	// distinct failing hashes can never grow it without bound (mirrors the
	// WithMaxSize bound on orphanTxs).
	blockFailureBackoffMaxTracked = 1024

	// recentlyFailedBlocksTTL is how long a block hash that failed to
	// store/validate is remembered so its descendants can be short-circuited
	// instead of each re-running HandleBlockDirect and logging a misleading
	// "previous block NOT_FOUND" ERROR (#1333). Entries self-evict after this
	// window and are deleted on a successful (re)process, so a transiently
	// failed parent that later stores unblocks its descendants automatically.
	// Independent of the #1187 backoff knobs so the cascade suppression works
	// even when that backoff is disabled.
	recentlyFailedBlocksTTL = 10 * time.Minute

	// maxRequestedBlocks is the maximum number of requested block
	// hashes to store in memory.
	maxRequestedBlocks = wire.MaxInvPerMsg

	// maxRequestedTxns is the maximum number of requested transactions
	// hashes to store in memory.
	maxRequestedTxns = wire.MaxInvPerMsg

	// maxLastBlockTime is the longest time in seconds that we will
	// stay with a sync peer while below the current blockchain height.
	// Set to 3 minutes.
	maxLastBlockTime = 60 * 3 * time.Second

	// maxMsgQueuePerPeer is the maximum number of messages that can be
	// queued for a peer. This is the size if the msgChan buffer.
	maxMsgQueueSize = 10_000

	// syncPeerTickerInterval is how often we check the current
	// syncPeer. Set to 30 seconds.
	syncPeerTickerInterval = 30 * time.Second

	// failedToGetBestBlockHeaderMsg is logged when the best block header
	// cannot be retrieved from the blockchain client.
	failedToGetBestBlockHeaderMsg = "Failed to get best block header: %v"

	// failedToConvertBlockHeightInt32Msg is logged when a block height cannot
	// be safely converted to an int32.
	failedToConvertBlockHeightInt32Msg = "failed to convert block height to int32: %v"

	// unexpectedFailureAddingInventoryMsg is logged when adding an inventory
	// vector to a getdata message fails unexpectedly.
	unexpectedFailureAddingInventoryMsg = "Unexpected failure when adding inventory to getdata message: %v"
)

// zeroHash is the zero-value hash (all zeros).  It is defined as a convenience.
var zeroHash chainhash.Hash

// ErrDuplicateBlockInFlight is the benign sentinel AcquireBlockPrefetch returns
// when the requested block hash is already admitted (or parked waiting for
// budget): a duplicate is dropped at admission rather than reserving a second
// slice of budget. The single production caller (OnBlock) matches it with
// errors.Is and drops the duplicate without disconnecting — it is the only
// ServiceError AcquireBlockPrefetch ever returns, so the code-based match is
// unambiguous there.
var ErrDuplicateBlockInFlight = errors.NewServiceError("duplicate block already in flight")

// newPeerMsg signifies a newly connected peer to the block handler.
type newPeerMsg struct {
	peer  *peerpkg.Peer
	reply chan struct{}
}

// blockMsg packages a bitcoin block message and the peer it came from together
// so the block handler has access to that information.
type blockMsg struct {
	block *bsvutil.Block
	peer  *peerpkg.Peer
	reply chan error
}

// headersMsg packages a bitcoin headers message and the peer it came from
// together so the block handler has access to that information.
type headersMsg struct {
	headers *wire.MsgHeaders
	peer    *peerpkg.Peer
}

// donePeerMsg signifies a newly disconnected peer to the block handler.
type donePeerMsg struct {
	peer  *peerpkg.Peer
	reply chan struct{}
}

// txMsg packages a bitcoin tx message and the peer it came from together
// so the block handler has access to that information.
type txMsg struct {
	tx    *bsvutil.Tx
	peer  *peerpkg.Peer
	reply chan struct{}
}

// getSyncPeerMsg is a message type to be sent across the message channel for
// retrieving the current sync peer.
type getSyncPeerMsg struct {
	reply chan int32
}

// isCurrentMsg is a message type to be sent across the message channel for
// requesting whether or not the sync manager believes it is synced with the
// currently connected peers.
type isCurrentMsg struct {
	reply chan bool
}

// pauseMsg is a message type to be sent across the message channel for
// pausing the sync manager.  This effectively provides the caller with
// exclusive access over the manager until a receive is performed on the
// unpause channel.
type pauseMsg struct {
	unpause <-chan struct{}
}

// headerNode is used as a node in a list of headers that are linked together
// between checkpoints.
type headerNode struct {
	height int32
	hash   *chainhash.Hash
	// listEpoch is the header list this node was made for. A node handed back
	// to the list after the list has been thrown away and started again — a
	// parked block given up on after its sync peer was rotated — carries an
	// older epoch than the list it is being handed back to, and must not be put
	// into it. See rewindHeaderCursor and SyncManager.headerListEpoch.
	listEpoch uint64
	// isAnchor marks a node that is in the list only so the next header can
	// prove it links: its block is already in this node's chain, and no peer
	// will ever deliver it to us again. resetHeaderStateLocked pushes one when
	// the list is rebuilt, and checkpointBlockCommitted marks the checkpoint
	// node it leaves behind to anchor the round that follows. Nothing else sets
	// it, and it is never cleared — an anchor stops being one by leaving the
	// list.
	//
	// It is recorded on the node rather than worked out from the node's
	// position, because position lies. Two of this package's own mechanisms put
	// something other than the anchor at the front of the list: a rewind after a
	// checkpoint transition inserts a lower header ahead of it
	// (reinsertHeaderLocked), and a peer can deliver the anchor early, which
	// takes it out of the list altogether. So "the front" and "the anchor" are
	// not the same node, and the one place that has to tell them apart is
	// removeHeaderAnchorLocked.
	isAnchor bool
}

// peerSyncState stores additional information that the SyncManager tracks
// about a peer.
type peerSyncState struct {
	syncCandidate bool
	requestQueue  *txmap.SyncedSlice[wire.InvVect]
	requestedTxns *expiringmap.ExpiringMap[chainhash.Hash, struct{}]

	// requestQueueMu serialises the drain of requestQueue.
	//
	// Inv messages are dispatched one goroutine per message (blockHandler's
	// "go sm.handleInvMsg(msg)"), so two drains for the same peer share this one
	// queue. SyncedSlice synchronises each call but not a pair of them, and the
	// drain reads the front without consuming it and consumes it several
	// branches later. Interleaved, two drains peek the same item, both deal with
	// it, and the second Shift throws away the item behind it without ever
	// looking at it. That is the exact loss the peek was introduced to prevent:
	// the queue is the only record that a block was announced, and outside
	// headers-first mode an inv is not guaranteed to come again.
	//
	// A compare-before-shift would close that. It would not close the mirror,
	// where both drains peek the same item, both pass RequestedWithin before
	// either Add lands, and the peer is asked twice for one hash: the first copy
	// discharges the obligation in handleBlockMsg and the second is disconnected
	// as unrequested. A lock closes both, so it is a lock.
	//
	// Leaf lock, held only for the drain loop. Nothing else takes it, and the
	// getdata send is deliberately outside it.
	requestQueueMu sync.Mutex

	// assocReadBytes and assocReadBytesLastTick are two consecutive samples of
	// this peer's association-wide read counter, taken on the frontier ticker.
	// The difference over one tick is how fast the peer is pulling bytes in.
	//
	// syncPeerState keeps the same pair, but only for the sync peer, and the
	// rotation decision needs it there. This copy exists because the frontier
	// race has to ask the question of whichever peer owes the stuck block, which
	// under the fan-out is routinely not the sync peer — svnode asks it of every
	// in-flight source. Sampled here rather than read from the peer layer so that
	// one sampler answers for every peer.
	assocReadBytes         atomic.Uint64
	assocReadBytesLastTick atomic.Uint64
	throughputTicks        atomic.Uint64

	// bestKnownHeight is the highest block height this peer has demonstrated it
	// has: the height it announced at handshake, raised whenever it delivers a
	// block, announces one we already have, or hands us headers.
	//
	// This is deliberately a SECOND copy of information Peer already carries in
	// LastBlock(). It is keyed by peerSyncState rather than by *Peer because the
	// multi-peer block scheduler that follows asks "which of the peers I am
	// tracking claims to have block N" while walking peerStates, and it is
	// netsync's own record rather than the peer package's.
	//
	// It is atomic because a *peerSyncState is shared by pointer across the
	// blockHandler goroutine and the per-message inv and headers handlers, each
	// of which runs on its own goroutine. Only noteBestKnownHeight writes it.
	bestKnownHeight atomic.Int32

	// demotedUntil is the UnixNano instant before which this peer must not be
	// re-elected sync peer, stamped when it is demoted for stalling.
	//
	// It is the replacement for the disconnect that used to keep a stalled sync
	// peer out of the election that runs immediately afterwards. startSync picks
	// at random from connected sync candidates, and its only liveness test is
	// Connected(), which the demoted peer still passes.
	//
	// Atomic for the same reason bestKnownHeight is: a *peerSyncState is shared
	// by pointer across the blockHandler goroutine and the per-message handlers.
	// Nothing sweeps it — the stamp is only ever read.
	demotedUntil atomic.Int64
}

// noteDemotedFor bars this peer from election as sync peer for d.
func (s *peerSyncState) noteDemotedFor(d time.Duration) {
	s.demotedUntil.Store(time.Now().Add(d).UnixNano())
}

// inDemotionCooldown reports whether this peer was demoted recently enough that
// it should not be elected sync peer again yet.
func (s *peerSyncState) inDemotionCooldown() bool {
	until := s.demotedUntil.Load()

	return until > 0 && time.Now().UnixNano() < until
}

// clearDemotionCooldown makes the peer immediately electable again. Tests use it
// to step past a cooldown without sleeping; nothing in the service calls it.
func (s *peerSyncState) clearDemotionCooldown() {
	s.demotedUntil.Store(0)
}

// sampleThroughput takes this tick's reading of the peer's association-wide read
// counter, keeping the previous one to subtract from.
func (s *peerSyncState) sampleThroughput(p *peerpkg.Peer) {
	if s == nil || p == nil {
		return
	}

	s.assocReadBytesLastTick.Store(s.assocReadBytes.Load())
	s.assocReadBytes.Store(p.AssociationReadBytes())
	s.throughputTicks.Add(1)
}

// isPullingBytes reports whether this peer's association brought data in over
// the last tick at or above minSpeed bytes per second.
//
// False before two samples exist, which reads as "not downloading". That is the
// safe bias for the frontier race: the cost of racing a peer that turned out to
// be fine is one duplicate block, and the cost of not racing a silent one is the
// stall the race exists to break.
func (s *peerSyncState) isPullingBytes(minSpeed uint64) bool {
	if s == nil || s.throughputTicks.Load() < 2 {
		return false
	}

	cur := s.assocReadBytes.Load()
	prev := s.assocReadBytesLastTick.Load()

	// AssociationReadBytes sums the streams present at sample time, so a stream
	// dying between samples drops the total. A decrease is the opposite of
	// progress, not a wrapped-around healthy figure.
	if cur < prev {
		return false
	}

	delta := cur - prev

	// Bytes must have moved, tested apart from the threshold: minSpeed may be
	// configured as 0, and a bare comparison against 0 would make a peer that
	// sent nothing look busy and switch the race off altogether.
	if delta == 0 {
		return false
	}

	// Multiplied rather than divided: `delta/seconds >= minSpeed` truncates, and
	// it divides by a figure that is only ever a few seconds, so a shorter
	// interval would floor it to zero and panic. The sibling on syncPeerState
	// still has that shape; this one does not need it.
	seconds := uint64(frontierCheckInterval.Seconds())
	if seconds == 0 {
		seconds = 1
	}

	return delta >= minSpeed*seconds
}

// noteBestKnownHeight raises the peer's best known height to h, and never lowers
// it. A compare-and-swap loop rather than a load-then-store, because concurrent
// reports would otherwise let a lower one overwrite a higher one.
func (s *peerSyncState) noteBestKnownHeight(h int32) {
	for {
		cur := s.bestKnownHeight.Load()
		if h <= cur {
			return
		}

		if s.bestKnownHeight.CompareAndSwap(cur, h) {
			return
		}
	}
}

// BestKnownHeight returns the highest block height this peer has demonstrated it
// has. Read it through here rather than touching the field, so how it is stored
// stays private to this type.
func (s *peerSyncState) BestKnownHeight() int32 {
	return s.bestKnownHeight.Load()
}

// syncPeerState stores additional info about the sync peer.
type syncPeerState struct {
	mu                sync.RWMutex // Protects all fields
	recvBytes         uint64
	recvBytesLastTick uint64
	// assocReadBytes tracks byte-granular read progress across the sync peer's
	// whole association (GENERAL + DATA1). Unlike recvBytes (the GENERAL peer's
	// message-granular total) it advances while a large block is still
	// streaming in on DATA1, so it can tell an active fat-block download apart
	// from a stalled peer.
	assocReadBytes         uint64
	assocReadBytesLastTick uint64
	lastBlockTime          time.Time
	violations             int
	ticks                  uint64
}

// validNetworkSpeed checks if the peer is slow and
// returns an integer representing the number of network
// violations the sync peer has.
func (sps *syncPeerState) validNetworkSpeed(minSyncPeerNetworkSpeed uint64) int {
	sps.mu.Lock()
	defer sps.mu.Unlock()

	// Fresh sync peer. We need another tick.
	if sps.ticks == 0 {
		return 0
	}

	// Number of bytes received in the last tick.
	recvDiff := sps.recvBytes - sps.recvBytesLastTick

	// If the peer was below the threshold, mark a violation and return.
	if recvDiff/uint64(syncPeerTickerInterval.Seconds()) < minSyncPeerNetworkSpeed {
		sps.violations++
		return sps.violations
	}

	// No violation found, reset the violation counter.
	sps.violations = 0

	return sps.violations
}

type orphanTxAndParents struct {
	tx      *bt.Tx
	parents *txmap.SyncedMap[chainhash.Hash, struct{}] // map of parent tx hashes
	addedAt time.Time
}

// updateNetwork updates the received bytes. Just tracks 2 ticks
// worth of network bandwidth.
func (sps *syncPeerState) updateNetwork(syncPeer *peerpkg.Peer) {
	sps.mu.Lock()
	defer sps.mu.Unlock()

	sps.ticks++
	sps.recvBytesLastTick = sps.recvBytes
	sps.recvBytes = syncPeer.BytesReceived()

	sps.assocReadBytesLastTick = sps.assocReadBytes
	sps.assocReadBytes = syncPeer.AssociationReadBytes()
}

// hasHealthyDownloadThroughput reports whether the sync peer's association
// pulled in data over the last tick at or above minSyncPeerNetworkSpeed. It is
// used to keep a sync peer that is actively downloading a large block — which
// streams in on DATA1 and so completes no block within maxLastBlockTime — from
// being rotated as if it were stalled. It does not mutate violation state.
func (sps *syncPeerState) hasHealthyDownloadThroughput(minSyncPeerNetworkSpeed uint64) bool {
	sps.mu.RLock()
	defer sps.mu.RUnlock()

	// Need at least one prior sample to compute a delta.
	if sps.ticks == 0 {
		return false
	}

	// Association.ReadBytes sums over the streams present at sample time. If a
	// stream (e.g. DATA1) was removed between samples the sum drops, so guard
	// the unsigned subtraction: a decrease means a stream just died, which is
	// the opposite of healthy progress — treat it as no throughput.
	if sps.assocReadBytes < sps.assocReadBytesLastTick {
		return false
	}

	recvDiff := sps.assocReadBytes - sps.assocReadBytesLastTick

	// Require actual bytes to have moved: a peer that delivered nothing is not
	// "downloading", regardless of how the speed threshold is configured (it may
	// be 0, which would otherwise make any rate pass).
	if recvDiff == 0 {
		return false
	}

	return recvDiff/uint64(syncPeerTickerInterval.Seconds()) >= minSyncPeerNetworkSpeed
}

// updateLastBlockTime updates the last block time
func (sps *syncPeerState) updateLastBlockTime() {
	sps.mu.Lock()
	defer sps.mu.Unlock()
	sps.lastBlockTime = time.Now()
}

// getLastBlockTime returns the last block time
func (sps *syncPeerState) getLastBlockTime() time.Time {
	sps.mu.RLock()
	defer sps.mu.RUnlock()

	return sps.lastBlockTime
}

// getViolations returns the current violation count
func (sps *syncPeerState) getViolations() int {
	sps.mu.RLock()
	defer sps.mu.RUnlock()

	return sps.violations
}

// setViolations sets the violation count
func (sps *syncPeerState) setViolations(v int) {
	sps.mu.Lock()
	defer sps.mu.Unlock()
	sps.violations = v
}

type TxHashAndFee struct {
	TxHash chainhash.Hash
	Fee    uint64
	Size   uint64
}

// blockSizeTracker tracks recent block sizes and dynamically adjusts the
// maximum number of in-flight blocks to avoid memory issues with large blocks.
type blockSizeTracker struct {
	mu          sync.RWMutex
	recentSizes []int64 // last N block sizes in bytes
	avgSize     int64   // rolling average block size
	maxSamples  int     // number of samples to track
}

// newBlockSizeTracker creates a new block size tracker.
func newBlockSizeTracker(maxSamples int) *blockSizeTracker {
	return &blockSizeTracker{
		recentSizes: make([]int64, 0, maxSamples),
		maxSamples:  maxSamples,
		avgSize:     0,
	}
}

// addBlockSize records a new block size and updates the rolling average.
func (bst *blockSizeTracker) addBlockSize(size int64) {
	bst.mu.Lock()
	defer bst.mu.Unlock()

	bst.recentSizes = append(bst.recentSizes, size)
	if len(bst.recentSizes) > bst.maxSamples {
		bst.recentSizes = bst.recentSizes[1:] // keep last maxSamples
	}

	// Calculate rolling average
	var sum int64
	for _, s := range bst.recentSizes {
		sum += s
	}
	if len(bst.recentSizes) > 0 {
		bst.avgSize = sum / int64(len(bst.recentSizes))
	}
}

// getAverageSize returns the current rolling average block size.
func (bst *blockSizeTracker) getAverageSize() int64 {
	bst.mu.RLock()
	defer bst.mu.RUnlock()
	return bst.avgSize
}

// calculateMaxInFlightBlocks returns the recommended max in-flight blocks
// based on average block size. Scales from 20 (small blocks) down to 1 (huge blocks).
func (bst *blockSizeTracker) calculateMaxInFlightBlocks() int {
	avgSize := bst.getAverageSize()

	const (
		MB = 1024 * 1024
		GB = 1024 * MB
	)

	switch {
	case avgSize >= 2*GB:
		return 1 // huge blocks: only 1 in flight
	case avgSize >= 1*GB:
		return 2 // very large blocks
	case avgSize >= 500*MB:
		return 3 // large blocks
	case avgSize >= 200*MB:
		return 5 // medium blocks
	case avgSize >= 100*MB:
		return 10 // smallish blocks
	default:
		return 20 // small blocks: default aggressive
	}
}

// blockFailureState tracks per-block transient-failure backoff. attempts is the
// consecutive failure count for a block hash; nextRetry is the earliest time the
// block may be re-processed. See SyncManager.blockFailureBackoff (#1187).
type blockFailureState struct {
	attempts  int
	nextRetry time.Time
}

// SyncManager is used to communicate block related messages with peers. The
// SyncManager is started as by executing Start() in a goroutine. Once started,
// it selects peers to sync from and starts the initial block download. Once the
// chain is in sync, the SyncManager handles incoming block and header
// notifications and relays announcements of new blocks to peers.
type SyncManager struct {
	ctx          context.Context
	logger       ulogger.Logger
	settings     *settings.Settings
	peerNotifier PeerNotifier
	started      int32
	shutdown     int32
	orphanTxs    *expiringmap.ExpiringMap[chainhash.Hash, *orphanTxAndParents]
	chainParams  *chaincfg.Params
	msgChan      chan interface{}
	handlerDone  chan struct{}
	quit         chan struct{}

	// TERANODE services
	blockchainClient  teranodeblockchain.ClientI
	validationClient  validator.Interface
	utxoStore         utxostore.Store
	subtreeStore      blob.Store
	subtreeValidation subtreevalidation.Interface
	blockValidation   blockvalidation.Interface
	blockAssembly     blockassembly.ClientI
	legacyKafkaInvCh  chan *kafka.Message
	// legacyKafkaInvProducer is retained (DC11) so SyncManager.Stop() can stop it
	// synchronously; without a field there is no handle to flush it on shutdown.
	legacyKafkaInvProducer kafka.KafkaAsyncProducerI
	txAnnounceBatcher      *batcher.BatcherWithDedup[TxHashAndFee]
	// txAnnounceMu / txAnnounceClosed guard txAnnounceBatcher.Put against the
	// batcher's Close in Stop(). go-batcher v2.0.4 PANICS on Put-after-Close, and
	// the txmeta Kafka listener (which Puts into the batcher) is a fire-and-forget
	// goroutine not joined by Stop(). The RLock/RWLock pairing guarantees no Put
	// runs concurrently with or after the drain: Stop takes the write lock (which
	// waits for any in-flight Put holding the read lock), sets closed, then drains;
	// subsequent Puts see closed and become no-ops. (DC15 / review C1.)
	txAnnounceMu     sync.RWMutex
	txAnnounceClosed bool

	// These fields should only be accessed from the blockHandler thread
	// (except syncPeer/syncPeerState which are protected by syncPeerMu).
	rejectedTxns  *txmap.SyncedMap[chainhash.Hash, struct{}]
	requestedTxns *expiringmap.ExpiringMap[chainhash.Hash, struct{}]
	// blockDownloads is the single record of which peers owe us which blocks,
	// replacing the global and per-peer request maps that used to hold half the
	// answer each. Unlike the fields above it carries its own lock, so it is
	// read and written from any goroutine — the frontier race timer and the
	// peer read-loops both consult it. See block_download_tracker.go.
	blockDownloads *blockDownloadTracker
	// blockFailureBackoff throttles re-processing of a block that just failed
	// with a transient storage/service error, so a re-delivered block does not
	// immediately re-run the full multi-million-record decorate at full
	// concurrency against an already-struggling UTXO store (#1187). Keyed by
	// block hash; entries self-evict after Legacy.BlockFailureBackoffMaxDuration.
	blockFailureBackoff *expiringmap.ExpiringMap[chainhash.Hash, *blockFailureState]
	// recentlyFailedBlocks tracks block hashes that just failed to store/validate
	// so their already-queued descendants are skipped before any RPC instead of
	// each failing their parent lookup and logging a misleading "previous block
	// NOT_FOUND" ERROR (#1333). Keyed by block hash; TTL-bounded and size-capped,
	// deleted on successful (re)process. A skipped descendant records its own hash
	// too, so the whole descendant chain is suppressed transitively.
	recentlyFailedBlocks *expiringmap.ExpiringMap[chainhash.Hash, struct{}]
	// blockPark holds blocks that arrived before their parent. Without it such a
	// block is fully downloaded, fully decoded and then discarded, and the
	// getblocks sent in its place is ignored for as long as headers-first mode is
	// on — so the download is simply wasted and nothing ever asks for the block
	// again. nil means the park is off and the old discard path runs; every entry
	// point is nil-safe, because tests build SyncManager as a struct literal that
	// never goes through New().
	blockPark     *blockPark
	syncPeerMu    sync.RWMutex // protects syncPeer and syncPeerState
	syncPeer      *peerpkg.Peer
	syncPeerState *syncPeerState
	peerStates    *txmap.SyncedMap[*peerpkg.Peer, *peerSyncState]

	// blockBacklog counts blocks sitting in the local processing pipeline:
	// queued in blockHandler's blockQueue plus the one inside handleBlockMsg.
	// While it is non-zero the node is backpressuring its own network reads
	// (OnBlock blocks until the previous block is processed), so the stall
	// detector must not hold the resulting zero throughput against the sync
	// peer. Written by the blockHandler goroutines, read by handleCheckSyncPeer.
	blockBacklog atomic.Int64

	// lastBacklogProgress is the UnixNano time the block backlog last advanced:
	// the 0->1 enqueue that opened the current backpressure window, or the most
	// recent block completion. localReadBackpressured suppresses the sync-peer
	// stall check only while this stays fresh — a backlog that stops advancing
	// for longer than blockProcessingStallTimeout is a genuine processing hang
	// (store/validator deadlock, Aerospike overload), not slow-but-progressing
	// validation, and must be allowed to rotate the peer. This restores the
	// liveness coverage lost when the per-message watchdog was disarmed for
	// prefetched blocks, without the false rotation of a merely-slow block.
	// Written by the blockHandler goroutines (the 0->1 enqueue and every
	// completion, via noteBacklogProgress), read by handleCheckSyncPeer.
	lastBacklogProgress atomic.Int64

	// blockPrefetchBudget bounds, by total serialized bytes, the blocks that
	// have been received from peers but not yet finished processing. It lets
	// OnBlock admit a block and return (so the read-loop downloads the next
	// block while this one validates) instead of blocking on per-block
	// completion, while capping the memory pinned by buffered blocks across ALL
	// peers and streams. nil when prefetch is disabled (budget <= 0), in which
	// case OnBlock keeps its original synchronous, one-block-in-flight behaviour.
	// A block larger than the whole budget is admitted alone (weight clamped to
	// the budget), preserving full backpressure for huge blocks.
	blockPrefetchBudget      *semaphore.Weighted
	blockPrefetchBudgetBytes int64

	// inFlightBlocks is the dedup half of the same block-admission gate whose
	// byte half is blockPrefetchBudget. It holds the hash of every block that is
	// currently admitted (has reserved budget) OR parked waiting for budget, so
	// at most one copy of any given block hash is ever in flight at a time.
	// AcquireBlockPrefetch inserts the hash BEFORE the (possibly blocking) budget
	// Acquire and ReleaseBlockPrefetch deletes it alongside the budget release, so
	// the two halves share exactly one lifetime and can never drift. Without it,
	// N duplicates of a single requested, near-budget-sized block would each
	// reserve budget, fill the whole budget, and park every legacy peer's
	// read-loop in Acquire — the very "a malicious peer cannot outrun the budget"
	// property this gate exists to guarantee. nil (alongside a nil
	// blockPrefetchBudget) when prefetch is disabled, so the synchronous/regtest
	// path skips dedup entirely. inFlightBlocksMu guards the map.
	inFlightBlocks   map[chainhash.Hash]struct{}
	inFlightBlocksMu sync.Mutex

	// blockPrefetchWaiters counts read-loops currently blocked acquiring
	// prefetch budget (i.e. local processing cannot keep up). While > 0 the node
	// is backpressuring its own network reads, so the stall detector must not
	// hold the resulting zero throughput against the sync peer — the prefetch
	// analogue of the blockBacklog guard. Read by handleCheckSyncPeer.
	blockPrefetchWaiters atomic.Int64

	// The following fields are used for headers-first mode.
	//
	// headerMu is the single owner of headerList, startHeader and
	// nextCheckpoint. Those three are reached from three goroutines — the
	// per-message headers handler (blockHandler dispatches one goroutine per
	// headers message), the block-queue consumer running handleBlockMsg, and
	// fetchHeaderBlocks, which both of those call — and container/list is not
	// goroutine-safe, so without this lock every push, walk and remove races.
	//
	// Two rules keep it that way:
	//
	// Rule A, lock ordering: headerMu -> frontierMu -> peerStates. headerMu is
	// the outermost of the three; nothing may take it while already holding
	// frontierMu or the peerStates map lock.
	//
	// Rule B, what may not run under it: no send to a peer (QueueMessage,
	// PushGetHeadersMsg, PushGetBlocksMsg, DisconnectWithWarning — a peer's
	// output queue is buffered but finite, so a send can block) and no
	// blockchain client call that can block for an unbounded time
	// (GetBestBlockHeader can take minutes during initial sync). There are no
	// exceptions. fetchHeaderBlocks' haveInventory lookups used to be one, on
	// the grounds that the number of them was bounded; they are now made with
	// the lock released, in rounds, because bounding the number of calls does
	// not bound the time they take — see fetchHeaderBlocks.
	headerMu         sync.Mutex
	headersFirstMode atomic.Bool // accessed from multiple goroutines, must be atomic
	// pendingCheckpoint holds the checkpoint block whose round of headers was
	// never asked for, because it committed when there was nobody to ask.
	// checkpointBlockCommitted stores it and drainPendingCheckpoint takes it,
	// restoring it when there is still nobody to ask. The two run on different
	// goroutines — the block-queue consumer drains the park, the sync-peer
	// ticker elects — so it is atomic and is read with a Swap. The restore is a
	// CompareAndSwap for the same reason: the round in the ticker's hand may
	// already be the stale one. Nil means there is no round owing.
	pendingCheckpoint atomic.Pointer[deferredCheckpoint]
	// currentCached is the last answer current() worked out, so a peer goroutine
	// can read it without making the blockchain call itself. See IsCurrentCached.
	currentCached atomic.Bool
	headerList    *list.List
	// headerIndex resolves a block hash to its element in headerList in O(1),
	// so a caller does not have to walk the list to find a header. Guarded by
	// headerMu, and maintained at every single place headerList changes —
	// resetHeaderStateLocked's wipe and its anchor push, the front removal in
	// handleBlockMsg, the push in handleHeadersMsg, the front removal on the
	// checkpoint branch, and the wipe in leaveHeadersFirstMode. Miss one and the
	// index hands back an element that is no longer in any list.
	headerIndex map[chainhash.Hash]*list.Element
	// headerListEpoch counts how many times the header list has been thrown
	// away and started from scratch — resetHeaderStateLocked when the sync peer
	// is rotated, and leaveHeadersFirstMode at the final checkpoint. Every
	// header node is stamped with the epoch it was made under, which is what
	// lets a rewind tell a node that belongs in this list from one left over
	// from a list that no longer exists. Guarded by headerMu.
	headerListEpoch  uint64
	startHeader      *list.Element
	nextCheckpoint   *chaincfg.Checkpoint
	blockSizeTracker *blockSizeTracker // tracks block sizes for dynamic in-flight adjustment

	// The download frontier: the oldest block we have asked for and not yet
	// received. Because blocks are committed strictly in order, that one block
	// gates everything behind it, so it is the only block worth asking a second
	// peer for (see frontier_race.go). It is published here under frontierMu,
	// rather than read straight off headerList, so the five-second race timer
	// never touches the header list at all and so never needs headerMu — which
	// is what keeps Rule A's ordering one-directional.
	// frontierMu is a leaf lock and is never held across a send to a peer.
	frontierMu     sync.Mutex
	frontierHash   chainhash.Hash
	frontierHeight int32
	frontierSince  time.Time
	frontierRacers map[*peerpkg.Peer]struct{}

	// racedBlocks remembers, for each block we asked more than one peer for,
	// exactly which peers were left holding a request we then cancelled once a
	// copy arrived. A late copy from one of those peers is our own doing, so it
	// is dropped quietly instead of getting the peer evicted for sending an
	// unrequested block. Scoped to those peers and those hashes, so the eviction
	// defence is unchanged for every peer we did not ask. nil-guarded because
	// tests build SyncManager as a struct literal.
	racedBlocks *expiringmap.ExpiringMap[chainhash.Hash, map[*peerpkg.Peer]struct{}]

	// An optional fee estimator.
	// feeEstimator *mempool.FeeEstimator
	currentFeeFilter atomic.Uint64

	// minSyncPeerNetworkSpeed is the minimum speed allowed for
	// a sync peer.
	minSyncPeerNetworkSpeed uint64
}

// loadSyncPeer returns the current sync peer, safe for concurrent access.
func (sm *SyncManager) loadSyncPeer() *peerpkg.Peer {
	sm.syncPeerMu.RLock()
	defer sm.syncPeerMu.RUnlock()
	return sm.syncPeer
}

// loadSyncPeerAndState returns the current sync peer and its state, safe for concurrent access.
func (sm *SyncManager) loadSyncPeerAndState() (*peerpkg.Peer, *syncPeerState) {
	sm.syncPeerMu.RLock()
	defer sm.syncPeerMu.RUnlock()
	return sm.syncPeer, sm.syncPeerState
}

// syncPeerStateFor returns the sync peer's state if p is the current sync peer
// or another stream of its association, and whether it matched. Under the
// BlockPriority policy a block is delivered on the DATA1 stream — a different
// Peer from the GENERAL sync peer — so a plain `p == syncPeer` check misses it
// and the sync peer's lastBlockTime is never refreshed during multistream sync.
func (sm *SyncManager) syncPeerStateFor(p *peerpkg.Peer) (*syncPeerState, bool) {
	sp, sps := sm.loadSyncPeerAndState()
	if sp == nil || sps == nil || p == nil {
		return nil, false
	}

	if p == sp {
		return sps, true
	}

	if a := p.AssociationRef(); a != nil && a == sp.AssociationRef() {
		return sps, true
	}

	return nil, false
}

// storeSyncPeer sets the sync peer and its state, safe for concurrent access.
func (sm *SyncManager) storeSyncPeer(peer *peerpkg.Peer, state *syncPeerState) {
	sm.syncPeerMu.Lock()
	defer sm.syncPeerMu.Unlock()
	sm.syncPeer = peer
	sm.syncPeerState = state
}

// resetHeaderState sets the headers-first mode state to values appropriate for
// syncing from a new peer. It takes headerMu; callers already holding it must
// use resetHeaderStateLocked instead, because sync.Mutex is not reentrant.
func (sm *SyncManager) resetHeaderState(newestHash *chainhash.Hash, newestHeight int32) {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	sm.resetHeaderStateLocked(newestHash, newestHeight)
}

// resetHeaderStateLocked is resetHeaderState's body. The caller must hold
// headerMu. clearFrontier takes frontierMu from in here, which is what
// establishes headerMu -> frontierMu as the lock order (Rule A).
func (sm *SyncManager) resetHeaderStateLocked(newestHash *chainhash.Hash, newestHeight int32) {
	sm.headersFirstMode.Store(false)
	sm.headerList.Init()
	sm.clearHeaderIndexLocked()
	sm.startHeader = nil
	sm.clearFrontier()
	// The list that follows is a different list. Anything still holding a node
	// from the old one — a parked block carrying the header its arrival took off
	// the front — must not be able to put that node back; see rewindHeaderCursor.
	sm.headerListEpoch++

	// The list is being rebuilt from newestHeight, so the checkpoint the old
	// list was working towards may already be behind it: a checkpoint block that
	// committed with nobody to ask leaves exactly that state, because the
	// nil-peer arm of checkpointBlockCommitted deliberately does not advance.
	// Left stale, startSync's headers-first gate reads
	// bestHeight < nextCheckpoint.Height as false for good and the node never
	// turns the mode back on. Deriving it here from the same two inputs New uses
	// (manager.go, the findNextHeaderCheckpoint call in New) means whoever
	// rebuilds the list also rebuilds the target it is aimed at.
	//
	// Monotonic, because newestHeight cannot be trusted to be current. Both
	// callers read it outside headerMu and then wait to acquire the lock, and on
	// this path that lock is contended by every arriving block, so a checkpoint
	// transition can land in the gap and advance the checkpoint before this runs.
	// An unconditional assignment would then put it back onto the checkpoint
	// whose block has just committed, and startSync's gate can never be
	// satisfied by a checkpoint already in the chain: headers-first mode would
	// stay off for the life of the process, silently, with the header walk
	// having nothing to walk. Only ever move it forward, and treat nil as
	// terminal, so a stale height can leave it alone but never rewind it.
	//
	// Writing the rule out rather than arguing an invariant is deliberate. It
	// also makes the DisableCheckpoints case structural instead of a
	// reachability argument, because findNextHeaderCheckpoint reads
	// chainParams.Checkpoints directly and that slice is not emptied by the flag.
	if sm.nextCheckpoint != nil {
		if derived := sm.findNextHeaderCheckpoint(newestHeight); derived == nil || derived.Height >= sm.nextCheckpoint.Height {
			sm.nextCheckpoint = derived
		}
	}

	// When there is a next checkpoint, add an entry for the latest known
	// block into the header pool.  This allows the next downloaded header
	// to prove it links to the chain properly.
	if sm.nextCheckpoint != nil {
		// isAnchor: this block is already in the database. It is here to be
		// linked to and then removed, which is what the trim at the next
		// checkpoint has to be able to recognise.
		node := headerNode{height: newestHeight, hash: newestHash, listEpoch: sm.headerListEpoch, isAnchor: true}
		sm.indexHeaderLocked(sm.headerList.PushBack(&node), *newestHash)
	}
}

// indexHeaderLocked records e as the element holding hash. The caller must hold
// headerMu.
//
// Last write wins: the header list tolerates the same hash appearing twice and a
// map cannot, so the newest element for a hash owns the entry. That rule only
// works paired with unindexHeaderLocked's identity check — read the two
// together.
//
// The map is allocated lazily because tests build SyncManager as a struct
// literal that never goes through New().
func (sm *SyncManager) indexHeaderLocked(e *list.Element, hash chainhash.Hash) {
	if e == nil {
		return
	}

	if sm.headerIndex == nil {
		sm.headerIndex = make(map[chainhash.Hash]*list.Element)
	}

	sm.headerIndex[hash] = e
}

// unindexHeaderLocked drops hash from the index, but only if the entry still
// points at e. The caller must hold headerMu.
//
// The identity check is what makes last-write-wins safe: when the same hash is
// in the list twice, removing the older element must not evict the entry that
// points at the newer one still in the list.
func (sm *SyncManager) unindexHeaderLocked(e *list.Element, hash chainhash.Hash) {
	if sm.headerIndex == nil {
		return
	}

	if sm.headerIndex[hash] == e {
		delete(sm.headerIndex, hash)
	}
}

// clearHeaderIndexLocked empties the index. The caller must hold headerMu.
// It must be called wherever the header list itself is emptied, or the index
// keeps resolving hashes to elements that are no longer in any list.
func (sm *SyncManager) clearHeaderIndexLocked() {
	if sm.headerIndex == nil {
		return
	}

	sm.headerIndex = make(map[chainhash.Hash]*list.Element)
}

// headerElement returns the header list element holding hash, or nil when the
// hash is not queued. It takes headerMu itself; callers already holding it must
// read sm.headerIndex directly.
func (sm *SyncManager) headerElement(hash chainhash.Hash) *list.Element {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	return sm.headerIndex[hash]
}

// resetHeaderStateIfEmpty recovers the header state only if the list is still
// empty, and reports whether it did.
//
// The empty-list recovery in handleHeadersMsg has to drop headerMu across
// GetBestBlockHeader, which can block for minutes during initial sync (Rule B).
// Once the lock has been dropped, what was read before the call is no longer
// true: another headers message may have recovered the state and pushed real
// headers in the meantime. Resetting unconditionally on the way back would throw
// those away, so the emptiness is re-checked under the lock instead.
func (sm *SyncManager) resetHeaderStateIfEmpty(newestHash *chainhash.Hash, newestHeight int32) bool {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	if sm.headerList == nil || sm.headerList.Back() != nil {
		return false
	}

	sm.resetHeaderStateLocked(newestHash, newestHeight)

	return true
}

// leaveHeadersFirstMode switches out of headers-first mode and wipes the header
// list. It is the body of handleBlockMsg's "reached the final checkpoint"
// branch, named so the wipe has one place to be maintained: every field the
// header list owns has to be cleared together, and inline three-line versions of
// that are exactly how one of them gets forgotten.
//
// It takes headerMu itself, so it must not be called from a locked region.
func (sm *SyncManager) leaveHeadersFirstMode() {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	sm.headersFirstMode.Store(false)
	sm.headerList.Init()
	sm.clearHeaderIndexLocked()
	// Same reason as resetHeaderStateLocked: the list is gone, so a header node
	// somebody else is still holding no longer belongs anywhere.
	sm.headerListEpoch++
	// Same wipe as resetHeaderStateLocked, and startHeader is part of it. Left
	// pointing into the list that has just been emptied it reads, to every
	// caller that asks "is there anything left to fetch?", as "yes" — which is
	// how handleBlockMsg's fallback, the one thing that re-primes sync with a
	// getblocks once the peer has gone quiet, stops being reachable at all.
	sm.startHeader = nil
	sm.clearFrontier()
}

// headerListLen returns the number of headers currently queued. Nil-guarded
// because tests build SyncManager as a struct literal.
func (sm *SyncManager) headerListLen() int {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	if sm.headerList == nil {
		return 0
	}

	return sm.headerList.Len()
}

// headerListEmpty reports whether there is no header to link the next batch to.
func (sm *SyncManager) headerListEmpty() bool {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	return sm.headerList == nil || sm.headerList.Back() == nil
}

// nextCheckpointSnapshot returns the next checkpoint under headerMu.
//
// The returned pointer outlives the lock, which is safe only because
// checkpoints are immutable: findNextHeaderCheckpoint only ever returns
// pointers into chainParams.Checkpoints, a fixed slice nothing writes to. Do
// not start mutating one.
func (sm *SyncManager) nextCheckpointSnapshot() *chaincfg.Checkpoint {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	return sm.nextCheckpoint
}

// findNextHeaderCheckpoint returns the next checkpoint after the passed height.
// It returns nil when there is not one either because the height is already
// later than the final checkpoint or some other reason such as disabled
// checkpoints.
func (sm *SyncManager) findNextHeaderCheckpoint(height int32) *chaincfg.Checkpoint {
	checkpoints := sm.chainParams.Checkpoints
	if len(checkpoints) == 0 {
		return nil
	}

	// There is no next checkpoint if the height is already after the final
	// checkpoint.
	finalCheckpoint := &checkpoints[len(checkpoints)-1]
	if height >= finalCheckpoint.Height {
		return nil
	}

	// Find the next checkpoint.
	nextCheckpoint := finalCheckpoint

	for i := len(checkpoints) - 2; i >= 0; i-- {
		if height >= checkpoints[i].Height {
			break
		}

		nextCheckpoint = &checkpoints[i]
	}

	return nextCheckpoint
}

// startSync will choose the best peer among the available candidate peers to
// download/sync the blockchain from.  When syncing is already running, it
// simply returns.  It also examines the candidates for any which are no longer
// candidates and removes them as needed.
func (sm *SyncManager) startSync() {
	// Return now if we're already syncing.
	if sm.loadSyncPeer() != nil {
		return
	}

	sm.logger.Debugf("startSync - Syncing from %v", sm.loadSyncPeer())

	bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		sm.logger.Errorf(failedToGetBestBlockHeaderMsg, err)
		return
	}

	bestPeers := make([]*peerpkg.Peer, 0)

	okPeers := make([]*peerpkg.Peer, 0)

	// Peers that would be candidates but were demoted for stalling too recently.
	// Kept aside rather than dropped: they are elected below if there is nobody
	// else at all, because a node with one peer must still sync.
	cooledPeers := make([]*peerpkg.Peer, 0)

	sm.logger.Debugf("[startSync] selecting sync peer from %d candidates", sm.peerStates.Length())

	for peer, state := range sm.peerStates.Range() {
		if !state.syncCandidate {
			sm.logger.Debugf("[startSync] peer %v is not a sync candidate", peer.String())

			continue
		}

		// Defence-in-depth: never elect a peer whose socket has already been
		// torn down. If one slips into peerStates (e.g. a future regression in
		// the new-peer registration path), picking it here would push
		// getheaders into a closed connection and stall sync for the duration
		// of maxLastBlockTime before rotating.
		if !peer.Connected() {
			sm.logger.Debugf("[startSync] peer %v is not connected, skipping", peer.String())

			continue
		}

		// Add any peers on the same block to okPeers. These should
		// only be used as a last resort.

		bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
		if err != nil {
			sm.logger.Errorf("[startSync] failed to convert block height to int32: %v", err)

			continue
		}

		if peer.LastBlock() == bestBlockHeightInt32 {
			okPeers = append(okPeers, peer)
			sm.logger.Debugf("[startSync][%v] peer is at the same height %d as us (%d), added to okPeers", peer.String(), peer.LastBlock(), bestBlockHeaderMeta.Height)

			continue
		}

		// Skip sync candidate peers that are no longer candidates due
		// to passing their latest known block.
		if peer.LastBlock() < bestBlockHeightInt32 {
			sm.logger.Debugf("[startSync][%v] peer is behind us at height %d (us: %d), skipping", peer.String(), peer.LastBlock(), bestBlockHeaderMeta.Height)

			continue
		}

		// A peer demoted for stalling is still connected and still a sync
		// candidate, so the disconnect that used to keep it out of the election
		// running immediately afterwards no longer does. Without this the node
		// hands the role straight back to the peer it just judged stalled and
		// buys another stall window of no progress.
		if state.inDemotionCooldown() {
			sm.logger.Debugf("[startSync][%v] peer is inside its demotion cooldown, deferring", peer.String())
			cooledPeers = append(cooledPeers, peer)

			continue
		}

		// Append each good peer to bestPeers for selection later.
		sm.logger.Debugf("[startSync][%v] peer is a sync candidate at height %d (us: %d), adding to bestPeers", peer.String(), peer.LastBlock(), bestBlockHeaderMeta.Height)
		bestPeers = append(bestPeers, peer)
	}

	var bestPeer *peerpkg.Peer

	// Try to select a random peer that is at a higher block height,
	// if that is not available, then use a random peer at the same
	// height and hope they find blocks.
	if len(bestPeers) > 0 {
		// #nosec G404
		bestPeer = bestPeers[rand.IntN(len(bestPeers))]
		sm.logger.Debugf("[startSync] selected best peer %s from %d peers ahead of us", bestPeer.String(), len(bestPeers))
	} else if len(okPeers) > 0 {
		// #nosec G404
		bestPeer = okPeers[rand.IntN(len(okPeers))]
		sm.logger.Debugf("[startSync] no peers ahead, selected ok peer %s from %d peers at same height", bestPeer.String(), len(okPeers))
	} else if len(cooledPeers) > 0 {
		// Nobody else at all, so the cooldown is ignored rather than leaving the
		// node with no sync peer. On a two-peer network this lets the role
		// ping-pong every stall window, which is noisy but harmless now that a
		// swap no longer throws the header list away.
		// #nosec G404
		bestPeer = cooledPeers[rand.IntN(len(cooledPeers))]
		sm.logger.Warnf("[startSync] every candidate is inside its demotion cooldown, electing %s anyway rather than leaving the node with no sync peer", bestPeer.String())
	}

	// Start syncing from the best peer if one was selected.
	if bestPeer == nil {
		sm.logger.Warnf("[startSync] No sync peer candidates available after evaluating %d total peers (%d ahead, %d at same height)", sm.peerStates.Length(), len(bestPeers), len(okPeers))

		return
	}

	sm.logger.Debugf("[startSync] best peer selected: %s", bestPeer.String())

	bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf("[startSync] failed to convert block height to int32: %v", err)

		return
	}

	// check whether we are in sync with this peer and send RUNNING FSM state
	if bestPeer.LastBlock() == bestBlockHeightInt32 {
		sm.logger.Debugf("[startSync] peer %v is at the same height %d as us, sending RUNNING", bestPeer.String(), bestPeer.LastBlock())

		if err = sm.blockchainClient.Run(sm.ctx, "legacy/netsync/manager/startSync"); err != nil {
			sm.logger.Errorf("[startSync] failed to set blockchain state to running: %v", err)
		}

		sm.resetFeeFilterToDefault()

		return
	}

	// Nothing reopens the whole ledger here any more. A sync-peer change used to
	// back-date every outstanding assignment at once, which was survivable only
	// because it threw the header list away in the same breath and left nothing
	// to re-walk. A demotion keeps the list, so a whole-ledger back-date would
	// have the very next pass hand every in-flight block to a second peer, and
	// both copies committed — the duplicate-commit storm. The demoted peer's own
	// slice is reopened by demoteSyncPeer instead, and only its own.

	// Where to continue the headers round from. Mid-round, that is the back of
	// the header list we already have: handleHeadersMsg requires every incoming
	// header to connect to the back, so a locator built from our own database
	// best block — hundreds of headers below it — would have the new sync peer
	// answer honestly and be disconnected for it. Only a rebuilt list falls back
	// to the database.
	locator, err := sm.headersRoundLocator(bestBlockHeader.Hash(), bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf("[startSync] Failed to get block locator for the latest block: %v", err)

		return
	}

	sm.logger.Infof("[startSync] Syncing from block height %d to block height %d using peer %v", bestBlockHeaderMeta.Height, bestPeer.LastBlock(), bestPeer.String())

	// If we are behind the peer more than 10 blocks, move to CATCHING BLOCKS
	if bestPeer.LastBlock()-bestBlockHeightInt32 > 10 {
		// move FSM state to CATCHING BLOCKS, we are behind the peer more than 10 blocks
		if err = sm.blockchainClient.CatchUpBlocks(sm.ctx); err != nil {
			sm.logger.Errorf("[startSync] failed to set blockchain state to catching blocks: %v", err)
		}
	}

	// When the current height is less than a known checkpoint we
	// can use block headers to learn about which blocks comprise
	// the chain up to the checkpoint and perform less validation
	// for them.  This is possible since each header contains the
	// hash of the previous header and a merkle root.  Therefore, if
	// we validate all of the received headers linked together
	// properly and the checkpoint hashes match, we can be sure the
	// hashes for the blocks in between are accurate.  Further, once
	// the full blocks are downloaded, the merkle root is computed
	// and compared against the value in the header which proves the
	// full block hasn't been tampered with.
	//
	// Once we have passed the final checkpoint, or checkpoints are
	// disabled, use standard inv messages learn about the blocks
	// and fully validate them.  Finally, regression test mode does
	// not support the headers-first approach so do normal block
	// downloads when in regression test mode.
	// Snapshot the checkpoint under headerMu, then work from the snapshot: the
	// getheaders send below must not happen with the lock held (Rule B).
	nextCP := sm.nextCheckpointSnapshot()

	// Re-aim from the height read on this path, which is the only current one
	// available here, rather than trusting whoever last rebuilt the header list
	// to have read a fresh one. Both rebuild callers take their height outside
	// headerMu and then wait for the lock, so a checkpoint transition can commit
	// in that gap and leave the stored checkpoint naming a block already in our
	// chain. The gate below can never be satisfied by such a checkpoint, so
	// headers-first mode would stay off for good and the header walk would have
	// nothing to walk. Repairing it here makes the gate self-healing whatever
	// the last rebuild saw.
	if nextCP != nil && bestBlockHeightInt32 >= nextCP.Height {
		sm.headerMu.Lock()
		sm.nextCheckpoint = sm.findNextHeaderCheckpoint(bestBlockHeightInt32)
		nextCP = sm.nextCheckpoint
		sm.headerMu.Unlock()

		sm.logger.Infof("[startSync] checkpoint was already in the chain at height %d, re-aimed", bestBlockHeightInt32)
	}

	if nextCP != nil &&
		bestBlockHeightInt32 < nextCP.Height &&
		sm.chainParams != &chaincfg.RegressionNetParams {
		if err = bestPeer.PushGetHeadersMsg(locator, nextCP.Hash); err != nil {
			sm.logger.Warnf("[startSync] Failed to send getheaders message to peer %s: %v", bestPeer.String(), err)

			return
		}

		sm.headersFirstMode.Store(true)

		sm.logger.Infof("[startSync] Downloading headers for blocks %d to %d from peer %s", bestBlockHeaderMeta.Height+1, nextCP.Height, bestPeer.String())
	} else {
		if err = bestPeer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
			sm.logger.Warnf("[startSync] Failed to send getblocks message to peer %s: %v", bestPeer.String(), err)

			return
		}
	}

	bestPeer.SetSyncPeer(true)
	sm.storeSyncPeer(bestPeer, &syncPeerState{
		lastBlockTime:     time.Now(),
		recvBytes:         bestPeer.BytesReceived(),
		recvBytesLastTick: uint64(0),
	})
}

func (sm *SyncManager) resetFeeFilterToDefault() {
	if sm.currentFeeFilter.Load() != uint64(bsvutil.SatoshiPerBitcoin*sm.settings.Policy.MinMiningTxFee) {
		feeFilter := wire.NewMsgFeeFilter(int64(sm.settings.Policy.MinMiningTxFee)) // nolint:gosec

		for p := range sm.peerStates.Range() {
			if p == nil {
				continue
			}

			if !p.Connected() {
				continue
			}

			p.QueueMessage(feeFilter, nil)
		}

		sm.currentFeeFilter.Store(uint64(bsvutil.SatoshiPerBitcoin * sm.settings.Policy.MinMiningTxFee))
	}
}

// SyncHeight returns latest known block being synced to.
func (sm *SyncManager) SyncHeight() uint64 {
	if sm.loadSyncPeer() == nil {
		return 0
	}

	return uint64(sm.topBlock())
}

// IsHeadersFirstMode returns whether the sync manager is currently in headers-first mode.
// This is used to avoid serving headers to other peers during checkpoint sync, which
// can cause significant delays (18s+ per batch) due to database query contention.
func (sm *SyncManager) IsHeadersFirstMode() bool {
	return sm.headersFirstMode.Load()
}

// isRegtest reports whether the active chain params are regression net by
// network magic rather than pointer identity with chaincfg.RegressionNetParams,
// so a copied Params value (as some tests construct) is still recognized, and a
// nil chainParams is safely not-regtest. It exists to give BlockRequested the
// SAME value semantics as peerpkg.UseBlockPrefetchIngestion (.Net != RegTestNet)
// so those two prefetch-path siblings cannot drift on a copied-params manager.
//
// It deliberately does NOT replace the pointer-equality regtest checks
// elsewhere in this file (startSync's headers-first gate, isSyncCandidate,
// handleBlockMsg's unrequested-block disconnect). Those run on the synchronous
// (non-prefetch) path that regtest always takes, and the E2E harness builds
// chainParams as a *copy* of RegressionNetParams — so switching them to value
// semantics flips real behavior (e.g. isSyncCandidate would apply the regtest
// localhost restriction, and startSync would drop headers-first) and breaks
// legacy-sync/smoketest. Pointer equality there is load-bearing; leave it.
func (sm *SyncManager) isRegtest() bool {
	return sm.chainParams != nil && sm.chainParams.Net == wire.RegTestNet
}

// isSyncCandidate returns whether or not the peer is a candidate to consider
// syncing from.
func (sm *SyncManager) isSyncCandidate(peer *peerpkg.Peer) bool {
	// Typically a peer is not a candidate for sync if it's not a full node,
	// however regression test is special in that the regression tool is
	// not a full node and still needs to be considered a sync candidate.
	if sm.chainParams == &chaincfg.RegressionNetParams {
		// The peer is not a candidate if it's not coming from localhost
		// or the hostname can't be determined for some reason.
		// If we need to allow the peer with different host to be a sync candidate
		if !sm.settings.Legacy.AllowSyncCandidateFromLocalPeers {
			host, _, err := net.SplitHostPort(peer.String())
			if err != nil {
				return false
			}

			if host != "127.0.0.1" && host != "localhost" {
				return false
			}
		}
	} else {
		// The peer is not a candidate for sync if it's not a full
		// node.
		nodeServices := peer.Services()

		sm.logger.Debugf("Checking sync candidate %s: Services=%v, Required=%v", peer.String(), nodeServices, wire.SFNodeNetwork)

		if nodeServices&wire.SFNodeNetwork != wire.SFNodeNetwork {
			sm.logger.Debugf("Peer %s rejected as sync candidate: Missing SFNodeNetwork flag", peer.String())

			return false
		}
	}

	sm.logger.Debugf("Peer %s accepted as sync candidate", peer.String())
	// Candidate if all checks passed.
	return true
}

// handleNewPeerMsg deals with new peers that have signalled they may
// be considered as a sync peer (they have already successfully negotiated).  It
// also starts syncing if needed.  It is invoked from the syncHandler goroutine.
func (sm *SyncManager) handleNewPeerMsg(peer *peerpkg.Peer) {
	// Ignore if in the process of shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	// If the peer's socket was already torn down by the time this newPeerMsg
	// drained from msgChan, don't insert it into peerStates at all. Pairs
	// with the Connected() guard in startSync to close the window during
	// which a dead pointer can sit in the map waiting for a donePeerMsg.
	if !peer.Connected() {
		sm.logger.Debugf("[handleNewPeerMsg] peer %s already disconnected before registration, skipping", peer.String())
		return
	}

	sm.logger.Infof("New valid peer %s (%s)", peer, peer.UserAgent())

	// Initialize the peer state
	isSyncCandidate := sm.isSyncCandidate(peer)

	// While catching up, ask every newly-connected peer to hold back
	// transaction announcements to reduce load during sync. The raise is queued
	// per-peer; the global currentFeeFilter is only the marker the reset path
	// (resetFeeFilterToDefault) checks, so it must NOT gate the per-peer queue —
	// otherwise only the first peer to connect during catch-up would be told.
	// The filter is restored to the policy default once we reach RUNNING.
	if state, ferr := sm.blockchainClient.GetFSMCurrentState(sm.ctx); ferr != nil {
		sm.logger.Errorf("[handleNewPeerMsg] failed to get current FSM state: %v", ferr)
	} else if state != nil && *state == teranodeblockchain.FSMStateCATCHINGBLOCKS {
		feeFilter := wire.NewMsgFeeFilter(bsvutil.SatoshiPerBitcoin)
		peer.QueueMessage(feeFilter, nil)
		sm.currentFeeFilter.Store(bsvutil.SatoshiPerBitcoin)
	}

	state := &peerSyncState{
		syncCandidate: isSyncCandidate,
		requestQueue:  txmap.NewSyncedSlice[wire.InvVect](maxRequestedBlocks),
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](10 * time.Second), // allow the node 10 seconds to respond to the tx request
	}

	// Seed the peer's best known height from the height it advertised during the
	// handshake, so a peer is never mistaken for one that has nothing. An atomic
	// cannot be initialised in the struct literal above.
	state.noteBestKnownHeight(peer.StartingHeight())

	sm.peerStates.Set(peer, state)

	// Start syncing by choosing the best candidate if needed.
	if isSyncCandidate && sm.loadSyncPeer() == nil {
		sm.startSync()
	}
}

// handleCheckSyncPeer selects a new sync peer.
func (sm *SyncManager) handleCheckSyncPeer() {
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	// After everything this tick might do to the sync peer, not before: the
	// arms below elect one when there is none and demote-then-re-elect when the
	// current one has stalled, and a deferred checkpoint round needs whatever
	// peer that leaves behind. Deferred rather than placed at each return
	// because there are several, and missing one loses the round until the next
	// tick. See drainPendingCheckpoint.
	defer sm.drainPendingCheckpoint()

	sp, sps := sm.loadSyncPeerAndState()

	// If we don't have a sync peer, select a new one and return.
	if sp == nil {
		sm.startSync()

		return
	}

	// Update network stats at the end of this tick.
	defer sps.updateNetwork(sp)

	// While the node is throttling its own network reads because local block
	// processing cannot keep up, zero throughput and a stale last-block-time
	// measure our own validation speed, not the peer's health. Skip stall checks
	// until that self-backpressure clears — a genuinely stalled peer keeps
	// failing them afterwards. The deferred updateNetwork still runs, keeping
	// throughput samples fresh for the next tick.
	//
	// Any queued/mid-validation backlog suppresses the check (see
	// localReadBackpressured): a stale last-block-time then measures our
	// validation speed, not the peer. A genuinely stalled peer stops feeding the
	// queue, the backlog drains, and the check resumes — so this delays, but does
	// not prevent, rotation of a truly stalled peer.
	if sm.localReadBackpressured() {
		sm.logger.Debugf("[CheckSyncPeer] sync peer %s check skipped: read-loop backpressured by local block processing", sp.String())
		return
	}

	headersFirst := sm.headersFirstMode.Load()
	lastBlockSince := time.Since(sps.getLastBlockTime())

	// During headers-first mode, only suppress network speed checks since
	// downloading 80-byte headers makes the peer appear slow. Still check
	// last-block-time so stalled peers get rotated even during headers-first.
	var isNetworkSpeedViolation bool
	if !headersFirst {
		validNetworkSpeed := sps.validNetworkSpeed(sm.minSyncPeerNetworkSpeed)
		isNetworkSpeedViolation = validNetworkSpeed >= maxNetworkViolations
		sm.logger.Debugf("[CheckSyncPeer] sync peer %s check, network violations: %v (limit %v), time since last block: %v (limit %v)", sp.String(), validNetworkSpeed, maxNetworkViolations, lastBlockSince, maxLastBlockTime)
	} else {
		sm.logger.Debugf("[CheckSyncPeer] sync peer %s check (headers-first mode, speed check skipped), time since last block: %v (limit %v)", sp.String(), lastBlockSince, maxLastBlockTime)
	}
	isLastBlockTimeViolation := lastBlockSince > maxLastBlockTime

	// A multi-GB block can take longer than maxLastBlockTime to arrive. Under
	// the BlockPriority stream policy it streams in on the DATA1 stream, so no
	// block "completes" (lastBlockTime stays put) even though bytes are
	// actively flowing across the association. Don't rotate a sync peer that is
	// still pulling data at a healthy rate — it is making progress on a large
	// block, not stalled. A genuinely stalled peer delivers no throughput and
	// is still rotated.
	//
	// This suppression is itself capped at peer.MaxBlockDownloadTime: past that
	// wall-clock window the peer is rotated regardless of throughput, so a
	// malicious peer cannot dribble bytes just above the threshold forever to
	// hold the single sync-peer slot and stall IBD.
	if isLastBlockTimeViolation &&
		lastBlockSince < peerpkg.MaxBlockDownloadTime &&
		sps.hasHealthyDownloadThroughput(sm.minSyncPeerNetworkSpeed) {
		sm.logger.Debugf("[CheckSyncPeer] sync peer %s exceeded last-block-time but association still downloading at a healthy rate (%.0fs in, cap %s); not rotating", sp.String(), lastBlockSince.Seconds(), peerpkg.MaxBlockDownloadTime)
		isLastBlockTimeViolation = false
	}

	// If no violations detected, the sync peer is healthy — nothing to do.
	if !isNetworkSpeedViolation && !isLastBlockTimeViolation {
		return
	}

	var reason string
	if isNetworkSpeedViolation {
		reason = "network speed violation"
	} else if isLastBlockTimeViolation {
		reason = "last block time out of range"
	}
	sm.logger.Debugf("[CheckSyncPeer] sync peer %s is stalled due to %s, updating sync peer", sp.String(), reason)

	state, exists := sm.peerStates.Get(sp)
	if !exists {
		return
	}

	sm.logger.Debugf("[CheckSyncPeer] removing sync peer %s", sp.String())

	// With block bodies coming from every eligible peer, the sync peer's job is
	// headers, and a peer that is slow at headers is often a perfectly good body
	// source. Demote it: it keeps its connection, its registration and the blocks
	// it owes, and the header list survives. With the fan-out off the sync peer is
	// the only body source, so keeping a stalled one buys nothing and the old
	// disconnect-and-reset is the right behaviour.
	if sm.settings.Legacy.MultiPeerBlockDownload {
		sm.demoteSyncPeer(sp, state)

		return
	}

	sm.clearRequestedState(sp, state)
	sm.updateSyncPeer(state)
}

// demoteSyncPeer takes the headers role off a sync peer that has stopped
// delivering blocks, and takes nothing else off it.
//
// What it deliberately does NOT do, all of which the disconnect-and-reset path
// still does:
//   - it does not call clearRequestedState. The peer is staying, so stopping its
//     requested-transaction map would leave it with no cleanup at all, and
//     revoking its block ownership would make its late copies arrive looking
//     unrequested and cost it its connection.
//   - it does not disconnect. Up to 2000 verified headers and a live connection
//     were being thrown away because one peer was slow at headers.
//   - it does not reset the header state, so headers-first mode, the header list,
//     the download cursor and the frontier all survive.
//
// The exclusion the disconnect used to provide is the demotion cooldown, read by
// startSync: Connected() is the only liveness test the election makes, and a
// demoted peer still passes it.
func (sm *SyncManager) demoteSyncPeer(sp *peerpkg.Peer, state *peerSyncState) {
	sm.logger.Infof("[demoteSyncPeer] demoting stalled sync peer %s: connection and block assignments kept, headers role moving on", sp.String())

	// The same window that judged it, so it cannot be re-elected before it would
	// be re-judged.
	state.noteDemotedFor(maxLastBlockTime)

	sp.SetSyncPeer(false)
	sm.storeSyncPeer(nil, nil)

	sm.reopenDemotedPeerSlice(sp)

	sm.startSync()
}

// reopenDemotedPeerSlice makes the blocks a demoted peer still owes askable of
// somebody else, and puts the download walk back in front of them.
//
// It is the replacement for the recovery the header-state reset used to provide.
// The walk is forward-only, so without the rewind a demoted peer's slice is
// recovered one block at a time by the frontier race, at its stall window each.
//
// Why this cannot bring back the duplicate-commit storm, which was caused by
// exactly this pairing — a cursor rewind beside a ledger that had been reopened:
//   - only the demoted peer's own assignments are reopened, so every other
//     peer's in-flight block still answers true to RequestedWithin and the
//     re-walk skips it. The whole-ledger back-date that did not is deleted.
//   - the demoted peer keeps ownership of those blocks, so if its copies do turn
//     up they are admitted rather than treated as unrequested.
//   - a hash the walk does re-assign is still assigned to exactly one peer per
//     pass, and AcquireBlockPrefetch refuses a second concurrent copy of a hash
//     outright.
func (sm *SyncManager) reopenDemotedPeerSlice(sp *peerpkg.Peer) {
	// Leaf lock only, and taken with headerMu released.
	reopened := sm.blockDownloads.ForgetForRetryPeer(sp, blockRequestRetryInterval)
	if len(reopened) == 0 {
		return
	}

	lowestHeight, rewound := sm.rewindToLowestHeader(reopened)
	if !rewound {
		sm.logger.Warnf("[demoteSyncPeer] reopened %d blocks owed by %s but none of them is still in the header list; recovery is down to the frontier race", len(reopened), sp.String())

		return
	}

	sm.logger.Infof("[demoteSyncPeer] reopened %d blocks owed by %s and moved the download cursor back to height %d", len(reopened), sp.String(), lowestHeight)
}

// rewindToLowestHeader moves the download cursor back onto the lowest of hashes
// that is still in the header list, and reports its height.
func (sm *SyncManager) rewindToLowestHeader(hashes []chainhash.Hash) (int32, bool) {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	var (
		lowest       *list.Element
		lowestHeight int32
	)

	for _, h := range hashes {
		e := sm.headerIndex[h]
		if e == nil {
			continue
		}

		node, ok := e.Value.(*headerNode)
		if !ok {
			continue
		}

		if lowest == nil || node.height < lowestHeight {
			lowest, lowestHeight = e, node.height
		}
	}

	if lowest == nil {
		return 0, false
	}

	sm.moveStartHeaderBackLocked(lowest)

	return lowestHeight, true
}

// headersRoundLocator returns the locator to send the next getheaders with.
//
// Mid-round it is built from the header list we already have, not from our own
// database best block: handleHeadersMsg requires every incoming header to connect
// to the back of the list, so a locator from the database — which after a
// demotion is hundreds or thousands of headers below it — would have the new sync
// peer answer honestly and be disconnected for it.
//
// With the fan-out off, a sync-peer change resets the header state first, so the
// database locator is the only correct one and is what this returns.
func (sm *SyncManager) headersRoundLocator(bestHash *chainhash.Hash, bestHeight uint32) (blockchain.BlockLocator, error) {
	if sm.settings.Legacy.MultiPeerBlockDownload && sm.headersFirstMode.Load() {
		if locator := sm.headerListLocator(bestHash); len(locator) > 0 {
			return blockchain.BlockLocator(locator), nil
		}
	}

	return sm.blockchainClient.GetBlockLocator(sm.ctx, bestHash, bestHeight)
}

// headerListLocator builds a block locator out of the header list: the back
// first, then stepping back through the list at a doubling stride, then the front
// of the list and our own database best block.
//
// The back has to come first, because a peer that has it answers from it and the
// round continues where it left off. Everything after the back is what makes the
// question answerable by a peer that has not got that far. startSync elects any
// connected candidate above our own height, which mid-round can be up to a full
// headers batch below the back of the list; asked only about the back, such a
// peer recognises nothing, its node falls back to the genesis block, and it
// replies from height 1 — headers whose parent we have never heard of, which
// costs it its connection with a misbehaviour warning for answering honestly.
// A single hash is the degenerate case of this locator, and the degenerate case
// is the one that loses peers.
//
// Bounding it on the peer's claimed height instead would be cheaper but worse in
// two ways: a claimed height is a lower bound that goes stale downward, so a peer
// that does have the back would be sent the database locator and the round would
// not continue; and the database locator's own reply does not connect to the back
// either, so it buys nothing beyond not being disconnected.
//
// Every entry is a block we hold, so a reply is either a continuation from the
// back or an answer whose first header connects to a header we hold — which
// handleHeadersMsg recognises as a late or short answer, ignores, and leaves the
// peer connected. The list front is appended explicitly because the stride can
// step over it, and our database best block last because after the checkpoint
// transition the round's anchor is removed and the front is one above the tip.
func (sm *SyncManager) headerListLocator(bestHash *chainhash.Hash) []*chainhash.Hash {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	if sm.headerList == nil || sm.headerList.Len() == 0 {
		return nil
	}

	locator := make([]*chainhash.Hash, 0, 24)

	step := 1
	skip := 0

	for e := sm.headerList.Back(); e != nil; e = e.Prev() {
		node, ok := e.Value.(*headerNode)
		if !ok || node.hash == nil {
			continue
		}

		if skip > 0 {
			skip--

			continue
		}

		locator = append(locator, node.hash)

		// The first ten are consecutive, as in every other bitcoin locator, so a
		// peer only a few headers behind the back finds its fork point exactly.
		if len(locator) > 10 {
			step *= 2
		}

		skip = step - 1
	}

	if front, ok := sm.headerList.Front().Value.(*headerNode); ok && front.hash != nil {
		if len(locator) == 0 || !locator[len(locator)-1].IsEqual(front.hash) {
			locator = append(locator, front.hash)
		}
	}

	if bestHash != nil && (len(locator) == 0 || !locator[len(locator)-1].IsEqual(bestHash)) {
		locator = append(locator, bestHash)
	}

	return locator
}

// topBlock returns the best chains top block height
func (sm *SyncManager) topBlock() int32 {
	sp := sm.loadSyncPeer()
	if sp == nil {
		return 0
	}

	if sp.LastBlock() > sp.StartingHeight() {
		return sp.LastBlock()
	}

	return sp.StartingHeight()
}

// handleDonePeerMsg deals with peers that have signalled they are done.  It
// removes the peer as a candidate for syncing and in the case where it was
// the current sync peer, attempts to select a new best peer to sync from.  It
// is invoked from the syncHandler goroutine.
func (sm *SyncManager) handleDonePeerMsg(peer *peerpkg.Peer) {
	sm.logger.Debugf("Received done peer message from peer %s", peer)

	state, exists := sm.peerStates.Get(peer)
	if !exists {
		sm.logger.Debugf("Received done peer message for unknown peer %s", peer)
		return
	}

	// Remove the peer from the list of candidate peers.
	sm.peerStates.Delete(peer)

	sm.logger.Infof("Lost peer %s (removed from peerStates)", peer)

	// Cleanup state of requested items.
	sm.clearRequestedState(peer, state)

	// Fetch a new sync peer if this is the sync peer.
	if peer == sm.loadSyncPeer() {
		sm.updateSyncPeer(state)
	}
}

// clearRequestedState releases everything we were still waiting on from a peer
// we are giving up on, so the next inv that announces one of those items fetches
// it from somewhere else.
//
// It used to only call Stop() on the two expiring maps. Stop closes the cleanup
// goroutine's channel and never touches the entries, so nothing was released:
// the per-peer map became garbage the moment the peer was dropped from
// peerStates, and the entries that actually mattered — the departing peer's
// entries in the global map — were never consulted at all. A block owed by a
// peer that has gone was therefore owed forever, and was never asked for again.
func (sm *SyncManager) clearRequestedState(peer *peerpkg.Peer, state *peerSyncState) {
	// Drop the transactions we were waiting on, then stop the map's cleanup
	// goroutine. Clear before Stop: after Stop nothing sweeps it any more.
	state.requestedTxns.Clear()
	state.requestedTxns.Stop()

	// Release every block this peer owed us, and put the download walk back in
	// front of them.
	sm.reopenStrandedSlice(peer, sm.blockDownloads.ClearPeer(peer))
}

// reopenStrandedSlice moves the download cursor back onto the blocks a peer we
// have just given up on was still carrying.
//
// It is reopenDemotedPeerSlice's other half. That one exists because a demoted
// sync peer's slice would otherwise be recovered one block at a time by the
// frontier race, at its stall window each; the same is true of every other peer
// the scheduler hands a contiguous run to, and a departing peer is worse than a
// demoted one — its blocks are owed by nobody at all, so nothing is ever coming.
// Without the rewind the run sits behind a forward-only cursor, and because the
// runs are ascending the lowest of them is routinely the front of the header
// list, which is the block every commit behind it is waiting on. The escape is
// then the sync peer's own 180-second stall window, and only if the sync peer is
// also unhealthy — the frontier race declines to fire while the peer that owes
// the frontier is pulling bytes at a healthy rate.
//
// Nothing here can bring back the duplicate-commit storm: these blocks are owed
// by nobody once ClearPeer has run, so a re-walk hands each of them to exactly
// one peer, and any hash the departing peer shared with a live peer still answers
// true to RequestedWithin and is skipped.
//
// With the fan-out off this is dead weight: only the sync peer is ever asked for
// a body, and its departure resets the header state, so there is no list left to
// rewind.
func (sm *SyncManager) reopenStrandedSlice(p *peerpkg.Peer, released []chainhash.Hash) {
	if len(released) == 0 || !sm.settings.Legacy.MultiPeerBlockDownload {
		return
	}

	lowestHeight, rewound := sm.rewindToLowestHeader(released)
	if !rewound {
		sm.logger.Debugf("[clearRequestedState] released %d blocks owed by %s, none of them still in the header list", len(released), p.String())

		return
	}

	sm.logger.Infof("[clearRequestedState] released %d blocks owed by %s and moved the download cursor back to height %d", len(released), p.String(), lowestHeight)
}

// updateSyncPeer picks a new peer to sync from.
func (sm *SyncManager) updateSyncPeer(_ *peerSyncState) {
	sp, sps := sm.loadSyncPeerAndState()
	sm.logger.Infof("Updating sync peer, last block: %v, violations: %v, headers-first mode: %v",
		sps.getLastBlockTime(),
		sps.getViolations(),
		sm.headersFirstMode.Load())

	// Only disconnect if we have a valid sync peer
	if sp != nil {
		// Log current sync state before disconnecting
		if sm.headersFirstMode.Load() {
			sm.headerMu.Lock()
			hlLen := sm.headerList.Len()
			haveStart := sm.startHeader != nil
			sm.headerMu.Unlock()

			sm.logger.Debugf("Current header sync state - headerList length: %d, startHeader exists: %v", hlLen, haveStart)
		}

		sp.SetSyncPeer(false)
		sp.DisconnectWithInfo("updateSyncPeer - disconnect old sync peer")
	}

	// Reset sync peer state
	sm.storeSyncPeer(nil, nil)

	bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		// TODO we should return an error here to the caller
		sm.logger.Errorf(failedToGetBestBlockHeaderMsg, err)
		return
	}

	bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf(failedToConvertBlockHeightInt32Msg, err)
		return // add return to prevent continuing with invalid height
	}

	if sm.headersFirstMode.Load() {
		sm.logger.Infof("Resetting header sync state at height %d with hash %v",
			bestBlockHeightInt32, bestBlockHeader.Hash())

		sm.resetHeaderState(bestBlockHeader.Hash(), bestBlockHeightInt32)
	}

	sm.startSync()
}

// handleTxMsg handles transaction messages from all peers.
func (sm *SyncManager) handleTxMsg(tmsg *txMsg) {
	ctx, _, _ := tracing.Tracer("SyncManager").Start(sm.ctx, "handleTxMsg",
		tracing.WithHistogram(prometheusLegacyNetsyncHandleTxMsg),
		tracing.WithDebugLogMessage(sm.logger, "handling transaction message for %s from %s", tmsg.tx.Hash(), tmsg.peer),
	)

	peer := tmsg.peer

	state, exists := sm.peerStates.Get(peer)
	if !exists {
		sm.logger.Warnf("Received tx message from unknown peer %s", peer)
		return
	}

	// NOTE: BitcoinJ, and possibly other wallets, don't follow the spec of
	// sending an inventory message and allowing the remote peer to decide
	// whether or not they want to request the transaction via a getdata
	// message.  Unfortunately, the reference implementation permits
	// unrequested data, so it has allowed wallets that don't follow the
	// spec to proliferate.  While this is not ideal, there is no check here
	// to disconnect peers for sending unsolicited transactions to provide
	// interoperability.
	txHash := tmsg.tx.Hash()

	// Ignore transactions that we have already rejected.  Do not
	// send a reject message here because if the transaction was already
	// rejected, the transaction was unsolicited.
	if _, exists = sm.rejectedTxns.Get(*txHash); exists {
		sm.logger.Debugf("Ignoring unsolicited previously rejected transaction %v from %s", txHash, peer)
		return
	}

	// Validate the transaction using the validation service
	buf := bytes.NewBuffer(make([]byte, 0, tmsg.tx.MsgTx().SerializeSize()))
	_ = tmsg.tx.MsgTx().Serialize(buf)

	// Single inbound tx per call, passed downstream to the validator. Stays
	// on the standard heap path — no arena amortisation possible for a
	// one-shot decode where the tx must outlive this function frame.
	btTx, err := bt.NewTxFromBytes(buf.Bytes())
	if err != nil {
		sm.logger.Errorf("Failed to create transaction from bytes: %v", err)
		return
	}

	var txMeta *meta.Data

	timeStart := time.Now()
	// passing in block height 0, which will default to utxo store block height in validator
	txMeta, err = sm.validationClient.Validate(ctx, btTx, 0)

	prometheusLegacyNetsyncHandleTxMsgValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	// Remove transaction from request maps. Either the mempool/chain
	// already knows about it and as such we shouldn't have any more
	// instances of trying to fetch it, or we failed to insert and thus
	// we'll retry next time we get an inv.
	state.requestedTxns.Delete(*txHash)
	sm.requestedTxns.Delete(*txHash)

	if err != nil {
		// ErrTxCreating is the same situation as ErrTxLocked — the parent this tx spends
		// from is still completing its own commit, just via the multi-record write path —
		// so it parks for the same reason.
		if errors.Is(err, errors.ErrTxMissingParent) || errors.Is(err, errors.ErrTxLocked) || errors.Is(err, errors.ErrTxCreating) {
			// this is an orphan transaction, we will accept it when the parent comes in
			// first check if the transaction already exists in the orphan pool, otherwise add it
			if _, orphanTxExists := sm.orphanTxs.Get(*txHash); !orphanTxExists {
				sm.logger.Debugf("orphan transaction %v added from %s", txHash, peer)

				// create a map of the parents of the transaction for faster lookups
				txParents := txmap.NewSyncedMap[chainhash.Hash, struct{}]()
				for _, input := range tmsg.tx.MsgTx().TxIn {
					txParents.Set(input.PreviousOutPoint.Hash, struct{}{})
				}

				sm.orphanTxs.Set(*txHash, &orphanTxAndParents{
					tx:      btTx,
					parents: txParents,
					addedAt: time.Now(),
				})
			}

			return
		} else {
			// Do not request this transaction again until a new block
			// has been processed.
			sm.rejectedTxns.Set(*txHash, struct{}{})

			// When the error is a rule error, it means the transaction was
			// simply rejected as opposed to something actually going wrong,
			// so log it as such.  Otherwise, something really did go wrong,
			// so log it as an actual error.
			sm.logger.Errorf("Failed to process transaction %v: %v", txHash, err)

			// Convert the error into an appropriate reject message and send it.
			// TODO better rejection code and message from the error
			peer.PushRejectMsg(wire.CmdTx, wire.RejectInvalid, "rejected", txHash, false)

			return
		}
	}

	// acceptedTxs also should contain any orphan transactions that were accepted when this transaction was processed
	acceptedTxs := []*TxHashAndFee{{
		TxHash: *btTx.TxIDChainHash(),
		Fee:    txMeta.Fee,
	}}

	// process any orphan transactions that were waiting for this transaction to be accepted
	// this is a recursive call, but the orphan pool should be limited in size
	sm.processOrphanTransactions(ctx, btTx.TxIDChainHash(), &acceptedTxs)

	if len(acceptedTxs) > 0 {
		sm.peerNotifier.AnnounceNewTransactions(acceptedTxs)
	}
}

// processOrphanTransactions recursively processes orphan transactions that were waiting for a transaction to be accepted
func (sm *SyncManager) processOrphanTransactions(ctx context.Context, txHash *chainhash.Hash, acceptedTxs *[]*TxHashAndFee) {
	// check whether any transaction in the orphan pool has this transaction as a parent
	ctx, _, deferFn := tracing.Tracer("SyncManager").Start(ctx, "processOrphanTransactions",
		tracing.WithHistogram(prometheusLegacyNetsyncProcessOrphanTransactions),
	)
	defer deferFn()

	// remove the transaction from the orphan pool
	sm.orphanTxs.Delete(*txHash)

	// first we get all the orphan transactions, this will not block the orphan tx pool while processing
	orphanTxs := sm.orphanTxs.Items()

	for _, orphanTx := range orphanTxs {
		// check if the orphan transaction has this transaction as a parent
		if _, ok := orphanTx.parents.Get(*txHash); !ok {
			continue
		}

		// validate the orphan transaction
		// passing in block height 0, which will default to utxo store block height in validator
		txMeta, err := sm.validationClient.Validate(ctx, orphanTx.tx, 0)
		if err != nil {
			if errors.Is(err, errors.ErrTxMissingParent) || errors.Is(err, errors.ErrTxLocked) || errors.Is(err, errors.ErrTxCreating) {
				// silently exit, we will accept this transaction when the other parent(s) comes in
				// or when the transaction is spendable again
				continue
			}

			if errors.Is(err, errors.ErrTxConflicting) {
				// remove the tx from the orphan pool, it is a double spend
				sm.orphanTxs.Delete(*txHash)
				continue
			}

			// if the transaction was rejected, we will not process any of the orphan transactions that were waiting for it
			sm.logger.Errorf("Failed to process orphan transaction %v: %v", txHash, err)

			continue
		}

		// add the orphan transaction to the list of accepted transactions
		*acceptedTxs = append(*acceptedTxs, &TxHashAndFee{
			TxHash: *orphanTx.tx.TxIDChainHash(),
			Fee:    txMeta.Fee,
			Size:   txMeta.SizeInBytes,
		})

		// add the time it took to process the orphan transaction to the histogram
		prometheusLegacyNetsyncOrphanTime.Observe(float64(time.Since(orphanTx.addedAt).Microseconds()) / 1_000_000)

		// process any orphan transactions that were waiting for this transaction to be accepted
		sm.processOrphanTransactions(ctx, orphanTx.tx.TxIDChainHash(), acceptedTxs)
	}
}

// isCurrent returns whether the sync manager believes it is synced with the chain.
// this function is a rewrite of the function in the original bsvd blockchain package
func (sm *SyncManager) isCurrent(bestBlockHeaderMeta *model.BlockHeaderMeta) bool {
	// Not current if the latest main (best) chain height is before the
	// latest known good checkpoint (when checkpoints are enabled).
	if len(sm.chainParams.Checkpoints) > 0 {
		bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
		if err != nil {
			sm.logger.Errorf(failedToConvertBlockHeightInt32Msg, err)
		}

		checkpoint := &sm.chainParams.Checkpoints[len(sm.chainParams.Checkpoints)-1]
		if bestBlockHeightInt32 < checkpoint.Height {
			return false
		}
	}

	// Not current if the latest best block has a timestamp before 24 hours ago.
	//
	// The chain appears to be current if none of the checks reported otherwise.
	// minus24Hours := b.timeSource.AdjustedTime().Add(-24 * time.Hour).Unix()
	minus24Hours := time.Now().Add(-24 * time.Hour).Unix()

	current := int64(bestBlockHeaderMeta.BlockTime) >= minus24Hours

	return current
}

// current returns true if we believe we are synced with our peers, false if we
// still have blocks to check.
//
// It costs a blockchain round trip, and GetBestBlockHeader can block for minutes
// during initial sync, so it must only ever be called from a goroutine that can
// afford to wait. Anything on a peer's own goroutine has to read the answer this
// leaves behind instead — see IsCurrentCached.
func (sm *SyncManager) current() bool {
	answer := sm.computeCurrent()
	sm.currentCached.Store(answer)

	return answer
}

// IsCurrentCached reports the last answer current() worked out, without asking
// the blockchain anything.
//
// It exists because the peer layer needs to know whether we are catching up in
// order to size a block download deadline, and it asks on a fifteen-second timer
// from every peer's stall handler. Answering that with the real call put an
// unbounded blockchain round trip on the one goroutine that drains a peer's
// stallControl channel — a buffered-1 channel whose sends from inHandler are
// blocking — so a slow blockchain service would stop that peer reading its
// socket, and stop the stall detector disconnecting anybody, which is the exact
// failure the stall handler exists to catch.
//
// current() runs on the sync manager's own goroutine on every message it handles
// while the node is behind, and on every inventory announcement once it is not,
// so the cached answer is refreshed constantly in both regimes. It starts false,
// which reads as "still catching up" — the safe direction, because the wider
// deadline that follows from it does not disconnect a peer early.
func (sm *SyncManager) IsCurrentCached() bool {
	return sm.currentCached.Load()
}

// computeCurrent is current() without the caching, and is what actually asks the
// blockchain.
func (sm *SyncManager) computeCurrent() bool {
	_, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		sm.logger.Errorf("[current] failed to get best block header: %v", err)
		return false
	}

	if !sm.isCurrent(bestBlockHeaderMeta) {
		return false
	}

	// if blockChain thinks we are current, and we have no syncPeer, it is probably right.
	sp := sm.loadSyncPeer()
	if sp == nil {
		return true
	}

	bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf(failedToConvertBlockHeightInt32Msg, err)
	}

	// No matter what the chain thinks, if we are below the block we are syncing to we are not current.
	if bestBlockHeightInt32 < sp.LastBlock() {
		return false
	}

	return true
}

// newBlockFailureBackoffMap builds the per-block transient-failure backoff map
// (#1187), or returns nil when the backoff is disabled (either knob <= 0). A nil
// map is a clean no-op via the nil-guards in handleBlockMsg; returning nil when
// disabled also avoids constructing an expiringmap with a zero TTL, which spawns
// no cleanup goroutine and would leak entries. WithMaxSize bounds the map.
//
// The map TTL is deliberately DECOUPLED from the backoff cap (window): it is
// window + maxAttempt, not window. window caps the retry SPACING and must stay
// below the 180s sync-peer stall window; but the failure COUNT that drives the
// linear ramp only survives while the entry is live, and the gap between two
// consecutive recordBlockFailureBackoff calls is (retry spacing ≤ window) + (one
// full failing HandleBlockDirect attempt). On the exact #1187 overload path that
// attempt rides the Aerospike overload-retry budget and can reach ~2.5min — well
// over window alone — so a TTL of just window would expire the entry mid-attempt
// and reset the count to 1 every time, pinning the backoff at its base and
// defeating the ramp. Adding maxAttempt (the per-attempt processing bound) keeps
// the entry alive across one slow attempt so the count ramps as intended.
func newBlockFailureBackoffMap(base, window, maxAttempt time.Duration) *expiringmap.ExpiringMap[chainhash.Hash, *blockFailureState] {
	if base <= 0 || window <= 0 {
		return nil
	}

	retention := window
	if maxAttempt > 0 {
		retention += maxAttempt
	}

	return expiringmap.New[chainhash.Hash, *blockFailureState](retention).WithMaxSize(blockFailureBackoffMaxTracked)
}

// recordBlockFailureBackoff records or extends the transient-failure backoff for
// a block hash (#1187). The failure count increases by one per consecutive
// failure (resetting once the map TTL forgets the hash) and the next-retry window
// grows linearly (count * base), capped at Legacy.BlockFailureBackoffMaxDuration.
// Callers must ensure sm.blockFailureBackoff is non-nil.
func (sm *SyncManager) recordBlockFailureBackoff(blockHash chainhash.Hash) {
	attempts := 1
	if fs, ok := sm.blockFailureBackoff.Get(blockHash); ok {
		attempts = fs.attempts + 1
	}

	backoff := time.Duration(attempts) * sm.settings.Legacy.BlockFailureBackoffBase
	if maxBackoff := sm.settings.Legacy.BlockFailureBackoffMaxDuration; backoff > maxBackoff {
		backoff = maxBackoff
	}

	sm.blockFailureBackoff.Set(blockHash, &blockFailureState{
		attempts:  attempts,
		nextRetry: time.Now().Add(backoff),
	})
}

// peerStateResolvingPrimary returns the sync state for peer, resolving a stream
// sub-peer (e.g. a BlockPriority DATA1 stream, not itself registered in
// peerStates) to its association's primary peer. It returns the resolved peer
// (the primary when a stream peer resolved, otherwise the input peer) and
// whether a state was found. Centralizes the stream→primary walk previously
// inlined in handleBlockMsg/handleHeadersMsg/handleInvMsg/BlockRequested; call
// sites that log the resolution or reassign to the primary compare the returned
// peer against their input (resolved != input means a stream peer resolved).
func (sm *SyncManager) peerStateResolvingPrimary(peer *peerpkg.Peer) (*peerSyncState, *peerpkg.Peer, bool) {
	if state, exists := sm.peerStates.Get(peer); exists {
		return state, peer, true
	}

	if assoc := peer.AssociationRef(); assoc != nil {
		if primary := assoc.PrimaryPeer(); primary != nil {
			if state, exists := sm.peerStates.Get(primary); exists {
				return state, primary, true
			}
		}
	}

	return nil, peer, false
}

// handleBlockMsg handles block messages from all peers.
// requestMissingBlocks answers a missing-parent condition by sending a getblocks
// message from our best block, so block validation can proceed in order. In the
// legacy sync protocol the orphan tip also doubles as the batch-continuation
// signal, so this must fire even when the missing parent is a known-failed block
// (#1333) — otherwise sync stalls until the stall detector rotates the peer.
// PushGetBlocksMsg filters duplicate requests and the peer only invs blocks past
// the locator fork point, so a redundant request costs one inv message at most.
// Errors are logged and swallowed; the request is best-effort.
func (sm *SyncManager) requestMissingBlocks(peer *peerpkg.Peer, blockHash chainhash.Hash) {
	bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		sm.logger.Errorf(failedToGetBestBlockHeaderMsg, err)
		return
	}

	// Create a block locator starting from our best block.
	locator, err := sm.blockchainClient.GetBlockLocator(sm.ctx, bestBlockHeader.Hash(), bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf("Failed to get block locator for the block hash %s: %v", blockHash, err)
		return
	}

	zeroHash := chainhash.Hash{}
	if err = peer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
		sm.logger.Errorf("Failed to send getblocks message: %v", err)
	}
}

// advanceHeaderListFor moves the header list on for a block that has arrived.
//
// When in headers-first mode, if the block matches the hash of the first header
// in the list of headers that are being fetched, it is eligible for less
// validation since the headers have already been verified to link together and
// are valid up to the next checkpoint. The list entry is removed for all blocks
// except the checkpoint, which is needed to verify that the next round of
// headers links properly.
//
// It returns whether this was the checkpoint block, and the header node it took
// off the front — nil when the block was not the front, or was the checkpoint
// and so was left in place. Both the arriving-block path and the park drain go
// through here: a block committed off disk never passes the arrival path, so
// without this the front would stick on a block we already have, the next block
// would never match it, the frontier would never be republished and the
// checkpoint transition would never fire.
func (sm *SyncManager) advanceHeaderListFor(blockHash chainhash.Hash) (isCheckpointBlock bool, removedFront *headerNode) {
	if !sm.headersFirstMode.Load() {
		return false, nil
	}

	// Explicit Unlock, not defer: the callers run for hundreds of lines past
	// here and make blocking client calls, so a deferred unlock would turn this
	// into a serialisation bug.
	sm.headerMu.Lock()

	firstNodeEl := sm.headerList.Front()
	if firstNodeEl != nil {
		firstNode := firstNodeEl.Value.(*headerNode)

		if blockHash.IsEqual(firstNode.hash) {
			if sm.nextCheckpoint != nil && firstNode.hash.IsEqual(sm.nextCheckpoint.Hash) {
				isCheckpointBlock = true
			} else {
				sm.unindexHeaderLocked(firstNodeEl, *firstNode.hash)
				sm.headerList.Remove(firstNodeEl)

				removedFront = firstNode
			}

			// The block everything else was queued behind has arrived. Tidy
			// up after any race for it, then move the frontier on. At a
			// checkpoint the header node stays in the list to anchor the
			// next round of headers, but the block itself is no longer
			// outstanding, so there is nothing to race until the next batch
			// of getdata requests goes out.
			sm.noteRaceWinner(blockHash)

			if isCheckpointBlock {
				sm.clearFrontier()
			} else {
				sm.publishFrontierLocked(time.Now())
			}
		}
	}

	sm.headerMu.Unlock()

	return isCheckpointBlock, removedFront
}

func (sm *SyncManager) handleBlockMsg(bmsg *blockQueueMsg) error {
	sm.logger.Debugf("[handleBlockMsg][%s] received block height %d from %s", bmsg.blockHash, bmsg.blockHeight, bmsg.peer)
	peer := bmsg.peer

	state, resolved, exists := sm.peerStateResolvingPrimary(peer)
	if !exists {
		sm.logger.Errorf("[handleBlockMsg][%s] Received block message from unknown peer %s", bmsg.blockHash, peer)
		return errors.NewServiceError("[handleBlockMsg] Received block message from unknown peer %s", peer)
	}
	if resolved != peer {
		// Stream peers (e.g. BlockPriority) are not registered in peerStates
		// directly - resolved via their association's primary peer instead.
		sm.logger.Debugf("[handleBlockMsg][%s] resolved stream peer %s to primary peer %s", bmsg.blockHash, peer, resolved)
		peer = resolved
	}

	// Under async prefetch, awaitBlockResult disconnects the source peer on its
	// first validation failure, but blocks it already admitted keep draining the
	// queue FIFO until handleDonePeerMsg evicts peerStates — a racy window in
	// which we would validate the whole tail of a peer that has already proven it
	// serves bad blocks. Peer.Disconnect* flips the connected flag synchronously
	// (atomic), so skipping here once that flag drops stops the rest of the tail.
	//
	// This is a BEST-EFFORT tail-stop, NOT a barrier (#1280). awaitBlockResult
	// runs in its own goroutine and only disconnects after it receives block N's
	// failure reply, so in the window between N failing and that flag flipping,
	// this FIFO consumer can dequeue and fully validate N+1, N+2, … . The guard
	// bounds the wasted work to the handful of blocks dequeued inside that
	// async-disconnect window — not strictly one — and that is an accepted,
	// self-limiting cost: the peer is being dropped regardless, the window is
	// short, and total in-flight bytes are already capped by the prefetch budget.
	// A true barrier (mark the peer un-processable synchronously on failure, in
	// this single FIFO consumer, and skip its whole queued tail) was considered
	// and deliberately not taken — not worth the hot-path state and teardown
	// lifecycle for a bounded, low-severity cost inherent to decoupling download
	// from processing.
	//
	// We test bmsg.peer — the exact peer OnBlock queued and awaitBlockResult
	// tears down (sp.Peer) — NOT the resolved primary. On a bad block
	// awaitBlockResult calls disconnectMisbehaving, which drops the WHOLE
	// association (the primary first, then the stream sub-peer), so either flag
	// would flip for the misbehaviour case; but bmsg.peer is the peer that queued
	// this tail, and it also drops on sub-peer-scoped teardowns (TCP loss,
	// RemoveStream) that a primary check would not reflect — so it is the tighter
	// guard. The ServiceError is benign to shouldDisconnectOnBlockErr, so it only
	// makes awaitBlockResult release budget and log — no second disconnect. Gated
	// on UsePrefetchIngestion so the regtest/synchronous path, where
	// block-acceptance tooling feeds blocks in ways this must not disturb, is
	// completely untouched.
	if sm.UsePrefetchIngestion() && !bmsg.peer.Connected() {
		sm.logger.Debugf("[handleBlockMsg][%s] skipping block from disconnected peer %s", bmsg.blockHash, bmsg.peer)
		return errors.NewServiceError("[handleBlockMsg] skipping block %s from disconnected peer %s", bmsg.blockHash, bmsg.peer)
	}

	catchingBlocks := false

	sm.logger.Debugf("[handleBlockMsg][%s] checking current FSM state", bmsg.blockHash)

	fsmState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
	if err != nil {
		return errors.NewProcessingError("[handleBlockMsg] failed to get current FSM state", err)
	}

	if fsmState != nil && *fsmState == teranodeblockchain.FSMStateCATCHINGBLOCKS {
		catchingBlocks = true
	}

	// If we didn't ask for this block then the peer is misbehaving.
	if !sm.blockDownloads.HasOwner(peer, bmsg.blockHash) {
		// The regression test intentionally sends some blocks twice
		// to test duplicate block insertion fails.  Don't disconnect
		// the peer or ignore the block when we're in regression test
		// mode, in this case, so the chain code is actually fed the
		// duplicate blocks.
		if sm.chainParams != &chaincfg.RegressionNetParams {
			// Unless this is a peer we deliberately asked for a second copy of a
			// block that has since arrived. Then it is answering our own
			// question, just too late to be useful, and disconnecting it would
			// make the stall recovery cost more than the stall.
			if sm.BlockRacedTo(peer, &bmsg.blockHash) {
				sm.logger.Debugf("[handleBlockMsg][%s] discarding late copy from %s, another peer already delivered it", bmsg.blockHash, peer)

				return errors.NewServiceError("[handleBlockMsg] late copy of block %v from %s", bmsg.blockHash, peer)
			}

			reason := fmt.Sprintf("Got unrequested block %v", bmsg.blockHash)
			peer.DisconnectWithWarning(reason)

			return errors.NewServiceError("Got unrequested block %v", bmsg.blockHash)
		}
	}

	// When in headers-first mode, if the block matches the hash of the
	// first header in the list of headers that are being fetched, it's
	// eligible for less validation since the headers have already been
	// verified to link together and are valid up to the next checkpoint.
	// Also, remove the list entry for all blocks except the checkpoint
	// since it is needed to verify the next round of headers links
	// properly.
	// isCheckpointBlock says the block just taken off the header list is the
	// checkpoint the list was anchored on; removedFront is the header node its
	// arrival took off the front, kept so a drop further down can put it back.
	isCheckpointBlock, removedFront := sm.advanceHeaderListFor(bmsg.blockHash)

	// This peer has answered, so it no longer owes us the block: either the
	// chain will know about it and nobody needs to fetch it again, or the insert
	// fails and we retry next time we get an inv.
	//
	// Only this peer's obligation is cancelled. Any other peer we also asked
	// keeps its ownership, exactly as the per-peer map it replaced did, so a
	// second copy already on the wire lands as an answer to our own question
	// rather than as an unrequested block that costs an honest peer its
	// connection.
	sm.blockDownloads.RemoveOwner(peer, bmsg.blockHash)

	// What it keeps is the permission, not the debt. Forgiving the rest is what
	// gives their budget back and makes the hash re-requestable at once, and it
	// has to happen on every arrival rather than only on a raced one: the
	// frontier race was the only path that released them, and it returns early
	// unless this block is the current frontier with a non-empty racer set. A
	// second owner arises without any race whenever a rewind puts the walk in
	// front of a block whose owner was asked more than blockRequestRetryInterval
	// ago and is still transferring, because the skip above no longer covers it
	// and the assigner is free to hand it to somebody else. Left un-forgiven
	// that first owner spends an in-flight slot in CountForPeer until its own
	// copy lands, it disconnects, or the assignment TTL expires an hour later.
	sm.blockDownloads.ForgiveOwners(bmsg.blockHash, blockRequestRetryInterval)

	// Per-block transient-failure backoff (#1187): if this block recently failed
	// with a storage/service error, skip the expensive HandleBlockDirect path
	// until the backoff window elapses instead of re-running the full decorate at
	// full concurrency. Returning a retryable error (not sleeping) keeps the
	// single block-processing goroutine free. The block was already removed from
	// download ledger above, so re-delivery is driven by the existing recovery
	// plumbing — a later block arrives as an orphan of this un-stored one and
	// triggers a getblocks that re-requests it — not by a proactive re-request
	// here. Two things keep this from stalling sync (#1187, review): the backoff
	// cap defaults below the stall-detector window (maxLastBlockTime) so the
	// window reliably outlasts a transient backoff, and the delivering sync
	// peer's last-block-time is refreshed on skip (below) so that peer is not
	// rotated for a fault that is local, not the peer's — rotating it in would
	// only re-deliver the same still-backed-off block and thrash peers with zero
	// forward progress. Placed before the block-size sampling below so a block
	// re-delivered repeatedly while backed off does not keep re-sampling its size
	// into the moving average and biasing calculateMaxInFlightBlocks() (only
	// actually-processed blocks should feed the tracker). Nil-guarded: tests build
	// SyncManager as a struct literal that bypasses New().
	if sm.blockFailureBackoff != nil {
		if fs, ok := sm.blockFailureBackoff.Get(bmsg.blockHash); ok && time.Now().Before(fs.nextRetry) {
			sm.logger.Warnf("[handleBlockMsg][%s] in backoff after %d transient failure(s), skipping until %s", bmsg.blockHash, fs.attempts, fs.nextRetry)
			// The peer just delivered this block — the fault is our local store,
			// not the peer — so keep its stall timer fresh. No-op unless peer is
			// the current sync peer.
			if sps, ok := sm.syncPeerStateFor(peer); ok {
				sps.updateLastBlockTime()
			}

			// advanceHeaderListFor has already taken this block's header off the
			// front, so without this the block leaves the download walk here and
			// nothing in headers-first mode ever asks for it again. The walk does
			// not re-request it straight away: commitHeaderCandidates stops on a
			// block that is still inside its backoff and leaves the cursor on it.
			sm.rewindHeaderCursor(bmsg.blockHash, removedFront)

			return errors.NewServiceUnavailableError("[handleBlockMsg][%s] block in backoff after %d transient failure(s)", bmsg.blockHash, fs.attempts)
		}
	}

	// Hand sole ownership of the decoded block to HandleBlockDirect. The
	// blockHandler goroutine keeps *bmsg alive until the reply is sent, so
	// leaving the field set would pin the multi-GB wire block (and its decode
	// arena) for the whole minutes-long processing of a big block. Copy the
	// parent hash first — the missing-parent error path below needs it.
	msgBlock := bmsg.block
	if msgBlock == nil {
		// No rewind here on purpose. This is not a block we downloaded and then
		// dropped, it is a queue message that never carried one — a programming
		// fault, not a sync one — and every test that advances the header list
		// by hand comes through here.
		return errors.NewProcessingError("[handleBlockMsg][%s] block message carries no block", bmsg.blockHash)
	}

	prevBlockHash := msgBlock.Header.PrevBlock
	bmsg.block = nil

	// #1333: if this block's parent recently failed to store/validate, skip the
	// descendant before the block-lookup RPCs. Each descendant would otherwise
	// fail its parent lookup and log a misleading "previous block NOT_FOUND"
	// ERROR, burying the one root failure that matters. Mark this block failed too
	// so the whole descendant chain is suppressed transitively; refresh the
	// delivering peer's stall timer (the fault is a rejected ancestor, not the
	// peer); and still answer with a getblocks so sync recovers once the root
	// block is resolved. Placed before the block-size sampling below (like the
	// #1187 backoff skip above) so a skipped, re-delivered descendant does not
	// keep re-sampling its size into the moving average and biasing
	// calculateMaxInFlightBlocks().
	if sm.recentlyFailedBlocks != nil {
		if _, failed := sm.recentlyFailedBlocks.Get(prevBlockHash); failed {
			sm.recentlyFailedBlocks.Set(bmsg.blockHash, struct{}{})
			sm.logger.Debugf("[handleBlockMsg][%s] parent %s recently failed to store/validate; skipping descendant (root failure already logged)", bmsg.blockHash, prevBlockHash)

			if sps, ok := sm.syncPeerStateFor(peer); ok {
				sps.updateLastBlockTime()
			}

			// No rewind here, and it is worth saying why rather than leaving the
			// omission to be found again. This block's header is only taken off
			// the front if this block IS the front, which needs its parent's
			// header to be gone already. Every path that gives a parent up puts
			// its header straight back (dropBlockFromWalk), judged or merely
			// unlucky, so this block is never the front and never leaves the
			// list. The walk reaches it in order once the parent clears, and
			// until then the parent's own backoff holds the walk on the parent
			// rather than letting it run on into descendants like this one.
			sm.requestMissingBlocks(peer, bmsg.blockHash)

			return nil
		}
	}

	// Track block size for dynamic in-flight adjustment during headers-first mode.
	// This allows us to start aggressive (20 blocks) and automatically reduce
	// to 1 block when encountering large (>2GB) blocks on mainnet.
	if sm.headersFirstMode.Load() {
		blockSize := int64(msgBlock.SerializeSize())
		sm.blockSizeTracker.addBlockSize(blockSize)

		dynamicMax := sm.blockSizeTracker.calculateMaxInFlightBlocks()
		avgSize := sm.blockSizeTracker.getAverageSize()
		sm.logger.Debugf("[handleBlockMsg][%s] Block size: %d bytes, avg: %d bytes, dynamic max in-flight: %d",
			bmsg.blockHash, blockSize, avgSize, dynamicMax)
	}

	sm.logger.Debugf("[handleBlockMsg][%s] calling HandleBlockDirect", bmsg.blockHash)

	// Process the block directly. A missing-parent error (ErrBlockNotFound)
	// always triggers a getblocks request from our best block so block
	// validation can proceed in order — see the orphan-continuation note below.
	if err = sm.HandleBlockDirect(sm.ctx, bmsg.peer, bmsg.blockHash, msgBlock); err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) {
			// We don't have the parent of this block. While catching blocks
			// this is typically the peer announcing its tip while we are
			// still behind — and in the legacy sync protocol that orphan tip
			// doubles as the batch-continuation signal: the peer pushes its
			// tip inv after delivering a getblocks batch and waits for the
			// next getblocks before sending more. Swallowing the orphan here
			// stalls the sync until the stall detector rotates the peer, so
			// always answer with a getblocks from our best block.
			// PushGetBlocksMsg filters duplicate requests and the peer only
			// invs blocks past the locator fork point, so a redundant
			// request costs one inv message at most.
			// Keep the block instead of throwing it away. It is checked before
			// anything reaches the disk, so a peer cannot fill the park with
			// rubbish, and it is committed from disk as soon as its parent
			// lands.
			entry := parkedBlock{
				hash:      bmsg.blockHash,
				prevBlock: prevBlockHash,
				height:    bmsg.blockHeight,
				// The resolved association primary, not bmsg.peer. A block
				// delivered on a stream sub-peer (BlockPriority DATA1) carries
				// that sub-peer, and sub-peers are not registered in peerStates
				// — so noteCommittedParkedBlock's lookup missed and the height
				// bookkeeping a committed block is supposed to do was silently
				// skipped. The ledger records the primary too, which is the
				// identity HasOwner and the reject path ask about.
				peer: peer,
				// The header node this block's arrival already took off the
				// front, so whichever path eventually gives the block up can put
				// it back. Without it a parked front block is unreachable: its
				// header is gone from the list and from the index, and the
				// rewind has nothing to work from.
				removedFront: removedFront,
			}

			result := sm.blockPark.Park(sm.ctx, entry, msgBlock)

			// The same table the drain and the sweep answer to: whether the
			// blob survives, whether the walk goes back onto the block, and
			// whether the peer hears about it. See block_park_policy.go.
			d := parkWriteOutcome(result)

			if catchingBlocks {
				// While catching blocks handleBlockMsg suppresses every other
				// reject, because we are replaying history rather than judging
				// a peer's tip. Parking must not judge a peer differently from
				// discarding.
				d = d.withoutBlame()
			}

			// The reject, like every other reject in handleBlockMsg, goes to the
			// resolved primary rather than to a stream sub-peer, so it is
			// applied to a copy of the entry that names it. Safe to copy because
			// every write outcome leaves the blob alone: whatever the park kept
			// it kept under its own entry, and nothing the caller is holding is
			// charged against the budget.
			blamed := entry
			blamed.peer = peer

			if result == parkAccepted {
				sm.logger.Infof("Block %v is waiting on its parent %v, parked", bmsg.blockHash, prevBlockHash)

				// No stall-timer refresh here. HandleBlockDirect already
				// refreshed it at receipt, before the parent lookup that made
				// this an orphan, so a second refresh bought nothing — and a
				// peer that answers with an endless stream of orphans is exactly
				// what the stall detector exists to rotate. The drop path below
				// has never refreshed it, and parking must not judge a peer
				// differently from discarding.
				sm.applyParkDisposition(blamed, d)

				// The block is kept, so the walk must NOT be rewound onto it: it
				// is already downloaded and the park commits it from disk when
				// the parent lands. Every path that later gives the block up
				// rewinds then instead. What does need doing is topping the
				// pipeline back up, because this peer's in-flight count just
				// dropped and nothing else will notice.
				sm.fetchMoreHeaderBlocks(peer)
			} else {
				sm.logger.Infof("Block %v has missing parent %v and was not kept (%s), requesting missing blocks",
					bmsg.blockHash, prevBlockHash, d.reason)

				// The block is being dropped, so put the download walk back on
				// it. Without this the getblocks below is the only recovery
				// there is, and in headers-first mode it is inert: processInvMsg
				// returns before it can request anything, so the block is never
				// asked for again.
				sm.applyParkDisposition(blamed, d)
			}

			// Parked or dropped, the parent still has to be asked for, and the
			// getblocks is not an alternative to keeping the block — it is the
			// only thing that fetches the gap. It is also the batch-continuation
			// signal the legacy protocol runs on: the peer pushes its tip after
			// a batch and then sends nothing at all until the next getblocks
			// arrives. Sent in both modes, exactly as the drop path has always
			// sent it, so turning the park on cannot change what a peer sees.
			// Inside headers-first mode the reply is dropped by processInvMsg
			// and the request costs one message; outside it — every node past
			// the final checkpoint — it is the whole of the recovery, and
			// fetchMoreHeaderBlocks above does nothing at all.
			sm.requestMissingBlocks(peer, bmsg.blockHash)

			return nil
		} else {
			if errors.Is(err, context.Canceled) || errors.IsContextError(err) {
				// Neither committed nor judged: our own context went away
				// mid-processing. The block is still wanted and its header is
				// already off the front of the walk, so it needs the same
				// rewind the other drop paths get. On shutdown the rewind is
				// wasted work on a manager that is about to stop; on a
				// mid-flight cancellation it is the only thing that asks for
				// this block again.
				sm.dropBlockFromWalk(bmsg.blockHash, removedFront)

				return nil
			}

			// Remember this block failed to store/validate so its already-queued
			// descendants are short-circuited above rather than each failing their
			// parent lookup and logging a misleading "previous block NOT_FOUND"
			// ERROR (#1333). Covers both transient and permanent failures — from a
			// descendant's view the parent is missing either way; the TTL and the
			// delete-on-success below heal the transient case.
			//
			// Read before the write: judgedBefore says we had already given this
			// hash up once before this delivery, which is what decides below
			// whether the delivering peer is blamed for it. Nil-guarded because
			// ExpiringMap.Get takes m.mu.RLock() on the receiver and panics on a
			// nil map, and tests build SyncManager as a struct literal that
			// bypasses New() (newRaceManager in frontier_race_test.go).
			var judgedBefore bool

			if sm.recentlyFailedBlocks != nil {
				_, judgedBefore = sm.recentlyFailedBlocks.Get(bmsg.blockHash)
				sm.recentlyFailedBlocks.Set(bmsg.blockHash, struct{}{})
			}

			// Transient local-infrastructure failures (not peer faults): service
			// errors, storage errors, ErrServiceUnavailable — which the UTXO store
			// returns when a batch (notably the outpoint/decorate batch, the #1187
			// wedge) does not complete in time (stores/utxo/aerospike/get.go) — and
			// ErrStorageUnavailable ("no aerospike nodes available"). These must
			// neither reject the block to the peer nor (below) skip the backoff.
			// errors.IsTransientLocalError is the shared classifier, kept in
			// lock-step with shouldDisconnectOnBlockError in peer_server.go.
			serviceError := errors.IsTransientLocalError(err)
			if !catchingBlocks && !serviceError {
				peer.PushRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", &bmsg.blockHash, false)
			}

			// Put the walk back on the block, throttled, whether we judged it
			// or merely had bad luck with it. serviceError still decides
			// whether the peer is told the block was rejected, above; it no
			// longer decides whether the block keeps its place in the walk.
			//
			// A judged block used to be left out of this on the grounds that it
			// "keeps the recovery it has always had — the stall detector
			// rebuilds the header list from a fresh peer". That recovery does
			// not exist any more: with legacy_multiPeerBlockDownload on, a
			// stalled sync peer is demoted rather than disconnected and
			// demoteSyncPeer deliberately leaves the header state alone, so
			// nothing calls resetHeaderState on that path at all.
			sm.dropBlockFromWalk(bmsg.blockHash, removedFront)

			sm.logger.Errorf("Failed to process new block in service blockQueueMsg %v: %v", bmsg.blockHash, err)

			// Blame the first deliverer only. The rewind above means the walk
			// hands this hash out again once its backoff expires, and the caller
			// turns a non-transient error into disconnectMisbehaving, which
			// evicts the delivering peer's whole ASSOCIATION resolved to the
			// association primary (peer_server.go:1197, :1486, :1389). Without
			// this, every retry of a block we cannot validate costs another peer
			// its association, so the rewind that recovers the block spends the
			// suppliers this sync depends on.
			//
			// A peer that answers a request we made after judging the block once
			// already has answered our own question, and below a checkpoint it
			// cannot have fabricated the answer: the header is checkpoint-verified
			// and the block hashes to it. The reject still goes out above, so the
			// peer is told the block is bad; only the eviction is spared.
			//
			// Limit worth naming: recentlyFailedBlocks is also written by the
			// #1333 cascade suppression, so a descendant marked while its ancestor
			// was failing reads as judgedBefore on the first delivery that reaches
			// this arm after the ancestor clears. That costs one unblamed peer for
			// that block, and only inside the 10-minute TTL.
			if judgedBefore && !serviceError {
				return errors.NewServiceUnavailableError("[handleBlockMsg][%s] block already judged, not blaming %s", bmsg.blockHash, peer, err)
			}

			// Never panic in sync processing goroutines; bubble error to caller.
			return err
		}
	}

	// The block is in the chain. The consumer reads this to decide whether to
	// drain the blocks parked behind it; handleBlockMsg also returns nil from
	// several paths that did NOT commit (a missing parent, a failed ancestor, a
	// cancelled context), and draining after one of those would try to commit
	// children of a block that is not in the chain.
	bmsg.committed = true

	// Block processed successfully — clear any transient-failure backoff so a
	// future failure starts a fresh count rather than inheriting a stale one (#1187).
	if sm.blockFailureBackoff != nil {
		sm.blockFailureBackoff.Delete(bmsg.blockHash)
	}

	// Also clear any cascade-suppression marker (#1333): this hash now stores, so
	// its descendants must no longer be short-circuited as children of a failure.
	if sm.recentlyFailedBlocks != nil {
		sm.recentlyFailedBlocks.Delete(bmsg.blockHash)
	}

	// Meta-data about the new block this peer is reporting. We use this
	// below to update this peer's latest block height and the heights of
	// other peers based on their last announced block hash. This allows us
	// to dynamically update the block heights of peers, avoiding stale
	// heights when looking for a new sync peer. Upon acceptance of a block
	// or recognition of an orphan, we also use this information to update
	// the block heights over other peers who's invs may have been ignored
	// if we are actively syncing while the chain is not yet current or
	// who may have lost the lock announcement race.
	var (
		heightUpdate  int32
		blkHashUpdate *chainhash.Hash
	)

	if sps, ok := sm.syncPeerStateFor(peer); ok {
		sps.updateLastBlockTime()
	}

	// When the block is not an orphan, log information about it and update the chain state.

	// Update this peer's latest block height, for future potential sync node candidacy.
	// bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	// if err != nil {
	//	return errors.NewServiceError("failed to get best block header", err)
	// }

	heightUpdate = bmsg.blockHeight
	blkHashUpdate = &bmsg.blockHash

	if heightUpdate <= 0 {
		// get the height of the new block from the blockchain store
		_, blockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &bmsg.blockHash)
		if err != nil {
			sm.logger.Errorf("Failed to get block header for block %v: %v", bmsg.blockHash, err)
		} else {
			blockHeightInt32, err := safeconversion.Uint32ToInt32(blockHeaderMeta.Height)
			if err != nil {
				sm.logger.Errorf(failedToConvertBlockHeightInt32Msg, err)
			}

			heightUpdate = blockHeightInt32
		}
	}

	sm.logger.Infof("accepted block %v at height %d", bmsg.blockHash, heightUpdate)

	// Clear the rejected transactions.
	sm.rejectedTxns.Clear()

	// Update the block height for this peer. But only send a message to
	// the server for updating peer heights if this is an orphan or our
	// chain is "current". This avoids sending a spammy amount of messages
	// if we're syncing the chain from scratch.
	if heightUpdate != 0 {
		peer.UpdateLastBlockHeight(heightUpdate)
		state.noteBestKnownHeight(heightUpdate)
		sm.logger.Debugf("peer %s reports new best height %d, current %v", peer.String(), peer.LastBlock(), sm.current())

		if sm.current() { // used to check for isOrphan || sm.current()
			go sm.peerNotifier.UpdatePeerHeights(blkHashUpdate, heightUpdate, peer)

			// Since we are current, we can tell FSM to transition to RUN
			// Blockchain client will check if miner is registered, if so it will send Mine event, and FSM will transition to Mine
			if err = sm.blockchainClient.Run(sm.ctx, "legacy/netsync/manager/handleBlockMsg"); err != nil {
				sm.logger.Errorf("[Sync Manager] failed to send FSM RUN event %v", err)
			}

			sm.resetFeeFilterToDefault()
		}
	}

	// This is headers-first mode, so if the block is not a checkpoint
	// request more blocks using the header list to maintain the pipeline
	// at the dynamic max limit (adjusts based on block size).
	if !isCheckpointBlock {
		dynamicMax := sm.blockSizeTracker.calculateMaxInFlightBlocks()

		// startHeader is now sampled a few microseconds before the in-flight
		// count rather than in the same expression. Harmless: fetchHeaderBlocks
		// re-checks startHeader under headerMu before doing anything.
		sm.headerMu.Lock()
		haveMoreHeaders := sm.startHeader != nil
		anchorIsStillTheFront := sm.anchorIsStillTheFrontLocked()
		sm.headerMu.Unlock()

		if anchorIsStillTheFront {
			// The same "not yet" fetchMoreHeaderBlocks makes, for the same
			// reason, because this top-up can now run in that state too. A
			// header round reaching towards a checkpoint several batches away
			// has startHeader set with the anchor still in front, and multi-peer
			// assignment plus demotion mean a body can still commit in that
			// window — a late copy, or a block another peer was carrying. Asking
			// for the next round then starts blocks that arrive, match nothing at
			// the front, and sit in the list until the stall detector rebuilds
			// it. handleHeadersMsg's own call is not affected: it trims the
			// anchor first, which is why the ordering comment there says it must.
			sm.logger.Debugf("[handleBlockMsg][%s] the round's anchor is still the front of the header list, not topping the pipeline up yet", bmsg.blockHash)
		} else if haveMoreHeaders && sm.blockDownloads.CountForPeer(peer) < dynamicMax {
			sm.fetchHeaderBlocks()
		} else if !sm.current() && sm.blockDownloads.CountForPeer(peer) == 0 {
			sm.logger.Debugf("Not current, and no headers to sync to, fetching more headers")

			latestBlockHeader, _, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
			if err != nil {
				return errors.NewServiceError("Failed to get best block header", err)
			}

			locator := blockchain.BlockLocator([]*chainhash.Hash{latestBlockHeader.Hash()})
			if err = peer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
				return errors.NewServiceError("Failed to send getblocks message to peer %s", peer.String(), err)
			}
		}

		return nil
	}

	// This is headers-first mode and the block is a checkpoint.
	return sm.checkpointBlockCommitted(peer, bmsg.blockHash)
}

// checkpointBlockCommitted moves headers-first sync past the checkpoint the
// header list was anchored on. When there is a next checkpoint it asks for the
// next round of headers, from the block after this one up to that checkpoint;
// when there is not, it leaves headers-first mode and goes back to asking for
// blocks by inventory.
//
// A parked block can be the checkpoint block, so this runs from the park drain
// too. peer is whoever the caller decided to aim it at: the delivering peer when
// that peer is still connected, otherwise the current sync peer — because if the
// getheaders never goes out, headers-first sync stops at this checkpoint
// forever. A nil peer is a defined state and costs a warning, not a panic; the
// next sync-peer check restarts sync.
func (sm *SyncManager) checkpointBlockCommitted(peer *peerpkg.Peer, blockHash chainhash.Hash) error {
	// Before anything is changed, not after. Moving the checkpoint on is the
	// node's record of which round of headers it still has to fetch, and this
	// function is the only thing that asks for that round. With no peer to ask,
	// advancing it first threw away the question as well as the answer: nothing
	// went out, and when a peer did turn up the node asked for the round AFTER
	// the one it was missing, so the gap was never filled and headers-first sync
	// stopped at this checkpoint.
	//
	// Leaving the checkpoint where it is is necessary but not sufficient: the
	// two callers that commit a block are a block committing off the wire and
	// the park drain, and the replay in drainPendingCheckpoint is the third.
	// For the two commit routes the block is in the chain by the time either
	// returns, so haveInventory answers true for it and the download walk never
	// asks for it again. There is no second delivery to re-enter this arm with.
	// So the round is remembered here and drainPendingCheckpoint replays it from
	// the sync-peer ticker once an election has produced somebody to ask.
	if peer == nil {
		// Mark the anchor anyway. The block is already in this node's chain, so
		// if a headers round reaches the list by any other route before the
		// replay lands, the front must not wedge on a block no peer will send
		// again — see anchorIsStillTheFrontLocked.
		sm.headerMu.Lock()
		sm.markCheckpointAnchorLocked(blockHash)
		owed := sm.nextCheckpoint
		sm.headerMu.Unlock()

		// Past the last checkpoint there is no round to ask for and nothing to
		// defer: the peer-less arm of that case is a no-op, exactly as it is
		// with a peer.
		if owed == nil {
			return nil
		}

		// The checkpoint this round belongs to travels with the hash, so the
		// replay can tell whether it is still owed. Nothing else advances the
		// checkpoint, but if something did the replay would otherwise skip a
		// round rather than repeat one, which is the direction that loses
		// headers.
		sm.pendingCheckpoint.Store(&deferredCheckpoint{hash: blockHash, at: owed})

		sm.logger.Warnf("[checkpointBlockCommitted][%s] checkpoint reached with no peer to ask for the next round of headers; the round is deferred until one is elected", blockHash)

		return nil
	}

	// Advance the checkpoint under headerMu and work from the snapshot, so the
	// getheaders send and the loadSyncPeer lookup below stay outside the lock.
	sm.headerMu.Lock()

	sm.markCheckpointAnchorLocked(blockHash)

	if sm.nextCheckpoint == nil {
		sm.headerMu.Unlock()

		return nil
	}

	prevHeight := sm.nextCheckpoint.Height
	prevHash := sm.nextCheckpoint.Hash
	sm.nextCheckpoint = sm.findNextHeaderCheckpoint(prevHeight)
	nextCP := sm.nextCheckpoint
	sm.headerMu.Unlock()

	if nextCP != nil {
		locator := blockchain.BlockLocator([]*chainhash.Hash{prevHash})

		if err := peer.PushGetHeadersMsg(locator, nextCP.Hash); err != nil {
			return errors.NewServiceError("failed to send getheaders message to peer %s", peer.String(), err)
		}

		if sp := sm.loadSyncPeer(); sp != nil {
			sm.logger.Infof(
				"handleBlockMsg - Downloading headers for blocks %d to %d from peer %s",
				prevHeight+1,
				nextCP.Height,
				sp.String(),
			)
		}

		return nil
	}

	// The block is a checkpoint and there are no more checkpoints, so switch to
	// normal mode by requesting blocks from the block after this one up to the
	// end of the chain (zero hash).
	sm.leaveHeadersFirstMode()

	sm.logger.Infof("Reached the final checkpoint -- switching to normal mode")

	locator := blockchain.BlockLocator([]*chainhash.Hash{&blockHash})
	if err := peer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
		return errors.NewServiceError("Failed to send getblocks message to peer %s", peer.String(), err)
	}

	return nil
}

// markCheckpointAnchorLocked records that a checkpoint block is now the header
// list's anchor. Caller holds headerMu.
//
// advanceHeaderListFor leaves the checkpoint node in the list so the next
// round's first header can prove it links to it. That makes it the next round's
// anchor: a block now in this node's chain that no peer will deliver again. Say
// so on the node, because this is the moment it becomes true and nothing later
// can work it out from where the node sits — see headerNode.
func (sm *SyncManager) markCheckpointAnchorLocked(blockHash chainhash.Hash) {
	if e := sm.headerIndex[blockHash]; e != nil {
		if node, ok := e.Value.(*headerNode); ok {
			node.isAnchor = true
		}
	}
}

// deferredCheckpoint is a checkpoint transition that reached its block but could
// not ask for the round of headers that follows it, because there was no peer to
// ask. at is the checkpoint that was still owed at the moment the block
// committed, kept so the replay can tell the round has not been asked for since.
type deferredCheckpoint struct {
	hash chainhash.Hash
	at   *chaincfg.Checkpoint
}

// drainPendingCheckpoint replays a checkpoint transition that could not be made
// because there was no peer to ask, now that the sync-peer check has had its
// chance to elect one.
//
// This is the step that was missing. The two routes into
// checkpointBlockCommitted that commit a block reach it once each, because a
// checkpoint block commits exactly once: after that it is in the chain,
// haveInventory answers true and the download walk never asks for it again. So
// the nil-peer arm's decision to leave the checkpoint alone preserved the
// question but nothing ever asked it, and headers-first sync stopped at that
// checkpoint for the life of the process.
//
// This function is the third caller of checkpointBlockCommitted and the only one
// that does not arrive through advanceHeaderListFor, so it owes by hand the
// preconditions that gate returns isCheckpointBlock false on: headers-first mode
// has to be on, and the checkpoint has to be the one the pending round belongs
// to. Both are checked below. A getheaders sent with the mode off is not a
// no-op, because handleHeadersMsg disconnects a peer that answers it.
//
// Called from handleCheckSyncPeer, which runs on the sync-peer ticker, while the
// nil-peer arm is written from the block-queue consumer that drains the park.
// The Swap is what makes that safe: whichever goroutine takes the round owns it.
// If this tick's election still produced nobody, this function puts the round
// straight back for the next one, but never over a newer one.
func (sm *SyncManager) drainPendingCheckpoint() {
	pending := sm.pendingCheckpoint.Swap(nil)
	if pending == nil {
		return
	}

	// The round is only still owed while the checkpoint has not moved. Nothing
	// else advances it today, so this cannot currently fire; it is here because
	// replaying a transition whose round has already been asked for would
	// advance the checkpoint a second time and skip a round of headers, and a
	// skipped round is not recoverable by anything.
	if cp := sm.nextCheckpointSnapshot(); cp == nil || pending.at == nil || cp.Height != pending.at.Height {
		sm.logger.Infof("[drainPendingCheckpoint][%s] the deferred round has already been asked for, dropping it", pending.hash)

		return
	}

	// Put the round back for the next tick, but never over a newer one: this runs
	// on the ticker while checkpointBlockCommitted stores from the block-queue
	// consumer, so the round in hand may already be the stale one. No reachable
	// path stores a second round while one is pending, because that needs a
	// second checkpoint block to commit, which needs that round's headers, which
	// only the first transition or startSync asks for, and while a round is
	// pending startSync reads the stale checkpoint and takes the getblocks
	// branch. The primitive matches the claim anyway, for the same reason the
	// guard above is here on an unreachable branch.
	peer := sm.loadSyncPeer()
	if peer == nil {
		sm.pendingCheckpoint.CompareAndSwap(nil, pending)

		return
	}

	// Belt and braces over the re-derive in resetHeaderStateLocked. This is the
	// only caller of checkpointBlockCommitted that does not arrive through
	// advanceHeaderListFor, which returns isCheckpointBlock false outright when
	// the mode is off, so it is the only one that has to establish the mode for
	// itself. Sending a getheaders with the mode off is not a no-op: the reply
	// lands in handleHeadersMsg, which disconnects a peer that sends headers
	// while the mode is off before it even checks whether the message is empty.
	// With the mode off the round is the election's to ask for, and startSync's
	// headers-first branch will ask for it once the re-derive has put a
	// reachable checkpoint back in place.
	if !sm.headersFirstMode.Load() {
		sm.logger.Infof("[drainPendingCheckpoint][%s] headers-first mode is off, so the round is the election's to ask for; dropping it", pending.hash)

		return
	}

	sm.logger.Infof("[drainPendingCheckpoint][%s] a peer is available, asking for the round of headers the checkpoint deferred", pending.hash)

	if err := sm.checkpointBlockCommitted(peer, pending.hash); err != nil {
		sm.logger.Errorf("[drainPendingCheckpoint][%s] deferred checkpoint transition failed: %v", pending.hash, err)
	}
}

// processQueuedBlock is what the block-queue consumer does with one block:
// commit it, and then commit everything that was parked waiting for it.
//
// The committed guard is load-bearing. handleBlockMsg returns nil from several
// paths that did NOT put the block in the chain — a missing parent, a
// short-circuited descendant of a failed block, a cancelled context. Draining
// after one of those would try to commit the children of a block that is not in
// the chain; every one of them would fail its own parent lookup, and each would
// cost a wasted read, a deleted blob and a re-download.
func (sm *SyncManager) processQueuedBlock(msg *blockQueueMsg) error {
	err := sm.handleBlockMsg(msg)

	if err == nil && msg.committed {
		sm.drainParkedDescendants(msg.blockHash)
	}

	return err
}

// anchorIsStillTheFrontLocked reports whether the front of the header list is
// an anchor: a block that is already in this node's chain, kept in the list only
// so the next header can prove it links, and which no peer will ever deliver to
// us again. The caller must hold headerMu.
//
// Nothing may fetch blocks while that is the front. The header list is only ever
// advanced by a block that matches its front, so blocks fetched now arrive,
// match nothing and stay in the list; then the batch that reaches the checkpoint
// trims the anchor and the front becomes a block that has already been
// delivered, which nothing after it matches either. The checkpoint block is
// never recognised as the checkpoint, the next round of headers is never asked
// for, and sync sits until the 180-second stall detector rotates the peer and
// rebuilds the list. Mainnet checkpoint gaps run to 50,000 blocks — 25
// sequential header round-trips — so that window is minutes wide, not an
// instant.
//
// The node says so itself: headerNode.isAnchor is set by the only two places
// that ever create an anchor — resetHeaderStateLocked when it rebuilds the list,
// and checkpointBlockCommitted for the checkpoint node it leaves behind to
// anchor the round that follows. Asking the front node is the same question the
// checkpoint trim asks (removeHeaderAnchorLocked) and the same one the frontier
// racer asks before it publishes a front block, so all three now agree by
// construction.
//
// It used to be inferred from heights instead — a list whose tail was still
// below the checkpoint height meant the round's headers were still coming in, so
// the anchor must still be at the front. That is right while headers arrive and
// wrong straight after a checkpoint transition: the transition leaves the anchor
// far below the new checkpoint height, and a block given up on in that window is
// put back into the list AHEAD of the anchor (reinsertHeaderLocked inserts by
// height). The tail is the anchor and below the checkpoint either way, so the
// height reading answered "the anchor is still the front" when the front was in
// fact a block nobody had asked for since — and suppressed the one thing that
// would have asked for it again.
func (sm *SyncManager) anchorIsStillTheFrontLocked() bool {
	if sm.headerList == nil {
		return false
	}

	front := sm.headerList.Front()
	if front == nil {
		return false
	}

	node, ok := front.Value.(*headerNode)
	if !ok {
		return false
	}

	return node.isAnchor
}

// removeHeaderAnchorLocked takes the round's anchor out of the header list, and
// does nothing if it has already gone. The caller must hold headerMu.
//
// It is what handleHeadersMsg does when a batch reaches the checkpoint, and it
// used to be "remove Front()" on the strength of a comment: the first entry of
// the list is always the block already in the database. Two of this package's
// own mechanisms make that false, and both cost the round a real header.
//
// A rewind after a checkpoint transition inserts a lower header AHEAD of the
// anchor — reinsertHeaderLocked inserts by height, and the epoch check that
// stops a stale node going back into a rebuilt list does not fire at a
// transition, because a transition does not rebuild the list. Removing the front
// there deletes the block that was just put back to be asked for again, from the
// list and from the index, and leaves the anchor in place to wedge the front
// once more.
//
// And the frontier racer can have the anchor delivered early: while the headers
// are still coming in the front is the anchor and the cursor is on the first
// real header, so publishFrontierLocked used to publish the anchor as the
// frontier and raceFrontierBlock would ask a second peer for it. That reply
// takes the anchor off the front, so the trim then deletes the round's FIRST
// REAL HEADER — every block above it in the round arrives as an orphan of a
// block nobody will ask for again, the checkpoint block parks with them, and the
// round stalls with nothing left to recover it: with
// legacy_multiPeerBlockDownload on, the stall detector demotes the sync peer
// rather than disconnecting it, and demoteSyncPeer leaves the header state
// alone. publishFrontierLocked
// now refuses to publish an anchor, which closes that at source; this closes it
// at the one point both routes pass through.
//
// So the anchor is removed by identity, wherever it sits. Anything still marked
// as an anchor is a block already in our chain (see headerNode.isAnchor), and
// keeping one anywhere in the list wedges the walk as soon as the front reaches
// it. The loop runs the whole list rather than stopping at the first hit,
// because leaving a second one behind would be the same bug one round later; in
// practice there is exactly one, at or near the front.
func (sm *SyncManager) removeHeaderAnchorLocked() {
	if sm.headerList == nil {
		return
	}

	for e := sm.headerList.Front(); e != nil; {
		next := e.Next()

		node, ok := e.Value.(*headerNode)
		if !ok || !node.isAnchor {
			e = next

			continue
		}

		if node.hash != nil {
			sm.unindexHeaderLocked(e, *node.hash)
		}

		// The cursor must not be left on a detached element: startHeader is what
		// every walk starts from, and an element out of the list answers Next()
		// with nil, so the round would ask for nothing at all. Nil is the right
		// answer when there is nothing behind the anchor — that is what
		// re-enables the getblocks fallback.
		if sm.startHeader == e {
			sm.startHeader = next
		}

		sm.headerList.Remove(e)

		e = next
	}
}

// fetchMoreHeaderBlocks tops the download pipeline back up after a block from
// this peer stopped being outstanding, whether it was committed or parked.
// Without it a parked block is a silent loss of one in-flight slot, and the
// pipeline drains one block at a time until nothing is outstanding at all.
//
// All three of its callers — a block accepted into the park, a parked block
// committed off disk, and the park sweep's ticker carrying a rewound cursor
// forward — can run while the round's anchor is still the front of the header
// list, so the check that says "not yet" lives here rather than in any one of
// them. It is deliberately not the check the ticker makes: "the cursor is on the
// front" is true of a rewound cursor and false of an ordinary forward walk,
// which is right for driving the walk from a timer and would silently switch off
// the top-up this function exists for.
func (sm *SyncManager) fetchMoreHeaderBlocks(peer *peerpkg.Peer) {
	if !sm.headersFirstMode.Load() || sm.blockSizeTracker == nil {
		return
	}

	sm.headerMu.Lock()
	haveMoreHeaders := sm.startHeader != nil
	anchorIsStillTheFront := sm.anchorIsStillTheFrontLocked()
	sm.headerMu.Unlock()

	if anchorIsStillTheFront {
		return
	}

	if haveMoreHeaders && sm.blockDownloads.CountForPeer(peer) < sm.blockSizeTracker.calculateMaxInFlightBlocks() {
		sm.fetchHeaderBlocks()
	}
}

// reinsertHeaderLocked puts a header node that left the list back into it, and
// reports the element it now occupies — or nil when the node does not belong in
// this list at all. The caller must hold headerMu.
//
// A plain PushFront was wrong twice over.
//
// The list is not always a queue of blocks still wanted. resetHeaderStateLocked
// throws it away on a sync-peer rotation and pushes exactly one node, the best
// block already in the database, there only so the next round of headers can
// prove it links — and handleHeadersMsg removes Front() at the next checkpoint
// on the strength of it being exactly that. A block parked before the rotation
// and given up on after it would be pushed in front of that anchor, and the
// checkpoint would then remove the rewound header instead of the anchor: the
// rewind achieves nothing, the anchor stays in the list, and the walk goes back
// and downloads a block that is already in the database. The epoch check is what
// says no: a node stamped under an older list does not go into this one.
//
// And a second rewind in the same list would put a higher block in front of a
// lower one, because PushFront knows nothing about what the first rewind put
// there. The list runs in ascending height, and fetchHeaderBlocks walks it in
// order, so out-of-order entries ask for blocks out of order. Inserting by
// height says what is meant and is a single step in the ordinary case, where the
// node belongs on the front.
func (sm *SyncManager) reinsertHeaderLocked(node *headerNode) *list.Element {
	if node == nil || node.hash == nil || sm.headerList == nil || node.listEpoch != sm.headerListEpoch {
		return nil
	}

	for e := sm.headerList.Front(); e != nil; e = e.Next() {
		existing, ok := e.Value.(*headerNode)
		if !ok {
			return nil
		}

		if existing.height == node.height {
			// The list already holds this height. Whatever is there is the live
			// entry; adding a second one at the same height would have the walk
			// ask for one of them twice.
			return nil
		}

		if existing.height > node.height {
			return sm.headerList.InsertBefore(node, e)
		}
	}

	return sm.headerList.PushBack(node)
}

// moveStartHeaderBackLocked puts the download cursor on e, unless it is already
// on something earlier. The caller must hold headerMu.
//
// Two blocks can be given up on before either is asked for again — a parent and
// its child both parked, both expiring in the same sweep — and the walk starts
// wherever the cursor is left. Left on the second of them, the first is never
// reached and never asked for, which is the whole failure the rewind exists to
// prevent. A cursor only ever moves backwards here, so whichever of them is
// lowest wins however the sweep happened to order them.
func (sm *SyncManager) moveStartHeaderBackLocked(e *list.Element) {
	if e == nil {
		return
	}

	if sm.startHeader != nil {
		current, currentOK := sm.startHeader.Value.(*headerNode)
		candidate, candidateOK := e.Value.(*headerNode)

		if currentOK && candidateOK && current.height <= candidate.height && sm.headerIndex[*current.hash] == sm.startHeader {
			return
		}
	}

	sm.startHeader = e
}

// rewindHeaderCursor puts the download walk back on a block that has just been
// dropped, so it is asked for again.
//
// It exists because the walk is forward-only. commitHeaderCandidates advances
// startHeader past every header it considers, and nothing in headers-first mode
// ever moves it back, so a block that is downloaded and then discarded falls out
// of the walk for good. The getblocks the drop paths send instead cannot cover
// it: processInvMsg returns early while headers-first mode is on, so the reply
// is ignored. Without the rewind a single dropped block stops sync permanently.
//
// removed is the header node this block's arrival took off the front of the
// list, or nil if it was never the front. Both cases happen and both must work:
// by the time a block is dropped its header has usually already been removed and
// unindexed, so an index lookup alone finds nothing for exactly the hash that
// matters. Pushing it back on the front restores the list to the shape it had,
// because headers only ever leave the list from the front — an element that was
// removed while it was the front has nothing that belongs in front of it.
//
// The download ledger is deliberately left alone. The delivering peer's
// obligation was already released upstream, and any other peer racing the same
// block keeps its right to deliver it; stripping every owner here would make an
// honest peer's copy look unrequested and cost it its connection.
//
// Two things it will not do. Outside headers-first mode there is no walk to
// rewind: nothing reads the header list to decide what to fetch, and recovery is
// the getblocks the drop paths already send, which works there. And a header
// node from a list that has since been thrown away is not put back at all — see
// reinsertHeaderLocked.
func (sm *SyncManager) rewindHeaderCursor(hash chainhash.Hash, removed *headerNode) bool {
	if !sm.headersFirstMode.Load() {
		return false
	}

	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	switch {
	case sm.headerIndex[hash] != nil:
		// Still in the list: a checkpoint header, which is kept to anchor the
		// next round, or a block that was never the front.
		sm.moveStartHeaderBackLocked(sm.headerIndex[hash])

	case removed != nil && sm.headerList != nil:
		e := sm.reinsertHeaderLocked(removed)
		if e == nil {
			sm.logger.Warnf("[rewindHeaderCursor][%s] the header list has been rebuilt since this block left it; recovery is the next headers round", hash)

			return false
		}

		sm.indexHeaderLocked(e, hash)
		sm.moveStartHeaderBackLocked(e)

	default:
		sm.logger.Warnf("[rewindHeaderCursor][%s] block dropped with no header to rewind to; recovery is down to the next headers round", hash)

		return false
	}

	// Republish, because the front of the list may have changed. When the
	// rewound cursor is itself the front, publishFrontierLocked clears the
	// frontier — which is right: we have not asked for it again yet, so there is
	// nothing for the race timer to race.
	sm.publishFrontierLocked(time.Now())

	sm.logger.Warnf("[rewindHeaderCursor][%s] download cursor moved back after the block was dropped", hash)

	return true
}

// dropBlockFromWalk gives a delivered-then-dropped block its place in the
// download walk back, throttled so the re-request cannot spin.
//
// Every caller runs after advanceHeaderListFor has already taken the block's
// header off the front, and in headers-first mode the walk is the only thing
// that fetches blocks: processInvMsg discards inv replies while headers-first
// mode is on, and headerListLocator builds its getheaders from the list BACK,
// which is above the hole, so a fresh headers round cannot refill a header
// removed from the middle of the list. resetHeaderStateIfEmpty cannot either,
// because it returns without doing anything unless the list is already empty.
// Nothing else puts the header back.
//
// The throttle is the #1187 transient-failure backoff, and it throttles the WALK
// and not just the decorate: fetchHeaderBlocks stops the round on a block that
// is still inside its backoff and leaves the cursor sitting on it. So a block
// that is never going to validate is asked for once per backoff window instead
// of once per round, and its descendants are not walked past it and downloaded
// only to be short-circuited.
//
// With the backoff turned off by configuration there is no throttle, and so
// there is no rewind either: a walk wedged below a checkpoint is bad, an
// unthrottled re-download loop against a block we have just rejected is worse,
// and an operator who has set legacy_blockFailureBackoffBase or
// legacy_blockFailureBackoffMaxDuration to zero has asked for neither.
func (sm *SyncManager) dropBlockFromWalk(blockHash chainhash.Hash, removedFront *headerNode) {
	if sm.blockFailureBackoff == nil {
		return
	}

	sm.recordBlockFailureBackoff(blockHash)
	sm.rewindHeaderCursor(blockHash, removedFront)
}

// fetchHeaderBlocks asks for the next run of blocks the header list describes,
// spread over every peer eligible to carry one.
//
// Which peer gets which hash is the whole of the multi-peer part, and it lives in
// block_scheduler.go: the assigner is built before any lock is taken, hands each
// header to the first peer with budget that claims the chain, and collects one
// getdata per peer to be sent once the header lock is released. With
// legacy_multiPeerBlockDownload off the assigner holds exactly one peer, the sync
// peer, with the block-size ladder's budget — which is what this function did
// before the scheduler existed.
//
// The header list is only ever read or written under headerMu, but the "do we
// already have this block?" question is a gRPC round-trip to the blockchain
// service on a context with no deadline, so it is asked with the lock released.
// The walk therefore runs in rounds: snapshot the next run of candidate hashes
// under the lock, ask about them unlocked, re-take the lock and commit. Bounding
// the number of round-trips bounds the cost in calls but not in time, and time
// is the only thing a goroutine waiting on headerMu cares about — the block
// queue consumer takes this same lock as its first act in headers-first mode.
func (sm *SyncManager) fetchHeaderBlocks() {
	sm.headerMu.Lock()
	haveStartHeader := sm.startHeader != nil
	headerListLen := sm.headerList.Len()
	sm.headerMu.Unlock()

	// Record which block everything behind it is now waiting on, so the race
	// timer can tell how long it has been outstanding. Deferred rather than
	// written at the end, because EVERY exit from this walk has to publish and
	// two of them are early returns.
	//
	// The early returns are the ones that matter. newDownloadAssigner answers nil
	// when there is no eligible peer, when the node-wide download window is
	// spent, or when every eligible peer is at its per-peer cap, and all three
	// mean blocks are outstanding and undelivered — which is precisely the state
	// raceFrontierBlock exists to rescue. Returning without publishing left the
	// race with no target, so the one block holding up sync was never asked of a
	// second peer, nothing committed, the assigner stayed nil, and the walk never
	// published again. A mainnet soak wedged there permanently. The other two
	// publish sites cannot cover it: the one in advanceHeaderListFor fires only
	// when a block commits and the list front moves, and the one in
	// rewindHeaderCursor only on a successful rewind.
	//
	// Self-locking form, and safe as a defer for the same reason it was safe at
	// the end: headerMu is released above and every helper below takes and
	// releases it internally, so the function always returns with it released.
	// The publish-after-send order is unchanged, since the defer runs after
	// assigner.send. time.Now() is read inside the closure so the frontier is
	// timestamped when it is published, not when the walk started.
	//
	// This publishes what is stuck; it does not invent one. publishFrontierLocked
	// still clears the frontier when the front block has not been asked for yet,
	// which is right: nothing is waiting on a block nobody requested.
	defer func() { sm.publishFrontier(time.Now()) }()

	// Nothing to do if there is no start header.
	if !haveStartHeader {
		sm.logger.Warnf("fetchHeaderBlocks called with no start header")

		return
	}

	// Every budget this pass spends, and every peer it may spend it on, decided
	// with no lock held. A nil answer means there is nothing to hand out.
	assigner := sm.newDownloadAssigner()
	if assigner == nil {
		return
	}

	// Deliberately without sm.blockDownloads.Len(): a Debugf argument list is
	// evaluated whether or not debug logging is on, and Len() walks every tracked
	// hash under the ledger's lock on a path that runs for every arriving block.
	sm.logger.Debugf("[fetchHeaderBlocks] Header list: %d blocks, avg size: %d bytes, budget: %d over %d peer(s)",
		headerListLen, sm.blockSizeTracker.getAverageSize(), assigner.remaining, len(assigner.peers))

	// A round asks about at most the blocks still wanted. Blocks we turn out to
	// already have cost a slot in the round but not a request, so a second round
	// picks up the shortfall — which is what keeps the getdata contents the same
	// as the old single-locked walk, where the loop simply carried on past them.
	for assigner.remaining > 0 {
		hashes, anchor, anchorHash, ok := sm.snapshotHeaderCandidates(assigner.remaining)
		if !ok {
			break
		}

		alreadyHave := make([]bool, len(hashes))

		for i := range hashes {
			iv := wire.NewInvVect(wire.InvTypeBlock, &hashes[i])

			haveInv, err := sm.haveInventory(iv)
			if err != nil {
				sm.logger.Warnf("Unexpected failure when checking for "+
					"existing inventory during header block "+
					"fetch: %v", err)
			}

			alreadyHave[i] = haveInv
		}

		_, more := sm.commitHeaderCandidates(assigner, anchor, anchorHash, hashes, alreadyHave)
		if !more {
			break
		}
	}

	// One send per peer that got work, and every one of them with headerMu
	// released. The deferred publishFrontier above runs after this.
	assigner.send(sm)
}

// lookaheadCeilingLocked returns the highest block height this round may ask for,
// and whether there is a limit at all. The caller must hold headerMu.
//
// legacy_blockDownloadWindow and legacy_maxBlocksInTransitPerPeer bound how MANY
// requests are outstanding. This bounds how far ahead of itself the node reads,
// which is a different quantity and the one that decides how much disk the park
// needs: blocks commit strictly in order, so a block fetched a long way ahead of
// the block we are waiting for cannot be committed when it arrives and sits in
// the park until everything between it and the chain has landed.
//
// Measured from the front of the header list, which is the block being waited on.
// svnode measures its -blockdownloadlowerwindow from chainActive.Height(), the
// validated tip; in headers-first mode with in-order commits those are the same
// place to within one block, and the front is available here without asking the
// blockchain service anything.
//
// Clamped to the node-wide window, as svnode clamps its lower window to its
// window: a limit looser than that could never bind.
func (sm *SyncManager) lookaheadCeilingLocked() (int64, bool) {
	if sm.settings == nil {
		return 0, false
	}

	lower := sm.settings.Legacy.BlockDownloadLowerWindow
	if lower <= 0 {
		return 0, false
	}

	if window := sm.settings.Legacy.BlockDownloadWindow; window > 0 && lower > window {
		lower = window
	}

	if sm.headerList == nil {
		return 0, false
	}

	front := sm.headerList.Front()
	if front == nil {
		return 0, false
	}

	node, isHeaderNode := front.Value.(*headerNode)
	if !isHeaderNode {
		return 0, false
	}

	return int64(node.height) + int64(lower), true
}

// snapshotHeaderCandidates copies up to limit hashes from startHeader forward,
// and returns the element startHeader points at together with its hash, so the
// blockchain lookups can be made with headerMu released and the commit can tell
// whether the list moved in the meantime. ok is false when there is nothing
// usable to walk.
func (sm *SyncManager) snapshotHeaderCandidates(limit int) (hashes []chainhash.Hash, anchor *list.Element, anchorHash chainhash.Hash, ok bool) {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	anchor = sm.startHeader
	if anchor == nil {
		return nil, nil, chainhash.Hash{}, false
	}

	ceiling, hasCeiling := sm.lookaheadCeilingLocked()

	hashes = make([]chainhash.Hash, 0, limit)

	for e := anchor; e != nil && len(hashes) < limit; e = e.Next() {
		node, isHeaderNode := e.Value.(*headerNode)
		if !isHeaderNode || node.hash == nil {
			// Unreachable: nothing but a *headerNode carrying a hash is ever put
			// in the list. Stopping the walk is the safe reading of a list that
			// says otherwise — stepping over an entry we cannot identify would
			// leave startHeader anchored past headers nobody ever asked for.
			sm.logger.Warnf("Header list node is not a headerNode carrying a hash")

			break
		}

		// Past the lookahead limit. Stopping here leaves the cursor on this
		// header, so the next round picks it up once the frontier has moved —
		// the same shape as running out of budget.
		if hasCeiling && int64(node.height) > ceiling {
			break
		}

		hashes = append(hashes, *node.hash)
	}

	if len(hashes) == 0 {
		return nil, nil, chainhash.Hash{}, false
	}

	return hashes, anchor, hashes[0], true
}

// commitHeaderCandidates re-takes headerMu and, provided the list has not moved
// under the unlocked lookups, records each block we do not already have against
// the peer the assigner picked for it, adds it to that peer's getdata, and
// advances startHeader past every header it considered.
//
// more reports whether another round may follow. It is false when the walk
// reached the end of the list, when a request was held back, and when the list
// moved while the lookups were in flight — in that last case nothing at all is
// committed, because a getdata built from a stale reading of the list could ask
// for a block twice or step over one nobody asked for. The next tick redoes the
// round against the list as it then is.
func (sm *SyncManager) commitHeaderCandidates(assigner *downloadAssigner, anchor *list.Element, anchorHash chainhash.Hash,
	hashes []chainhash.Hash, alreadyHave []bool) (requested int, more bool) {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	// startHeader must still be the element the round was walked from. When it
	// is not, another round or a reset has already moved it on, and it points
	// into the list as it now is — so there is nothing to repair and nothing to
	// commit: the headers this round walked are either already requested or no
	// longer wanted. This is the ordinary case, because two rounds run
	// concurrently all the time: the block-queue consumer starts one on every
	// block that arrives and every headers message starts another.
	if sm.startHeader != anchor {
		sm.logger.Debugf("[fetchHeaderBlocks] header list moved while checking inventory, leaving the rest to the next pass")

		return 0, false
	}

	// startHeader still points at the anchor, but the anchor is no longer the
	// live holder of its hash: the block arrived while the lookups were in
	// flight and handleBlockMsg took that element out of the list. Nothing may
	// be committed from a reading of the list that stale — but the pointer
	// cannot simply be left where it is either, and that is the part that was
	// missing. container/list clears a removed element's links, so a detached
	// startHeader answers Next() with nil: every later round would snapshot one
	// header, be refused here, and commit nothing, so the header list would
	// never drain again; and handleBlockMsg reads a non-nil startHeader as
	// "there is still work queued", so its getblocks fallback would never fire
	// either. Sync would fetch nothing at all until the 180 second stall
	// detector rotated the peer, over and over. Before the walk was restructured
	// to do its lookups unlocked, this healed itself by accident — the walk read
	// Next() off the detached element, got nil, and stored that.
	//
	// Re-anchor on the front of the list rather than on nil, because the queued
	// headers are still worth fetching and nil throws them away. It is provably
	// the right element: headers only ever leave the list from the front, so an
	// element that is detached while still being startHeader was the front when
	// it went, which means nothing between the front and startHeader was
	// outstanding and the new front is exactly the first header nobody has asked
	// for yet. An empty list leaves startHeader nil, which is what re-enables
	// the getblocks fallback.
	if sm.headerIndex[anchorHash] != anchor {
		var front *list.Element
		if sm.headerList != nil {
			front = sm.headerList.Front()
		}

		sm.startHeader = front

		sm.logger.Debugf("[fetchHeaderBlocks] block %s arrived while checking inventory, re-anchoring the header walk on the front of the list", anchorHash)

		return 0, false
	}

	e := anchor

	for i := range hashes {
		if e == nil {
			return requested, false
		}

		node, isHeaderNode := e.Value.(*headerNode)
		if !isHeaderNode || node.hash == nil || !node.hash.IsEqual(&hashes[i]) {
			// The run of headers we asked about is no longer the run in the
			// list, so stop where the two still agree.
			return requested, false
		}

		// A rewind puts this walk back in front of blocks that are already in
		// flight. Asking for those again makes the peer send each of them a
		// second time, and the second copy arrives after the first one released
		// that peer's obligation — so it looks unrequested and costs an honest
		// peer its connection. Skipping anything somebody was asked for recently
		// is the same rule the inv path already applies, and it is a no-op on the
		// ordinary forward walk, where a header is reached before it has ever
		// been requested.
		//
		// A block skipped here is still owed by a live peer, and the walk moves
		// past it, so what brings it back is one of the four things that put the
		// cursor in front of it again: the peer is demoted as sync peer
		// (reopenDemotedPeerSlice), it disconnects (reopenStrandedSlice), it
		// answers notfound for it (NotFound), or — for the one block that is
		// actually holding up commits — the frontier race asks a second peer
		// without moving the cursor at all. Nothing else recovers a skipped
		// block, so a change that removes one of those four has to replace it.
		// A block still inside its transient-failure backoff must not be asked
		// for yet — throttling the re-decorate storm is the whole point of the
		// backoff — and it must not be walked past either, because the rewind
		// that put the walk back on it would then be undone and the block would
		// leave the walk for good. So the round stops here with the cursor still
		// on it, and the next round picks it up once the backoff has expired.
		// The cap on that backoff is deliberately below the sync-peer stall
		// window, so the wait always ends before the peer would be rotated.
		if !alreadyHave[i] && sm.blockFailureBackoff != nil {
			if fs, backedOff := sm.blockFailureBackoff.Get(hashes[i]); backedOff && time.Now().Before(fs.nextRetry) {
				sm.logger.Debugf("[fetchHeaderBlocks] block %s is still inside its transient-failure backoff, holding the walk here", hashes[i])

				return requested, false
			}
		}

		if !alreadyHave[i] && sm.blockDownloads.RequestedWithin(hashes[i], blockRequestRetryInterval) {
			e = e.Next()
			sm.startHeader = e

			continue
		}

		if !alreadyHave[i] {
			// Which peer carries this block. Nobody available means the pass is
			// out of budget, or every peer with budget left has claimed a chain
			// shorter than this block — either way the round stops here with the
			// cursor still on this header, exactly as a full ledger does below.
			// Advancing past a header nobody was asked for loses that block from
			// the walk for good.
			target, ok := assigner.take(node.height)
			if !ok {
				sm.logger.Debugf("[fetchHeaderBlocks] no peer can take block %s, holding the walk here", hashes[i])

				return requested, false
			}

			// The peer the assigner picked may be the one that already owes us
			// this block. A demoted peer's reopened slice is exactly that case:
			// reopening back-dates the record rather than dropping it, so the
			// walk is free to place the block again and nothing kept it off the
			// same peer. That peer already has our request, so re-arm what we
			// hold rather than asking twice — a peer that answers twice has its
			// second copy arrive after the first discharged its obligation, and
			// loses its whole association for answering us.
			if sm.blockDownloads.ReassertOwner(target.peer, hashes[i]) {
				// Charge it like a request, because that is what it is to the
				// two caps. ReassertOwner clears the forgiven flag, so the block
				// is back in CountForPeer and back in Len from here on, and both
				// budgets were computed before this pass ran with the forgiven
				// records excluded. Skipping the two decrements let every
				// reassert add one to the peer's live in-flight count without
				// taking one out of the pass: a demoted peer with a full
				// reopened slice could finish a pass owing its whole reopened
				// slice plus another perPeer on top. Only the getdata is
				// skipped; that peer already has the request.
				target.budget--
				assigner.remaining--

				e = e.Next()
				sm.startHeader = e

				continue
			}

			// Record the request before it goes out. A block the ledger will
			// not take is a block we must not ask for: the reply would arrive
			// with nothing vouching for it and cost this peer its connection.
			// Leaving startHeader where it is means the header is simply picked
			// up again on the next pass, once arrivals or expiry have made room.
			if !sm.blockDownloads.Add(target.peer, hashes[i]) {
				sm.logger.Warnf("[fetchHeaderBlocks] block download ledger full at %d blocks, holding off on %s", maxTrackedBlockDownloads, node.hash)

				return requested, false
			}

			if err := assigner.recordRequest(target, node.hash); err != nil {
				// The ledger was told about a request that is not going to be
				// sent, so take it back. Left in place the hash is owned by a
				// peer that was never asked, which answers RequestedWithin and
				// HasOwner for the hour-long ceiling and so quietly holds the
				// walk off it. Only reachable above wire.MaxInvPerMsg, which the
				// per-peer budget keeps far out of reach, but the branch above
				// holds the cursor without adding and these two should fail the
				// same way.
				sm.blockDownloads.RemoveOwner(target.peer, hashes[i])

				sm.logger.Warnf(unexpectedFailureAddingInventoryMsg, err)

				return requested, false
			}

			requested++
		}

		e = e.Next()
		sm.startHeader = e
	}

	return requested, e != nil
}

// handleHeadersMsg handles block header messages from all peers.  Headers are
// requested when performing a headers-first sync.
func (sm *SyncManager) handleHeadersMsg(hmsg *headersMsg) {
	sm.logger.Debugf("[handleHeadersMsg] received headers message with %d headers from %s", len(hmsg.headers.Headers), hmsg.peer)
	peer := hmsg.peer

	state, resolved, exists := sm.peerStateResolvingPrimary(peer)
	if !exists {
		sm.logger.Warnf("Received headers message from unknown peer %s", peer)
		return
	}
	if resolved != peer {
		// Stream peers (e.g. BlockPriority DATA1) are not registered in
		// peerStates directly - resolved via their association's primary peer.
		sm.logger.Debugf("[handleHeadersMsg] resolved stream peer %s to primary peer %s", peer, resolved)
		peer = resolved
	}

	// The remote peer is misbehaving if we didn't request headers.
	msg := hmsg.headers
	numHeaders := len(msg.Headers)

	if !sm.headersFirstMode.Load() {
		reason := fmt.Sprintf("Got %d unrequested headers from %s", numHeaders, peer.String())
		peer.DisconnectWithWarning(reason)

		return
	}

	// Nothing to do for an empty headers message.
	if numHeaders == 0 {
		return
	}

	// Ensure we have a valid starting point for header validation.
	//
	// GetBestBlockHeader can block for minutes during initial sync, so headerMu
	// must not be held across it (Rule B) — which means the emptiness read
	// above cannot be trusted on the way back. resetHeaderStateIfEmpty re-checks
	// under the lock and does nothing if another headers message recovered the
	// state while we were waiting, rather than wiping the headers it added.
	if sm.headerListEmpty() {
		sm.logger.Warnf("Header list is empty, attempting to recover sync state")

		bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
		if err != nil {
			peer.DisconnectWithWarning(fmt.Sprintf(failedToGetBestBlockHeaderMsg, err))
			return
		}

		bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
		if err != nil {
			peer.DisconnectWithWarning(fmt.Sprintf("Failed to convert block height: %v", err))
			return
		}

		sm.resetHeaderStateIfEmpty(bestBlockHeader.Hash(), bestBlockHeightInt32)

		if sm.headerListEmpty() {
			peer.DisconnectWithWarning("Failed to initialize header sync state")
			return
		}
	}

	// Process all the received headers ensuring each one connects to the
	// previous and that checkpoints match.
	receivedCheckpoint := false

	var finalHash *chainhash.Hash

	// One lock for the whole loop is the point: it is what stops two concurrent
	// headers messages interleaving their pushes into the same list. The three
	// disconnect paths inside collect a reason and break instead of
	// disconnecting on the spot, because a peer send must not run under headerMu
	// (Rule B).
	var disconnectReason string

	// Highest height this batch linked up to, reported to the peer's state after
	// the unlock below so the atomic write stays outside the locked region.
	var maxHeaderHeight int32

	// How many headers in this batch linked onto the list, and whether the batch
	// turned out to be a late answer to a getheaders we ourselves sent.
	var (
		pushed     int
		staleReply bool
	)

	sm.headerMu.Lock()

	for _, blockHeader := range msg.Headers {
		blockHash := blockHeader.BlockHash()
		finalHash = &blockHash

		// Ensure there is a previous header to compare against.
		prevNodeEl := sm.headerList.Back()
		if prevNodeEl == nil {
			disconnectReason = "Header list does not contain a previous element as expected"

			break
		}

		// Ensure the header properly connects to the previous one and
		// add it to the list of headers.
		node := headerNode{hash: &blockHash, listEpoch: sm.headerListEpoch}

		prevNode := prevNodeEl.Value.(*headerNode)
		if prevNode.hash.IsEqual(&blockHeader.PrevBlock) {
			node.height = prevNode.height + 1
			e := sm.headerList.PushBack(&node)
			sm.indexHeaderLocked(e, blockHash)
			pushed++

			if node.height > maxHeaderHeight {
				maxHeaderHeight = node.height
			}

			if sm.startHeader == nil {
				sm.startHeader = e
			}
		} else {
			// A peer we demoted still has our getheaders outstanding, and by the
			// time it answers the new sync peer has usually extended the list —
			// so its reply connects to a header we hold rather than to the back.
			// That is an honest answer to our own question, and disconnecting the
			// sender with a misbehaviour warning throws away the very peer we
			// kept connected so it could carry block bodies. Recognised only
			// while nothing in this batch has linked yet: a batch that starts
			// connecting and then stops is a different animal, and still costs
			// the sender its connection.
			//
			// Scoped to the two senders whose non-connecting reply we caused,
			// and to nobody else. Without a scope holdHeader is satisfied by ANY
			// header currently in the index, so any peer could re-send a batch it
			// once contributed, or any prefix of it, for ever: up to 2000 headers
			// of bandwidth and decode plus a headerMu acquisition each time, the
			// same lock the block-queue consumer takes first in headers-first
			// mode, and nothing at all for the sender.
			//
			// The two are the peer we just demoted, whose getheaders we sent
			// before the swap, and the current sync peer, which startSync elects
			// on height alone and so can be hundreds of headers below the back of
			// the list: it answers our locator from the newest block it has, which
			// connects to a header we hold rather than to the back. Both expire.
			// The cooldown expires on its own timer, and a sync peer that only
			// ever sends headers that do not connect refreshes no block time, so
			// the stall detector takes the role off it.
			//
			// Only reachable with the fan-out on, because that is what keeps a
			// demoted peer connected in the first place.
			_, holdParent := sm.headerIndex[blockHeader.PrevBlock]
			_, holdHeader := sm.headerIndex[blockHash]
			weAsked := state.inDemotionCooldown() || peer == sm.loadSyncPeer()

			if sm.settings.Legacy.MultiPeerBlockDownload && pushed == 0 &&
				weAsked && (holdParent || holdHeader) {
				staleReply = true

				break
			}

			disconnectReason = "Received block header that does not properly connect to the chain"

			break
		}

		// Verify the header at the next checkpoint height matches.
		//
		// nextCheckpoint is nil once the final one has been passed.
		// checkpointBlockCommitted advances it under headerMu and only leaves
		// headers-first mode after releasing the lock, so a second headers
		// goroutine that had already passed the mode check can hold the lock in
		// that window and read the nil. Multi-peer demotion makes overlapping
		// headers replies from the outgoing and incoming sync peer ordinary,
		// which is what makes the window worth guarding rather than arguing
		// about. There is no checkpoint left to verify against, so there is
		// nothing to do but carry on with the batch.
		if sm.nextCheckpoint != nil && node.height == sm.nextCheckpoint.Height {
			if node.hash.IsEqual(sm.nextCheckpoint.Hash) {
				receivedCheckpoint = true

				sm.logger.Infof("Verified downloaded block "+
					"header against checkpoint at height "+
					"%d/hash %s", node.height, node.hash)
			} else {
				disconnectReason = fmt.Sprintf("Block header at height %d/hash "+
					"%s does NOT match expected checkpoint hash of %s",
					node.height, node.hash,
					sm.nextCheckpoint.Hash)
			}

			break
		}
	}

	sm.headerMu.Unlock()

	// A peer that hands us headers up to height N has demonstrably got the chain
	// that far. Done after the unlock, so no peer state is touched under
	// headerMu.
	if maxHeaderHeight > 0 {
		state.noteBestKnownHeight(maxHeaderHeight)
	}

	if staleReply {
		sm.logger.Debugf("[handleHeadersMsg] ignoring %d late headers from %s: they connect to a header we already hold rather than to the back of the list", numHeaders, peer.String())

		return
	}

	if disconnectReason != "" {
		peer.DisconnectWithWarning(disconnectReason)

		return
	}

	// When this header is a checkpoint, switch to fetching the blocks for
	// all the headers since the last checkpoint.
	if receivedCheckpoint {
		// The round's anchor is a block already in this node's database, in the
		// list only so this round's first header could prove it links. It has to
		// go before any of these blocks is asked for: the list is advanced by an
		// arriving block matching its front, and no peer will ever deliver the
		// anchor again.
		sm.headerMu.Lock()

		sm.removeHeaderAnchorLocked()

		remaining := sm.headerList.Len()
		sm.headerMu.Unlock()

		sm.logger.Infof("Received %v block headers: Fetching blocks", remaining)

		// fetchHeaderBlocks takes headerMu itself, so it must be called after
		// the unlock — sync.Mutex is not reentrant.
		sm.fetchHeaderBlocks()

		return
	}

	// This header is not a checkpoint, so request the next batch of
	// headers starting from the latest known header and ending with the
	// next checkpoint.
	locator := blockchain.BlockLocator([]*chainhash.Hash{finalHash})

	// Same window as the checkpoint compare above: no checkpoint left means
	// headers-first mode is on its way out and there is no stop hash to ask up
	// to. Asking for another round here would be asking on behalf of a mode we
	// are leaving, so leave it to the getblocks that leaveHeadersFirstMode sends.
	nextCP := sm.nextCheckpointSnapshot()
	if nextCP == nil {
		sm.logger.Debugf("[handleHeadersMsg] no checkpoint left to ask up to; leaving the next round to normal mode")

		return
	}

	if err := peer.PushGetHeadersMsg(locator, nextCP.Hash); err != nil {
		sm.logger.Warnf("Failed to send getheaders message to peer %s: %v", peer.String(), err)
	}
}

// haveInventory returns whether the inventory represented by the passed
// inventory vector is known.  This includes checking all the various places
// inventory can be when it is in different states such as blocks that are part
// of the main chain, on a side chain, in the orphan pool, and transactions that
// are in the memory pool (either the main pool or orphan pool).
func (sm *SyncManager) haveInventory(invVect *wire.InvVect) (bool, error) {
	switch invVect.Type {
	case wire.InvTypeBlock:
		// A parked block is downloaded, checked and on disk; it is simply not in
		// the chain yet, because its parent is not. Asking the blockchain alone
		// answers "no" for it, and past the final checkpoint — which is every
		// mainnet node — that answer is what the whole recovery loop runs on: the
		// getblocks a park sends brings back an inv, the inv is not recognised,
		// and the block we are already holding is downloaded all over again,
		// once every blockRequestRetryInterval for as long as it stays parked.
		// Without this the park saves the disk write and none of the bandwidth
		// outside headers-first mode.
		if sm.blockPark.Has(invVect.Hash) {
			return true, nil
		}

		// single round-trip: GetBlockHeader tells us both existence and validity
		_, meta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &invVect.Hash)
		if err != nil {
			// block not found (or transient error) — trigger re-request
			return false, nil
		}

		// block exists but was marked invalid — re-request so it can be reprocessed
		return !meta.Invalid, nil

	case wire.InvTypeTx:
		// check whether this transaction exists in the utxo store
		// which means it has been processed completely at our end
		utxo, err := sm.utxoStore.Get(sm.ctx, &invVect.Hash, fields.Fee)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) {
				return false, nil
			}

			return false, err
		}

		return utxo != nil, nil
	}

	// The requested inventory is is an unsupported type, so just claim
	// it is known to avoid requesting it.
	return true, nil
}

// handleInvMsg handles inv messages from all peers.
// We examine the inventory advertised by the remote peer and act accordingly.
func (sm *SyncManager) handleInvMsg(imsg *invMsg) {
	sm.logger.Debugf("[handleInvMsg] received inv message with %d inv vectors from %s", len(imsg.inv.InvList), imsg.peer)
	peer := imsg.peer

	state, resolved, exists := sm.peerStateResolvingPrimary(peer)
	if !exists {
		sm.logger.Warnf("[handleInvMsg] Received inv message from unknown peer %s", peer)
		return
	}
	if resolved != peer {
		// Stream peers (e.g. BlockPriority DATA1) are not registered in
		// peerStates directly - resolved via their association's primary peer.
		sm.logger.Debugf("[handleInvMsg] resolved stream peer %s to primary peer %s", peer, resolved)
		peer = resolved
	}

	// Attempt to find the final block in the inventory list.  There may
	// not be one.
	lastBlock := -1
	invVects := imsg.inv.InvList

	for i := len(invVects) - 1; i >= 0; i-- {
		if invVects[i].Type == wire.InvTypeBlock {
			lastBlock = i
			break
		}
	}

	// If this inv contains a block announcement, and this isn't coming from
	// our current sync peer, then update the last
	// announced block for this peer. We'll use this information later to
	// update the heights of peers based on blocks we've accepted that they
	// previously announced.
	sp := sm.loadSyncPeer()
	if lastBlock != -1 && peer != sp {
		peer.UpdateLastAnnouncedBlock(&invVects[lastBlock].Hash)
	}

	// Ignore invs from peers that aren't the sync if we are not current.
	// Helps prevent fetching a mass of orphans.
	if peer != sp && !sm.current() {
		return
	}

	// If a peer announces a block we already
	// know of, then update their current block height.
	if lastBlock != -1 {
		_, blockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &invVects[lastBlock].Hash)
		if err == nil {
			blockHeightInt32, err := safeconversion.Uint32ToInt32(blockHeaderMeta.Height)
			if err != nil {
				sm.logger.Errorf(failedToConvertBlockHeightInt32Msg, err)
			}

			peer.UpdateLastBlockHeight(blockHeightInt32)
			state.noteBestKnownHeight(blockHeightInt32)
		}
	}

	// by default, we do not process transactions / blocks
	// only when we are in the running state we process transaction and new block messages
	processInvs := false

	fsmState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
	if err != nil {
		sm.logger.Errorf("[handleInvMsg] Failed to get current FSM state: %v", err)
	} else if fsmState != nil && *fsmState == teranodeblockchain.FSMStateRUNNING {
		processInvs = true
	}

	wg := sync.WaitGroup{}

	// Request the advertised inventory if we don't already have it.  Also,
	// request parent blocks of orphans if we receive one we already have.
	// Finally, attempt to detect potential stalls due to long side chains
	// we already have and request more blocks to prevent them.
	for i, iv := range invVects {
		if iv.Type == wire.InvTypeBlock {
			// process blocks in serial
			sm.processInvMsg(i, iv, processInvs, peer, exists, state, lastBlock)
			continue
		}

		// process all remaining inv vectors in parallel
		wg.Add(1)

		go func(i int, iv *wire.InvVect) {
			defer wg.Done()

			// Ignore unsupported inventory types.
			sm.processInvMsg(i, iv, processInvs, peer, exists, state, lastBlock)
		}(i, iv)
	}

	// wait for all inv vectors to be processed
	wg.Wait()

	// Request as much as possible at once.  Anything that won't fit into
	// the request will be requested on the next inv message.
	gdmsg := sm.drainRequestQueue(peer, state)

	if len(gdmsg.InvList) > 0 {
		sm.logger.Debugf("[handleInvMsg] Requesting %d items from %s", len(gdmsg.InvList), peer)
		peer.QueueMessage(gdmsg, nil)
	}
}

// drainRequestQueue turns as much of a peer's announcement queue as it can into
// one getdata, and returns it for the caller to send.
//
// One drain at a time per peer: see peerSyncState.requestQueueMu for why the
// peek and the consume have to be one operation. The lock covers the whole loop
// and nothing else, so an append from a concurrent processInvMsg still lands
// (SyncedSlice is safe on its own) and the getdata goes out unlocked.
func (sm *SyncManager) drainRequestQueue(peer *peerpkg.Peer, state *peerSyncState) *wire.MsgGetData {
	state.requestQueueMu.Lock()
	defer state.requestQueueMu.Unlock()

	numRequested := 0
	gdmsg := wire.NewMsgGetData()

outside:
	for state.requestQueue.Length() != 0 {
		// Read the front without consuming it. Everything below either deals
		// with the item and falls through to the Shift at the bottom, or breaks
		// out leaving it where it is — which is what a full ledger needs: the
		// queue is the only record that this block was announced, and an inv is
		// not guaranteed to come again.
		iv, found := state.requestQueue.Get(0)
		if !found {
			break
		}

		switch iv.Type {
		case wire.InvTypeBlock:
			// Request the block if there is not already a pending request.
			if !sm.blockDownloads.RequestedWithin(iv.Hash, blockRequestRetryInterval) {
				// As in fetchHeaderBlocks: a block the ledger will not take is
				// a block we must not ask for, or the reply looks unrequested
				// and costs this peer its connection. And as there, the work
				// item is held in place rather than discarded — this used to
				// rely on the peer announcing the block again, which a one-shot
				// inv past the final checkpoint never does.
				if !sm.blockDownloads.Add(peer, iv.Hash) {
					sm.logger.Warnf("[handleInvMsg] block download ledger full at %d blocks, holding off on %s from %s", maxTrackedBlockDownloads, iv.Hash, peer)
					break outside
				}

				if err := gdmsg.AddInvVect(iv); err != nil {
					sm.logger.Warnf(unexpectedFailureAddingInventoryMsg, err)
					break outside
				}

				numRequested++
			}

		case wire.InvTypeTx:
			// Request the transaction if there is not already a pending request.
			if _, requested := sm.requestedTxns.Get(iv.Hash); !requested {
				if err := gdmsg.AddInvVect(iv); err != nil {
					sm.logger.Warnf(unexpectedFailureAddingInventoryMsg, err)
					break outside
				}

				sm.requestedTxns.Set(iv.Hash, struct{}{})
				state.requestedTxns.Set(iv.Hash, struct{}{})

				numRequested++
			}
		}

		// Dealt with one way or the other, so it comes off the queue. Every path
		// that wants to keep it has broken out above.
		state.requestQueue.Shift()

		if numRequested >= maxRequestedBlocks {
			sm.logger.Debugf("[handleInvMsg] Limiting to %d item(s) from %s", numRequested, peer)
			break
		}
	}

	return gdmsg
}

func (sm *SyncManager) processInvMsg(i int, iv *wire.InvVect, processInvs bool, peer *peerpkg.Peer, exists bool, state *peerSyncState, lastBlock int) {
	switch iv.Type {
	case wire.InvTypeBlock:
	case wire.InvTypeTx:
		if !processInvs {
			// If we are not in running state, we are not interested in new transaction or block messages
			sm.logger.Debugf("[handleInvMsg] Ignoring inv message from %s, not in running state", peer)
			return
		}
	default:
		return
	}

	// Add the inventory to the cache of known inventory
	// for the peer.
	peer.AddKnownInventory(iv)

	// Ignore inventory when we're in headers-first mode.
	if sm.headersFirstMode.Load() {
		return
	}

	// Request the inventory if we don't already have it.
	haveInv, err := sm.haveInventory(iv)
	if err != nil {
		sm.logger.Warnf("[handleInvMsg] Unexpected failure when checking for "+
			"existing inventory during inv message "+
			"processing: %v", err)

		return
	}

	if !haveInv {
		if iv.Type == wire.InvTypeTx {
			// Skip the transaction if it has already been rejected.
			if _, exists = sm.rejectedTxns.Get(iv.Hash); exists {
				return
			}
		}

		// Add it to the request queue.
		state.requestQueue.Append(iv)

		return
	}

	if iv.Type == wire.InvTypeBlock {
		// We already have the final block advertised by this inventory message, so force a request for more.  This
		// should only happen if we're on a really long side chain.
		if i == lastBlock {
			// Request blocks after this one up to the final one the remote peer knows about (zero stop hash).
			locator, err := sm.blockchainClient.GetBlockLocator(sm.ctx, &iv.Hash, 0)
			if err != nil {
				sm.logger.Errorf("[handleInvMsg] Failed to get block locator for the block hash %s, %v", iv.Hash.String(), err)
			} else {
				_ = peer.PushGetBlocksMsg(locator, &zeroHash)
			}
		}
	}
}

type blockQueueMsg struct {
	block       *wire.MsgBlock
	blockHash   chainhash.Hash
	blockHeight int32
	peer        *peerpkg.Peer
	reply       chan error
	// committed says handleBlockMsg actually put this block in the chain, as
	// opposed to the several paths on which it returns nil having done no such
	// thing. Only the consumer reads it, and only to decide whether to drain the
	// blocks parked behind this one.
	committed bool
}

// blockHandler is the main handler for the sync manager.  It must be run as a
// goroutine.  It processes block and inv messages in a separate goroutine
// from the peer handlers so the block (MsgBlock) messages are handled by a
// single thread without needing to lock memory data structures.  This is
// important because the sync manager controls which blocks are needed and how
// the fetching should proceed.
func (sm *SyncManager) blockHandler() {
	ticker := time.NewTicker(syncPeerTickerInterval)
	defer ticker.Stop()

	// Checks whether the one block that is holding up in-order commit has been
	// outstanding long enough to be worth asking a second peer for. Runs here
	// because this goroutine is the one place that can safely look at the
	// frontier without touching the header list.
	frontierTicker := time.NewTicker(frontierCheckInterval)
	defer frontierTicker.Stop()

	// This buffer holds one *blockQueueMsg (a *wire.MsgBlock pointer) per slot.
	// With prefetch disabled a small fixed depth suffices: OnBlock keeps at most
	// one block per peer in flight, so the queue barely fills.
	//
	// With prefetch enabled the depth must be at least the byte-budget admission
	// ceiling (budget / minInFlightBlockWeight). Otherwise a full pipeline would
	// block blockHandler on `blockQueue <-`, and since that goroutine is the sole
	// consumer of msgChan, disconnects, sync-peer rotation, inv, headers and tx
	// dispatch would stall for EVERY peer — cross-peer head-of-line blocking. The
	// deeper queue does not raise the memory ceiling: the blocks it references are
	// still bounded in total bytes by the prefetch budget (AcquireBlockPrefetch),
	// so at most ~budget bytes of MsgBlocks are pinned regardless of slot count.
	// The slot count is clamped so a misconfigured multi-TB budget can't size a
	// huge channel backing array; beyond the clamp the budget still bounds memory
	// and the sm.quit-guarded enqueue still can't deadlock, only backpressure.
	maxBlockQueue := 100
	if sm.blockPrefetchBudget != nil {
		if ceiling := int(sm.blockPrefetchBudgetBytes / minInFlightBlockWeight); ceiling > maxBlockQueue {
			maxBlockQueue = ceiling
		}
		if maxBlockQueue > maxBlockQueueSlots {
			maxBlockQueue = maxBlockQueueSlots
		}
	}

	// create a block queue to handle block messages in a separate goroutine, in order
	blockQueue := make(chan *blockQueueMsg, maxBlockQueue)

	// start the block queue handler
	go func() {
		// The park sweep runs on THIS goroutine, not the outer handler below,
		// because committing a block is minutes of work and the outer handler
		// dispatches disconnects, invs, headers and tx for every peer.
		parkSweep := time.NewTicker(parkSweepInterval)
		defer parkSweep.Stop()

		for {
			select {
			case <-sm.quit:
				// Best-effort drain of already-queued blocks with an error reply
				// before exiting. Under prefetch each queued block has an
				// awaitBlockResult goroutine holding budget and waiting on its
				// reply; replying here lets them exit promptly on shutdown instead
				// of waiting for the peer's quit/ctx to fire. The feeder (the outer
				// loop) races the same sm.quit close, so a block it enqueues after
				// this drain returns is not caught here — that block's
				// awaitBlockResult still exits via sp.quit/sp.ctx.Done() (the
				// backstop), and the feeder's enqueue is itself sm.quit-guarded so
				// it can never deadlock. This drain only makes the common case
				// prompt; it is not relied on for correctness.
				for {
					select {
					case msg := <-blockQueue:
						// Keep the backlog decrement and its progress stamp paired
						// on every completion path (here the shutdown drain) so the
						// liveness invariant holds uniformly; rotation is moot during
						// shutdown, but the uniform pairing is easier to reason about.
						sm.blockBacklog.Add(-1)
						sm.noteBacklogProgress()

						if msg.reply != nil {
							msg.reply <- errors.NewServiceError("sync manager shutting down")
						}
					default:
						return
					}
				}
			case <-parkSweep.C:
				sm.sweepParkedBlocks(time.Now())
				// A rewind — from the sweep just above, or from a block given up
				// on since the last tick — moves the download cursor back and
				// sends nothing. This is what carries it out. See
				// resumeHeaderWalk.
				sm.resumeHeaderWalk()

			case msg := <-blockQueue:
				sm.logger.Debugf("[blockHandler][%s] processing block queue message into handleBlockMsg", msg.blockHash)

				err := sm.processQueuedBlock(msg)

				// A completion advances the backlog: stamp it so the stall check
				// treats the pipeline as live for another window (see
				// noteBacklogProgress / localReadBackpressured).
				sm.blockBacklog.Add(-1)
				sm.noteBacklogProgress()

				if msg.reply != nil {
					msg.reply <- err
				}
			}
		}
	}()

out:
	for {
		select {
		case <-ticker.C:
			sm.handleCheckSyncPeer()
		case <-frontierTicker.C:
			// Sampled immediately before the only thing that reads it, on the
			// same tick and over the same interval, so the rate and the decision
			// cannot be measured against different clocks.
			sm.samplePeerThroughput()
			sm.raceFrontierBlock(time.Now())
		case m := <-sm.msgChan:
			// whenever legacy receives a message, check if we are current
			// this call should have the current state cached, so it should be fast
			currentState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
			if err != nil {
				sm.logger.Errorf("[SyncManager] failed to get fsm current state")
			}

			// we reached current in legacy, and current FSM state is not Running, send RUN event
			if currentState != nil && *currentState != teranodeblockchain.FSMStateRUNNING {
				if sm.current() { // only call this when we are not in the running state, it's an expensive call
					sm.logger.Infof("[SyncManager] Legacy reached current, sending RUN event to FSM")
					if err = sm.blockchainClient.Run(sm.ctx, "legacy/netsync/manager/blockHandler"); err != nil {
						sm.logger.Infof("[Sync Manager] failed to send FSM RUN event %v", err)
					}

					sm.resetFeeFilterToDefault()
				}
			}

			switch msg := m.(type) {
			case *newPeerMsg:
				sm.handleNewPeerMsg(msg.peer)
				if msg.reply != nil {
					msg.reply <- struct{}{}
				}

			case *txMsg:
				go func(msg *txMsg) {
					// process tx messages in parallel
					sm.handleTxMsg(msg)
					if msg.reply != nil {
						msg.reply <- struct{}{}
					}
				}(msg)

			case *blockMsg:
				sm.logger.Debugf("[blockHandler][%s] queueing block for validation", msg.block.Hash())

				// A 0->1 transition opens a fresh backpressure window: stamp its
				// start so localReadBackpressured can tell slow-but-progressing
				// validation from a genuine processing hang. Enqueues into an
				// already-non-empty backlog deliberately do NOT stamp — only
				// completions advance processing, so letting a peer refresh the
				// liveness signal merely by feeding more blocks into a hung
				// pipeline would mask the hang.
				if sm.blockBacklog.Add(1) == 1 {
					sm.noteBacklogProgress()
				}

				// Guard the enqueue with sm.quit. This is the sole feeder of
				// blockQueue; without the guard, a full queue whose consumer has
				// already exited on shutdown would block here forever, so the loop
				// would never reach the sm.quit case, close(handlerDone) would never
				// run, and Stop() (which waits on handlerDone) would hang.
				select {
				case blockQueue <- &blockQueueMsg{
					block:       msg.block.MsgBlock(),
					blockHash:   *msg.block.Hash(),
					blockHeight: msg.block.Height(),
					peer:        msg.peer,
					reply:       msg.reply,
				}:
				case <-sm.quit:
					// Enqueue aborted on shutdown: undo the Add(1) above and keep
					// the decrement paired with its progress stamp, matching every
					// other completion path (uniform invariant; harmless here).
					sm.blockBacklog.Add(-1)
					sm.noteBacklogProgress()

					if msg.reply != nil {
						msg.reply <- errors.NewServiceError("sync manager shutting down")
					}
				}

			case *invMsg:
				go sm.handleInvMsg(msg)

			case *headersMsg:
				go sm.handleHeadersMsg(msg)

			case *donePeerMsg:
				sm.handleDonePeerMsg(msg.peer)
				if msg.reply != nil {
					msg.reply <- struct{}{}
				}

			case getSyncPeerMsg:
				var peerID int32

				if sp := sm.loadSyncPeer(); sp != nil {
					peerID = sp.ID()
				}
				msg.reply <- peerID

			case isCurrentMsg:
				sm.logger.Warnf("isCurrentMsg is deprecated, use current() instead")
				msg.reply <- sm.current()

			case pauseMsg:
				// Wait until the sender unpauses the manager.
				<-msg.unpause

			default:
				sm.logger.Warnf("Invalid message type in block handler: %T", msg)
			}

		case <-sm.quit:
			break out
		}
	}

	close(sm.handlerDone)
	sm.logger.Infof("Block handler done")
}

// NewPeer informs the sync manager of a newly active peer.
func (sm *SyncManager) NewPeer(peer *peerpkg.Peer, done chan struct{}) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		if done != nil {
			done <- struct{}{}
		}
		return
	}
	sm.msgChan <- &newPeerMsg{peer: peer, reply: done}
}

// QueueTx adds the passed transaction message and peer to the block handling
// queue. Responds to the done channel argument after the tx message is
// processed.
func (sm *SyncManager) QueueTx(tx *bsvutil.Tx, peer *peerpkg.Peer, done chan struct{}) {
	// Don't accept more transactions if we're shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		if done != nil {
			done <- struct{}{}
		}
		return
	}

	sm.msgChan <- &txMsg{tx: tx, peer: peer, reply: done}
}

// QueueBlock adds the passed block message and peer to the block handling
// queue. Responds to the done channel argument after the block message is
// processed.
func (sm *SyncManager) QueueBlock(block *bsvutil.Block, peer *peerpkg.Peer, done chan error) {
	// Don't accept more blocks if we're shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		done <- nil
		return
	}

	sm.msgChan <- &blockMsg{block: block, peer: peer, reply: done}
}

// UsePrefetchIngestion reports whether OnBlock should take the bounded async
// prefetch path. It requires a configured budget AND that we are not on
// regression net: the block-acceptance tooling depends on submit-then-query
// ordering, which only the synchronous path (OnBlock returns after the block is
// fully processed) guarantees. So regtest keeps synchronous ingestion — paired
// with, and for the same reason as, the regtest exception in BlockRequested. It
// shares the peerpkg.UseBlockPrefetchIngestion predicate with the read-loop's
// shouldArmProcessingTimer so both agree on when prefetch is active (a positive
// budget matches a non-nil budget semaphore, since it is created iff the byte
// budget is positive). A nil chainParams fails closed to the synchronous path.
func (sm *SyncManager) UsePrefetchIngestion() bool {
	if sm.chainParams == nil {
		// Fail closed to the synchronous path: without params we cannot rule out
		// regtest, and sync ingestion is the conservative default. Guarding here
		// matters because sm.chainParams.Net is evaluated as a call argument,
		// before UseBlockPrefetchIngestion's budget short-circuit could guard it.
		return false
	}

	return peerpkg.UseBlockPrefetchIngestion(sm.blockPrefetchBudgetBytes, sm.chainParams.Net)
}

// BlockRequested reports whether blockHash is one we have an outstanding
// getdata request for from the given peer (resolving stream peers to their
// association primary, as handleBlockMsg does). It lets the read-loop reject
// unrequested blocks BEFORE they consume prefetch budget, mirroring the
// unrequested-block check in handleBlockMsg. Under async prefetch this is what
// preserves the original backpressure: without it a misbehaving peer could
// admit a flood of unrequested blocks against the shared budget — starving the
// real sync peer and inflating buffered-block memory — before the downstream
// per-block disconnect fires. On regtest it always returns true; the regression
// harness intentionally feeds unrequested/duplicate blocks.
func (sm *SyncManager) BlockRequested(peer *peerpkg.Peer, blockHash *chainhash.Hash) bool {
	if sm.isRegtest() {
		return true
	}

	// Resolve stream sub-peers to their association primary, as handleBlockMsg
	// does; the ledger records the primary, so that is the identity to ask about.
	_, primary, exists := sm.peerStateResolvingPrimary(peer)
	if !exists {
		return false
	}

	return sm.blockDownloads.HasOwner(primary, *blockHash)
}

// AcquireBlockPrefetch reserves prefetch budget for a block of the given
// serialized size and returns the amount actually reserved, which the caller
// MUST later hand back to ReleaseBlockPrefetch exactly once. The weight is
// clamped to the total budget so a block larger than the whole budget is
// admitted alone (it waits until every other in-flight block has drained),
// which preserves the original one-block-at-a-time backpressure for huge
// blocks and guarantees Acquire can never deadlock on an oversized block.
//
// It returns an error only if ctx is cancelled while waiting (shutdown), in
// which case nothing was reserved, OR the benign ErrDuplicateBlockInFlight
// sentinel when blockHash is already in flight (dedup — again nothing reserved).
// When prefetch is disabled it is a no-op returning (0, nil), which also skips
// dedup (the synchronous path already keeps one block in flight per peer). While
// blocked waiting for budget it increments blockPrefetchWaiters so the stall
// detector can tell self-backpressure apart from a genuinely stalled peer.
//
// The caller MUST hand blockHash back to ReleaseBlockPrefetch with the returned
// weight on success: the hash lives in the in-flight set for exactly the same
// lifetime as the reserved budget (inserted here, deleted on release), so the
// dedup half and the byte half of this admission gate never drift.
func (sm *SyncManager) AcquireBlockPrefetch(ctx context.Context, quit <-chan struct{}, blockHash chainhash.Hash, size int64) (int64, error) {
	if sm.blockPrefetchBudget == nil {
		return 0, nil
	}

	// Floor the weight so a flood of tiny blocks can't admit an unbounded number
	// of in-flight goroutines within the byte budget, then clamp to the budget so
	// an oversized block is admitted alone (and budgets smaller than the floor
	// still process one block at a time rather than deadlocking).
	weight := size
	if weight < minInFlightBlockWeight {
		weight = minInFlightBlockWeight
	}
	if weight > sm.blockPrefetchBudgetBytes {
		weight = sm.blockPrefetchBudgetBytes
	}

	// Dedup: reserve the hash BEFORE reserving budget. Inserting ahead of the
	// (possibly blocking) Acquire is deliberate — it bounds duplicates even while
	// a copy is parked waiting for budget, so N copies of one requested,
	// near-budget-sized block cannot each grab budget and fill it. A hash already
	// present is a duplicate: drop it (nothing reserved, nothing inserted).
	sm.inFlightBlocksMu.Lock()
	if _, dup := sm.inFlightBlocks[blockHash]; dup {
		sm.inFlightBlocksMu.Unlock()
		return 0, ErrDuplicateBlockInFlight
	}
	sm.inFlightBlocks[blockHash] = struct{}{}
	sm.inFlightBlocksMu.Unlock()

	// removeInFlight undoes the reservation above. It runs only when the budget
	// Acquire fails (ctx/quit cancel): nothing was reserved, so the hash must not
	// linger. On success the hash stays until ReleaseBlockPrefetch deletes it.
	removeInFlight := func() {
		sm.inFlightBlocksMu.Lock()
		delete(sm.inFlightBlocks, blockHash)
		sm.inFlightBlocksMu.Unlock()
	}

	// Fast path: budget available right now, no waiter accounting needed.
	if sm.blockPrefetchBudget.TryAcquire(weight) {
		return weight, nil
	}

	// Slow path: we must wait for in-flight blocks to drain. Flag that this
	// read-loop is backpressured by our own processing so the stall detector
	// does not mistake the resulting read stall for a slow peer.
	sm.blockPrefetchWaiters.Add(1)
	defer sm.blockPrefetchWaiters.Add(-1)

	// Abort the wait on peer teardown too, not just whole-process ctx cancellation:
	// the caller's ctx (the ServiceManager errgroup Init context) is cancelled on
	// daemon shutdown but not by legacy.Server.Stop() alone, while quit (the peer's
	// quit channel) closes on both individual disconnect and shutdown. This mirrors
	// awaitBlockResult so a budget-parked read-loop never outlives its peer. The
	// linking goroutine only exists while we are blocked (the rare backpressure
	// case) and exits as soon as the acquire resolves.
	if quit != nil {
		var cancel context.CancelFunc

		ctx, cancel = context.WithCancel(ctx)
		defer cancel()

		go func() {
			select {
			case <-quit:
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	if err := sm.blockPrefetchBudget.Acquire(ctx, weight); err != nil {
		// Nothing reserved: drop the hash we inserted before parking so a torn-down
		// or cancelled acquire never leaks a slot in the dedup set.
		removeInFlight()
		return 0, err
	}

	return weight, nil
}

// ReleaseBlockPrefetch returns budget reserved by AcquireBlockPrefetch and drops
// the block's hash from the in-flight dedup set. The two are released together
// (same lifetime as the reservation) so the dedup and byte halves of the
// admission gate never drift. A zero weight (nothing reserved) still deletes the
// hash but skips the budget Release; a nil budget (prefetch disabled) is a no-op.
// Only ever called for hashes that AcquireBlockPrefetch successfully admitted —
// the dup/early-return paths never reach here (OnBlock does not spawn
// awaitBlockResult for them), so no hash is deleted that was not first inserted.
func (sm *SyncManager) ReleaseBlockPrefetch(blockHash chainhash.Hash, weight int64) {
	if sm.blockPrefetchBudget == nil {
		return
	}

	sm.inFlightBlocksMu.Lock()
	delete(sm.inFlightBlocks, blockHash)
	sm.inFlightBlocksMu.Unlock()

	if weight <= 0 {
		return
	}
	sm.blockPrefetchBudget.Release(weight)
}

// noteBacklogProgress records that the block backlog just advanced — a block was
// enqueued to open a fresh backpressure window, or one finished processing. It
// must run on every backlog transition that constitutes progress: the 0->1
// enqueue and every completion decrement. localReadBackpressured treats a stamp
// older than blockProcessingStallTimeout as a hung pipeline rather than
// slow-but-progressing validation, so keeping this current is what lets the
// stall detector distinguish the two. Enqueues into an already-non-empty backlog
// deliberately do NOT call this (only completions advance processing).
func (sm *SyncManager) noteBacklogProgress() {
	sm.lastBacklogProgress.Store(time.Now().UnixNano())
}

// blockProcessingStallTimeout is how long a non-empty block backlog may go
// without advancing before localReadBackpressured stops suppressing the
// sync-peer stall check. It tracks settings.Legacy.PeerProcessingTimeout — the
// per-message watchdog that this progress-aware rule replaces for prefetched
// blocks — and falls back to defaultBlockProcessingStallTimeout when settings
// are absent (unit-test SyncManagers) or the value is unset.
func (sm *SyncManager) blockProcessingStallTimeout() time.Duration {
	if sm.settings != nil && sm.settings.Legacy.PeerProcessingTimeout > 0 {
		return sm.settings.Legacy.PeerProcessingTimeout
	}

	return defaultBlockProcessingStallTimeout
}

// localReadBackpressured reports whether the node is currently throttling its
// own network reads because local block processing cannot keep up. The stall
// detector skips its checks while this holds, since zero throughput then
// reflects our validation speed, not the sync peer's health. With prefetch
// enabled that is when read-loops are blocked acquiring budget; with prefetch
// disabled it is the original condition of any block queued or mid-validation.
// On the kill-switch path (prefetch disabled, budget nil) suppression stays
// UNCONDITIONAL, exactly as pre-prefetch: the per-message watchdog is still
// armed for blocks there and owns processing-stall liveness, so timeout-gating
// would rotate a healthy sync peer on a legitimately slow block. The
// progress-aware timeout applies only under prefetch, where that watchdog is
// disarmed for blocks and this is the compensating liveness signal.
func (sm *SyncManager) localReadBackpressured() bool {
	// A non-empty local backlog means blocks are queued or mid-validation, so a
	// stale last-block-time and zero throughput normally reflect our own
	// validation speed, not the sync peer's health. Suppress the stall check —
	// but only while the backlog is still ADVANCING. Disarming the per-message
	// watchdog for prefetched blocks removed the only timeout over the processing
	// phase; if we suppressed on any non-zero backlog, a genuine hang
	// (store/validator deadlock, Aerospike overload) would leave the backlog
	// pinned >=1 forever and the node would silently stop syncing with no
	// rotation. So a backlog that has not advanced for longer than
	// blockProcessingStallTimeout is treated as a stalled pipeline, not
	// slow-but-progressing validation: stop suppressing so handleCheckSyncPeer
	// logs and rotates — restoring the pre-prefetch liveness signal without the
	// false rotation of a merely-slow block that motivated disarming the
	// watchdog. Deliberately do NOT fall through to the waiter check when the
	// backlog is stale: a hung pipeline with a full budget accumulates waiters,
	// and we WANT rotation then.
	if sm.blockBacklog.Load() > 0 {
		// Kill switch (prefetch disabled, budget nil): the per-message processing
		// watchdog is still armed for blocks and owns processing-stall liveness,
		// exactly as pre-prefetch. Keep the original UNCONDITIONAL suppression here —
		// timeout-gating would rotate a healthy sync peer on a legitimately slow
		// block, churn the "proven synchronous" path never had. The progress-aware
		// timeout below applies only under prefetch, where the watchdog is disarmed
		// for blocks and this is the compensating liveness signal.
		if sm.blockPrefetchBudget == nil {
			return true
		}

		return time.Since(time.Unix(0, sm.lastBacklogProgress.Load())) < sm.blockProcessingStallTimeout()
	}

	// Under prefetch also suppress while a read-loop is parked in
	// AcquireBlockPrefetch waiting for budget. In the running system that implies
	// a backlog too, but the explicit waiter signal keeps the accounting clear
	// (and unit-testable in isolation).
	return sm.blockPrefetchBudget != nil && sm.blockPrefetchWaiters.Load() > 0
}

// sendDuringShutdown delivers v on ch, recovering from the "send on closed
// channel" panic that races teardown. Inv delivery runs on peer read-loop
// goroutines (OnInv -> QueueInv), but the channels they target are torn down by
// a different goroutine during shutdown: the kafka async producer closes
// legacyKafkaInvCh in its Stop(), and the block handler stops draining msgChan.
// The shutdown flag check in QueueInv narrows but cannot close that window — a
// flag check and a channel send are not atomic against a concurrent close — so
// a late inv would otherwise crash the whole process. Dropping an inv during
// shutdown is safe: inv is an advisory announcement, re-sent by the peer (or a
// later session) on the next connection. Returns false if the channel was closed.
func sendDuringShutdown[T any](ch chan T, v T) (sent bool) {
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()

	ch <- v

	return true
}

// QueueInv adds the passed inv message and peer to the block handling queue.
func (sm *SyncManager) QueueInv(inv *wire.MsgInv, peer *peerpkg.Peer) {
	// No channel handling here because peers do not need to block on inv
	// messages.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	// write all tx inv messages to Kafka and read from there
	// this allows us to stop reading in certain cases, but still have the inv messages to catch up on
	if sm.legacyKafkaInvCh != nil {
		// split inv message to transactions and blocks
		invBlockMsg := wire.NewMsgInv()
		invTxMsg := wire.NewMsgInv()

		for _, invVect := range inv.InvList {
			if invVect.Type == wire.InvTypeBlock {
				if err := invBlockMsg.AddInvVect(invVect); err != nil {
					sm.logger.Errorf("failed to add inv vector to inv block message: %v", err)
					continue
				}
			} else {
				if err := invTxMsg.AddInvVect(invVect); err != nil {
					sm.logger.Errorf("failed to add inv vector to inv tx message: %v", err)
					continue
				}
			}
		}

		if len(invBlockMsg.InvList) > 0 {
			netsyncInvMsg := invMsg{inv: invBlockMsg, peer: peer}
			sendDuringShutdown[interface{}](sm.msgChan, &netsyncInvMsg)
		}

		if len(invTxMsg.InvList) > 0 {
			msg := sm.newKafkaMessageFromInv(invTxMsg, peer)

			value, err := proto.Marshal(msg)
			if err != nil {
				sm.logger.Errorf("failed to marshal kafka inv topic message: %v", err)
				return
			}

			// write to Kafka
			sm.logger.Debugf("writing INV message to Kafka from peer %s, length: %d", peer.String(), len(value))
			sendDuringShutdown(sm.legacyKafkaInvCh, &kafka.Message{
				Value: value,
			})
		}
	} else {
		netsyncInvMsg := invMsg{inv: inv, peer: peer}
		sendDuringShutdown[interface{}](sm.msgChan, &netsyncInvMsg)
	}
}

// QueueHeaders adds the passed headers message and peer to the block handling
// queue.
func (sm *SyncManager) QueueHeaders(headers *wire.MsgHeaders, peer *peerpkg.Peer) {
	// No channel handling here because peers do not need to block on
	// headers messages.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	sm.msgChan <- &headersMsg{headers: headers, peer: peer}
}

// NotFound handles a peer telling us it does not have blocks we asked it for.
//
// A notfound is the peer discharging a getdata honestly, and the two things it
// has to change are the two the log-only handler this replaces left alone: the
// peer is no longer down for that block, and the block is wanted again. Neither
// happens on its own. Ownership would otherwise stand for the hour-long
// assignment ceiling, holding a queue slot the peer can never fill, and the
// download walk is forward-only, so the hash sits behind the cursor with nobody
// owing it — which is the same stranded state a departing peer leaves behind,
// reached without losing the peer.
//
// It happens legitimately: take() deliberately falls back to a peer that has not
// claimed the height rather than stopping the walk, and a pruned peer answers
// notfound to every request for an old block.
//
// Only blocks this peer actually owed are touched, so a notfound naming a hash
// somebody else is carrying — or one we never asked for — cannot move the cursor
// or discharge anyone. Releasing rather than back-dating is deliberate: the peer
// has told us its copy is not coming, so holding it to an obligation it has
// already answered would spend its queue slot on nothing for the rest of the
// hour.
//
// This runs on the caller's goroutine, which is the peer's read loop. It takes
// the download ledger's leaf lock and then, released, the header lock; both are
// held for pure in-memory work, so the read loop is never parked on I/O.
//
// With the fan-out off, the sync peer is the only peer ever asked for a body and
// asking it again for a block it has just said it does not have has nowhere else
// to go, so the old log-only behaviour is kept.
func (sm *SyncManager) NotFound(notFound *wire.MsgNotFound, peer *peerpkg.Peer) {
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	sm.logger.Warnf("[NotFound] peer %s does not have %d of the items we asked it for", peer.String(), len(notFound.InvList))

	if !sm.settings.Legacy.MultiPeerBlockDownload {
		return
	}

	released := make([]chainhash.Hash, 0, len(notFound.InvList))

	for _, iv := range notFound.InvList {
		if iv.Type != wire.InvTypeBlock {
			continue
		}

		if !sm.blockDownloads.HasOwner(peer, iv.Hash) {
			continue
		}

		sm.blockDownloads.RemoveOwner(peer, iv.Hash)

		released = append(released, iv.Hash)
	}

	if len(released) == 0 {
		return
	}

	lowestHeight, rewound := sm.rewindToLowestHeader(released)
	if !rewound {
		sm.logger.Debugf("[NotFound] released %d blocks %s says it does not have, none of them still in the header list", len(released), peer.String())

		return
	}

	sm.logger.Infof("[NotFound] released %d blocks %s says it does not have and moved the download cursor back to height %d", len(released), peer.String(), lowestHeight)
}

// DonePeer informs the blockmanager that a peer has disconnected.
func (sm *SyncManager) DonePeer(peer *peerpkg.Peer, done chan struct{}) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		if done != nil {
			done <- struct{}{}
		}
		return
	}

	sm.logger.Infof("Done peer %s", peer)
	sm.msgChan <- &donePeerMsg{peer: peer, reply: done}
}

// Start begins the core block handler which processes block and inv messages.
func (sm *SyncManager) Start() {
	// Already started?
	if atomic.AddInt32(&sm.started, 1) != 1 {
		return
	}

	sm.logger.Infof("Starting sync manager")

	// Adopt whatever a previous run left parked, before anything can drain it.
	// No RPCs are made here; the parents are reconciled with the chain by the
	// park sweep once the block-queue consumer is running.
	sm.blockPark.Recover(sm.ctx)

	go sm.blockHandler()
}

// Stop gracefully shuts down the sync manager by stopping all asynchronous
// handlers and waiting for them to finish.
func (sm *SyncManager) Stop() error {
	if atomic.AddInt32(&sm.shutdown, 1) != 1 {
		sm.logger.Warnf("Sync manager is already in the process of " +
			"shutting down")
		return nil
	}

	sm.logger.Infof("Sync manager shutting down")
	close(sm.quit)
	<-sm.handlerDone

	sm.orphanTxs.Stop()
	sm.requestedTxns.Stop()

	if sm.blockFailureBackoff != nil {
		sm.blockFailureBackoff.Stop()
	}

	if sm.recentlyFailedBlocks != nil {
		sm.recentlyFailedBlocks.Stop()
	}

	if sm.racedBlocks != nil {
		sm.racedBlocks.Stop()
	}

	// DC15 / review C1: quiesce Put then drain the tx-announce batcher before
	// tearing down transports.
	sm.closeTxAnnounceBatcher()

	// DC11: stop the legacy INV async producer so its final flush runs during
	// shutdown. Safe here — handlerDone above guarantees no more sends to
	// legacyKafkaInvCh, which producer.Stop() closes. Stop() has no caller ctx to
	// honour (Stop() takes none), so it is raced against an internal timeout: a
	// wedged broker flush can't block shutdown, and the outstanding Stop() finishes
	// the flush later if it can.
	if sm.legacyKafkaInvProducer != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), util.DefaultBatcherDrainTimeout)
		kafka.StopProducerCtx(stopCtx, sm.logger, "legacy INV", sm.legacyKafkaInvProducer)
		cancel()
	}

	return nil
}

// announceTx queues a transaction for peer announcement via the tx-announce
// batcher, unless the batcher has been closed by closeTxAnnounceBatcher during
// shutdown. go-batcher v2.0.4 panics on Put-after-Close, and this is called from
// the txmeta Kafka listener goroutine (not joined by Stop), so the read lock
// pairs with the write lock in closeTxAnnounceBatcher to make a post-close Put a
// safe no-op.
func (sm *SyncManager) announceTx(item *TxHashAndFee) {
	sm.txAnnounceMu.RLock()
	defer sm.txAnnounceMu.RUnlock()

	if !sm.txAnnounceClosed && sm.txAnnounceBatcher != nil {
		sm.txAnnounceBatcher.Put(item)
	}
}

// closeTxAnnounceBatcher marks the tx-announce batcher closed (so further
// announceTx calls become no-ops) and then drains it under a bounded timeout.
// Taking the write lock first waits for any in-flight announceTx (holding the
// read lock) to finish, so no Put can race the drain. Idempotent.
func (sm *SyncManager) closeTxAnnounceBatcher() {
	sm.txAnnounceMu.Lock()
	alreadyClosed := sm.txAnnounceClosed
	sm.txAnnounceClosed = true
	sm.txAnnounceMu.Unlock()

	if alreadyClosed || sm.txAnnounceBatcher == nil {
		return
	}

	util.DrainBatcher(sm.logger, "netsync_tx_announce", util.DefaultBatcherDrainTimeout, sm.txAnnounceBatcher.Close)
}

// SyncPeerID returns the ID of the current sync peer, or 0 if there is none.
func (sm *SyncManager) SyncPeerID() int32 {
	reply := make(chan int32)
	sm.msgChan <- getSyncPeerMsg{reply: reply}

	return <-reply
}

// PeersWithBlockDownloads reports how many peers currently have at least one
// block request outstanding.
//
// The peer layer uses this to widen a block's wall-clock ceiling: pulling blocks
// from several peers at once shares our downstream link between them, so every
// transfer is honestly slower and judging each against a single-peer deadline
// disconnects peers that are doing nothing wrong.
//
// Only peers with a genuine outstanding request count. A peer cannot inflate our
// patience by announcing blocks it never sends, because nothing is recorded until
// we ask for it, and a request old enough to have aged out of the download ledger
// stops counting too — a getdata that was lost on the wire must not go on buying
// every peer extra time forever.
func (sm *SyncManager) PeersWithBlockDownloads() int {
	return sm.blockDownloads.PeersWithDownloads()
}

// IsCurrent returns whether the sync manager believes it is synced with
// the connected peers.
func (sm *SyncManager) IsCurrent() bool {
	return sm.current()
}

// Pause pauses the sync manager until the returned channel is closed.
//
// Note that while paused, all peer and block processing is halted.  The
// message sender should avoid pausing the sync manager for long durations.
func (sm *SyncManager) Pause() chan<- struct{} {
	c := make(chan struct{})
	sm.msgChan <- pauseMsg{c}

	return c
}

// New constructs a new SyncManager. Use Start to begin processing asynchronous
// block, tx, and inv updates.
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockchainClient teranodeblockchain.ClientI,
	validationClient validator.Interface, utxoStore utxostore.Store, subtreeStore blob.Store, tempStore blob.Store,
	subtreeValidation subtreevalidation.Interface, blockValidation blockvalidation.Interface,
	blockAssembly blockassembly.ClientI, config *Config) (*SyncManager, error) {
	initPrometheusMetrics()

	sm := SyncManager{
		ctx:          ctx,
		settings:     tSettings,
		peerNotifier: config.PeerNotifier,
		// txMemPool:     config.TxMemPool,
		orphanTxs:      expiringmap.New[chainhash.Hash, *orphanTxAndParents](tSettings.Legacy.OrphanEvictionDuration).WithMaxSize(tSettings.Legacy.MaxOrphanTxs),
		chainParams:    config.ChainParams,
		rejectedTxns:   txmap.NewSyncedMap[chainhash.Hash, struct{}](maxRejectedTxns), // limit map size to maxRejectedTxns
		requestedTxns:  expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),   // give peers 10 seconds to respond
		blockDownloads: newBlockDownloadTracker(blockRequestAssignmentTTL),
		peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		// Peers we asked for a second copy of a stalled block, so their late
		// copy is dropped rather than costing them their connection.
		racedBlocks: expiringmap.New[chainhash.Hash, map[*peerpkg.Peer]struct{}](racedBlockGraceTTL).WithMaxSize(racedBlockGraceMaxTracked),
		// progressLogger:  newBlockProgressLogger("Processed", log),
		msgChan:          make(chan interface{}, maxMsgQueueSize),
		headerList:       list.New(),
		headerIndex:      make(map[chainhash.Hash]*list.Element),
		blockSizeTracker: newBlockSizeTracker(10), // track last 10 blocks for rolling average
		quit:             make(chan struct{}),
		// feeEstimator:            config.FeeEstimator,
		minSyncPeerNetworkSpeed: config.MinSyncPeerNetworkSpeed,
		handlerDone:             make(chan struct{}),
		// teranode stores etc.
		logger:            logger,
		blockchainClient:  blockchainClient,
		validationClient:  validationClient,
		utxoStore:         utxoStore,
		subtreeStore:      subtreeStore,
		subtreeValidation: subtreeValidation,
		blockValidation:   blockValidation,
		blockAssembly:     blockAssembly,
	}

	// Where a block whose parent is not stored yet waits instead of being thrown
	// away. nil when the park is switched off or the temp store is one whose
	// contents a restart could not enumerate; every call site reads nil as
	// "discard the block", which is what the node did before the park existed.
	sm.blockPark = newBlockPark(logger, tSettings, tempStore)

	// Bounded async block prefetch: with a positive budget OnBlock admits a
	// block against this global byte-weighted semaphore and returns, so the
	// read-loop downloads the next block while the current one is validated.
	// The budget caps the total serialized bytes of in-flight blocks; a budget
	// of 0 disables prefetch entirely (synchronous, one-block-in-flight).
	if budget := tSettings.Legacy.BlockPrefetchBufferBytes; budget > 0 {
		sm.blockPrefetchBudgetBytes = budget
		sm.blockPrefetchBudget = semaphore.NewWeighted(budget)
		// Dedup half of the same admission gate as the budget semaphore, created
		// in lockstep with it: paired 1:1 with each budget reservation so at most
		// one copy of a block hash is ever admitted/queued at a time.
		sm.inFlightBlocks = make(map[chainhash.Hash]struct{})
	}

	// The fail-closed inline lever is a no-op unless the outpoint-only below-checkpoint
	// path is also enabled (legacyFailClosed depends on legacyOutpointOnly). Warn so an
	// operator A/B-testing the new flag alone is not silently getting nothing.
	if tSettings.BlockValidation.LegacyBelowCheckpointFailClosed && !tSettings.BlockValidation.OutpointOnlyBelowCheckpoint {
		logger.Warnf("[netsync] blockvalidation_legacy_below_checkpoint_fail_closed is set but has no effect without blockvalidation_outpoint_only_below_checkpoint")
	}

	// create the transaction announcement batcher
	sm.txAnnounceBatcher = batcher.NewWithDeduplicationAndPool[TxHashAndFee](maxRequestedTxns, 1*time.Second, func(batch []*TxHashAndFee) {
		sm.logger.Debugf("announcing %d transactions to peers", len(batch))

		// process the batch
		sm.peerNotifier.AnnounceNewTransactions(batch)
	}, true,
		batcher.WithName("netsync_tx_announce"),
		batcher.WithLogger(logger),
		batcher.WithMetrics(batchermetrics.Provider()),
		batcher.WithTracer(tracing.Tracer("SyncManager").OTelTracer()),
	)

	// set an eviction function for orphan transactions
	// this will be called when an orphan transaction is evicted from the map
	sm.orphanTxs.WithEvictionFunction(func(txHash chainhash.Hash, orphanTx *orphanTxAndParents) bool {
		// try to process one last time
		// passing in block height 0, which will default to utxo store block height in validator
		if _, err := sm.validationClient.Validate(sm.ctx, orphanTx.tx, 0); err != nil {
			sm.logger.Debugf("failed to validate orphan transaction when evicting %v: %v", txHash, err)
		} else {
			sm.logger.Debugf("evicted orphan transaction %v", txHash)
		}

		return true
	})

	// add the number of orphan transactions to the prometheus metric
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-sm.quit:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				// update the number of orphan transactions
				prometheusLegacyNetsyncOrphans.Set(float64(sm.orphanTxs.Len()))
			}
		}
	}()

	bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	// Build the per-block backoff map only after the last fallible step above.
	// newBlockFailureBackoffMap starts a background eviction goroutine that is
	// only stopped via SyncManager.Stop(); constructing it before an early
	// error return would leak that goroutine, since the caller receives a nil
	// SyncManager and can never call Stop() (#1187, review).
	sm.blockFailureBackoff = newBlockFailureBackoffMap(tSettings.Legacy.BlockFailureBackoffBase, tSettings.Legacy.BlockFailureBackoffMaxDuration, tSettings.Legacy.PeerProcessingTimeout)

	// Tracks recently-failed block hashes so descendants of an unstored/rejected
	// block are short-circuited rather than triggering a NOT_FOUND ERROR cascade
	// (#1333). Like blockFailureBackoff this starts a background eviction goroutine
	// stopped only via Stop(), so build it after the last fallible step above.
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](recentlyFailedBlocksTTL).WithMaxSize(blockFailureBackoffMaxTracked)

	if !config.DisableCheckpoints {
		bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
		if err != nil {
			sm.logger.Errorf(failedToConvertBlockHeightInt32Msg, err)
		}

		// Initialize the next checkpoint based on the current height. New is
		// single-threaded, but the lock is taken anyway so the rule that
		// nextCheckpoint is only ever written under headerMu has no exceptions
		// for a future reader. resetHeaderState takes headerMu itself and so
		// must stay outside the hold — sync.Mutex is not reentrant.
		sm.headerMu.Lock()
		sm.nextCheckpoint = sm.findNextHeaderCheckpoint(bestBlockHeightInt32)
		haveCheckpoint := sm.nextCheckpoint != nil
		sm.headerMu.Unlock()

		if haveCheckpoint {
			sm.resetHeaderState(bestBlockHeader.Hash(), bestBlockHeightInt32)
		}
	} else {
		sm.logger.Infof("Checkpoints are disabled")
	}

	sm.startKafkaListeners(ctx, err)

	return &sm, nil
}

func (sm *SyncManager) startKafkaListeners(ctx context.Context, _ error) {
	blockControlChan := make(chan bool, 1) // control channel for block-related listeners (buffered to prevent blocking)
	txControlChan := make(chan bool, 1)    // control channel for transaction-related listeners (buffered to prevent blocking)

	// start a go routine to control the kafka listeners based on FSM state
	// Block-related listeners (INV, blocks final): always enabled
	// Transaction-related listeners (txmeta): enabled only when in RUNNING state
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				// Block-related listeners are always enabled. The only FSM state
				// that previously disabled them (legacy sync mode) was removed; no
				// automated path ever entered it — an operator could only reach it
				// manually via the setfsmstate CLI / FSM admin endpoint.
				blockEnabled := true

				// Non-blocking send to avoid deadlock if no one is reading
				select {
				case blockControlChan <- blockEnabled:
				default:
				}

				// Transaction-related listeners: enable only when RUNNING
				isRunning, _ := sm.blockchainClient.IsFSMCurrentState(sm.ctx, teranodeblockchain.FSMStateRUNNING)

				// Non-blocking send to avoid deadlock if no one is reading
				select {
				case txControlChan <- isRunning:
				default:
				}
			}
		}
	}()

	var blockListenersCh []chan bool // channels for block-related listeners
	var txListenersCh []chan bool    // channels for tx-related listeners

	// Kafka for INV messages (responds to requests from other nodes)
	legacyInvConfigURL := sm.settings.Kafka.LegacyInvConfig
	if legacyInvConfigURL != nil {
		sm.legacyKafkaInvCh = make(chan *kafka.Message, 10_000)

		producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, sm.logger, legacyInvConfigURL, &sm.settings.Kafka)
		if err != nil {
			sm.logger.Errorf("[Legacy Manager] error starting kafka producer: %v", err)
			return
		}

		// Retain the producer (DC11) so SyncManager.Stop() can flush it synchronously.
		sm.legacyKafkaInvProducer = producer

		// start a go routine to start the kafka producer
		go func() {
			producer.Start(sm.ctx, sm.legacyKafkaInvCh)
		}()

		// INV listener receives inventory messages from other nodes
		controlCh := make(chan bool)
		blockListenersCh = append(blockListenersCh, controlCh)

		go kafka.StartKafkaControlledListener(ctx, sm.logger, "inv.legacy"+"."+sm.settings.ClientName, controlCh, legacyInvConfigURL, sm.kafkaINVListener)
	}

	// Kafka for blocks final messages (announces blocks to peers)
	blocksFinalConfigURL := sm.settings.Kafka.BlocksFinalConfig
	if blocksFinalConfigURL != nil {
		controlCh := make(chan bool)
		blockListenersCh = append(blockListenersCh, controlCh)

		go kafka.StartKafkaControlledListener(ctx, sm.logger, "blocksfinal.legacy"+"."+sm.settings.ClientName, controlCh, blocksFinalConfigURL, sm.kafkaBlocksFinalListener)
	}

	// Kafka for txmeta messages (announces transactions to peers)
	txmetaKafkaURL := sm.settings.Kafka.TxMetaConfig

	if txmetaKafkaURL != nil {
		controlCh := make(chan bool)
		txListenersCh = append(txListenersCh, controlCh)

		// disable replay for txmeta in the legacy service, we do not have to replay anything, ever
		values := txmetaKafkaURL.Query()
		values.Set("replay", "0")

		txmetaKafkaURL.RawQuery = values.Encode()

		go kafka.StartKafkaControlledListener(ctx, sm.logger, "txmeta.legacy"+"."+sm.settings.ClientName, controlCh, txmetaKafkaURL, sm.kafkaTXmetaListener)
	}

	// Tx announcements to legacy peers are handled entirely by the txmeta Kafka path.
	// Subtree notifications are NOT used for tx announcements — they caused all txs in
	// reorganized subtrees to be re-announced to peers after every new block.

	// Control block listeners based on blockControlChan
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case control := <-blockControlChan:
				for _, ch := range blockListenersCh {
					ch <- control
				}
			}
		}
	}()

	// Control transaction listeners based on txControlChan
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case control := <-txControlChan:
				for _, ch := range txListenersCh {
					ch <- control
				}
			}
		}
	}()
}

func (sm *SyncManager) kafkaINVListener(ctx context.Context, kafkaURL *url.URL, groupID string) {
	kafka.StartKafkaListener(ctx, sm.logger, kafkaURL, groupID, true, func(msg *kafka.KafkaMessage) error {
		var message kafkamessage.KafkaInvTopicMessage

		err := proto.Unmarshal(msg.Value, &message)
		if err != nil {
			sm.logger.Errorf("[kafkaINVListener] failed to unmarshal kafka inv topic message: %v", err)
			return nil // ignore any errors, the message might be old and/or the peer is already disconnected
		}

		invMsg, err := sm.newInvFromKafkaMessage(&message)
		if err != nil {
			sm.logger.Errorf("[kafkaINVListener] failed to create inv msg from kafka message: %v", err)
			return nil
		}

		sm.logger.Debugf("[kafkaINVListener] Received INV message from Kafka from peer %s", message.PeerAddress)

		// Process the INV message directly, requesting data from other nodes will be queued on the outputQueue
		go sm.handleInvMsg(invMsg)

		return nil
	}, &sm.settings.Kafka)
}

func (sm *SyncManager) kafkaBlocksFinalListener(ctx context.Context, kafkaURL *url.URL, groupID string) {
	kafka.StartKafkaListener(ctx, sm.logger, kafkaURL, groupID, true, func(msg *kafka.KafkaMessage) error {
		if msg.Key == nil {
			sm.logger.Errorf("[kafkaBlocksFinalListener] no Kafka message key specified, skipping message")
			// not going to retry, if we don't have a key/hash
			return nil
		}

		hash, err := chainhash.NewHashFromStr(string(msg.Key))
		if err != nil {
			sm.logger.Errorf("[kafkaBlocksFinalListener][%s] failed to create hash from Kafka message key: %v", hash, err)
			// not going to retry, if we cannot parse the message
			return nil
		}

		var blockMsg kafkamessage.KafkaBlocksFinalTopicMessage
		if err := proto.Unmarshal(msg.Value, &blockMsg); err != nil {
			sm.logger.Errorf("[kafkaBlocksFinalListener][%s] failed to unmarshal kafka block topic message: %v", hash, err)
			// not going to retry, if we cannot parse the message
			return nil
		}

		header, err := model.NewBlockHeaderFromBytes(blockMsg.Header)
		if err != nil {
			sm.logger.Errorf("[kafkaBlocksFinalListener][%s] failed to create block header from Kafka message: %v", hash, err)
			// not going to retry, if we cannot parse the message
			return nil
		}

		// create wireBlockHeader
		wireBlockHeader := header.ToWireBlockHeader()

		sm.logger.Infof("[kafkaBlocksFinalListener] received block final message from Kafka: %s, %s", hash, header.String())
		sm.peerNotifier.RelayInventory(wire.NewInvVect(wire.InvTypeBlock, hash), wireBlockHeader)

		return nil
	}, &sm.settings.Kafka)
}

// kafkaTXmetaListener processes TxMeta Kafka messages in binary batch format.
// Messages use a binary batch format:
// [4 bytes]  - entry count (uint32, little-endian)
// For each entry:
//
//	[32 bytes] - tx hash (raw bytes)
//	[1 byte]   - action (0=ADD, 1=DELETE)
//	[4 bytes]  - content length (uint32, little-endian) - 0 for DELETE
//	[N bytes]  - content (metaBytes) - only for ADD
func (sm *SyncManager) kafkaTXmetaListener(ctx context.Context, kafkaURL *url.URL, groupID string) {
	kafka.StartKafkaListener(ctx, sm.logger, kafkaURL, groupID, true, func(msg *kafka.KafkaMessage) error {
		return sm.processTXmetaBatchMessage(msg.Value)
	}, &sm.settings.Kafka)
}

// processTXmetaBatchMessage processes a binary batch message from the txmeta Kafka topic.
// It parses the batch format, deserializes metadata for ADD entries, and announces
// non-coinbase transactions to peers via the txAnnounceBatcher.
// Coinbase transactions are intentionally skipped to avoid peer bans.
//
// Two wire formats are accepted, distinguished by a multi-byte signature at
// the start of the message (mirrors services/subtreevalidation/txmetaHandler.go):
//
//	v1 (legacy)
//	  [4 bytes] entry count (uint32 LE)
//	  per entry: [32 hash][1 action][4 contentLen][N content]
//
//	v2 (partition-aware)
//	  [1 byte magic=0xFF][1 byte version=0x02][2 reserved=0][4 entry count LE]
//	  per entry: [8 xxhash][32 hash][1 action][4 contentLen][N content]
//
// v2 detection requires the full 4-byte header signature AND a plausible
// entry count for the buffer length, otherwise the message is parsed as v1.
// This avoids misclassifying v1 messages whose entry count happens to begin
// with 0xFF (counts 255, 511, 767, ...).
//
// The xxhash prefix in v2 is read and discarded — netsync only needs the
// 32-byte tx hash to announce; partition-aligned cache writes are a
// subtreevalidation concern.
func (sm *SyncManager) processTXmetaBatchMessage(data []byte) error {
	if len(data) < 4 {
		return nil
	}

	var (
		offset     int
		entryCount uint32
		isV2       bool
	)

	// Speculative v2 detection: require the full header signature
	// (magic + version + reserved bytes) and an entry count that fits in the
	// remaining buffer at the minimum v2 entry size. Any failure falls
	// through to v1 — never silently drops a valid v1 message.
	if len(data) >= txmetacache.WireV2HeaderLen &&
		data[0] == txmetacache.WireV2Magic &&
		data[1] == txmetacache.WireV2Version &&
		data[2] == 0 && data[3] == 0 {
		candidateCount := binary.LittleEndian.Uint32(data[4:])
		remaining := uint64(len(data) - txmetacache.WireV2HeaderLen)
		if uint64(candidateCount)*uint64(txmetacache.WireV2MinEntrySize) <= remaining {
			entryCount = candidateCount
			offset = txmetacache.WireV2HeaderLen
			isV2 = true
		}
	}

	if !isV2 {
		entryCount = binary.LittleEndian.Uint32(data[:4])
		offset = 4
	}

	// Per-entry header size (excluding content). The shared constants in
	// stores/txmetacache encode the same numbers; using them here keeps
	// the producer and the receiver pinned to one source of truth.
	entryHeaderSize := txmetacache.WireV1MinEntrySize
	if isV2 {
		entryHeaderSize = txmetacache.WireV2MinEntrySize
	}

	// Process each entry
	for i := uint32(0); i < entryCount; i++ {
		if offset+entryHeaderSize > len(data) {
			sm.logger.Errorf("[kafkaTXmetaListener] truncated message at entry %d", i)
			return nil
		}

		// v2: skip the 8-byte xxhash prefix; netsync doesn't use it.
		if isV2 {
			offset += 8
		}

		// Read hash (32 bytes)
		var hash chainhash.Hash
		copy(hash[:], data[offset:offset+32])
		offset += 32

		// Read action (1 byte)
		action := data[offset]
		offset++

		// Read content length (4 bytes)
		contentLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if action == txmetacache.WireActionADD {
			// Handle ADD
			if offset+int(contentLen) > len(data) {
				sm.logger.Errorf("[kafkaTXmetaListener] truncated content at entry %d", i)
				return nil
			}

			content := data[offset : offset+int(contentLen)]
			offset += int(contentLen)

			sm.logger.Debugf("Received tx message from Kafka: %v", hash)

			var txMeta meta.Data
			if err := meta.NewMetaDataFromBytes(content, &txMeta); err != nil {
				sm.logger.Errorf("Failed to create tx meta data from bytes: %v", err)
				continue
			}

			if txMeta.IsCoinbase {
				continue
			}

			// Never announce transactions that arrived as part of a block or
			// announced subtree. The txmeta topic also carries those (block
			// validation, subtree validation, legacy sync pre-warm) to populate
			// the subtree-validation cache; relaying them as fresh mempool txs
			// floods peers with getdata for transactions that are long mined —
			// and often already pruned.
			if txMeta.InBlock {
				continue
			}

			sm.announceTx(&TxHashAndFee{
				TxHash: hash,
				Fee:    txMeta.Fee,
				Size:   txMeta.SizeInBytes,
			})
		} else {
			offset += int(contentLen)
			continue
		}
	}

	return nil
}

// samplePeerThroughput takes one throughput reading for every registered peer.
//
// It runs on the frontier ticker rather than a timer of its own, immediately
// before the race that reads it: the reading is three atomic operations per peer,
// and the interval the rate is measured over has to be the interval it is sampled
// on. The race asks the result whether the peer that owes the stuck block is
// actually sending it, which is the question svnode asks of every in-flight
// source before racing one.
func (sm *SyncManager) samplePeerThroughput() {
	if sm.peerStates == nil {
		return
	}

	for p, state := range sm.peerStates.Range() {
		state.sampleThroughput(p)
	}
}
