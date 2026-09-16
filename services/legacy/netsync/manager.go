// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package netsync provides network synchronization functionality for the legacy Bitcoin protocol.
// It handles peer coordination, block synchronization, and transaction relay operations.
package netsync

import (
	"bytes"
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

	// pipelineBlockSlotPeerAllowance sizes the download-admission semaphore, in
	// block-count units, when the pipeline path charges slots instead of bytes.
	//
	// This reasoning was written for a position the admission charge no longer
	// occupies. It assumed one peer could hold up to MaxBlocksInTransitPerPeer
	// slots at once — true at OnBlock, where a read loop admits a block and
	// returns immediately, so the next block can be admitted before the first is
	// even processed. Fix round 1 moved the charge into admitPipelineSink
	// (streaming_install.go), which wraps the sink itself: a read loop is
	// synchronously inside exactly one call to it at a time, so one peer holds
	// AT MOST ONE slot, never sixteen. The real ceiling on simultaneous holders
	// is the download fan-out — how many peers can have a block in flight at
	// once — which this codebase caps around defaultMaxInFlightBlocks (20), well
	// under MaxBlocksInTransitPerPeer(16) × 4 = 64. So at shipped defaults this
	// gate is reachable — AcquireBlockPrefetch is genuinely called, and would
	// refuse if the count ever exceeded capacity — but not BINDING: real fan-out
	// cannot reach 64 concurrent holders, so it never actually refuses anything.
	// The gate is bindable, not bounded, in its current position. Recorded here
	// rather than silently left wrong; not resized, because shrinking it without
	// evidence of what fan-out this branch actually produces under load is a
	// separate, measured decision, not a comment-fix.
	pipelineBlockSlotPeerAllowance = 4

	// maxBlockQueueSlots caps the block-queue channel capacity so a misconfigured
	// (e.g. multi-TB) prefetch budget cannot size an enormous channel backing
	// array. 65536 slots covers budgets up to 4 GiB at the weight floor before the
	// clamp binds; the channel holds pointers, so this is ~512 KiB.
	maxBlockQueueSlots = 65536

	// defaultBlockProcessingStallTimeout bounds how long localReadBackpressured
	// keeps suppressing the sync-peer stall check while the block backlog is
	// non-empty but not committing (see lastChainProgress). It is only the
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

	// syncManagerShuttingDownMsg is the service error returned to every queued or
	// in-flight block that is drained when the sync manager stops.
	syncManagerShuttingDownMsg = "sync manager shutting down"
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

	// handedOff is closed once the block's memory is charged to the budget that
	// owns it next, so the peer can give its download bytes back without waiting
	// for validation. Carried through to the queue message unchanged.
	handedOff chan struct{}
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

// blockRequestOrigin records what is known about a block's ancestry at the point
// it was asked for. It is the proof that backs every below-checkpoint fast path:
// the hardcoded checkpoints certify ONE CHAIN, not a height range, so "this block
// sits below the highest checkpoint" says nothing about whether it belongs to that
// chain.
//
// The zero value is deliberately untrusted, so a call site that has not thought
// about provenance fails closed.
//
// It arrives from upstream's PR 1390 security merge, where it was threaded through
// a map from block hash to origin filled by fetchHeaderBlocks as it walked the
// header list. That list is gone on this branch, so the flag is now answered from
// the header cache instead — see blockOrigin and headerCache.provenTo. The meaning
// is identical and the source is narrower: one run, replaced whole, never extended
// past the point that was verified.
type blockRequestOrigin struct {
	// headerProven is true when the block's hash is named by a header-cache run in
	// which a pinned checkpoint hash was matched at or above this block's height.
	// Blocks a peer merely advertised (handleInvMsg), blocks adopted from disk at
	// startup, and blocks re-requested by any route that cannot show that run carry
	// no such proof and are never header-proven.
	headerProven bool
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

	// peerChainClaimState holds what this peer has DEMONSTRATED it has, as
	// opposed to bestKnownHeight above, which starts from what the peer said
	// about itself. See peer_chain_claim.go.
	peerChainClaimState

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

// maxInFlightLadderTop is what calculateMaxInFlightBlocks returns for small
// blocks, and therefore the denominator when its answer is used as a ratio
// rather than as a count. Kept beside the ladder so the two cannot drift.
const maxInFlightLadderTop = 20

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
		return maxInFlightLadderTop // small blocks: default aggressive
	}
}

// blockFailureState tracks per-block transient-failure backoff. attempts is the
// consecutive failure count for a block hash; nextRetry is the earliest time the
// block may be re-processed. See SyncManager.blockFailureBackoff (#1187).
type blockFailureState struct {
	attempts  int
	nextRetry time.Time
}

// corruptAttemptState is the per-(hash, peerID) corrupt re-download counter and its fixed cooldown
// window (bitcoin-sv/teranode#4692). windowExpiry is set once from the first corrupt delivery and
// preserved across subsequent deliveries so the window is not extended by re-delivery; once it
// lapses the counter resets and an honest body is admitted again.
type corruptAttemptState struct {
	count        int
	windowExpiry time.Time
}

// legacyCorruptAttemptKey keys the legacy corrupt re-download cap on (block hash, serving peer
// address) (bitcoin-sv/teranode#4692). Keying on the pair — not the hash alone — stops one peer's
// corruption consuming the budget for a hash an honest peer can still serve: each serving identity
// is capped independently, so an honest sync-peer rotation is never wedged. A peer with no address
// degrades to a single shared (hash, "") bucket — the hard per-hash bound for that deployment.
type legacyCorruptAttemptKey struct {
	hash   chainhash.Hash
	peerID string
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
	// blockCorruptAttempts bounds corrupt-body (bitcoin-sv/teranode#4692) re-downloads per
	// (block hash, serving peer address), independently of the ban score. Once a (hash, peerID)
	// reaches MaxCorruptAttemptsPerBlock corrupt deliveries within a fixed cooldown window, that
	// peer's next delivery of the hash is dropped BEFORE the expensive HandleBlockDirect/decorate —
	// without rejecting-to-peer, without poisoning (invalid is never set), and without setting
	// recentlyFailedBlocks (so the recentlyFailedBlocks no-NOT_FOUND-cascade property is preserved).
	// Keying on (hash, peerID) means an honest sync-peer keeps a fresh budget for the same hash, so
	// a bad peer can never wedge the honest tip; the residual aggregate per-hash work is bounded by
	// the number of distinct serving peers (MaxPeers) times the cap per window. Unlike
	// blockFailureBackoff (serviceError-gated, disjoint from corrupt) this is keyed only on corrupt
	// failures. The window is fixed from the first corrupt delivery (stored in the value, not the map
	// TTL), so re-delivery cannot extend it and once it lapses an honest body is admitted again
	// (self-healing). Cleared on successful store.
	blockCorruptAttempts *expiringmap.ExpiringMap[legacyCorruptAttemptKey, *corruptAttemptState]
	// blockPark holds blocks that arrived before their parent. Without it such a
	// block is fully downloaded, fully decoded and then discarded, and the
	// getblocks sent in its place is ignored for as long as headers-first mode is
	// on — so the download is simply wasted and nothing ever asks for the block
	// again. nil means the park is off and the old discard path runs; every entry
	// point is nil-safe, because tests build SyncManager as a struct literal that
	// never goes through New().
	blockPark *blockPark

	// parkJobs carries an admitted block to a parking worker, and parkOutcomes
	// carries the answer back to the block-queue consumer. Both exist so the
	// blob write does not run on the goroutine that commits blocks in order;
	// see block_park_worker.go.
	parkJobs     chan parkJob
	parkOutcomes chan parkOutcome
	parkWorkers  sync.WaitGroup

	// parkCommits carries a parked block the sweep has found a stored parent for
	// back to the block-queue consumer, which is the one goroutine that commits.
	// The sweep runs on its own goroutine and may not commit from there: it
	// would race the dispatcher for admission into the window. nil on a manager
	// built as a struct literal, and submitParkCommit then commits inline, which
	// is what the sweep did before it had a goroutine of its own.
	parkCommits chan parkCommit

	// drainQueue is the parents whose parked children may now be committable, and
	// lastDispatchWasDrained alternates the two admission sources so neither
	// starves. Both are owned by the block-queue consumer alone, with no lock, on
	// the same terms as the dispatcher's frontier: every producer of a drain
	// request already runs on that goroutine.
	drainQueue             []drainRequest
	lastDispatchWasDrained bool

	// drainAsync says the consumer loop is running and will admit drained blocks
	// itself. It is false for a manager with no such loop, which is the
	// pre-window consumer and every manager a test builds as a struct literal,
	// and scheduleDrain then walks the stack synchronously as it always did.
	drainAsync atomic.Bool

	// consumerDone closes when the block-queue consumer has returned, which is
	// after its quit arm has settled every dispatch in flight. Stop waits on it,
	// because a parked entry the shutdown does not restore is adopted by the next
	// start's recovery with no height, no peer and no header node, and that is
	// the one entry that can never be rewound back into the download walk. Built
	// by blockHandler beside the consumer; nil on a manager whose handler never
	// ran, which every struct-literal test manager is.
	consumerDone chan struct{}

	// parkJobHeld is the one park job the head has admitted and not yet handed to
	// a worker, and parkJobAsync says there is a loop to hold it. Both are owned
	// by the block-queue consumer alone, no lock, on the same terms as drainQueue:
	// the head runs on that goroutine, so it can set the field directly.
	//
	// A field rather than a channel, and for a reason worth keeping. A channel
	// whose only consumer is the goroutine that sends on it deadlocks on the
	// first send. That is not hypothetical: it is what the first version of this
	// did.
	//
	// While it is set the consumer disables its queue arm, so nothing else is
	// head-processed, which is the same rule the pending dispatch slot follows and
	// is what keeps the park's backpressure intact. When there is no loop,
	// submitParkJob waits where it stands, which is the pre-window path.
	parkJobHeld  *parkJob
	parkJobAsync atomic.Bool

	// consumerWatchdogState is the diagnostic that explains a block loop which
	// has stopped admitting. Embedded so the whole thing can be read, and taken
	// out again, as one piece; see consumer_watchdog.go.
	consumerWatchdogState

	// parkSweepNow is the clock the park sweep measures its own tick against, so
	// a test can make one store delete look slow without sleeping. nil means
	// time.Now; nothing in production sets it.
	parkSweepNow func() time.Time

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

	// lastChainProgress is the UnixNano time a block last joined the chain.
	// localReadBackpressured suppresses the sync-peer stall check only while
	// this stays fresh — a backlog that stops producing commits for longer than
	// blockProcessingStallTimeout is a genuine processing hang (store or
	// validator deadlock, Aerospike overload), not slow-but-progressing
	// validation, and must be allowed to rotate the peer. This restores the
	// liveness coverage lost when the per-message watchdog was disarmed for
	// prefetched blocks, without the false rotation of a merely-slow block.
	// Written by the block-queue consumer and by the drain, via
	// noteChainProgress, and read by handleCheckSyncPeer.
	lastChainProgress atomic.Int64

	// headerCache names the block hashes for the heights just above the
	// committed tip (see committedTip), from the most recent getheaders reply.
	// It is a lookup, not a work queue: nothing walks it, nothing holds a
	// position in it, and discarding it costs one message.
	//
	headerCache *headerCache

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
	inFlightBlocks   map[chainhash.Hash]*inFlightBlock
	inFlightBlocksMu sync.Mutex

	// blockPrefetchWaiters counts read-loops currently blocked acquiring
	// prefetch budget (i.e. local processing cannot keep up). While > 0 the node
	// is backpressuring its own network reads, so the stall detector must not
	// hold the resulting zero throughput against the sync peer — the prefetch
	// analogue of the blockBacklog guard. Read by handleCheckSyncPeer.
	blockPrefetchWaiters atomic.Int64

	// blockPrefetchReserved shadows how many bytes of blockPrefetchBudget are
	// currently reserved, purely so the figure can be reported.
	//
	// It exists because golang.org/x/sync/semaphore does not expose its own
	// occupancy, and that is why every consumer-stall report to date has been
	// blind to the one budget that can silence every peer at once: a read-loop
	// blocked in AcquireBlockPrefetch reads nothing further from its socket, so
	// the peer goes quiet whatever it owes. The watchdog printed the window's
	// byte budget instead, which is empty during exactly that fault.
	//
	// Written only alongside a successful acquire or the single release that
	// hands bytes back, so it cannot drift from the semaphore it shadows. It
	// changes no decision — nothing reads it but the report.
	blockPrefetchReserved atomic.Int64

	// The following fields are used for headers-first mode.
	//
	// headerMu no longer owns a header list or a stored checkpoint — both are
	// gone, replaced by headerCache (its own lock) and findNextHeaderCheckpoint
	// (a pure function of the committed height, recomputed wherever it is
	// needed rather than cached). What is left still takes it for
	// wantedBlocks' sake, so the rules below stay in force for whatever runs
	// under it:
	//
	// Rule A, lock ordering: headerMu -> peerStates. headerMu is the outer of
	// the two; nothing may take it while already holding the peerStates map
	// lock.
	//
	// Rule B, what may not run under it: no send to a peer (QueueMessage,
	// PushGetHeadersMsg, PushGetBlocksMsg, DisconnectWithWarning — a peer's
	// output queue is buffered but finite, so a send can block) and no
	// blockchain client call that can block for an unbounded time
	// (GetBestBlockHeader can take minutes during initial sync). There are no
	// exceptions.
	headerMu         sync.Mutex
	headersFirstMode atomic.Bool // accessed from multiple goroutines, must be atomic
	// currentCached is the last answer current() worked out, so a peer goroutine
	// can read it without making the blockchain call itself. See IsCurrentCached.
	currentCached atomic.Bool
	// lastHeaderRequestAt is when assignWantedBlocks last sent its own getheaders
	// to refill the header cache, in UnixNano, read and written only through
	// maybeRequestMoreHeaders' compare-and-swap. Every commit can call that pass,
	// so without this a cache that has run dry would earn one getheaders per
	// commit instead of one per round trip.
	lastHeaderRequestAt atomic.Int64
	// headerRefillPeerIdx rotates which eligible peer maybeRequestMoreHeaders
	// asks next. PushGetHeadersMsg silently drops a repeat of the same
	// (locator, stop hash) pair from a given peer, and while the cache is
	// empty neither half of that pair can move: the stop hash is always the
	// zero hash and the locator is built from the committed tip, which does
	// not advance until a block actually commits. Asking peers[0] every time
	// therefore sends once and is filtered forever after on a single-peer
	// node. Rotating picks a different peer each call so the filter never
	// sees a repeat; it is read and incremented only through
	// nextHeaderRefillPeer's atomic Add, so concurrent callers never hand out
	// the same slot twice.
	headerRefillPeerIdx atomic.Uint64
	blockSizeTracker    *blockSizeTracker // tracks block sizes for dynamic in-flight adjustment

	// dispatcher owns the quick window: it decides how many queued blocks may have
	// their UTXO store work in flight at once and runs every chain-order step in
	// dispatch order. Built in New(); nil when SyncManager was built as a struct
	// literal in a test, which every dispatcher accessor tolerates.
	dispatcher *blockDispatcher

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

// leaveHeadersFirstMode switches out of headers-first mode.
//
// It used to also wipe the header list and bump its epoch, which do not exist
// any more: fillHeaderCache never pushes onto a list, so there is nothing left
// for a stray node to belong to. headersFirstMode is its own atomic, so
// nothing here needs headerMu either.
func (sm *SyncManager) leaveHeadersFirstMode() {
	sm.headersFirstMode.Store(false)
}

// maybeLeaveHeadersFirstMode turns headers-first mode off once there is no
// longer a checkpoint ahead of the committed height, and is a no-op otherwise.
//
// It replaces checkpointBlockCommitted's anchor bookkeeping and its
// deferred-retry for "no peer to ask": both existed only to keep a STORED
// nextCheckpoint field from drifting, and to remember a transition that had
// nowhere to send its getheaders. With the checkpoint recomputed fresh from
// the chain's own tip on every call, there is nothing stored to drift and
// nothing to defer — the single fact that matters, whether a checkpoint is
// still ahead, is simply asked again the next time any block commits, whether
// or not a peer happens to be available to hand a getheaders to right now. A
// later commit, or the next sync-peer election in startSync, asks the same
// question and gets the same, current answer.
func (sm *SyncManager) maybeLeaveHeadersFirstMode(reason string) {
	if !sm.headersFirstMode.Load() {
		return
	}

	height, _, _ := sm.committedTip()

	if sm.findNextHeaderCheckpoint(height) != nil {
		return
	}

	sm.leaveHeadersFirstMode()

	sm.logger.Infof("[headersFirstMode][%s] committed height %d has passed the final checkpoint, leaving headers-first mode", reason, height)
}

// isCheckpointHash reports whether hash is one of the chain's configured
// checkpoints, while headers-first mode is on.
//
// It used to be answered by advanceHeaderListFor, which took a node with this
// hash out of the header list — unless the hash matched the round's
// nextCheckpoint, in which case the node was kept in place to anchor the next
// round. That gave the right answer only because nextCheckpoint was a stored
// field, valid for as long as nothing else had advanced it. Comparing hash
// against the fixed list of configured checkpoints instead needs no such
// timing assumption: a block's hash either is one of the checkpoints or it
// never was, whether this runs before this block's own commit (the live
// path) or after it (the park drain, which commits before this is ever
// asked). Gated on headersFirstMode for the same reason advanceHeaderListFor
// was: outside a headers-first round a checkpoint match answers nothing this
// package still acts on.
func (sm *SyncManager) isCheckpointHash(hash chainhash.Hash) bool {
	if !sm.headersFirstMode.Load() || sm.chainParams == nil {
		return false
	}

	for i := range sm.chainParams.Checkpoints {
		if cp := sm.chainParams.Checkpoints[i].Hash; cp != nil && cp.IsEqual(&hash) {
			return true
		}
	}

	return false
}

// headerRoundSummary describes where the download stands, for the stall
// watchdog. It names the four things the pass actually reads, so a stalled node
// says which of them is empty.
//
// It replaces a summary of the header list — how long it was, what sat at each
// end of it, and which checkpoint the round was aiming at — which existed
// because Hetzner mainnet sat at height 800128 for seven hours on 2026-09-11
// and every line the node wrote described the window, the park and the
// download budget. None of them described the header list, and metrics.go had
// no gauge for it either. That structure and its front-of-list anchor are gone
// now, replaced by the cache and the chain's own tip this reads instead.
func (sm *SyncManager) headerRoundSummary() string {
	best, _, _ := sm.committedTip()

	top, haveTop := sm.headerCache.Top()
	if !haveTop {
		return fmt.Sprintf("best block processed %d, the header cache is empty so the next pass can name nothing and is waiting on a getheaders", best)
	}

	return fmt.Sprintf("best block processed %d, the header cache names %d heights up to %d, %d blocks are owed by peers",
		best, sm.headerCache.Len(), top, sm.blockDownloads.Len())
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

		if err = sm.runIfCatchingBlocks("legacy/netsync/manager/startSync"); err != nil {
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
	// Computed fresh from the height read on this path, rather than trusting a
	// stored value somebody else last wrote. That used to matter: a stored
	// nextCheckpoint could go stale between one goroutine reading a height and
	// another committing past it, and the gate below could never be satisfied
	// by a checkpoint already in the chain, so headers-first mode would stay
	// off for good with nothing to re-aim it. Recomputing it here every time
	// removes the staleness along with the field it used to live in — there is
	// nothing left to repair.
	nextCP := sm.findNextHeaderCheckpoint(bestBlockHeightInt32)

	if nextCP != nil &&
		bestBlockHeightInt32 < nextCP.Height &&
		sm.chainParams != &chaincfg.RegressionNetParams {
		if err = bestPeer.PushGetHeadersMsg(locator, &zeroHash); err != nil {
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

		// Owned here rather than left to a caller to reset first: this is the
		// one place that decides whether the round ahead needs headers-first
		// verification, and a caller re-electing a peer while the flag is still
		// true from a previous election has nothing else that will ever clear
		// it for this branch.
		sm.headersFirstMode.Store(false)
	}

	bestPeer.SetSyncPeer(true)
	sm.storeSyncPeer(bestPeer, &syncPeerState{
		lastBlockTime:     time.Now(),
		recvBytes:         bestPeer.BytesReceived(),
		recvBytesLastTick: uint64(0),
	})
}

// runIfCatchingBlocks uses the cached observation only to avoid redundant
// automatic RUN requests from recurring legacy events. It is not admission:
// the server still checks authoritative state under its transition lock. A
// temporary synthetic IDLE is rechecked on later events, without latching a
// pause or changing message/queue ownership.
func (sm *SyncManager) runIfCatchingBlocks(source string) error {
	state, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
	if err != nil {
		return err
	}
	if state == nil || *state != teranodeblockchain.FSMStateCATCHINGBLOCKS {
		return nil
	}
	return sm.blockchainClient.Run(sm.ctx, source)
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

	// The height the peer advertised about itself during the handshake. It is
	// kept, because a peer that says nothing must not be mistaken for one with
	// nothing when choosing a sync peer, and it is the only figure available at
	// this point.
	//
	// It is deliberately NOT a claim. A claim is what the peer has demonstrated,
	// and nothing here has been demonstrated: this number is the peer's own word,
	// the record only ever rises, so a self-report written here could never be
	// contradicted and every peer would claim every block forever. That is
	// exactly what made canServe a no-op and left the scheduler asking strangers
	// for blocks they never had. SV Node keeps the same number in
	// nStartingHeight and never writes it into pindexBestKnownBlock either.
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
	// Both arms are suppressed, not just the last-block-time one. validNetworkSpeed
	// reads BytesReceived, which is this peer object's own counter, and under the
	// BlockPriority stream policy a large block arrives on DATA1 while that
	// counter barely moves. So a peer downloading a multi-GB block at full rate
	// records a speed violation every tick, and gating only the last-block-time
	// arm left the speed arm free to rotate it anyway. Measured on mainnet at
	// 124 MB average blocks: the sync peer was demoted three times in seven
	// minutes, mid-transfer, each demotion reopening its assignments and rewinding
	// the cursor, so the work was done twice.
	//
	// This is what svnode does and for the same reason: DetectStalling asks
	// whether the peer is actually downloading before it acts, and resets the
	// stall clock rather than disconnecting a peer making real progress.
	//
	// The wall-clock cap is kept and now covers both arms, so a peer cannot
	// dribble bytes just above the floor to hold the sync-peer slot indefinitely.
	if (isLastBlockTimeViolation || isNetworkSpeedViolation) &&
		lastBlockSince < peerpkg.MaxBlockDownloadTime &&
		sps.hasHealthyDownloadThroughput(sm.minSyncPeerNetworkSpeed) {
		sm.logger.Debugf("[CheckSyncPeer] sync peer %s violated %s but its association is still downloading at a healthy rate (%.0fs in, cap %s); not rotating", sp.String(), violationNames(isNetworkSpeedViolation, isLastBlockTimeViolation), lastBlockSince.Seconds(), peerpkg.MaxBlockDownloadTime)

		isLastBlockTimeViolation = false
		isNetworkSpeedViolation = false
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
//   - it does not reset the header state, so headers-first mode and the header
//     list both survive.
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

	// Makes the blocks this peer still owes askable of somebody else straight
	// away, rather than waiting out the retry interval. The demoted peer keeps
	// ownership of those blocks — ForgetForRetryPeer back-dates the record, it
	// does not delete it — so a late copy is still admitted rather than treated
	// as unrequested. The wanted-range pass recomputes what to ask for on every
	// call, so nothing else needs telling: the next pass simply finds these
	// blocks unowed again.
	reopened := sm.blockDownloads.ForgetForRetryPeer(sp, blockRequestRetryInterval)
	if len(reopened) > 0 {
		sm.logger.Infof("[demoteSyncPeer] reopened %d blocks owed by %s for the next pass to re-ask", len(reopened), sp.String())
	}

	sm.startSync()
}

// headersRoundLocator returns the locator to send the next getheaders with.
//
// The locator is always the chain's own, anchored on what this node has
// actually committed. That is what makes every reply connect: a peer answers
// from the first hash it recognises, and the chain's locator steps back from
// the committed tip to genesis, so any peer sharing any ancestor with us
// recognises something.
//
// What this replaces anchored at the BACK of the header list, which during a
// sync sits up to 933,000 blocks above the committed tip. A peer that had not
// reached that point recognised nothing, fell back to genesis, and replied
// from height 1: headers whose parent we had never heard of, which cost it
// its connection for answering honestly. That disconnect took Hetzner
// mainnet's last working supplier on 2026-09-11.
func (sm *SyncManager) headersRoundLocator(bestHash *chainhash.Hash, bestHeight uint32) (blockchain.BlockLocator, error) {
	return sm.blockchainClient.GetBlockLocator(sm.ctx, bestHash, bestHeight)
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

	// Release every block this peer owed us. Nothing needs telling: the
	// wanted-range pass recomputes what it wants and who owes it on every call, so
	// a released block is simply unowed on the next pass rather than needing to
	// be re-anchored onto anything.
	sm.blockDownloads.ClearPeer(peer)
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
		sm.headerMu.Lock()
		// Log current sync state before disconnecting
		if sm.headersFirstMode.Load() {
			sm.logger.Debugf("Current header sync state - header cache names %d heights", sm.headerCache.Len())
		}

		sm.headerMu.Unlock()

		sp.SetSyncPeer(false)
		sp.DisconnectWithInfo("updateSyncPeer - disconnect old sync peer")
	}

	// Reset sync peer state
	sm.storeSyncPeer(nil, nil)

	// startSync re-derives the checkpoint fresh from the chain's own best
	// height and owns headersFirstMode in both directions (see its own
	// getheaders/getblocks branches), so there is nothing to reset here first
	// any more: no list to rebuild, no stored checkpoint that could have gone
	// stale since the last time this ran.
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

// blockGivenUpOn reports whether a block has failed so many times in a row that
// asking for it again is pointless.
//
// Without this the retry is unbounded. recordBlockFailureBackoff grows the
// attempt count without limit and caps only the WAIT, so a block that can never
// be accepted is downloaded again every BlockFailureBackoffMaxDuration for the
// life of the process, which at the 150 second default is roughly six hundred
// pointless downloads a day, each one a full block off the wire.
//
// Today that loop terminates only by accident: the block-validation service
// writes a durable invalid row and the next attempt short-circuits on a lookup
// that reads the block back as present. The newer validation route deliberately
// writes no such row, so turning it on makes the loop genuinely unbounded. This
// is the bound that does not depend on that accident.
//
// Nothing durable is written here, and that is deliberate rather than a
// shortcut. A durable mark keyed on the block hash is a poisoning surface: a
// block's hash commits only to its 80 byte header, so a peer can replay an
// honest header with a doctored body at no cost, and a durable mark on that hash
// would condemn the real block permanently. Losing the count on a restart is
// correct, because a restart is the most likely thing to have cleared the local
// fault that caused the rejection.
func (sm *SyncManager) blockGivenUpOn(hash chainhash.Hash) bool {
	if sm.blockFailureBackoff == nil || sm.settings == nil {
		return false
	}

	ceiling := sm.settings.Legacy.BlockFailureAttemptCeiling
	if ceiling <= 0 {
		return false
	}

	fs, ok := sm.blockFailureBackoff.Get(hash)
	if !ok {
		return false
	}

	return fs.attempts >= ceiling
}

// legacyCorruptAttemptCooldown returns the fixed cooldown window for the per-block corrupt
// re-download cap (bitcoin-sv/teranode#4692), falling back to settings.DefaultCorruptAttemptCooldown
// when settings are nil or the setting is unset or non-positive. Mirrors corruptAttemptCooldown in
// services/blockvalidation; both share the one fallback constant so the two caches can never drift
// apart.
func legacyCorruptAttemptCooldown(s *settings.Settings) time.Duration {
	if s != nil {
		if d := s.BlockValidation.CorruptAttemptCooldown; d > 0 {
			return d
		}
	}

	return settings.DefaultCorruptAttemptCooldown
}

// recordCorruptBlockAttempt increments and returns the per-(hash, peerID) corrupt-body failure count
// within a fixed cooldown window (bitcoin-sv/teranode#4692). The window is set once from the first
// corrupt delivery and preserved across subsequent deliveries (not extended), so once it lapses the
// counter resets and an honest body is admitted again. Called ONLY on an actual corrupt failure.
// Nil-safe (SyncManager struct-literal test fixtures that bypass New()): a nil map or nil settings
// is a no-op returning 0, so the cap simply does not accrue rather than panicking.
func (sm *SyncManager) recordCorruptBlockAttempt(blockHash chainhash.Hash, peerID string) int {
	if sm.blockCorruptAttempts == nil || sm.settings == nil {
		return 0
	}

	key := legacyCorruptAttemptKey{hash: blockHash, peerID: peerID}

	now := time.Now()
	if st, ok := sm.blockCorruptAttempts.Get(key); ok && now.Before(st.windowExpiry) {
		// Preserve the LOGICAL window (windowExpiry) so re-delivery cannot extend the cooldown;
		// the Set re-extends only the map's retention TTL. A new struct avoids mutating shared state.
		next := &corruptAttemptState{count: st.count + 1, windowExpiry: st.windowExpiry}
		sm.blockCorruptAttempts.Set(key, next)

		return next.count
	}

	sm.blockCorruptAttempts.Set(key, &corruptAttemptState{count: 1, windowExpiry: now.Add(legacyCorruptAttemptCooldown(sm.settings))})

	return 1
}

// corruptBlockAttemptsExhausted reports whether a (hash, peerID) has reached the corrupt
// re-download cap and is within its cooldown window (bitcoin-sv/teranode#4692). A cap of <= 0
// disables the bound (re-opens the corrupt-body bandwidth DoS). Nil-safe: a nil settings or nil map
// (SyncManager struct-literal test fixtures that bypass New()) behaves as CAP DISABLED — it returns
// false (never "exhausted"), so a missing config can never silently drop honest blocks.
func (sm *SyncManager) corruptBlockAttemptsExhausted(blockHash chainhash.Hash, peerID string) bool {
	if sm.settings == nil || sm.blockCorruptAttempts == nil {
		return false
	}

	maxAttempts := sm.settings.BlockValidation.MaxCorruptAttemptsPerBlock
	if maxAttempts <= 0 {
		return false
	}

	st, ok := sm.blockCorruptAttempts.Get(legacyCorruptAttemptKey{hash: blockHash, peerID: peerID})

	return ok && st.count >= maxAttempts && time.Now().Before(st.windowExpiry)
}

// clearCorruptBlockAttempts drops a (hash, peerID)'s corrupt counter on successful store so an
// honest body after the window never inherits a stale count (bitcoin-sv/teranode#4692). Nil-safe.
func (sm *SyncManager) clearCorruptBlockAttempts(blockHash chainhash.Hash, peerID string) {
	if sm.blockCorruptAttempts != nil {
		sm.blockCorruptAttempts.Delete(legacyCorruptAttemptKey{hash: blockHash, peerID: peerID})
	}
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

// parentFailedWhileWaiting reports whether the parent of a block that was waiting for
// window capacity has failed in the meantime, and if so gives the block the same #1333
// treatment the head gives a descendant of an already-failed block: mark it failed so
// its own descendants are suppressed transitively, refresh the delivering peer's stall
// timer (the fault is a rejected ancestor, not the peer), and answer with a getblocks so
// sync recovers once the root block is resolved. The caller replies nil, exactly as the
// head's branch does, so the window-off path behaves as it did before the window
// existed. A parent that is in flight right now is not a failed parent: it was
// re-admitted, and its child must not be dropped as part of a cascade being retried.
func (sm *SyncManager) parentFailedWhileWaiting(d *blockDispatch) bool {
	if sm.recentlyFailedBlocks == nil || sm.dispatcher.inFlight(d.prevHash) {
		return false
	}

	if _, failed := sm.recentlyFailedBlocks.Get(d.prevHash); !failed {
		return false
	}

	sm.recentlyFailedBlocks.Set(d.msg.blockHash, struct{}{})
	sm.logger.Debugf("[dispatchBlocks][%s] parent %s failed while this block waited for capacity; skipping descendant (root failure already logged)", d.msg.blockHash, d.prevHash)

	if sps, ok := sm.syncPeerStateFor(d.peer); ok {
		sps.updateLastBlockTime()
	}

	// No backoff of its own, for the same reason the tail records none for an
	// aborted successor: this block never ran a failing attempt, it merely waited
	// behind a parent that failed. The wanted-range pass recomputes what it wants
	// on every call, so once the parent's backoff clears, this block is simply
	// still in range and unowed — nothing needs to be put back for it.
	sm.requestMissingBlocks(d.peer, d.msg.blockHash)

	return true
}

// finishBlockMsg settles the backlog accounting and answers the caller for one
// queue message that has left the pipeline. The dispatcher's tail ends with it for
// every block that reached a worker (see newBlockDispatcher), the pre-window
// consumer ends every turn with it, and both shutdown drains use it.
//
// Progress is a block joining the chain, not a queue message finishing. A parked
// block finishes its message and moves nothing, so stamping unconditionally would
// keep localReadBackpressured suppressing the stall check while the tip stood
// still. The stamp is shared across peers, so one peer's stream of out-of-order
// blocks used to hold that suppression open over a DIFFERENT peer's silence.
//
// A block handed to a parking worker has had its reply taken by the job, so
// msg.reply is nil here and the worker answers instead. That is what holds the
// prefetch budget until the worker has finished with the decoded block.
func (sm *SyncManager) finishBlockMsg(msg *blockQueueMsg, err error) {
	sm.blockBacklog.Add(-1)

	if msg.committed {
		sm.noteChainProgress()
	}

	if msg.reply != nil {
		msg.reply <- err
	}
}

// consumeBlocksSerially is the pre-window block-queue consumer, run when
// blockvalidation_quick_window_blocks is 0: one block at a time, its pre-checks, its own
// work and its chain-order tail all on this goroutine, so block N+1 is not even taken off
// the queue until block N has finished. The shutdown drain is the dispatcher's, verbatim:
// under prefetch each queued block has an awaitBlockResult goroutine holding budget and
// waiting on its reply, so replying here lets them exit promptly instead of waiting for the
// peer's own quit/ctx. The feeder races the same sm.quit close, so a block enqueued after
// this drain returns is not caught here — that block's awaitBlockResult still exits via
// sp.quit/sp.ctx.Done(), and the feeder's enqueue is itself sm.quit-guarded, so the drain
// only makes the common case prompt and is not relied on for correctness.
//
// The two park arms are the same ones the dispatcher has, because whichever
// consumer is running is the one goroutine that commits blocks in order, and a
// parking worker's outcome and a sweep-posted commit both have to land there.
func (sm *SyncManager) consumeBlocksSerially(blockQueue <-chan *blockQueueMsg) {
	for {
		select {
		case <-sm.quit:
			for {
				select {
				case msg := <-blockQueue:
					sm.finishBlockMsg(msg, errors.NewServiceError(syncManagerShuttingDownMsg))
				default:
					return
				}
			}
		case outcome := <-sm.parkOutcomes:
			sm.applyParkOutcome(outcome)
		case commit := <-sm.parkCommits:
			sm.commitParkedBlockAndDrain(commit.entry)
		case msg := <-blockQueue:
			sm.consumeQueuedBlock(msg)
		}
	}
}

// dispatchBlocks is the block-queue consumer: it runs every pre-check and every
// chain-order step for one block on this goroutine and hands only the block's own
// work to a worker, so up to K consecutive below-checkpoint blocks can have their
// UTXO store work in flight while their tails still run in dispatch order. A block
// whose parent is neither in the chain nor in flight never reaches a worker: the
// head parks it, with its bytes, and the drain commits it when the parent lands.
// It returns when sm.quit closes.
func (sm *SyncManager) dispatchBlocks(blockQueue <-chan *blockQueueMsg) {
	// blockvalidation_quick_window_blocks=0 is a true bypass, not a one-deep window: the
	// dispatcher is not used at all and the queue is consumed the way it was before the
	// window existed, so a rollback to the setting is a rollback to the old code path. It
	// matters because even at depth 1 the dispatcher splits a block's head (the FSM state
	// call, requestedBlocks, the header cache, size sampling, the cascade marks) from its tail,
	// so block N+1's head would run while N was still in flight.
	if sm.settings != nil {
		if depth, _ := sm.settings.BlockValidation.QuickWindowConfiguredDepth(); depth == 0 {
			sm.consumeBlocksSerially(blockQueue)
			return
		}
	}

	bd := sm.dispatcher

	// From here the drain is this loop's second admission source rather than a
	// call made inside a tail, so scheduleDrain queues instead of walking, and
	// the head leaves park jobs for this loop rather than waiting for a worker.
	sm.drainAsync.Store(true)
	sm.parkJobAsync.Store(true)

	defer func() {
		sm.drainAsync.Store(false)
		sm.parkJobAsync.Store(false)
	}()

	// pending holds one head-processed block waiting for capacity. While it is
	// set the queue arm is disabled, so nothing else is head-processed until this
	// block is dispatched: "receive only while a slot is free", with the head
	// work already done.
	var pending *blockDispatch

	finish := sm.finishBlockMsg

	// Start the watchdog's clock here rather than at the first admission, so a
	// loop that wedges before it ever places work is still described. Left at
	// zero the report suppresses itself, which is the one case worth hearing
	// about most.
	sm.noteConsumerAdmitted(time.Now())

	for {
		var queueArm <-chan *blockQueueMsg

		// One admission per turn, chosen rather than raced. The choice itself is
		// nextAdmission, so it can be read and tested on its own.
		canLive := pending != nil && bd.canDispatch(pending)
		// Anything queued makes the drain a candidate. Capacity is drainStep's
		// own business: it peeks the block, tests it with the same canDispatch
		// the live arm uses, and declines the turn without cost if there is no
		// room, which the branch below already handles.
		//
		// This used to require an empty window as well, and that requirement is
		// what left the validator idle between every block drained from the
		// park. It was a consequence of a parked dispatch never being windowed
		// rather than a rule of its own: an unwindowed block is admitted only
		// into an empty window, so demanding one here merely restated it. With a
		// drained block able to be windowed, restating it would keep the fault
		// while looking like it had been fixed.
		drainOpen := len(sm.drainQueue) > 0

		choice := nextAdmission(sm.lastDispatchWasDrained, canLive, drainOpen)

		// A chosen drain may still decline: drainStep walks the queued parents
		// itself and can find that none of them has a child it can commit yet,
		// dropping the ones it has ruled out. That must not cost the turn.
		//
		// It used to. The loop fell through to its wait, and the wait was
		// unreachable from anywhere: the queue arm is shut while a block is
		// pending, the window was empty so no completion was coming, the park
		// workers were idle and the sweep had nothing to offer. Mainnet stopped
		// for good at height 755,112 with the next block ready to go and nothing
		// left alive to notice. The ordering is not a corner case either, because
		// a block's own tail is what queues a drain for its parked children and
		// the tail runs on this goroutine, so the turn straight after any commit
		// has both sources ready and the alternation gives it to the drain.
		//
		// Deciding again with the drain taken out of the running is what hands
		// that turn to a live block that was ready. It cannot loop: the drain is
		// only ever removed from the choice, never put back, so the second
		// decision is admitLive or admitNothing and never admitDrained.
		if choice == admitDrained {
			if sm.drainStep(bd) {
				sm.lastDispatchWasDrained = true

				sm.noteConsumerAdmitted(time.Now())

				continue
			}

			sm.noteDrainDeclined()

			choice = nextAdmission(sm.lastDispatchWasDrained, canLive, false)
		}

		switch choice {
		case admitLive:
			d := pending
			pending = nil
			sm.lastDispatchWasDrained = false

			// The parent may have failed while this block waited for capacity, on any
			// route: the head's own #1333 check exempted it because the parent was in
			// flight at the time, and the window's admission-time abort only covers a
			// block whose parent it actually resolved.
			if sm.parentFailedWhileWaiting(d) {
				finish(d.msg, nil)
			} else {
				bd.dispatch(d)
			}

			sm.noteConsumerAdmitted(time.Now())

			continue

		case admitNothing:
		}

		// One park job at a time, handed to a worker without this goroutine ever
		// waiting on one. Tried before the queue arm opens, for the same reason
		// the pending dispatch is: it is work already accepted.
		if sm.parkJobHeld != nil {
			select {
			case sm.parkJobs <- *sm.parkJobHeld:
				sm.parkJobHeld = nil

				// Deliberately NOT an admission. Handing a block to a parking
				// worker moves it from memory to disk; it commits nothing and
				// advances the chain by nothing. See noteConsumerAdmitted.

				continue
			default:
			}
		}

		if pending == nil && sm.parkJobHeld == nil {
			queueArm = blockQueue
		}

		// The hand-off, as an arm of the wait below rather than only the offer
		// above. The offer cannot block, so it fails whenever no worker happens
		// to be sitting in its receive at that instant, and without an arm here
		// the loop then waited with the job still in its hand for something that
		// was never coming: the window may be empty so no completion is due, the
		// queue arm is shut while a job is held, and no worker will post an
		// outcome if none is running. Mainnet stopped for thirty-four minutes on
		// exactly that at height 756,370, with both park workers idle in their
		// receive and the loop asleep holding their work.
		//
		// A nil channel disables a select arm, which is what makes this safe when
		// there is nothing to hand over. The value has to be built here either
		// way, because a select evaluates every send case's operands on the way
		// in whether or not that case is chosen, so dereferencing the field
		// inside the arm would fault when it is nil.
		var (
			parkArm  chan<- parkJob
			parkWork parkJob
		)

		if sm.parkJobHeld != nil {
			parkArm = sm.parkJobs
			parkWork = *sm.parkJobHeld
		}

		// Recorded here, not anywhere earlier, so it describes the wait rather
		// than the work that led to it. Nothing below reads it; the watchdog on
		// the message-handling goroutine does.
		sm.publishConsumerWait(time.Now(), queueArm != nil, pending)

		select {
		case parkArm <- parkWork:
			sm.parkJobHeld = nil

			// Not an admission, for the same reason as the offer above.

		case <-sm.quit:
			// Best-effort drain with an error reply before exiting: the block
			// waiting for capacity, every block already in flight (its worker
			// keeps running, but its completion is dropped) and everything still
			// queued. Under prefetch each of those has an awaitBlockResult
			// goroutine holding budget and waiting on its reply, so replying here
			// lets them exit promptly on shutdown instead of waiting for the
			// peer's own quit/ctx. The feeder races the same sm.quit close, so a
			// block it enqueues after this drain returns is not caught here — that
			// block's awaitBlockResult still exits via sp.quit/sp.ctx.Done() (the
			// backstop), and the feeder's enqueue is itself sm.quit-guarded so it
			// can never deadlock. This drain only makes the common case prompt; it
			// is not relied on for correctness.
			if pending != nil {
				finish(pending.msg, errors.NewServiceError(syncManagerShuttingDownMsg))

				pending = nil
			}

			// A held park job has the queued block's reply, so nobody else will
			// answer for it, and its prefetch budget is held until somebody does.
			// Its admission is given back too, because no write is coming.
			if sm.parkJobHeld != nil {
				sm.blockPark.Abandon(sm.parkJobHeld.entry)
				sm.replyToParkJob(*sm.parkJobHeld, errors.NewServiceError(syncManagerShuttingDownMsg))

				sm.parkJobHeld = nil
			}

			for _, e := range bd.drainFrontier() {
				// A parked dispatch has no queue message: nothing incremented the
				// backlog for it and nobody is waiting on a reply, so finishing it
				// would send on a nil channel and underflow the counter that
				// suppresses the sync-peer stall check for the life of the process.
				// What it needs instead is its park entry back, because the entry
				// was taken out of the index to dispatch it. Without the Restore the
				// blob is adopted by the next start's recovery with no height, no
				// peer and no header node, which is the one entry that can never be
				// rewound into the download walk.
				if e.d.parked != nil {
					sm.blockPark.Restore(*e.d.parked)

					continue
				}

				finish(e.d.msg, errors.NewServiceError(syncManagerShuttingDownMsg))
			}

			for {
				select {
				case msg := <-blockQueue:
					finish(msg, errors.NewServiceError(syncManagerShuttingDownMsg))
				default:
					return
				}
			}
		case msg := <-queueArm:
			// Taking a block off the queue is NOT progress, and the comment that
			// used to sit here said the opposite: that a loop parking blocks it
			// cannot commit is working, and only a loop doing nothing at all is
			// wedged. That premise is what blinded this watchdog to the fault an
			// operator actually watches.
			//
			// Measured on mainnet on 2026-09-10: the tip sat at 783,277 with the
			// block for 783,278 already on disk and its parent already committed,
			// 127 more blocks stacked behind it in one contiguous chain, and the
			// sweep announcing every thirty seconds that it was committing the
			// block. Nothing committed for over five minutes and the watchdog
			// never spoke, because arriving blocks kept being received and parked
			// and each of those reset its clock.
			//
			// What counts is a dispatch or a drained commit. Both stamp the clock
			// themselves, a few lines above.

			sm.logger.Debugf("[blockHandler][%s] processing block queue message into handleBlockMsgHead", msg.blockHash)

			d, finished, err := sm.handleBlockMsgHead(msg)
			if finished {
				finish(msg, err)

				continue
			}

			// Hold it unconditionally: the top of the loop runs before the next
			// receive, so a block that can start does so immediately, and there is one
			// place where a block is admitted.
			pending = d
		case c := <-bd.completions:
			bd.complete(c)
		case outcome := <-sm.parkOutcomes:
			// A parking worker has finished with a block the head admitted.
			// Everything that needs ordering or the header list waited for this.
			sm.applyParkOutcome(outcome)
		case commit := <-sm.parkCommits:
			// The sweep found a parked block whose parent is in the chain after
			// all. It decided that on its own goroutine and posts here, because
			// this is the one goroutine that admits blocks: committing from the
			// sweep would race the dispatcher for admission into the window.
			//
			// Put back, then queued, rather than committed here. The sweep took
			// the entry out of the index to hand it over, and restoring it means
			// the drain step claims it through the one path every other drained
			// block takes, with one admission test and one header-front advance.
			// It also keeps the block visible to the sweep's own eviction pass
			// while it waits, and it is two map operations rather than a full
			// commit, which is what stops a 128-entry tick queueing minutes of
			// work onto this goroutine.
			sm.blockPark.Restore(commit.entry)
			sm.scheduleDrain(commit.entry.prevBlock, commit.parentHeight)
		}
	}
}

// noteHandedOff tells the peer's awaiting goroutine that this block's memory is
// now charged to another budget, so the download bytes it reserved can go back.
// Closed exactly once, by the consumer goroutine, and safe on a message that
// carries no channel.
func noteHandedOff(msg *blockQueueMsg) {
	if msg == nil || msg.handedOff == nil {
		return
	}

	select {
	case <-msg.handedOff:
		// Already signalled. Cannot happen on today's paths, since each of the two
		// charge points runs once per block, but closing a closed channel panics
		// and the cost of asking is nothing.
	default:
		close(msg.handedOff)
	}
}

// admissionChoice is which of the consumer's two admission sources takes a turn.
type admissionChoice int

const (
	admitNothing admissionChoice = iota
	admitLive
	admitDrained
)

// nextAdmission decides which source admits this turn, given whether the last
// admission was a drained block and whether each source has something ready.
//
// It is a function of its own because it is a priority decision and the obvious
// implementation is wrong. Both sources are ready at once routinely: the drain is
// open whenever the frontier is empty and a parent has parked children, and a
// live block is admissible in exactly that state too. Leaving the choice to a
// select would pick uniformly among ready cases, which starves whichever source
// happens to lose repeatedly.
//
// Neither direction is safe to prefer unconditionally. Always preferring the
// drain makes a live block wait behind a parked chain that can be thousands long,
// and the live block is the one the whole chain is waiting for. Always preferring
// the live path starves the drain for as long as blocks keep arriving, which is
// the regime this exists for. So whichever went last yields, and each source gets
// every other turn while both are ready.
func nextAdmission(lastWasDrained, canLive, drainOpen bool) admissionChoice {
	switch {
	case canLive && drainOpen:
		if lastWasDrained {
			return admitLive
		}

		return admitDrained

	case drainOpen:
		return admitDrained

	case canLive:
		return admitLive

	default:
		return admitNothing
	}
}

// handleBlockMsgHead runs every pre-check for one queued block on the consumer
// goroutine — peer resolution, the FSM state, headers-first bookkeeping, the
// download ledger, the backoff and cascade skips, block-size sampling — and then
// answers the one question that decides where the block goes: is its parent in
// flight, in the chain, or neither. The first two become a dispatch; the third
// is parked here, while the decoded block is still in hand, because parking needs
// its bytes and nothing after dispatch may read them.
//
// It returns (dispatch, false, nil) when the block is ready to be handed to a
// worker, or (nil, true, err) when the block is finished here and err is what the
// caller must reply. Everything it touches (the header cache, the download
// ledger, the park index) stays on this one goroutine.
func (sm *SyncManager) handleBlockMsgHead(bmsg *blockQueueMsg) (*blockDispatch, bool, error) {
	sm.logger.Debugf("[handleBlockMsg][%s] received block height %d from %s", bmsg.blockHash, bmsg.blockHeight, bmsg.peer)
	peer := bmsg.peer

	state, resolved, exists := sm.peerStateResolvingPrimary(peer)
	if !exists {
		sm.logger.Errorf("[handleBlockMsg][%s] Received block message from unknown peer %s", bmsg.blockHash, peer)
		return nil, true, errors.NewServiceError("[handleBlockMsg] Received block message from unknown peer %s", peer)
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
		return nil, true, errors.NewServiceError("[handleBlockMsg] skipping block %s from disconnected peer %s", bmsg.blockHash, bmsg.peer)
	}

	catchingBlocks := false

	sm.logger.Debugf("[handleBlockMsg][%s] checking current FSM state", bmsg.blockHash)

	fsmState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
	if err != nil {
		return nil, true, errors.NewProcessingError("[handleBlockMsg] failed to get current FSM state", err)
	}

	if fsmState != nil && *fsmState == teranodeblockchain.FSMStateCATCHINGBLOCKS {
		catchingBlocks = true
	}

	// If we didn't ask for this block then the peer may be misbehaving, or may
	// simply be answering a question we have stopped asking.
	//
	// A second copy of a block whose quiet owner was forgiven and reassigned to
	// another peer is not caught by this: forgiveness drops the obligation but
	// keeps the permission (see unownedBlocks and ForgiveOwners), so the quiet
	// peer is still an owner and HasOwner above is already true for it. Nothing
	// further is needed to admit it.
	if !sm.blockDownloads.HasOwner(peer, bmsg.blockHash) {
		if sm.punishUnrequestedBlock(catchingBlocks) {
			reason := fmt.Sprintf("Got unrequested block %v", bmsg.blockHash)
			peer.DisconnectWithWarning(reason)

			return nil, true, errors.NewServiceError("Got unrequested block %v", bmsg.blockHash)
		}

		// Catching blocks: the block is free. Fall through and process it.
		sm.logger.Debugf("[handleBlockMsg][%s] accepting an unrequested block from %s while catching blocks", bmsg.blockHash, peer)
	}

	// isCheckpointBlock feeds the dispatcher's barrier (canDispatch, dispatch,
	// complete in block_dispatcher.go): nothing may be admitted behind a
	// checkpoint block until its tail has run. It is a direct hash comparison
	// against the configured checkpoints now, needing no list entry to remove
	// and nothing to put back if the block is later dropped.
	isCheckpointBlock := sm.isCheckpointHash(bmsg.blockHash)

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
	if sm.blockGivenUpOn(bmsg.blockHash) {
		sm.logger.Errorf("[handleBlockMsg][%s] given up on after %d consecutive failures; it will not be requested again in this process. Restarting clears this deliberately, because a restart is the most likely thing to have fixed the local fault", bmsg.blockHash, sm.settings.Legacy.BlockFailureAttemptCeiling)

		return nil, true, errors.NewServiceUnavailableError("[handleBlockMsg][%s] block given up on after %d failures", bmsg.blockHash, sm.settings.Legacy.BlockFailureAttemptCeiling)
	}

	if sm.blockFailureBackoff != nil {
		if fs, ok := sm.blockFailureBackoff.Get(bmsg.blockHash); ok && time.Now().Before(fs.nextRetry) {
			sm.logger.Warnf("[handleBlockMsg][%s] in backoff after %d transient failure(s), skipping until %s", bmsg.blockHash, fs.attempts, fs.nextRetry)
			// The peer just delivered this block — the fault is our local store,
			// not the peer — so keep its stall timer fresh. No-op unless peer is
			// the current sync peer.
			if sps, ok := sm.syncPeerStateFor(peer); ok {
				sps.updateLastBlockTime()
			}

			// Nothing to put back: the wanted-range pass recomputes what it wants
			// on every call, so this block is simply still in range and unowed,
			// and the next pass finds it again once the backoff has expired.
			return nil, true, errors.NewServiceUnavailableError("[handleBlockMsg][%s] block in backoff after %d transient failure(s)", bmsg.blockHash, fs.attempts)
		}
	}

	// Per-(hash, peerID) corrupt re-download cap (bitcoin-sv/teranode#4692): if THIS serving peer has
	// already failed with a corrupt body for this hash MaxCorruptAttemptsPerBlock times within the
	// cooldown window, drop this delivery BEFORE the expensive HandleBlockDirect/decorate. This is the
	// ban-score-independent bound on corrupt re-download amplification PER SERVING IDENTITY (keyed on
	// (hash, peerID), not the hash alone, so a bad peer never wedges the honest tip — an honest peer
	// keeps a fresh budget for the same hash; the residual aggregate per-hash work scales with the
	// number of distinct serving peers). Drop quietly: do NOT reject the block to the peer, do NOT
	// mark it failed (recentlyFailedBlocks) — preserving the recentlyFailedBlocks no-NOT_FOUND-cascade
	// property — and do NOT poison. The peer's stall timer is deliberately NOT refreshed, so if a peer
	// keeps serving the same corrupt hash the stall detector can rotate to one with an honest body.
	//
	// Recovery, precisely, and it is NOT upstream's. Upstream had to argue at length
	// that this gate must not re-request and must not refill, because its header list
	// was a linear chain walked by a cursor: anything a refill asked for descended
	// from the hash just dropped, arrived, refreshed the stall timer and then failed
	// its parent lookup, so the only recovery left was waiting 180 seconds for the
	// stall detector to rotate the sync peer.
	//
	// None of that applies here. The wanted range is recomputed from the committed
	// tip on every pass and stops at the first height the header cache cannot name,
	// so a hash this gate drops is simply asked for again on the next pass — of
	// whichever peer has budget, not necessarily the capped one — and nothing above
	// it is requested until it lands. Dropping quietly is therefore all this gate has
	// to do: no rotation to wait for, no descendants to waste.
	//
	// What is unchanged is everything the drop must NOT do: do not reject the block
	// to the peer, do not mark it failed (recentlyFailedBlocks), preserving the
	// no-NOT_FOUND-cascade property, and do not poison. The peer's stall timer is
	// deliberately not refreshed either, so a peer that keeps serving the same
	// corrupt hash is still rotated away from on the ordinary schedule.
	//
	// It self-heals without rotation in both directions: a peer below the cap is
	// never dropped here at all, and once the fixed window lapses the counter resets
	// and the same peer's honest body is admitted.
	if sm.corruptBlockAttemptsExhausted(bmsg.blockHash, bmsg.peer.Addr()) {
		sm.logger.Warnf("[handleBlockMsgHead][%s] corrupt re-download cap reached for peer %s, dropping delivery until the cooldown window expires (not rejected, not stored invalid)", bmsg.blockHash, bmsg.peer)

		return nil, true, nil
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
		return nil, true, errors.NewProcessingError("[handleBlockMsg][%s] block message carries no block", bmsg.blockHash)
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
	//
	// A parent the dispatcher is working on right now is not a failed parent, even
	// if a previous attempt at it failed: it was re-admitted, so its child must not
	// be short-circuited as part of a cascade that is already being retried.
	if sm.recentlyFailedBlocks != nil && !sm.dispatcher.inFlight(prevBlockHash) {
		if _, failed := sm.recentlyFailedBlocks.Get(prevBlockHash); failed {
			sm.recentlyFailedBlocks.Set(bmsg.blockHash, struct{}{})
			sm.logger.Debugf("[handleBlockMsg][%s] parent %s recently failed to store/validate; skipping descendant (root failure already logged)", bmsg.blockHash, prevBlockHash)

			if sps, ok := sm.syncPeerStateFor(peer); ok {
				sps.updateLastBlockTime()
			}

			// Nothing to put back, and it is worth saying why rather than
			// leaving the omission to be found again. unownedBlocks makes the
			// same check before this block is ever requested a second time,
			// but it only skips the one entry a hash names, not everything
			// above it: this block is marked here, on delivery, and is what
			// keeps unownedBlocks from handing it out again on the next pass,
			// not a walk stopping short the way the old cursor walk once did.
			// This delivery-side check is what catches a copy of this
			// descendant already in flight when its parent failed, which the
			// request-side one has no way to see.
			sm.requestMissingBlocks(peer, bmsg.blockHash)

			return nil, true, nil
		}
	}

	// Serializing a multi-GB block to measure it is not free, and exactly two consumers want
	// the number: the headers-first size tracker just below, and the dispatcher's byte charge
	// further down. Measure it at most once, and only when one of them actually asks, so a node
	// with headers-first off and no windowed block pays nothing it did not pay before the
	// window existed, which is where this measurement used to sit.
	var (
		blockSize  int64
		blockSized bool
	)

	sizeOf := func() int64 {
		if !blockSized {
			blockSize = int64(msgBlock.SerializeSize())
			blockSized = true
		}

		return blockSize
	}

	headersFirst := sm.headersFirstMode.Load()

	// Track block size for dynamic in-flight adjustment during headers-first mode.
	// This allows us to start aggressive (20 blocks) and automatically reduce
	// to 1 block when encountering large (>2GB) blocks on mainnet.
	//
	// This stays ahead of the parent lookup, where it has always been: the tracker is measuring
	// what the wire delivered, and an orphan is still a block that arrived.
	if headersFirst {
		size := sizeOf()

		sm.blockSizeTracker.addBlockSize(size)

		dynamicMax := sm.blockSizeTracker.calculateMaxInFlightBlocks()
		avgSize := sm.blockSizeTracker.getAverageSize()
		sm.logger.Debugf("[handleBlockMsg][%s] Block size: %d bytes, avg: %d bytes, dynamic max in-flight: %d",
			bmsg.blockHash, size, avgSize, dynamicMax)
	}

	sm.logger.Debugf("[handleBlockMsgHead][%s] pre-checks passed, resolving the parent", bmsg.blockHash)

	d := &blockDispatch{
		msg:            bmsg,
		peer:           peer,
		state:          state,
		msgBlock:       msgBlock,
		prevHash:       prevBlockHash,
		catchingBlocks: catchingBlocks,
		isCheckpoint:   isCheckpointBlock,

		// Read once, here, and carried through the dispatch, so the head and the
		// worker cannot reach different answers about the same block. Upstream read it
		// just before clearing the request bookkeeping, for the same reason: the proof
		// has to outlive whatever the delivery path tears down. Here it is a header-cache
		// lookup rather than a map the request path stamped — see blockOrigin — so the
		// ordering constraint is weaker, but taking it once is still the rule, because a
		// second read can disagree with the first if a headers batch lands in between.
		origin: sm.blockOrigin(bmsg.blockHash),
	}

	// HandleBlockDirect's opening — is the block stored, is its parent — asked
	// here so the answer decides where the block goes while its bytes are still
	// in hand. The worker is handed what was resolved, so it does not ask again.
	if finished, err := sm.resolveParent(d, msgBlock); finished {
		return nil, true, err
	}

	// The byte charge is the dispatcher's window bookkeeping, and only a windowed block ever
	// pays it, so the size is asked for here rather than above: resolveParent has just
	// settled the route, so a block on an enabled window route that turns out not to be
	// windowed after all (above the checkpoint, say) is never measured for it. On the window
	// route with headers-first on, sizeOf has already run and this is free.
	//
	// d.bytes stays zero otherwise, and the dispatcher is fine with that: it charges and
	// releases symmetrically, and canDispatch reads bytes only after the !d.windowed early
	// return. Off the window route a block needs an empty frontier whatever its size.
	if d.windowed {
		d.bytes = sizeOf()
	}

	// A re-admitted block clears the cascade mark on its own hash, so a child
	// delivered while this attempt is in flight is not dropped as the descendant of
	// a failure that is being retried right now. The tail sets the mark again if
	// this attempt fails too.
	if sm.recentlyFailedBlocks != nil {
		sm.recentlyFailedBlocks.Delete(bmsg.blockHash)
	}

	return d, false, nil
}

// resolveParent is HandleBlockDirect's opening, run on the consumer goroutine
// before the block is dispatched, and every answer is routed exactly as it was
// when HandleBlockDirect gave it: whether the block is already stored, and then
// whether its parent is in flight, in the chain, or neither. It fills in the
// dispatch's height, in-flight parent and windowed flag, and returns
// finished=true when the head is done with the block — parked, already stored
// and settled, or failed — with err then being what the caller must reply.
//
// The order is HandleBlockDirect's, and it matters. A block that is already in
// the chain was never parked, whatever its parent lookup says; and a lookup that
// fails for any reason other than "not found" was never an orphan either, it was
// a failure of the block's own work, and it took the failure arm of the tail:
// the walk is put back on the block, throttled, and the error is classified
// before anything is said to the peer. The head hands those to the tail itself,
// so the serial path and the dispatcher's path agree with 1606 to the branch.
//
// A stored block is not finished here. HandleBlockDirect returned nil for one
// and the success tail then ran, drain included, and that is what the head keeps
// by dispatching it: the worker's own existence check gives the same answer and
// the tail records a commit.
//
// The dispatcher is asked about the parent before the chain, because a parent in
// flight is not in the chain and a chain lookup would wrongly call it missing.
// Only the frontier's tail can be resolved as an in-flight parent: the frontier
// is a chain, so any earlier entry is the ancestor of a block already admitted,
// and a block naming one of those as its parent is a fork off a block still
// being worked on. Both that fork and a tail-parented block off the window route
// are dispatched without a resolved parent, which makes the dispatcher hold them
// for an empty frontier; by then the parent has either committed, and the
// worker's own lookup finds it, or failed, and parentFailedWhileWaiting gives
// the block the cascade treatment. Neither needs the park, because the parent's
// outcome is known before the block starts.
//
// A stored parent is handed to the worker with its height and no frontier entry.
// HandleBlockDirect reads the height off it and skips the lookup the head has
// just made, and the ordering hand-shake is guarded on the entry, so nothing
// waits on anything.
func (sm *SyncManager) resolveParent(d *blockDispatch, msgBlock *wire.MsgBlock) (bool, error) {
	bmsg := d.msg
	prevBlockHash := d.prevHash

	exists, err := sm.blockchainClient.GetBlockExists(sm.ctx, &bmsg.blockHash)
	if err != nil {
		sm.logger.Errorf("[handleBlockMsg][%s] failed to check if block exists: %s", bmsg.blockHash, err)

		return true, sm.handleBlockMsgTail(d, errors.NewProcessingError("failed to check if block exists", err))
	}

	if exists {
		sm.logger.Warnf("[handleBlockMsg][%s] block already exists", bmsg.blockHash)

		return false, nil
	}

	// The sync peer's association just delivered a full block. Refresh its
	// last-block time now, at receipt, so what follows — minutes of validation
	// for a block that is dispatched, or the park's own work for one that is
	// not — is not mistaken for a stall. HandleBlockDirect did this at the same
	// point for every block; the head does it for every block now, because the
	// parked ones never reach HandleBlockDirect. None of the park outcomes
	// refreshes it again: a peer that answers with an endless stream of orphans
	// is exactly what the stall detector exists to rotate, and parking must not
	// judge a peer differently from discarding.
	if sps, ok := sm.syncPeerStateFor(bmsg.peer); ok {
		sps.updateLastBlockTime()
	}

	if p := sm.dispatcher.parentFor(&prevBlockHash); p != nil {
		// The parent is still in the window, so it is not in the blockchain store
		// yet and only the dispatcher knows its height.
		d.height = p.height + 1

		if sm.windowRoute(d.height) {
			d.parent = p
			d.windowed = true
		}

		return false, nil
	}

	if sm.dispatcher.inFlight(prevBlockHash) {
		return false, nil
	}

	_, meta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &prevBlockHash)
	if err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) || errors.Is(err, errors.ErrNotFound) {
			sm.parkOrphanBlock(d, msgBlock)

			return true, nil
		}

		sm.logger.Errorf("[handleBlockMsg][%s] failed to get block header for previous block %s: %s", bmsg.blockHash, prevBlockHash, err)

		return true, sm.handleBlockMsgTail(d, errors.NewProcessingError("failed to get block header for previous block %s", prevBlockHash, err))
	}

	// No implementation returns a nil meta with a nil error, but this height decides
	// whether the block takes the window route at all, so fail closed rather than derive
	// it from a nil dereference. Mirrors the guard block validation already applies to the
	// same lookup in processBlockFound.
	if meta == nil {
		return true, sm.handleBlockMsgTail(d, errors.NewProcessingError("[handleBlockMsg][%s] nil metadata for previous block %s", bmsg.blockHash, prevBlockHash))
	}

	d.height = meta.Height + 1
	d.parent = &inflightParent{height: meta.Height}

	// A windowed block whose parent is not in flight still has to start the window
	// from an empty frontier: nothing before it in the frontier is its ancestor, so
	// the ordering hand-shake would have nothing to wait on.
	d.windowed = sm.windowRoute(d.height) && sm.dispatcher.frontierEmpty()

	return false, nil
}

// parkOrphanBlock keeps a block whose parent is neither in the chain nor in
// flight, and asks for the parent. It runs in the head, before the block is ever
// dispatched, because it is the one step that needs the decoded block after the
// pre-checks: the release of that block after dispatch is sound only because
// nothing downstream reads it.
//
// While catching blocks this is typically the peer announcing its tip while we
// are still behind — and in the legacy sync protocol that orphan tip doubles as
// the batch-continuation signal: the peer pushes its tip inv after delivering a
// getblocks batch and waits for the next getblocks before sending more.
// Swallowing the orphan stalls the sync until the stall detector rotates the
// peer, so it always ends with a getblocks from our best block. PushGetBlocksMsg
// filters duplicate requests and the peer only invs blocks past the locator fork
// point, so a redundant request costs one inv message at most.
//
// The block is kept instead of thrown away. It is checked before anything
// reaches the disk, so a peer cannot fill the park with rubbish, and it is
// committed from disk as soon as its parent lands.
func (sm *SyncManager) parkOrphanBlock(d *blockDispatch, msgBlock *wire.MsgBlock) {
	bmsg := d.msg
	peer := d.peer
	prevBlockHash := d.prevHash
	catchingBlocks := d.catchingBlocks

	entry := parkedBlock{
		hash:      bmsg.blockHash,
		prevBlock: prevBlockHash,
		// The height as the delivering peer reported it, with no fallback:
		// there is no header list any more to look a height up in when it
		// reads zero. A block recovered from disk after a restart always
		// carried zero anyway, so this simply extends that same honest case
		// to every block whose wire message did not report one.
		height: bmsg.blockHeight,
		// The resolved association primary, not bmsg.peer. A block
		// delivered on a stream sub-peer (BlockPriority DATA1) carries
		// that sub-peer, and sub-peers are not registered in peerStates
		// — so noteCommittedParkedBlock's lookup missed and the height
		// bookkeeping a committed block is supposed to do was silently
		// skipped. The ledger records the primary too, which is the
		// identity HasOwner and the reject path ask about.
		peer: peer,
	}

	// Admit is the cheap half: the duplicate check, the byte budget and
	// registering the entry. The blob write is the expensive half and it
	// goes to a parking worker, because this is the one goroutine that
	// commits blocks in order and a gigabyte block spends minutes writing.
	stored, admitted := sm.blockPark.Admit(entry, msgBlock)

	// The reject, like every other reject in handleBlockMsg, goes to the
	// resolved primary rather than to a stream sub-peer, so it is
	// applied to a copy of the entry that names it. Safe to copy because
	// no write outcome touches a blob the caller is holding.
	blamed := entry
	blamed.peer = peer

	switch admitted {
	case admitNoRoom:
		// The same table the drain and the sweep answer to: whether the
		// blob survives, whether the walk goes back onto the block, and
		// whether the peer hears about it. See block_park_policy.go.
		disposition := parkWriteOutcome(parkUnavailable)

		if catchingBlocks {
			// While catching blocks handleBlockMsg suppresses every
			// other reject, because we are replaying history rather than
			// judging a peer's tip. Parking must not judge a peer
			// differently from discarding.
			disposition = disposition.withoutBlame()
		}

		sm.logger.Infof("Block %v has missing parent %v and was not kept (%s), requesting missing blocks",
			bmsg.blockHash, prevBlockHash, disposition.reason)

		// The block is being dropped, so put the download walk back on
		// it. Without this the getblocks below is the only recovery
		// there is, and in headers-first mode it is inert: processInvMsg
		// returns before it can request anything, so the block is never
		// asked for again.
		sm.applyParkDisposition(blamed, disposition)

	case admitAlreadyHeld:
		sm.logger.Infof("Block %v is waiting on its parent %v, parked", bmsg.blockHash, prevBlockHash)

		sm.fetchMoreHeaderBlocks(peer)

	case admitRegistered:
		sm.logger.Infof("Block %v is waiting on its parent %v, parked", bmsg.blockHash, prevBlockHash)

		// The block is kept, so the walk must NOT be rewound onto it: it
		// is already downloaded and the park commits it from disk when
		// the parent lands. Every path that later gives the block up
		// rewinds then instead. What does need doing is topping the
		// pipeline back up, because this peer's in-flight count just
		// dropped and nothing else will notice.
		sm.fetchMoreHeaderBlocks(peer)

		job := parkJob{
			entry:          stored,
			blamed:         blamed,
			msgBlock:       msgBlock,
			reply:          bmsg.reply,
			catchingBlocks: catchingBlocks,
		}

		// The reply travels with the job, so the consumer does not answer for
		// this block when the head returns. That is what keeps the peer's
		// awaiting goroutine alive until the worker has finished with the
		// decoded block.
		bmsg.reply = nil

		// Admit has charged the block to the park's own byte budget, and the
		// park is what accounts for it from here, including while the worker
		// holds it to write. So the download bytes go back now rather than at
		// the reply, which is minutes later for a mainnet giant block.
		noteHandedOff(bmsg)

		sm.submitParkJob(job)
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
}

// handleBlockMsg processes one queued block end to end on the calling goroutine:
// head, work, tail. The dispatcher splits those three steps apart so up to K blocks
// can be in flight; this serial form is what the tests that drive a single block
// through the pipeline still use.
func (sm *SyncManager) handleBlockMsg(bmsg *blockQueueMsg) error {
	d, finished, err := sm.handleBlockMsgHead(bmsg)
	if finished {
		return err
	}

	// d.msg.peer is the delivering peer, which is what HandleBlockDirect has always
	// been given; d.peer is its association's primary, for the tail's bookkeeping.
	err = sm.HandleBlockDirect(sm.ctx, d.msg.peer, d.msg.blockHash, d.msgBlock, d.parent, d.origin)

	return sm.handleBlockMsgTail(d, err)
}

// handleBlockMsgTail is every chain-order step for one block: the failure
// classification and reject/backoff decision, the cascade bookkeeping, the peer
// height update, and the headers-first continuation. It runs on the consumer
// goroutine in dispatch order, never on a worker, so headers-first state stays
// single-threaded and blocks are accepted strictly in height order.
func (sm *SyncManager) handleBlockMsgTail(d *blockDispatch, err error) error {
	bmsg := d.msg
	peer := d.peer
	state := d.state
	catchingBlocks := d.catchingBlocks
	prevBlockHash := d.prevHash

	if err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) {
			// The head resolved this block's parent as stored or in flight before
			// the block was dispatched, so a missing parent here is not an orphan
			// — the park owns those, and it decided before any of this ran. It is
			// the parent going away between the head's lookup and the worker's, or
			// something else in block validation surfacing as ErrBlockNotFound,
			// which the park's own sweep already knows to be possible. Neither is
			// a judgement on the block: record the failure so the backoff
			// throttles a re-request, and answer with the getblocks the orphan
			// path has always sent.
			sm.logger.Infof("Block %v has missing parent %v after dispatch, requesting missing blocks", bmsg.blockHash, prevBlockHash)

			sm.dropBlockFromWalk(bmsg.blockHash)
			sm.requestMissingBlocks(peer, bmsg.blockHash)

			return nil
		} else {
			if errors.Is(err, context.Canceled) || errors.IsContextError(err) {
				// Neither committed nor judged: our own context went away
				// mid-processing. The block is still wanted, so this records the
				// same backoff the other drop paths get. On shutdown that is
				// wasted work on a manager that is about to stop; on a mid-flight
				// cancellation it is what throttles this block's next request.
				sm.dropBlockFromWalk(bmsg.blockHash)

				return nil
			}

			// Corrupt block body (bitcoin-sv/teranode#4692): a body-derived failure (merkle mismatch, CVE
			// duplicate) that is not bound to the header, so it cannot condemn the hash and is not
			// a clear peer fault (a body can be corrupted in transit). Drop it WITHOUT rejecting the
			// block to the peer, WITHOUT marking it failed (which would suppress its descendants),
			// and WITHOUT disconnecting (see shouldDisconnectOnBlockErr) — re-request is left free so
			// an honest copy can arrive on the next delivery / sync-peer rotation. Never poison.
			if errors.IsBlockCorrupt(err) {
				// Count this corrupt failure toward the per-(hash, peerID) cap (bitcoin-sv/teranode#4692).
				// Once this serving peer's count for the hash reaches MaxCorruptAttemptsPerBlock the gate
				// above drops that peer's further deliveries until the fixed window lapses. Recorded ONLY
				// on an actual corrupt failure, so a below-cap corrupt is still re-downloaded as today.
				attempts := sm.recordCorruptBlockAttempt(bmsg.blockHash, bmsg.peer.Addr())
				sm.logger.Warnf("[handleBlockMsg][%s] corrupt block body from peer %s (attempt %d), dropping for re-download (not rejected, not stored invalid): %v", bmsg.blockHash, bmsg.peer, attempts, err)

				// Re-request through the ordinary wanted-range pass rather than through a
				// direct getdata to the same peer.
				//
				// Upstream needed requestBlockDirect here because its recovery routes were
				// both blocked: a getblocks is answered with an inv and processInvMsg discards
				// invs while headers-first mode is on, and its header-block walk only ever went
				// forward from a cursor the dropped hash had already passed. Neither structure
				// exists on this branch. assignWantedBlocks recomputes the range from the
				// committed tip every time it runs, so the dropped hash is the first thing it
				// names; the head has already released this peer's ownership of it, so nothing
				// holds it back; and the assigner is free to place it with a different peer,
				// which upstream's direct getdata could not do.
				//
				// SKIPPED once this peer has reached the cap, for upstream's reason and gated on
				// upstream's predicate rather than on a re-derived attempts comparison, so this
				// decision and the gate above agree by construction: corruptBlockAttemptsExhausted
				// reads the counter recordCorruptBlockAttempt just wrote and already handles a cap
				// of <= 0 (DISABLED) and a nil map or settings fixture, both of which fall through
				// to "ask again", which is the pre-existing behaviour. Below the cap, asking again
				// and possibly getting the same peer is deliberate: a body can be corrupted in
				// transit by an honest relay.
				//
				// Pipeline maintenance ONLY. The corrupt branch must run no accepted-block
				// bookkeeping (rejected-tx clear, peer-height update, FSM RUN, fee-filter reset),
				// which is why this calls the assignment pass directly instead of falling through
				// to the acceptance footer.
				if !sm.corruptBlockAttemptsExhausted(bmsg.blockHash, bmsg.peer.Addr()) {
					sm.fetchHeaderBlocks()
				}

				// Keep the getblocks as well: in the legacy sync protocol it doubles as the
				// batch-continuation signal (see requestMissingBlocks), which a getdata does not carry.
				sm.requestMissingBlocks(peer, bmsg.blockHash)

				return err
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

			// Record the failure, throttled, whether we judged it or merely had
			// bad luck with it, so the wanted-range pass does not immediately
			// re-ask for the same block. serviceError still decides whether the
			// peer is told the block was rejected, above; it no longer decides
			// whether the block is still wanted.
			//
			// An aborted successor never ran a failing attempt of its own — a
			// predecessor in the window failed — so it earns no backoff of its
			// own; the predecessor's backoff already throttles the whole run.
			// Nothing else needs doing for it: the wanted-range pass recomputes
			// what it wants on every call, so once the predecessor's backoff
			// clears, this block is simply still in range and unowed.
			if !d.aborted {
				sm.dropBlockFromWalk(bmsg.blockHash)
			}

			// An aborted successor is not a failure worth an ERROR line: the one
			// root failure was already logged by the predecessor's own tail, and at
			// depth K every head failure would otherwise print K-1 misleading
			// ERRORs (the same argument as the #1333 cascade suppression).
			if d.aborted {
				sm.logger.Debugf("Block %v aborted: a predecessor in the window failed: %v", bmsg.blockHash, err)
			} else {
				sm.logger.Errorf("Failed to process new block in service blockQueueMsg %v: %v", bmsg.blockHash, err)
			}

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

	// Also clear the per-(hash, peerID) corrupt counter (bitcoin-sv/teranode#4692) so an honest body
	// after the window never inherits a stale corrupt count.
	sm.clearCorruptBlockAttempts(bmsg.blockHash, bmsg.peer.Addr())

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

	if heightUpdate <= 0 && d.height > 0 {
		// The head already resolved this block's height from its parent, so the
		// lookup below is a round trip we do not have to make.
		if h, cerr := safeconversion.Uint32ToInt32(d.height); cerr != nil {
			sm.logger.Errorf(failedToConvertBlockHeightInt32Msg, cerr)
		} else {
			heightUpdate = h
		}
	}

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
		// It sent us the block. There is no stronger demonstration than that.
		state.noteProvenClaim(*blkHashUpdate, heightUpdate)
		sm.logger.Debugf("peer %s reports new best height %d, current %v", peer.String(), peer.LastBlock(), sm.current())

		if sm.current() { // used to check for isOrphan || sm.current()
			go sm.peerNotifier.UpdatePeerHeights(blkHashUpdate, heightUpdate, peer)

			// Since we are current, we can tell FSM to transition to RUN
			// Blockchain client will check if miner is registered, if so it will send Mine event, and FSM will transition to Mine
			if err = sm.runIfCatchingBlocks("legacy/netsync/manager/handleBlockMsg"); err != nil {
				sm.logger.Errorf("[Sync Manager] failed to send FSM RUN event %v", err)
			}

			sm.resetFeeFilterToDefault()
		}
	}

	// Headers-first mode has nothing left to verify once there is no checkpoint
	// ahead of the height just committed. This used to be decided only on the
	// specific block that was the checkpoint, by checkpointBlockCommitted, and
	// deferred with its own retry machinery when there was no peer to hand a
	// fresh getheaders to. Recomputed fresh here on every commit there is
	// nothing to defer: whichever commit or sync-peer election asks next gets
	// the current answer, not a stale one.
	sm.maybeLeaveHeadersFirstMode(bmsg.blockHash.String())

	// Ask for more blocks to keep the pipeline at the dynamic max limit
	// (adjusts based on block size). This no longer distinguishes a checkpoint
	// block from any other: the wanted-range pass reads whatever heights the
	// header cache currently names above the committed height, checkpoint or
	// not, so there is nothing checkpoint-specific left to gate it on.
	dynamicMax := sm.blockSizeTracker.calculateMaxInFlightBlocks()

	// Sampled a few microseconds before the in-flight count rather than in
	// the same expression. Harmless: assignWantedBlocks recomputes what is
	// wanted for itself before doing anything.
	//
	// Its own committedTip call, not threaded in from elsewhere in this
	// function: this goroutine already makes chain calls nearby (sm.current()
	// above, GetBestBlockHeader below), and saving one more is not worth
	// passing a parameter through for.
	best, _, _ := sm.committedTip()
	haveMoreWanted := len(sm.wantedBlocks(best)) > 0

	if haveMoreWanted && sm.blockDownloads.CountForPeer(peer) < dynamicMax {
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

// consumeQueuedBlock is the block-queue consumer's whole turn: process the
// block, settle the backlog accounting, and answer the caller.
//
// Extracted from blockHandler's select so the accounting can be tested without
// standing the handler up, because the rule it enforces is easy to get wrong and
// was wrong until recently.
func (sm *SyncManager) consumeQueuedBlock(msg *blockQueueMsg) {
	sm.logger.Debugf("[blockHandler][%s] processing block queue message into handleBlockMsg", msg.blockHash)

	err := sm.processQueuedBlock(msg)

	// The rule itself lives in finishBlockMsg, which the dispatcher's tail ends
	// with too, so the two consumers cannot drift on what counts as progress.
	sm.finishBlockMsg(msg, err)
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

// fetchMoreHeaderBlocks tops the download pipeline back up after a block from
// this peer stopped being outstanding, whether it was committed or parked.
// Without it a parked block is a silent loss of one in-flight slot, and the
// pipeline drains one block at a time until nothing is outstanding at all.
//
// The sweep ticker's resume does NOT come through here, because the per-peer
// question this asks is the wrong one for it — it calls fetchHeaderBlocks
// directly instead.
func (sm *SyncManager) fetchMoreHeaderBlocks(peer *peerpkg.Peer) {
	sm.topUpHeaderBlocks(func() bool {
		return sm.blockDownloads.CountForPeer(peer) < sm.blockSizeTracker.calculateMaxInFlightBlocks()
	})
}

// topUpHeaderBlocks is fetchMoreHeaderBlocks with the "has this peer got room"
// question left to the caller.
//
// It used to refuse outright while the round's anchor was still the front of
// the header list — a gate that, in production, was permanently shut: nothing
// ever advanced the list past its final anchor once every remaining checkpoint
// was already in it, so every site that called this returned early forever,
// and only a 30-second cleanup ticker kept requesting anything at all. There
// is no anchor and no list any more, so there is nothing left to check here
// beyond whether headers-first mode is even on and the caller still has room.
func (sm *SyncManager) topUpHeaderBlocks(hasRoom func() bool) {
	if !sm.headersFirstMode.Load() || sm.blockSizeTracker == nil {
		return
	}

	if hasRoom != nil && !hasRoom() {
		return
	}

	sm.fetchHeaderBlocks()
}

// dropBlockFromWalk records that a delivered-then-dropped block failed, so a
// re-request of it is throttled by the #1187 transient-failure backoff rather
// than being retried on every pass.
//
// It used to also rewind the download cursor back onto the block, because the
// old walk advanced a forward-only pointer past every header it considered and
// nothing else ever moved it back. The wanted-range pass recomputes what it
// wants and who owes it from the committed tip on every call, so a block that is
// still in range and unowed is simply found again on the next pass; there is no
// position left to restore.
func (sm *SyncManager) dropBlockFromWalk(blockHash chainhash.Hash) {
	if sm.blockFailureBackoff == nil {
		return
	}

	sm.recordBlockFailureBackoff(blockHash)
}

// fetchHeaderBlocks asks peers for the blocks this node wants next.
//
// It is one call because there is one way to choose blocks. What it replaced
// was a walk over a linked list from a cursor, with a rewind, a pin, a
// stranded-walk repair and a list-epoch guard hung off it, each added to correct
// the last. The cursor answered three questions at once: where to resume,
// whether the round was still valid, and whether the front had been asked for.
// One pointer, three meanings, and every fix for one broke another.
func (sm *SyncManager) fetchHeaderBlocks() {
	sm.assignWantedBlocks()
}

// headerCacheRefillInterval bounds how often maybeRequestMoreHeaders may send its
// own getheaders. assignWantedBlocks runs on every commit, so a cache that has
// run dry stays dry for many calls in a row while the reply is still in
// flight; without a floor, each of those calls would send its own getheaders
// to whichever peer it happened to pick, for no gain over the first one. The
// floor is a value on lastHeaderRequestAt, not a count of anything in flight,
// so it self-heals if the peer asked never answers: the next pass past the
// interval simply tries again, possibly of a different peer.
const headerCacheRefillInterval = 5 * time.Second

// headerCacheRefillThreshold is how many heights the cache must still name
// above the committed tip before maybeRequestMoreHeaders leaves it alone.
// Below this, a refill goes out even though the cache is not yet exhausted,
// so a fresh batch is normally already in hand by the time the current one
// runs dry.
//
// This used to be derived from what a refill cost when the node was moving:
// three rate-limited attempts and about 40 seconds of dead air, observed at
// the 8,000 boundary. That cost was self-inflicted and is gone. It was the
// header cache refusing every reply whose front had slipped behind a tip that
// advances 18 times a second, so a refill could not succeed until commits
// stopped. headerCache.Fill now keeps the part of a reply that is still above
// the tip, so one round trip lands roughly 1,980 usable heights, and the old
// derivation no longer describes anything real.
//
// What sizes it now is the cadence, not the cost. Two bounds, and the value
// has to sit between them.
//
// The ceiling is what one reply delivers. A reply is 2,000 headers minus
// whatever the tip ate in flight, so call it 1,980. Set the threshold near
// that and the cache is below it the instant it is filled, and the node asks
// again every headerCacheRefillInterval for ever. Half a batch keeps a clear
// thousand heights between a fresh fill and the next trigger, which is one
// refill per batch committed — the cadence the sawtooth had, minus the gap.
//
// The floor is what a peer that does not answer costs. Rotation and the
// 5-second floor mean a silent peer costs one interval per attempt, so 1,000
// heights is about 50 seconds at the measured 20 blocks/s: ten attempts
// across rotated peers before the cache runs dry. Asking early is close to
// free here, because Fill replaces rather than merges and a fresh batch
// starts at the same committed tip the current one does, so an early reply
// supersedes what is held without discarding a single usable height.
//
// The read-ahead depth (legacy_blockDownloadLowerWindow, 128) is still the
// wrong quantity to reach for, for the reason it always was: it bounds how
// far downloads may run ahead of the commit frontier, not how much header
// lookahead a getheaders round trip needs, and at 128 it is 6.4 seconds of
// runway.
const headerCacheRefillThreshold = int32(wire.MaxBlockHeadersPerMsg / 2)

// maybeRequestMoreHeaders is assignWantedBlocks' other half: the wanted range
// only ever names what the cache already holds, and nothing before this
// existed to refill it once a round ran out. wanted is what wantedBlocks()
// returned for this pass, before the assigner's own download-budget cap —
// that cap answers "is there room for a block", a different question from
// "is there more of the run to read", so this must see the pass's full range
// and run whether or not a download budget happens to be free this time.
//
// Two checks decide whether to ask again, not one. The backstop: does the
// cache still name the height right above the last one this pass was handed?
// If not, wantedBlocksFromCache ran clean off the end of what the cache
// holds and a fresh getheaders is owed regardless of anything else — this is
// unchanged from before this comment was written and stays the ultimate
// fallback. The early trigger, checked only when the backstop finds
// something: how many heights does the cache still name above the committed
// tip, independent of the read-ahead depth that capped this pass's own
// wanted range? wantedBlocksFromCache stops at that depth (128 by default)
// long before the cache itself runs out, since one getheaders reply names up
// to 2,000 heights — so waiting for the depth-limited range to reach the
// cache's own end, as the backstop alone does, means the node only ever asks
// once the cache is completely drained. headerCacheRefillThreshold asks
// earlier than that, while there is still cache left to work through, so the
// reply has time to land before the current batch runs out.
//
// Below the last checkpoint, a third condition can also force a send: the
// header cache's own walkIncomplete, true when this cache's checkpoints name a
// checkpoint above the committed tip that its own top has not yet reached (see
// headerCache.NextCheckpointAbove). That walk's usual driver is
// continueCheckpointWalkIfNeeded, sent the instant a reply lands, straight
// back to the peer that answered — this function is its backstop for when
// that peer goes quiet, which is why walkIncomplete is checked regardless of
// headerCacheRefillThreshold: top-best can already be comfortably over that
// threshold (the walk races far ahead of what downloads need) while the walk
// itself still has tens of thousands of heights left to reach its checkpoint,
// and waiting for downloads to eat into that headroom before asking again
// would couple the walk's speed to the download pace it exists to outrun. The
// interval and peer rotation below still apply exactly as they do for the
// download-driven trigger, so a stalled walk retries at the normal cadence
// rather than being asked about on every commit.
//
// The locator is built from the committed tip, through the chain's own
// GetBlockLocator — the same one startSync and headersRoundLocator's other
// callers use — UNLESS walkIncomplete, in which case it is the list's own top
// hash followed by that same tip locator (extendingHeadersLocator), because
// below the last checkpoint every request past the first is answered from
// where the list already reaches, not from the tip a block or two behind it.
// A locator anchored ONLY above what this node has actually committed, with a
// non-zero stop hash, is what had a peer answer truthfully with zero headers
// and stalled the node for seven hours — see
// docs/superpowers/specs/2026-09-11-legacy-sync-stall-800128.md — so the tip's
// own locator entries and the zero stop hash are kept in both branches.
//
// Any eligible peer will do, not only the sync peer: eligibleBlockPeers is
// the same connected, sync-candidate pool the block-download scheduler draws
// from, and a getheaders is a lookup any of them can answer. The send is
// fire-and-forget; the reply lands asynchronously in fillHeaderCache and the
// next pass reads whatever it left there. A failure to build the locator or
// to send still counts as having asked, for rate-limiting purposes: retrying
// a peer or a client call that just failed on every subsequent commit would
// be the same storm this function exists to prevent.
func (sm *SyncManager) maybeRequestMoreHeaders(wanted []wantedBlock) {
	if !sm.headersFirstMode.Load() || sm.headerCache == nil {
		return
	}

	best, _, _ := sm.committedTip()

	last := best
	if n := len(wanted); n > 0 {
		last = wanted[n-1].height
	}

	top, haveTop := sm.headerCache.Top()

	next, checkpointAhead := sm.headerCache.NextCheckpointAbove(best)
	walkIncomplete := checkpointAhead && (!haveTop || top < next.Height)

	if !walkIncomplete {
		if _, ok := sm.headerCache.At(last + 1); ok {
			// The depth cap stopped the pass, not the cache's own end, so the
			// backstop alone has nothing to do here. Whether the early trigger
			// does depends on how much cache is left above the committed tip,
			// not on the depth cap: top-best can be far bigger than the read-ahead
			// depth that limited this pass's own wanted range. Only skip the
			// refill when there is still comfortably more than
			// headerCacheRefillThreshold left to work through.
			if haveTop && top-best >= headerCacheRefillThreshold {
				return
			}
		}
	}

	peers := sm.eligibleBlockPeers()
	if len(peers) == 0 {
		return
	}

	if !sm.allowedToRequestMoreHeadersNow(time.Now()) {
		return
	}

	if sm.blockchainClient == nil {
		return
	}

	best, tipHash, ok := sm.committedTip()
	if !ok {
		return
	}

	var (
		locator blockchain.BlockLocator
		err     error
	)

	if walkIncomplete && haveTop {
		if topHash, ok := sm.headerCache.At(top); ok {
			locator, err = sm.extendingHeadersLocator(topHash)
		} else {
			// The top height Top() just reported is gone from the map: a
			// concurrent Prune or a whole-list drop landed between the two
			// calls. Fall through to the ordinary tip-anchored locator, the
			// same one a genuinely empty list gets below.
			locator, err = sm.headersRoundLocator(&tipHash, uint32(best)) //nolint:gosec // a chain height
		}
	} else {
		locator, err = sm.headersRoundLocator(&tipHash, uint32(best)) //nolint:gosec // a chain height
	}

	if err != nil {
		sm.logger.Warnf("[assignWantedBlocks] could not build a getheaders locator to refill the header cache past height %d: %v", last, err)

		return
	}

	peer := sm.nextHeaderRefillPeer(peers)

	// This call only ever runs because the header cache came up short, so
	// every request from here is a deliberate retry: peer rotation closes
	// the multi-peer case, but with one eligible peer the (locator, stop
	// hash) pair is unchanged from last time — the stop hash is always the
	// zero hash and the locator cannot advance until a block commits — and
	// PushGetHeadersMsg's own dedup filter would otherwise discard it as an
	// accidental duplicate while logging success.
	peer.ForgetLastHeadersRequest()

	if err := peer.PushGetHeadersMsg(locator, &zeroHash); err != nil {
		sm.logger.Warnf("[assignWantedBlocks][%s] failed to send getheaders to refill the header cache past height %d: %v", peer.String(), last, err)

		return
	}

	switch {
	case walkIncomplete:
		sm.logger.Infof("[assignWantedBlocks][%s] the walk toward checkpoint height %d has stalled, retrying", peer.String(), next.Height)
	case haveTop:
		sm.logger.Infof("[assignWantedBlocks][%s] the header cache names %d more heights above the committed tip at %d, asked for more", peer.String(), top-best, best)
	default:
		sm.logger.Infof("[assignWantedBlocks][%s] the header cache is empty above height %d, asked for more", peer.String(), last)
	}
}

// allowedToRequestMoreHeadersNow is the compare-and-swap that makes the
// headerCacheRefillInterval floor safe under concurrent callers: assignWantedBlocks
// runs on the consumer goroutine and, since the park sweep calls fetchHeaderBlocks
// directly on its own goroutine, on that one too. A plain load-then-store here
// would let two callers that both read a stale timestamp both decide to send.
// The loop retries only on a lost CAS race, not on a genuinely-too-recent
// timestamp, so it always terminates.
func (sm *SyncManager) allowedToRequestMoreHeadersNow(now time.Time) bool {
	for {
		last := sm.lastHeaderRequestAt.Load()
		if now.Sub(time.Unix(0, last)) < headerCacheRefillInterval {
			return false
		}

		if sm.lastHeaderRequestAt.CompareAndSwap(last, now.UnixNano()) {
			return true
		}
	}
}

// nextHeaderRefillPeer picks the peer maybeRequestMoreHeaders asks this call,
// rotating one step further through peers than the last call did.
//
// PushGetHeadersMsg on a peer drops a repeat of the same (locator, stop hash)
// pair silently, returning nil as though it had sent. While the header cache
// is empty, neither half of that pair can move: the stop hash is always the
// zero hash, and the locator is built from the committed tip, which cannot
// advance until a block commits. Asking the same peer every call, as taking
// peers[0] used to, sends exactly once and is then filtered forever — with
// one usable peer, the node never asks again. Rotating means a repeat call
// reaches a different peer, whose own dedup state has not seen this pair.
//
// headerRefillPeerIdx only ever counts up, via a single atomic Add, so two
// concurrent callers (the commit path and the park sweep both reach this
// function) always get distinct, increasing slots and never hand out the one
// peer twice for the same logical call. Reducing modulo the current length,
// taken fresh from peers on every call, means a peer list that grew or shrank
// between calls is never indexed out of range.
func (sm *SyncManager) nextHeaderRefillPeer(peers []blockPeer) *peerpkg.Peer {
	idx := sm.headerRefillPeerIdx.Add(1) - 1

	return peers[idx%uint64(len(peers))].peer
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
// best is the last block this node has actually put into the chain, not the
// header list. It comes in as a parameter rather than being read here because
// this function runs with headerMu held and reading the chain is a blocking
// call this package's lock rule has no exception for; wantedBlocks reads it
// before taking the lock and passes it down. Anchoring on the committed height
// alone is what svnode does too: -blockdownloadlowerwindow measures from
// chainActive.Height(), the validated tip.
//
// "No limit at all" is now only the two honest cases: no settings to read the
// configuration from, or the configured lower window is zero or less. A depth
// that engages but then falls back to some other anchor because a data
// structure happened to be empty was never a real "no limit" state, just an
// incidental one.
//
// Clamped to the node-wide window, as svnode clamps its lower window to its
// window: a limit looser than that could never bind.
func (sm *SyncManager) lookaheadCeilingLocked(best int32) (int64, bool) {
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

	// Scaled by the block size actually being seen, because a fixed count of
	// blocks means completely different things at different points in the chain.
	// legacy_blockDownloadLowerWindow is 128, which is 128 KB of read-ahead at
	// height 100,000 and 440 GB of it at height 759,000 where blocks measure
	// 3.44 GB. One number cannot be right for both.
	//
	// This is the bound that matters now, since the two byte budgets that used to
	// sit beside it are gone: a block's size is unknown until it has been
	// downloaded, so a byte bound could only ever discard a block already paid
	// for, and mainnet threw away 1.02 TB in two days doing exactly that. What is
	// left is a count of blocks, checked before the request goes out, which is
	// what SV Node bounds by. A count is only honest if it tracks the era.
	//
	// It also decides how much disk the park can hold, because the park holds
	// what has been fetched and cannot yet be committed. At a flat 128 that is a
	// worst case near 440 GB. Scaled, it is about 20 GB.
	//
	// The scaling reuses the ladder the node already derives from its rolling
	// average block size, rather than introducing a second opinion about what a
	// big block is. That ladder already governs how many blocks one peer may have
	// in flight and how deep the quick window goes; the read-ahead depth was the
	// one bound ignoring it. Its range is 20 for small blocks down to 1 above
	// 2 GB, so the ratio to its own maximum is the scaling factor, and the
	// configured depth is what that ratio applies to.
	//
	// Never below one: a depth of zero would stop the walk asking for anything at
	// all, which is a stall rather than a conservative setting.
	if sm.blockSizeTracker != nil {
		if fetch := sm.blockSizeTracker.calculateMaxInFlightBlocks(); fetch >= 1 && fetch < maxInFlightLadderTop {
			scaled := lower * fetch / maxInFlightLadderTop
			if scaled < 1 {
				scaled = 1
			}

			lower = scaled
		}
	}

	// Anchored to the last COMMITTED block, and that is the whole rule: never ask
	// for a block more than the read-ahead depth above what has been validated.
	//
	// It used to be anchored to the front of the header list, which advances when
	// a block ARRIVES rather than when it commits. That made the ceiling a
	// ratchet driven by downloads: every arrival raised the front, which raised
	// the ceiling, which licensed another depth's worth of requests, with no
	// coupling to the committer at all. Measured on mainnet during a genesis
	// resync on 2026-09-12, the front stood at 4877 with the chain settled at
	// 868 and the park held its full 4096 entries.
	//
	// Anchoring here makes the park self-limiting, so the mechanisms that used to
	// compensate are unnecessary: the park cannot exceed the depth, so it never
	// reaches its entry cap, so no block is ever refused, so no hole is ever
	// punched in the chain of parked blocks. A count of outstanding blocks cannot
	// achieve any of that, because a block 5000 ahead and a block 1 ahead count
	// the same.
	//
	// There is no fallback to a header-list front any more, and none is needed:
	// best is read fresh from the chain by wantedBlocks on every call, so this
	// reads a genuine height on a node restarting mid-chain. A fallback keyed to
	// a structure nothing fills would only have reintroduced the bug this
	// function exists to fix.
	return int64(best) + int64(lower), true
}

// handleHeadersMsg handles block header messages from all peers.  Headers are
// requested when performing a headers-first sync.
func (sm *SyncManager) handleHeadersMsg(hmsg *headersMsg) {
	// No headerMu here, unlike upstream. Upstream holds it for the whole handler
	// because the handler splices into the header list, which this branch
	// deleted: a fill lands in headerCache under that cache's own mutex, and
	// peerStates is its own concurrent map. Holding headerMu would also deadlock,
	// because a successful fill runs fetchHeaderBlocks below, and wantedBlocks
	// takes headerMu, which is not re-entrant. The upstream merge reintroduced
	// exactly that and hung every successful fill.
	sm.logger.Debugf("[handleHeadersMsg] received headers message with %d headers from %s", len(hmsg.headers.Headers), hmsg.peer)
	peer := hmsg.peer

	_, resolved, exists := sm.peerStateResolvingPrimary(peer)
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

	// headersFirstMode alone, without upstream's "|| sm.nextCheckpoint == nil": the
	// stored checkpoint field is gone, and startSync — the one place that turns this
	// flag on — already derives the next checkpoint fresh and refuses to enter
	// headers-first mode when there is none ahead. A second, stored copy of that
	// decision is what went stale and left the mode unreachable.
	if !sm.headersFirstMode.Load() {
		reason := fmt.Sprintf("Got %d unrequested headers from %s", numHeaders, peer.String())
		peer.DisconnectWithWarning(reason)

		return
	}

	// Nothing to do for an empty headers message.
	if numHeaders == 0 {
		return
	}

	// A headers batch is a cache fill and nothing else: no splice, no front, no
	// anchor, no epoch, no list. A batch whose front has gone behind the
	// committed tip is not a batch that failed to link — the tip moved into it
	// while it was in flight, and Fill keeps the part still above the tip. Only
	// a batch that does not reach above the tip at all is refused, and the peer
	// keeps its connection either way, because a reply that connects to a point
	// we have moved past is an honest answer to a question we have stopped
	// asking.
	//
	// The wanted-range pass reads sm.headerCache: wantedBlocks (in
	// wanted_range_assign.go) calls wantedBlocksFromCache, which is the only
	// consumer of what a fill lands here.
	if !sm.fillHeaderCache(hmsg.peer, msg) {
		return
	}

	// A fill is the one event that makes a run of heights wantable without a
	// block having arrived or a block having committed, and nothing else was
	// watching for it. An assignment pass runs on a commit, on a block landing,
	// on a peer connecting, or on the park sweep's ticker — and at a cache
	// boundary the first three are all quiet by definition, because the node
	// has run out of heights to ask for. That left the sweep, at
	// parkSweepInterval (30 seconds), as the only thing that would notice.
	// Measured on mainnet on 2026-09-14: the fill landed at 13:12:40 and the
	// next block was accepted at 13:13:10, exactly one sweep interval later,
	// and the same 30-second fill-to-first-block delay appears at the 13:10:02
	// boundary.
	//
	// Called inline rather than on a goroutine of its own. This function is
	// already running on one: the handler's select dispatches headers with
	// "go sm.handleHeadersMsg(msg)", so a pass here delays no block message,
	// and spawning again would add an unbounded goroutine per headers message
	// for nothing. The lock rule is satisfied too — fillHeaderCache takes only
	// the cache's own mutex and releases it before returning, so headerMu is
	// free when assignWantedBlocks reaches for it inside wantedBlocks.
	sm.fetchHeaderBlocks()
}

// blockOrigin returns the ancestry proof for a block, read from the header cache.
//
// This is the replacement for upstream's blockOrigin, which read a per-peer
// requestedBlocks map that fetchHeaderBlocks had stamped while walking the header
// list. Neither structure exists here. What does exist is stronger in the one way
// that matters and weaker in one way that does not:
//
//   - stronger, because the cache holds ONE run, verified to be internally linked
//     and rooted in this node's own committed tip, and it is replaced whole rather
//     than appended to. Upstream's list could be extended past the point that had
//     actually been verified, which is the hole its second security commit had to
//     go back and close with headerNodeProven.
//   - weaker, because it is read at delivery rather than stamped at request, so a
//     refill that lands while a block is on the wire can lose the proof. That
//     direction costs full validation and nothing else. See headerCache.Proven.
//
// Every route that cannot show the run gets the zero value: peer advertisements,
// blocks adopted from disk by park recovery, and anything re-requested outside the
// wanted range.
func (sm *SyncManager) blockOrigin(blockHash chainhash.Hash) blockRequestOrigin {
	return blockRequestOrigin{headerProven: sm.headerCache.Proven(blockHash)}
}

// fillHeaderCache turns a headers batch into the cache the wanted range reads,
// and reports whether anything was cached.
//
// The batch must contain the block this node has committed, which is what the
// tip-anchored locator asks for — contain it, not begin just above it: the tip
// moves under the reply while it is in flight, and Fill keeps whatever part of
// the run still sits above the tip when it arrives. A batch that does not
// connect at all is dropped without blaming the sender: under the new model the
// locator steps back to genesis, so a peer answering from an older shared
// ancestor is answering correctly, just about a point this node has already
// passed.
//
// The return value is what handleHeadersMsg gates its assignment pass on, so
// only a batch that actually left heights behind triggers one.
func (sm *SyncManager) fillHeaderCache(peer *peerpkg.Peer, msg *wire.MsgHeaders) bool {
	if len(msg.Headers) == 0 {
		return false
	}

	// Read as one value, height and hash together, so the batch can never be
	// keyed under a height the hash no longer belongs to: a caller reading the
	// height and then making a second, separate call for the hash could see a
	// commit land in between and key the batch wrong. committedTip's single
	// GetBestBlockHeader call rules that out. It also means this always judges
	// linkage against the chain's real tip, including a tip a same-height reorg
	// just replaced: the stored, monotonic-on-height copy this used to read
	// could never be updated by such a reorg, so the cache refused every batch
	// forever until the process restarted.
	best, tipHash, ok := sm.committedTip()
	if !ok {
		sm.logger.Debugf("[fillHeaderCache] no committed tip recorded yet, dropping %d headers from %s", len(msg.Headers), peer)

		return false
	}

	// Read before Fill, which below the last checkpoint may EXTEND this cache
	// rather than replace it (see headerCache.Fill and extendLocked). Both are
	// needed afterward purely to report what happened correctly — the log line
	// below and the "list dropped" disconnect on the failure path — neither of
	// which Fill itself is in a position to say: it has no logger, and on a
	// wrong-hash drop it has already reset the state that would say so.
	prevTop, havePrevTop := sm.headerCache.Top()
	prevProven := sm.headerCache.ProvenTo()
	_, checkpointAhead := sm.headerCache.NextCheckpointAbove(best)

	if !sm.headerCache.Fill(tipHash, best+1, msg.Headers) {
		// Two different refusals arrive here as one false, and only one of them is
		// the peer's fault. Fill refuses a run that does not reach above the
		// committed tip, which is an honest answer to a question this node has
		// stopped asking, and it refuses a run that reaches a checkpoint height
		// carrying the wrong hash, which is a lie about the certified chain.
		// Re-deriving which it was costs one walk of the batch and is worth it,
		// because upstream disconnects for the second (handleHeadersMsg's
		// "does NOT match expected checkpoint hash") and this branch lost that
		// defence along with the header list.
		if cp := sm.contradictedCheckpoint(best+1, msg.Headers); cp != nil {
			peer.DisconnectWithWarning(fmt.Sprintf("block header at height %d does NOT match the expected checkpoint hash %s", cp.Height, cp.Hash))

			return false
		}

		// Below the last checkpoint, once this cache already named heights above
		// the tip, Fill judges the batch against the list's own top instead of
		// best+1 (extendLocked) — so a wrong hash reached that way shows up only
		// when the batch is re-walked from prevTop+1, never from best+1 above.
		// A match here means Fill has already wiped every entry the dropped run
		// had proven, because they were all one linked chain with the batch that
		// just failed to agree; this block is purely the classification for the
		// disconnect and the log, not the decision itself.
		if havePrevTop && prevTop >= best+1 && checkpointAhead {
			if cp := sm.contradictedCheckpoint(prevTop+1, msg.Headers); cp != nil {
				sm.logger.Warnf("[fillHeaderCache][%s] header list dropped: the walk reached checkpoint height %d without the pinned hash %s", peer, cp.Height, cp.Hash)
				peer.DisconnectWithWarning(fmt.Sprintf("block header at height %d does NOT match the expected checkpoint hash %s", cp.Height, cp.Hash))

				return false
			}
		}

		sm.logger.Debugf("[fillHeaderCache] batch of %d headers from %s does not reach above the committed tip at height %d, dropping it", len(msg.Headers), peer, best)

		return false
	}

	// Read the new top back from the cache rather than computed from the batch
	// length: Fill drops whatever prefix does not belong, and below the last
	// checkpoint it may append onto prevTop instead of starting at best+1, so
	// the batch's own length no longer names either end on its own.
	top, _ := sm.headerCache.Top()

	// The low end of what THIS call added. extended mirrors exactly the
	// condition Fill itself used to choose extendLocked over replaceLocked: an
	// extending fill appended onto prevTop, a replacing one starts at best+1
	// regardless of what the cache held before. Getting this wrong would have
	// the one log line an operator panel parses report the whole accumulated
	// list's size against a single reply's header count.
	extended := havePrevTop && prevTop >= best+1 && checkpointAhead

	low := best + 1
	if extended {
		low = prevTop + 1
	}

	cached := top - low + 1

	sm.logger.Infof("[fillHeaderCache] cached %d of %d headers from %s, heights %d to %d", cached, len(msg.Headers), peer, low, top)

	if newProven := sm.headerCache.ProvenTo(); newProven > prevProven {
		sm.logger.Infof("[fillHeaderCache][%s] header walk matched checkpoint at height %d", peer, newProven)
	}

	sm.continueCheckpointWalkIfNeeded(peer, best, top)

	return true
}

// continueCheckpointWalkIfNeeded sends the next getheaders immediately when
// the header list has not yet reached the checkpoint it is walking toward,
// rather than waiting out headerCacheRefillInterval. That interval exists to
// stop a dry cache being asked about again on every commit while a reply is
// already in flight; a reply has just landed here, so there is nothing in
// flight to duplicate, and at a flat 5 seconds per round trip a 28,000-height
// gap between mainnet checkpoints would take roughly two minutes to walk
// instead of however long the network actually takes.
//
// Asks the peer that just answered, not a rotated one: it has just proved it
// holds this chain and is reachable, and asking anyone else here would be a
// second request for the same range before the first has even had a chance to
// answer again. Rotation on a stalled walk is still maybeRequestMoreHeaders'
// job — see its own doc for how it now also owns this walk as a backstop.
//
// lastHeaderRequestAt is still updated on a successful send, so the periodic
// path this same call chain reaches a moment later (fetchHeaderBlocks, called
// unconditionally by handleHeadersMsg after this) does not also fire and send
// a second, redundant request for the same range.
//
// Guarded on peer.Connected(): every production caller's peer is connected by
// construction, but several of this package's tests fill the cache directly
// through fillHeaderCache with a bare, unconnected test peer that was never
// meant to receive a real getheaders, and this must not be the thing that
// changes that.
func (sm *SyncManager) continueCheckpointWalkIfNeeded(peer *peerpkg.Peer, best, top int32) {
	if !sm.headersFirstMode.Load() || !peer.Connected() {
		return
	}

	next, ok := sm.headerCache.NextCheckpointAbove(best)
	if !ok || top >= next.Height {
		return
	}

	topHash, ok := sm.headerCache.At(top)
	if !ok {
		return
	}

	locator, err := sm.extendingHeadersLocator(topHash)
	if err != nil {
		sm.logger.Warnf("[fillHeaderCache][%s] could not build a locator to continue the walk toward checkpoint height %d: %v", peer, next.Height, err)

		return
	}

	peer.ForgetLastHeadersRequest()

	if err := peer.PushGetHeadersMsg(locator, &zeroHash); err != nil {
		sm.logger.Warnf("[fillHeaderCache][%s] failed to send the immediate continuation toward checkpoint height %d: %v", peer, next.Height, err)

		return
	}

	sm.lastHeaderRequestAt.Store(time.Now().UnixNano())

	sm.logger.Infof("[fillHeaderCache][%s] the walk has not yet reached checkpoint height %d, asked immediately for more", peer, next.Height)
}

// extendingHeadersLocator builds the locator for a request that continues a
// below-checkpoint walk already under way: the current list top's hash first,
// so a peer that has it answers immediately from where the walk left off,
// followed by the ordinary committed-tip locator so an honest peer that has
// fallen behind the walk — or one that never had the top hash at all — still
// finds a shared ancestor rather than replying about a point it has moved
// past.
//
// See docs/superpowers/specs/2026-09-11-legacy-sync-stall-800128.md: a locator
// anchored ONLY above the committed tip, with a non-zero stop hash, produced a
// truthful empty reply that stalled the node for seven hours, because the
// range asked about was one the peer had already answered in full. Keeping the
// tip's own locator entries after the top hash, and leaving the stop hash at
// the zero hash exactly as every other getheaders in this package does, is
// what keeps this request safe in the same way: a peer that cannot place the
// top hash at all still has every reason to answer from the tip instead of
// answering "nothing" about a range it considers already closed.
func (sm *SyncManager) extendingHeadersLocator(topHash chainhash.Hash) (blockchain.BlockLocator, error) {
	best, tipHash, ok := sm.committedTip()
	if !ok {
		return nil, errors.NewProcessingError("no committed tip recorded yet")
	}

	tipLocator, err := sm.headersRoundLocator(&tipHash, uint32(best)) //nolint:gosec // a chain height
	if err != nil {
		return nil, err
	}

	locator := make(blockchain.BlockLocator, 0, len(tipLocator)+1)
	locator = append(locator, &topHash)
	locator = append(locator, tipLocator...)

	return locator, nil
}

// contradictedCheckpoint returns the checkpoint a headers batch disagrees with, or
// nil if it disagrees with none.
//
// It walks the batch as delivered, so the height it assigns each header is
// baseHeight plus its index — the same arithmetic Fill uses for the part of the
// run it keeps. That is an over-approximation for a batch whose front has slipped
// behind the committed tip, where Fill drops a prefix and re-bases: a header at a
// checkpoint height by this arithmetic may not be at that height at all. It is the
// safe over-approximation, though, and only on a batch Fill has ALREADY refused,
// so the worst it can do is blame a peer for a batch that was going to be dropped
// anyway. A batch whose front is behind the tip and whose linkage holds is never
// refused by Fill, so it never reaches here.
//
// Nil checkpoints, or a batch reaching no checkpoint height, both answer nil.
func (sm *SyncManager) contradictedCheckpoint(baseHeight int32, headers []*wire.BlockHeader) *chaincfg.Checkpoint {
	if sm.chainParams == nil || len(headers) == 0 {
		return nil
	}

	checkpoints := sm.chainParams.Checkpoints
	top := baseHeight + int32(len(headers)) - 1 //nolint:gosec // a batch index, bounded by the wire limit

	for i := range checkpoints {
		cp := &checkpoints[i]
		if cp.Hash == nil || cp.Height < baseHeight || cp.Height > top {
			continue
		}

		if hash := headers[cp.Height-baseHeight].BlockHash(); !hash.IsEqual(cp.Hash) {
			return cp
		}
	}

	return nil
}

// punishUnrequestedBlock reports whether a block nobody asked for should cost
// its sender the connection.
//
// It should not, while the node is catching blocks. SV Node never punishes an
// unrequested block at all: its own equivalent check only decides whether to
// force processing, and the block is processed either way. Below a checkpoint
// the block has already passed proof of work, its hash is the hash of a header
// we can check, and the chain rejects it if it is wrong, so the disconnect
// buys nothing validation is not already buying, and costs a supplier during
// the one phase where suppliers are scarce.
//
// It is also the reason the download ledger is as complicated as it is. Because
// a late or surprise copy is punished, an assignment has to be recorded before
// the request goes out, which means a peer that has gone quiet cannot simply
// have its assignment revoked, revoking would make its in-flight copy look
// unrequested. Hence a forgiven flag that keeps permission while dropping
// obligation, a separate retry window, and a re-assert operation. Remove the
// punishment and all three become deletable.
//
// At the tip the flood defence still applies, and on regtest nothing is ever
// punished because the regression harness feeds unrequested blocks on purpose.
func (sm *SyncManager) punishUnrequestedBlock(catchingBlocks bool) bool {
	if sm.chainParams == &chaincfg.RegressionNetParams {
		return false
	}

	return !catchingBlocks
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

	// One lookup, two outcomes. A block we already know of gives us the
	// announcer's height for nothing; a block we cannot place is an opportunity
	// to repair the header chain, and the else arm takes it.
	if lastBlock != -1 {
		_, blockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &invVects[lastBlock].Hash)
		if err == nil {
			blockHeightInt32, err := safeconversion.Uint32ToInt32(blockHeaderMeta.Height)
			if err != nil {
				sm.logger.Errorf(failedToConvertBlockHeightInt32Msg, err)
			}

			peer.UpdateLastBlockHeight(blockHeightInt32)
			state.noteBestKnownHeight(blockHeightInt32)
			// Announced a block we hold, so we know its height without taking
			// the peer's word for anything.
			state.noteProvenClaim(invVects[lastBlock].Hash, blockHeightInt32)
		} else {
			// A block we cannot place. This is the recovery route that does not
			// run through the headers round, and it is the one that would have
			// ended the 2026-09-11 Hetzner mainnet stall: the node held headers
			// to 849,999 with a committed tip of 800,128, its headers round
			// asked a question whose only honest answer was zero headers, and it
			// sat idle for seven hours while peers went on announcing a block
			// roughly every ten minutes.
			//
			// Recovery is one block interval, not seconds, and it needs a sync
			// peer. handleInvMsg returns early above on peer != sp && !current(),
			// and current() is false throughout a deep sync, so during IBD only
			// the ELECTED sync peer's announcements reach this arm. That is not a
			// defect of this code but it does bound what it buys: in the observed
			// stall the role rotated every 3m30s across eight peers, so each got
			// a turn well inside one block interval. Every test in
			// inv_repair_test.go makes the announcer the sync peer, which bakes
			// the restriction into the harness; it is stated here because the
			// harness cannot state it. A getheaders anchored on the committed
			// tip, sent on one of those announcements, is answered from the tip
			// forward: a batch that connects, is cached, and releases
			// fetchHeaderBlocks.
			//
			// Remembering the announcer is half the value and is done whatever
			// else happens here. It is SV Node's UpdateBlockAvailability on the
			// same path (net_processing.cpp:2426, and again on an unconnecting
			// headers batch at :3405): without it the peer that told us about
			// the block is not usable as a download source when its headers
			// finally arrive.
			announced := invVects[lastBlock].Hash
			state.notePendingClaim(announced)

			// Gated on headers-first mode being ON, which SV Node does not do —
			// it acts on a block inv in every state. The gate is forced by our
			// own code: handleHeadersMsg disconnects any peer that answers a
			// getheaders sent while headersFirstMode is false ("Got %d
			// unrequested headers"), so asking outside the mode would cost us
			// the peer that helped.
			//
			// headersRoundLocator anchored on the committed tip, the same
			// locator startSync uses, rather than a header-list-anchored one:
			// there is no list any more to anchor on. That does cost a
			// blockchain client call this repair path did not used to make, but
			// it is the only source left for a locator that walks back through
			// real ancestry, and this path fires rarely enough (an unresolved
			// inv, not every block) that the cost is not a hot-path concern.
			//
			// The stop hash is the ANNOUNCED block, as in SV Node
			// (net_processing.cpp:2440), and it varies per announcement — which
			// matters here more than it does there, because PushGetHeadersMsg
			// filters a repeat of the same (locator[0], stopHash) pair for the
			// peer's whole lifetime with no expiry (peer.go:1132-1142). The
			// round's own request has a constant key and can be swallowed by
			// that filter; this one cannot.
			//
			// No getdata, ever. SV Node removed exactly that send and says why at
			// net_processing.cpp:2429-2435: falling back to an inv usually means
			// a reorg, whose headers are needed before any block is worth asking
			// for.
			if sm.headersFirstMode.Load() && !sm.blockDownloads.RequestedWithin(announced, blockRequestRetryInterval) {
				if tipHeight, tipHash, ok := sm.committedTip(); ok {
					if locator, lerr := sm.headersRoundLocator(&tipHash, uint32(tipHeight)); lerr != nil { //nolint:gosec // a chain height
						sm.logger.Warnf("[handleInvMsg] could not build repair getheaders locator for announced block %s: %v", announced, lerr)
					} else if len(locator) > 0 {
						if err := peer.PushGetHeadersMsg(locator, &announced); err != nil {
							sm.logger.Warnf("[handleInvMsg] Failed to send repair getheaders for announced block %s to peer %s: %v", announced, peer, err)
						}
					}
				}
			}
		}
	}

	// Transaction announcements only. Blocks are accepted in every state, and
	// have to be: past the last checkpoint headers-first mode is off, and then an
	// inv is the only way this node hears that a block exists. The Kafka
	// listeners downstream are wired the same way, with the block listener
	// unconditionally enabled and the transaction listener gated on RUNNING.
	//
	// The name and the state read stay as they are; only the comment was wrong,
	// and it claimed to cover blocks for long enough that two readers reported
	// the switch below as a missing gate.
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
		// Deliberately empty, and Go does not fall through. A block
		// announcement is taken in every FSM state, because past the last
		// checkpoint headers-first mode is off and an inv is then the only way
		// this node learns a block exists. Gating it on RUNNING would leave a
		// node that is catching blocks with no block discovery at all.
	case wire.InvTypeTx:
		if !processInvs {
			// A transaction we are not going to validate yet is a transaction
			// not worth fetching. Blocks are the other case above.
			sm.logger.Debugf("[handleInvMsg] Ignoring transaction inv from %s, not in running state", peer)
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

	// handedOff is closed by the consumer once this block's memory has been
	// charged to the budget that owns it next: the window's byte charge for a
	// dispatched block, the park's byte budget for a parked one. The peer's
	// awaiting goroutine gives the download bytes back when it closes, rather
	// than holding them through validation and keeping other peers from reading.
	// Nil when nobody is waiting, which is every manager a test builds by hand.
	handedOff chan struct{}
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

	// The dispatcher owns the block queue from here: it runs every pre-check and
	// every chain-order step on this one goroutine and hands only the block's own
	// work (HandleBlockDirect) to a worker, so up to K consecutive below-checkpoint
	// blocks can have their UTXO store work in flight while their tails still run in
	// dispatch order. Nil-guarded because tests build SyncManager as a struct
	// literal that bypasses New().
	if sm.dispatcher == nil {
		sm.dispatcher = newBlockDispatcher(sm)
	}

	// start the block queue handler
	sm.consumerDone = make(chan struct{})

	go func() {
		defer close(sm.consumerDone)

		sm.dispatchBlocks(blockQueue)
	}()

	// One pass over whatever Recover adopted, now that the consumer above is
	// started and has somewhere to hand a committable block to. Must run before
	// the sweep goroutine below, or a full park would sit through up to
	// parkStuckThreshold's worth of the sweep's own thirty-second cadence before
	// anything asked about blocks this pass would have settled at once.
	sm.reconcileRecoveredParents(sm.ctx)

	// The park sweep gets a goroutine of its own rather than a ticker arm on the
	// consumer. It used to share the goroutine that committed blocks in order,
	// and a slow tick there held up commits; under the dispatcher that goroutine
	// admits blocks, and the ordering guarantee comes from block validation's own
	// single committer, so a slow tick there would only delay admission of blocks
	// that have nothing to do with the park. The sweep decides, looks parents up,
	// evicts and deletes on its own; the one thing it may not do is commit, and
	// it posts those back to the consumer through parkCommits.
	go sm.runParkSweep()

out:
	for {
		select {
		case <-ticker.C:
			sm.handleCheckSyncPeer()

			// Deliberately on this goroutine and not the block loop's: a report
			// the stuck goroutine had to print could never be printed. Folded
			// onto the sync-peer ticker now that nothing else needs a five-second
			// tick of its own; consumerStallAfter (90s) and
			// consumerStallReportInterval (60s) are both comfortably above this
			// ticker's 30-second interval, so a stall is still caught and
			// reported well inside its own thresholds.
			sm.reportConsumerStall(time.Now())
		case m := <-sm.msgChan:
			// whenever legacy receives a message, check if we are current
			// this call should have the current state cached, so it should be fast
			currentState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
			if err != nil {
				sm.logger.Errorf("[SyncManager] failed to get fsm current state")
			}

			// Only observed catchup needs automatic promotion. This cached
			// prefilter also avoids the expensive current() check while parked.
			if err == nil && currentState != nil && *currentState == teranodeblockchain.FSMStateCATCHINGBLOCKS {
				if sm.current() { // only call this when we are not in the running state, it's an expensive call
					sm.logger.Infof("[SyncManager] Legacy reached current, sending RUN event to FSM")
					if err = sm.runIfCatchingBlocks("legacy/netsync/manager/blockHandler"); err != nil {
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

			case *blockOnDiskMsg:
				// A body already on disk. No budget to release and no reply to
				// send: the read loop was free the moment the bytes landed.
				sm.handleBlockOnDiskMsg(msg)

			case *blockMsg:
				sm.logger.Debugf("[blockHandler][%s] queueing block for validation", msg.block.Hash())

				// A 0->1 transition opens a fresh backpressure window: stamp its
				// start so localReadBackpressured can tell slow-but-progressing
				// validation from a genuine processing hang. Enqueues into an
				// already-non-empty backlog deliberately do NOT stamp — only
				// completions advance processing, so letting a peer refresh the
				// liveness signal merely by feeding more blocks into a hung
				// pipeline would mask the hang.
				sm.blockBacklog.Add(1)

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
					handedOff:   msg.handedOff,
				}:
				case <-sm.quit:
					// Enqueue aborted on shutdown: undo the Add(1) above. Nothing
					// committed, so nothing is stamped.
					sm.blockBacklog.Add(-1)

					if msg.reply != nil {
						msg.reply <- errors.NewServiceError(syncManagerShuttingDownMsg)
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
func (sm *SyncManager) QueueBlock(block *bsvutil.Block, peer *peerpkg.Peer, done chan error, handedOff ...chan struct{}) {
	// Don't accept more blocks if we're shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		done <- nil
		return
	}

	msg := &blockMsg{block: block, peer: peer, reply: done}

	// Variadic so the many callers that do not run under the prefetch path, the
	// regtest tooling and this package's tests among them, are unchanged.
	if len(handedOff) > 0 {
		msg.handedOff = handedOff[0]
	}

	sm.msgChan <- msg
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

	// Whenever the park is enabled, streaming (and so conversion) is
	// unconditional, and the block's bytes are gone by the time this runs: the
	// wire layer streamed them through the subtree builder and out to files, and
	// what OnBlock holds is a handle. Charging the serialized size would reserve
	// hundreds of megabytes against a fixed pool for memory nobody is holding, and
	// a read loop parked in this acquire reads nothing further from its socket —
	// which is how a peer-wide byte counter once switched the frontier racer off.
	//
	// One slot per in-flight block is what this path can honestly pay, and it is
	// the unit SV Node bounds by. The semaphore, the dedup set and every release
	// path are unchanged: the weight is chosen here and handed back verbatim.
	//
	// sm.blockPark.Enabled() is the same test installStreamingBlockPath uses to
	// decide whether the pipeline sink is installed at all: with the park
	// enabled, every block this function is ever called for arrived through
	// admitPipelineSink; with it disabled, the wire layer never streams and the
	// only caller left is OnBlock's decoded path, where the byte-weighted branch
	// below is the correct one.
	weight := size
	if sm.blockPark != nil && sm.blockPark.Enabled() {
		weight = 1
	} else {
		// Floor the weight so a flood of tiny blocks can't admit an unbounded
		// number of in-flight goroutines within the byte budget, then clamp to
		// the budget so an oversized block is admitted alone (and budgets
		// smaller than the floor still process one block at a time rather than
		// deadlocking). Neither applies to the slot path above: 1 is always
		// payable against a budget that is itself sized as a count of at least 1.
		if weight < minInFlightBlockWeight {
			weight = minInFlightBlockWeight
		}
		if weight > sm.blockPrefetchBudgetBytes {
			weight = sm.blockPrefetchBudgetBytes
		}
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
	sm.inFlightBlocks[blockHash] = &inFlightBlock{}
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
		sm.blockPrefetchReserved.Add(weight)

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

	sm.blockPrefetchReserved.Add(weight)

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
	sm.ReleaseBlockPrefetchBytes(blockHash, weight)
	sm.ReleaseBlockPrefetchHash(blockHash)
}

// inFlightBlock is what the admission gate remembers about one block between its
// acquire and its departure from the pipeline. Its presence is the dedup half of
// the gate; bytesReleased is what makes the byte half exactly once.
type inFlightBlock struct {
	bytesReleased bool
}

// ReleaseBlockPrefetchHash drops a block's hash from the in-flight dedup set,
// which is the half of the admission gate that stops a second copy of a block
// being validated while the first is still in the pipeline. It runs when the
// block leaves that pipeline, which is when its reply is sent.
func (sm *SyncManager) ReleaseBlockPrefetchHash(blockHash chainhash.Hash) {
	if sm.blockPrefetchBudget == nil {
		return
	}

	sm.inFlightBlocksMu.Lock()
	delete(sm.inFlightBlocks, blockHash)
	sm.inFlightBlocksMu.Unlock()
}

// ReleaseBlockPrefetchBytes gives a block's byte weight back to the download
// budget. It runs as soon as the block's memory has been charged to whichever
// budget owns it next, which is earlier than the reply and is the point of the
// split.
//
// The two halves used to share one lifetime, deliberately, so that neither could
// drift from the other. The dedup half still ends at the reply. The byte half
// must not, because the budget is acquired AFTER a block has been read off the
// wire, in OnBlock, and a read loop blocked in that acquire cannot read its next
// message at all. Holding the bytes through validation therefore stops other
// peers downloading: measured on mainnet at height 752,100 with a 256 MiB budget,
// one or two blocks in flight and five or six read loops blocked, on a link
// delivering 27 MB/s.
//
// Nothing becomes unbounded. A dispatched block's memory is charged to the
// window's byte budget in blockDispatcher.dispatch, and a parked block's to the
// park's own budget in blockPark.Admit. What the split removes is the download
// budget double-counting memory another budget is already accounting for, and it
// is that second count which shuts the peers out.
//
// Exactly-once is the caller's to guarantee, and awaitBlockResult is the only
// caller for a live block: it holds the weight from its own successful acquire
// and releases it on whichever of the hand-off and the reply comes first.
// Releasing a weight twice, or one never acquired, panics the semaphore on a
// peer's read loop.
func (sm *SyncManager) ReleaseBlockPrefetchBytes(blockHash chainhash.Hash, weight int64) {
	if sm.blockPrefetchBudget == nil || weight <= 0 {
		return
	}

	sm.inFlightBlocksMu.Lock()

	if b, ok := sm.inFlightBlocks[blockHash]; ok {
		if b.bytesReleased {
			sm.inFlightBlocksMu.Unlock()

			return
		}

		b.bytesReleased = true
	}

	sm.inFlightBlocksMu.Unlock()

	sm.blockPrefetchBudget.Release(weight)
	sm.blockPrefetchReserved.Add(-weight)
}

// committedTip reads the chain's own best block header and returns its height
// and hash together, taken from the same response so a caller can never see
// one belonging to a different block than the other. false when there is no
// blockchain client, the call fails, or the chain has no best block yet;
// height and hash are the zero value in that case.
//
// It replaces a stored, atomically-swapped copy of the same pair that was
// seeded once at startup and updated only when this package's own commits
// advanced it. A chain advance by any other route left that copy behind for
// good, and its update refused any height not strictly greater than the one
// it already held — a rule meant to stop a reorg's lower tip from reviving
// blocks a floor had already been right to evict, back when the copy was also
// the park's eviction floor. That reader was removed earlier in this branch,
// leaving the monotonic rule with nothing left to protect and one failure mode
// it actively caused: a same-height reorg can never produce a height strictly
// greater than what is stored, so the copy's hash could never be replaced, and
// fillHeaderCache — which requires an incoming batch's first header to link to
// that exact hash — refused every batch forever, because peers answer from the
// chain that is actually current. Reading the chain directly has neither
// failure mode, at the cost of a blockchain round trip on every call.
func (sm *SyncManager) committedTip() (height int32, hash chainhash.Hash, ok bool) {
	if sm.blockchainClient == nil {
		return 0, chainhash.Hash{}, false
	}

	header, meta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil || header == nil || meta == nil {
		return 0, chainhash.Hash{}, false
	}

	h, err := safeconversion.Uint32ToInt32(meta.Height)
	if err != nil {
		return 0, chainhash.Hash{}, false
	}

	return h, *header.Hash(), true
}

// noteChainProgress records that a block joined the chain. Nothing else counts,
// and the name is the whole of the rule.
//
// localReadBackpressured reads it to tell slow-but-progressing validation from a
// hung pipeline, and suppresses the sync-peer stall check while the former is
// true. It used to be stamped by any queue message finishing, which was right
// while every finished message was a commit and became wrong when the park
// started deferring blocks instead: a node doing nothing but parking looked
// exactly like a node committing steadily.
//
// The stamp is shared across peers, which is what made that worse than it
// sounds. One peer's stream of out-of-order blocks kept the suppression open
// over a different peer's silence, so the stall check never ran and the silent
// peer was never rotated.
//
// What this does NOT do is rotate a peer that keeps delivering out-of-order
// blocks. HandleBlockDirect refreshes that peer's own last-block time at
// receipt, deliberately, so a multi-gigabyte block's minutes of validation are
// not read as a stall. A peer that answers every request is doing what was asked
// of it; a peer that answers nothing for maxLastBlockTime is the one this
// restores rotation for.
func (sm *SyncManager) noteChainProgress() {
	sm.lastChainProgress.Store(time.Now().UnixNano())
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

		return time.Since(time.Unix(0, sm.lastChainProgress.Load())) < sm.blockProcessingStallTimeout()
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
// A notfound is the peer discharging a getdata honestly, and the thing it has to
// change is the one the log-only handler this replaces left alone: the peer is
// no longer down for that block, so it stops holding a queue slot it can never
// fill. Nothing needs telling that the block is wanted again — the wanted-range
// pass recomputes what it wants and who owes it on every call, so a released
// block is simply unowed on the next pass.
//
// It happens legitimately: take() deliberately falls back to a peer that has not
// claimed the height rather than stopping the walk, and a pruned peer answers
// notfound to every request for an old block.
//
// Only blocks this peer actually owed are touched, so a notfound naming a hash
// somebody else is carrying — or one we never asked for — discharges nobody.
// Releasing rather than back-dating is deliberate: the peer has told us its copy
// is not coming, so holding it to an obligation it has already answered would
// spend its queue slot on nothing for the rest of the hour.
//
// This runs on the caller's goroutine, which is the peer's read loop. It takes
// the peer-state map and the download ledger's leaf lock, both held for pure
// in-memory work, so the read loop is never parked on I/O.
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

	// The ledger is keyed by association primaries, so a notfound arriving on a
	// stream sub-peer has to be resolved before anything is asked of it. Under
	// the BlockPriority policy the getdata goes out on one stream and the reply
	// can land on another, and every sibling receive path resolves first for
	// exactly that reason. Without this the loop below matches nothing, and the
	// handler silently does the one thing it was written to stop: the peer keeps
	// the assignment for the whole ownership ceiling, its in-flight slot stays
	// charged, and the block sits wanted with nobody owing it.
	//
	// A peer we have no state for resolves to itself, which is the behaviour this
	// handler already had: whatever that peer owes the ledger is still released,
	// and a peer that owes nothing releases nothing either way.
	_, owner, _ := sm.peerStateResolvingPrimary(peer)
	if owner != peer {
		sm.logger.Debugf("[NotFound] resolved stream peer %s to primary peer %s", peer, owner)
	}

	released := make([]chainhash.Hash, 0, len(notFound.InvList))

	for _, iv := range notFound.InvList {
		if iv.Type != wire.InvTypeBlock {
			continue
		}

		if !sm.blockDownloads.HasOwner(owner, iv.Hash) {
			continue
		}

		sm.blockDownloads.RemoveOwner(owner, iv.Hash)

		released = append(released, iv.Hash)
	}

	if len(released) == 0 {
		return
	}

	sm.logger.Infof("[NotFound] released %d blocks %s says it does not have", len(released), peer.String())
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
	// No RPCs are made here. blockHandler makes the one pass that reconciles
	// these against the chain (reconcileRecoveredParents), right after it starts
	// the consumer that a hand-off needs somewhere to go; the park sweep still
	// runs behind that as a safety net for a parent committed by something other
	// than legacy sync, which fires no event either of the other two mechanisms
	// listens for.
	sm.blockPark.Recover(sm.ctx, sm.subtreeStore)

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

	// The block-queue consumer's quit arm is what restores a park entry whose
	// dispatch was still in flight, and the restore is only a guarantee if
	// somebody waits for it. Bounded by the deadlined client calls the consumer
	// can be inside, so it costs shutdown latency rather than risking it hanging.
	// Nil on a manager whose handler never ran.
	if sm.consumerDone != nil {
		<-sm.consumerDone
	}

	// The workers select on sm.quit, and one that is mid-write finishes that
	// write first: the blob store call carries its own deadline, so this waits
	// for at most legacy_parkStoreTimeout.
	sm.parkWorkers.Wait()

	sm.orphanTxs.Stop()
	sm.requestedTxns.Stop()

	if sm.blockFailureBackoff != nil {
		sm.blockFailureBackoff.Stop()
	}

	if sm.recentlyFailedBlocks != nil {
		sm.recentlyFailedBlocks.Stop()
	}

	if sm.blockCorruptAttempts != nil {
		sm.blockCorruptAttempts.Stop()
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
//
// It reads syncPeer under its mutex rather than round-tripping through
// msgChan. The old message path could block for ever: reply was unbuffered and
// blockHandler is its only responder, so a call racing SyncManager.Stop had
// nothing left to answer it. The value is identical either way — storeSyncPeer
// is the only writer and takes the same lock — and this keeps a caller off
// blockHandler, which is the sync manager's single serialization point for
// disconnects, sync-peer rotation, inv, headers and tx dispatch.
func (sm *SyncManager) SyncPeerID() int32 {
	if sp := sm.loadSyncPeer(); sp != nil {
		return sp.ID()
	}

	return 0
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

	// Derived from the same settings inputs as the peer layer's download budget,
	// so the ledger cannot expire an assignment while that budget is still
	// legitimately keeping the same transfer alive.
	assignmentCeiling := blockRequestAssignmentCeiling(tSettings, config.ChainParams)

	sm := SyncManager{
		ctx:          ctx,
		settings:     tSettings,
		peerNotifier: config.PeerNotifier,
		// txMemPool:     config.TxMemPool,
		orphanTxs:      expiringmap.New[chainhash.Hash, *orphanTxAndParents](tSettings.Legacy.OrphanEvictionDuration).WithMaxSize(tSettings.Legacy.MaxOrphanTxs),
		chainParams:    config.ChainParams,
		rejectedTxns:   txmap.NewSyncedMap[chainhash.Hash, struct{}](maxRejectedTxns), // limit map size to maxRejectedTxns
		requestedTxns:  expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),   // give peers 10 seconds to respond
		blockDownloads: newBlockDownloadTracker(assignmentCeiling),
		peerStates:     txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		// progressLogger:  newBlockProgressLogger("Processed", log),
		msgChan:          make(chan interface{}, maxMsgQueueSize),
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

	// Now the park exists, the wire layer can be told where to put a block body
	// it reads straight off the socket. Before this call the streaming handler
	// was registered but inert: it checks for a sink and a gate and found
	// neither, so every block took the decoding path. See streaming_install.go
	// for why that mattered beyond the allocation.
	sm.installStreamingBlockPath(peerpkg.SetBlockBodyStreaming)

	// Bounded async block prefetch: with a positive budget OnBlock admits a
	// block against this global weighted semaphore and returns, so the
	// read-loop downloads the next block while the current one is validated.
	// A budget of 0 disables prefetch entirely (synchronous, one-block-in-flight).
	if budget := tSettings.Legacy.BlockPrefetchBufferBytes; budget > 0 {
		if sm.blockPark != nil && sm.blockPark.Enabled() {
			// Whenever the park is enabled the streaming pipeline is what
			// receives every block, and AcquireBlockPrefetch charges one slot
			// per block on that path, not its serialized size (the bytes are
			// gone by the time it runs — see that function), so
			// legacy_blockPrefetchBufferBytes no longer describes anything
			// real for this path. Size the same semaphore as a block count
			// instead, derived from the per-peer queue-depth setting rather
			// than a new one: see pipelineBlockSlotPeerAllowance for the
			// multiplier's reasoning.
			capacity := int64(tSettings.Legacy.MaxBlocksInTransitPerPeer) * pipelineBlockSlotPeerAllowance
			if capacity < 1 {
				capacity = 1
			}

			sm.blockPrefetchBudgetBytes = capacity
			sm.blockPrefetchBudget = semaphore.NewWeighted(capacity)
			logger.Infof("[legacy] streaming pipeline active: download admission budget sized as %d block slots (maxBlocksInTransitPerPeer=%d x %d)",
				capacity, tSettings.Legacy.MaxBlocksInTransitPerPeer, pipelineBlockSlotPeerAllowance)
		} else {
			// The budget caps the total serialized bytes of in-flight blocks.
			sm.blockPrefetchBudgetBytes = budget
			sm.blockPrefetchBudget = semaphore.NewWeighted(budget)
		}
		// Dedup half of the same admission gate as the budget semaphore, created
		// in lockstep with it: paired 1:1 with each budget reservation so at most
		// one copy of a block hash is ever admitted/queued at a time.
		sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)
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

	// The checkpoint list goes in with the cache, because the cache is where the
	// ancestry proof is decided: Fill judges a run against the pinned hashes under
	// the same lock that installs it, so no reader can ever see this run's contents
	// with the previous run's proof. config.ChainParams is non-nil on every path
	// that reaches here (startSync dereferences it unguarded a few lines below);
	// a nil list simply means no proof is ever granted.
	sm.headerCache = newHeaderCache().WithCheckpoints(config.ChainParams.Checkpoints)

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

	// Per-(hash, peerID) corrupt re-download cap (bitcoin-sv/teranode#4692), keyed on the serving peer
	// so a bad peer never wedges an honest tip. The map's retention equals the cooldown window so an
	// entry survives its own window even with no further deliveries; the LOGICAL fixed window lives in
	// corruptAttemptState.windowExpiry (Set re-extends map retention but never the logical window).
	// Like the maps above, started here after the last fallible step.
	sm.blockCorruptAttempts = expiringmap.New[legacyCorruptAttemptKey, *corruptAttemptState](legacyCorruptAttemptCooldown(tSettings)).WithMaxSize(blockFailureBackoffMaxTracked)

	// The dispatcher holds a pointer to the manager returned below, so it must be
	// built from &sm, not from the local value.
	sm.dispatcher = newBlockDispatcher(&sm)
	// Below the last fallible step above for the same reason the two maps are: a
	// goroutine started before it leaks when that step returns an error, because
	// the caller receives a nil SyncManager and can never call Stop.
	sm.startParkWorkers(tSettings.Legacy.ParkWorkers)

	// There is nothing left to prime here. headersFirstMode starts false (the
	// atomic's own zero value) and startSync derives the checkpoint fresh from
	// the chain's own best height the first time it runs, rather than from a
	// value primed once at construction and never revisited until the next
	// full rebuild.
	if config.DisableCheckpoints {
		sm.logger.Infof("Checkpoints are disabled")
	}

	sm.startKafkaListeners(ctx, nil)

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

// violationNames renders which stall arms tripped, for one log line.
func violationNames(networkSpeed, lastBlockTime bool) string {
	switch {
	case networkSpeed && lastBlockTime:
		return "network speed and last-block-time"
	case networkSpeed:
		return "network speed"
	default:
		return "last-block-time"
	}
}
