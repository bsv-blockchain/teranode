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
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"path/filepath"
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

	// maxNetworkViolations is the max number of network violations a
	// sync peer can have before a new sync peer is found.
	maxNetworkViolations = 3

	// maxRejectedTxns is the maximum number of rejected transactions
	// hashes to store in memory.
	maxRejectedTxns = 10_000

	// recentlyFailedBlocksMaxTracked bounds recentlyFailedBlocks. Legacy sync
	// only has a handful of failing block hashes in flight, but capping the map
	// guarantees a pathological stream of distinct failing hashes can never grow
	// it without bound (mirrors the WithMaxSize bound on orphanTxs).
	recentlyFailedBlocksMaxTracked = 1024

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
	//
	// A sync peer that delivers nothing for this long may simply be busy: an SV
	// Node peer can spend many minutes on blocks queued ahead of ours, or reading
	// a multi-GB block from disk before its first byte. Rotating the sync role is
	// fine; treating the peer as broken is not. See blockRequestRetryInterval in
	// block_download_tracker.go.
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
// the header cache instead: see blockOrigin and headerCache.Proven. The meaning
// is identical: a header is proven only when a held checkpoint node at or above
// it, matched against the pinned hash, descends from it.
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
	// discharges the obligation in handleBlockOnDiskMsg and the second is refused
	// as unrequested. A lock closes both, so it is a lock.
	//
	// Held only for the drain loop; the getdata send is deliberately outside
	// it. Not a leaf lock: the drain's held-block and busy-owner checks take
	// the park's mu, the dispatcher's frontierMu, the stream registry's mu, the
	// admission gate's inFlightBlocksMu and the download ledger's mu under it.
	// None of those ever takes this one, and drainRequestQueue is its only
	// taker, so there is no inverse order and no deadlock.
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

	// headersAsked is set by requestHeaders, before it sends this peer a
	// getheaders, and never cleared: the state is dropped with the peer. In
	// headers-first mode handleHeadersMsg reads a headers message only from a
	// peer with it set that mayAskForHeaders still allows, so only a peer this
	// node asked, and may still ask, can create or extend a header branch.
	headersAsked atomic.Bool
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
	// largestWindow is the last largestSizeSamples block sizes, for largestRecentSize.
	largestWindow []int64
}

// largestSizeSamples is how many recent blocks largestRecentSize looks back over. Ten was too
// few: small blocks finish quickly, and ten of them pushed a 1.8 GB block out of the window
// within 90 seconds on mainnet, so the per-peer limit jumped from 2 to 16.
const largestSizeSamples = 100

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

	bst.largestWindow = append(bst.largestWindow, size)
	if len(bst.largestWindow) > largestSizeSamples {
		bst.largestWindow = bst.largestWindow[1:]
	}

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

// largestRecentSize is the largest of the recent block sizes, or zero with none. The download
// queue is estimated from it rather than the average: sizes vary a hundredfold at some heights,
// and an average dragged down by a run of small blocks let one peer be handed nine large ones.
func (bst *blockSizeTracker) largestRecentSize() int64 {
	bst.mu.RLock()
	defer bst.mu.RUnlock()

	var largest int64
	for _, s := range bst.largestWindow {
		largest = max(largest, s)
	}

	return largest
}

// seedBlockSizes fills the size tracker with the sizes of the last largestSizeSamples blocks of the
// chain, oldest first, before the first download pass. Without it the tracker held only the blocks
// completed since the start: on 2026-10-08 after a restart its largest block was 193 MB at heights
// where blocks are up to 4 GB, and a 4 MB/s peer got 16 far blocks that each took more than an
// hour. The backstop and the watcher count unknown sizes at the tracker's mean. A failed read
// leaves the tracker empty until blocks complete.
func (sm *SyncManager) seedBlockSizes(ctx context.Context) {
	if sm.blockchainClient == nil || sm.blockSizeTracker == nil {
		return
	}

	tip, _, err := sm.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil || tip == nil {
		sm.logger.Warnf("[legacy] could not read the tip to seed the block sizes: %v", err)

		return
	}

	_, metas, err := sm.blockchainClient.GetBlockHeaders(ctx, tip.Hash(), largestSizeSamples)
	if err != nil {
		sm.logger.Warnf("[legacy] could not read the last %d block sizes: %v", largestSizeSamples, err)

		return
	}

	seeded := 0

	for i := len(metas) - 1; i >= 0; i-- {
		if metas[i] == nil || metas[i].SizeInBytes == 0 || metas[i].SizeInBytes > math.MaxInt64 {
			continue
		}

		sm.blockSizeTracker.addBlockSize(int64(metas[i].SizeInBytes))
		seeded++
	}

	sm.logger.Infof("[legacy] seeded the block size tracker with %d block sizes from the chain: largest %.0f MB, mean of the last 10 %.0f MB",
		seeded, float64(sm.blockSizeTracker.largestRecentSize())/1e6, float64(sm.blockSizeTracker.getAverageSize())/1e6)
}

// meanRecentSize is the mean of the last largestSizeSamples block sizes, or zero with none. The
// backstop counts each block a pass gives at it (downloadAssigner.overBackstop).
func (bst *blockSizeTracker) meanRecentSize() int64 {
	bst.mu.RLock()
	defer bst.mu.RUnlock()

	if len(bst.largestWindow) == 0 {
		return 0
	}

	var sum int64
	for _, s := range bst.largestWindow {
		sum += s
	}

	return sum / int64(len(bst.largestWindow))
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
	return maxInFlightForSize(bst.getAverageSize())
}

// maxInFlightForSize is the block-size ladder for a block size.
func maxInFlightForSize(avgSize int64) int {
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

	// announceParents holds the parent tx hashes of txs waiting in
	// txAnnounceBatcher, which cannot carry them itself: its items must be
	// comparable for deduplication. orderAnnounceBatch consumes the entries.
	// Bounded; an entry lost to eviction, or left behind by a deduplicated
	// Put, only costs that tx its place in the ordering.
	announceParents *txmap.SyncedMap[chainhash.Hash, []chainhash.Hash]

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
	blockPark *blockPark

	// parkCommits carries a parked block the sweep has found a stored parent for
	// back to the block-queue consumer, which is the one goroutine that commits.
	// The sweep runs on its own goroutine and may not commit from there: it
	// would race the dispatcher, which commits one block at a time. nil on a manager
	// built as a struct literal, and submitParkCommit then commits inline, which
	// is what the sweep did before it had a goroutine of its own.
	parkCommits chan parkCommit

	// drainQueue is the parents whose parked children may now be committable. It
	// is owned by the block consumer alone, with no lock, on the same terms as the
	// dispatcher's frontier: every producer of a drain request already runs on that
	// goroutine.
	drainQueue []drainRequest

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

	// headerCache names the block hashes for the heights just above the
	// committed tip (see committedTip), from the most recent getheaders reply.
	// It is a lookup, not a work queue: nothing walks it, nothing holds a
	// position in it, and discarding it costs one message.
	//
	headerCache *headerCache

	// blockPrefetchBudget bounds, by block count, the conversions that are
	// admitted from the wire but not yet finished streaming to disk. It lets
	// admitPipelineSink admit a block and let the peer's read loop carry on
	// reading (so download overlaps the previous block's conversion) instead of
	// serializing one block's whole conversion behind the next one's read.
	// Built unconditionally in New, sized as a block count derived from
	// legacy_maxBlocksInTransitPerPeer (see pipelineBlockSlotPeerAllowance).
	blockPrefetchBudget      *semaphore.Weighted
	blockPrefetchBudgetSlots int64

	// inFlightBlocks is the dedup half of the same block-admission gate whose
	// other half is blockPrefetchBudget. It holds the hash of every block that is
	// currently admitted (holds a slot) OR parked waiting for one, so at most one
	// copy of any given block hash is ever in flight at a time.
	// AcquireBlockPrefetch inserts the hash BEFORE the (possibly blocking) budget
	// Acquire and ReleaseBlockPrefetch deletes it alongside the budget release, so
	// the two halves share exactly one lifetime and can never drift. Without it,
	// N duplicates of a single requested block would each reserve a slot, fill
	// the whole budget, and park every legacy peer's read-loop in Acquire — the
	// very "a malicious peer cannot outrun the budget" property this gate exists
	// to guarantee. inFlightBlocksMu guards the map.
	inFlightBlocks map[chainhash.Hash]*inFlightBlock

	// drainedDuplicates counts, per block, copies drained off the wire unwritten because another
	// copy was converting. Each drained copy still produces an on-disk message, which consumes
	// one count and parks nothing. Guarded by drainedDuplicatesMu; the map is made on first use.
	// waste counts block bytes received and every way download bandwidth was lost.
	waste downloadWaste

	// assignMu runs download passes one at a time. Commits, arrivals, header replies and the
	// park sweep each start one, and two running together could both find the same block
	// unowned and both ask for it. It is taken before headerMu and never while holding it.
	assignMu            sync.Mutex
	drainedDuplicatesMu sync.Mutex
	drainedDuplicates   map[chainhash.Hash]int
	// localFaultDrains counts, per block, copies drained because this node failed to store
	// them (absorbLocalSinkFault). Guarded by drainedDuplicatesMu; made on first use.
	localFaultDrains map[chainhash.Hash]int
	// conversions is the conversion in progress for each block, so a faster copy can take it
	// over (conversion_race.go).
	conversionsMu sync.Mutex
	conversions   map[chainhash.Hash]*conversionCtl
	// sideCopyReader wraps each read of a side file (raceDuplicateCopy), so a test can make the
	// disk fail part way. nil reads the file as it is; nothing in production sets it.
	sideCopyReader func(io.Reader) io.Reader
	// takeoverAfter is the timer of a copy that took over and waits for the copy it took over
	// (awaitTakeover), so a test can fire it. nil is time.After; nothing in production sets it.
	takeoverAfter    func(time.Duration) <-chan time.Time
	inFlightBlocksMu sync.Mutex

	// blockPrefetchWaiters counts read-loops currently blocked acquiring a
	// prefetch slot (i.e. local processing cannot keep up). While > 0 the node
	// is backpressuring its own network reads, so the stall detector must not
	// hold the resulting zero throughput against the sync peer — the prefetch
	// analogue of the blockBacklog guard. Read by handleCheckSyncPeer.
	blockPrefetchWaiters atomic.Int64

	// blockPrefetchReserved shadows how many slots of blockPrefetchBudget are
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
	// hands the slot back, so it cannot drift from the semaphore it shadows. It
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
	blockSizeTracker    *blockSizeTracker  // tracks block sizes for dynamic in-flight adjustment
	commitRate          *commitRateTracker // blocks a second joining the chain, for the frontier race
	streams             *streamRegistry    // block bodies arriving now and peers' delivery rates, for the frontier race
	farProbes           farProbeSet        // blocks placeUnmeasured gave from the top of the window, which highestHeld does not count
	peerRatesPath       string             // the rates file (peerRatesFile), or empty to keep no rates

	// dispatcher runs each parked block's work on a worker, one block at a time,
	// and every chain-order step in dispatch order. Built in New(); nil when
	// SyncManager was built as a struct literal in a test, which every dispatcher
	// accessor tolerates.
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
//
// Above the last checkpoint the cache is not what names blocks, so the line
// says what is: how many blocks peers owe, split from how many are arriving
// now, since Len counts a block minutes into a transfer as owed too and the
// common tip report fires during exactly such a transfer. The cache clause is
// kept while the cache still names heights, because at the crossing out of
// headers-first mode it can name up to two thousand and the pass keeps placing
// them.
func (sm *SyncManager) headerRoundSummary() string {
	best, _, _ := sm.committedTip()

	top, haveTop := sm.headerCache.Top()

	if !sm.headersFirstMode.Load() {
		_, arriving := sm.streams.arrivingBytes()

		line := fmt.Sprintf("best block processed %d, past the final checkpoint so blocks are named by announcements and the download ledger; %d blocks are owed by peers and %d of them are arriving now",
			best, sm.blockDownloads.Len(), arriving)

		if haveTop {
			line += fmt.Sprintf("; the header cache still names %d heights up to %d", sm.headerCache.Len(), top)
		}

		return line
	}

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

	// Below the last checkpoint the elected peer is sent the round's first
	// getheaders, so the election follows SV Node's preferred-download rule
	// (net_processing.cpp:120 and :5057): a preferred peer, outbound or
	// whitelisted, is elected when one is ahead, and an inbound peer that is not
	// whitelisted only when none is. requestHeadersAs applies the same rule to
	// the elected peer.
	electedHeight, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf("[startSync] failed to convert block height to int32: %v", err)

		return
	}

	headerRule := sm.chainParams != nil && sm.findNextHeaderCheckpoint(electedHeight) != nil
	inboundFallback := headerRule && !sm.preferredPeerAhead(electedHeight)
	skippedByRule := 0

	for peer, state := range sm.peerStates.Range() {
		if !state.syncCandidate {
			sm.logger.Debugf("[startSync] peer %v is not a sync candidate", peer.String())

			continue
		}

		if headerRule && !inboundFallback && !preferredDownloadPeer(peer) {
			sm.logger.Debugf("[startSync] peer %v is inbound and not whitelisted, the committed height %d is below the last checkpoint and a preferred download peer (outbound or whitelisted) is ahead, so it is not elected", peer.String(), electedHeight)

			skippedByRule++

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
		sm.logger.Warnf("[startSync] No sync peer candidates available after evaluating %d total peers (%d ahead, %d at same height, %d inbound skipped because a preferred download peer is ahead below the last checkpoint)", sm.peerStates.Length(), len(bestPeers), len(okPeers), skippedByRule)

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
		if err = sm.requestHeadersAs(bestPeer, bestBlockHeightInt32, locator, &zeroHash, bestPeer); err != nil {
			sm.logger.Warnf("[startSync] Failed to send getheaders message to peer %s: %v", bestPeer.String(), err)

			return
		}

		sm.headersFirstMode.Store(true)

		if !preferredDownloadPeer(bestPeer) {
			sm.logger.Infof("[startSync] no outbound or whitelisted peer is ahead of committed height %d below the last checkpoint, so inbound peer %s is elected as the one inbound peer asked for headers (SV Node's preferred-download fallback)", bestBlockHeightInt32, bestPeer.String())
		}

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
	// It is the peer's own word, not something it has demonstrated: the record
	// only ever rises, so a self-report written here could never be
	// contradicted and every peer would appear to hold every block forever. That is
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
//     unrequested and be thrown away.
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

	// An inbound peer that is not whitelisted is asked for headers only as the
	// fallback sync peer (mayAskForHeaders), so its branch goes with the role.
	// That keeps at most one such peer holding a branch at any time.
	if !preferredDownloadPeer(sp) {
		sm.headerCache.DropPeer(sp)
	}

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

	// Its header branch goes with it, as SV Node forgets a peer's best known
	// header when the peer goes. Headers another peer also holds stay.
	sm.headerCache.DropPeer(peer)

	// A peer leaving with blocks owed costs whatever of them was already on the
	// wire, and the blocks must be asked of someone else.
	if owed := sm.blockDownloads.CountForPeer(peer); owed > 0 {
		sm.waste.droppedOwing.Add(1)
		sm.waste.blocksOwedAtDrop.Add(int64(owed))
		sm.logger.Infof("[reRequest] %s left owing %d blocks; they may be asked of other peers", peer, owed)
	}

	if sm.streams != nil {
		sm.streams.forgetPeer(peer)
	}

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

// peerStateResolvingPrimary returns the sync state for peer, resolving a stream
// sub-peer (e.g. a BlockPriority DATA1 stream, not itself registered in
// peerStates) to its association's primary peer. It returns the resolved peer
// (the primary when a stream peer resolved, otherwise the input peer) and
// whether a state was found. Centralizes the stream→primary walk previously
// inlined in handleHeadersMsg/handleInvMsg/BlockRequested; call
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

// dispatchBlocks is the block-queue consumer: it runs every pre-check and every
// chain-order step for one block on this goroutine and hands only the block's own
// work to a worker, one block at a time, so its tail still runs in dispatch order. A
// block whose parent is not in the chain never reaches a worker: the head parks it,
// with its bytes, and the drain commits it when the parent lands. It returns when
// sm.quit closes.
func (sm *SyncManager) dispatchBlocks() {
	bd := sm.dispatcher

	// From here the drain is this loop's admission source rather than a call made
	// inside a tail, so scheduleDrain queues instead of walking.
	sm.drainAsync.Store(true)

	defer sm.drainAsync.Store(false)

	// Start the watchdog's clock here rather than at the first admission, so a
	// loop that wedges before it ever places work is still described. Left at
	// zero the report suppresses itself, which is the one case worth hearing
	// about most.
	sm.noteConsumerAdmitted(time.Now())

	for {
		// A drain step may decline: it walks the queued parents itself and can find
		// that none of them has a child it can commit yet, or that the window has no
		// room, dropping the parents it has ruled out. The loop then waits for a
		// completion or a posted commit, either of which can change the answer.
		if len(sm.drainQueue) > 0 {
			if sm.drainStep(bd) {
				sm.noteConsumerAdmitted(time.Now())

				continue
			}

			sm.noteDrainDeclined()
		}

		// Recorded here, not anywhere earlier, so it describes the wait rather
		// than the work that led to it. Nothing below reads it; the watchdog on
		// the message-handling goroutine does.
		sm.publishConsumerWait(time.Now())

		select {
		case <-sm.quit:
			for _, e := range bd.drainFrontier() {
				// A dispatched block was taken out of the park's index, so it is put
				// back. Without the Restore the record is adopted by the next start's
				// recovery with no height, no peer and no header node.
				if e.d.parked != nil {
					sm.blockPark.Restore(*e.d.parked)
				}
			}

			return
		case c := <-bd.completions:
			bd.complete(c)
		case commit := <-sm.parkCommits:
			// The sweep found a parked block whose parent is in the chain after
			// all. It decided that on its own goroutine and posts here, because
			// this is the one goroutine that admits blocks: committing from the
			// sweep would race the dispatcher, which commits one block at a time.
			//
			// Put back, then queued, rather than committed here. The sweep took
			// the entry out of the index to hand it over, and restoring it means
			// the drain step claims it through the one path every other drained
			// block takes, with one admission test and one header-front advance.
			sm.receiveParkCommit(commit)
		}
	}
}

// fetchMoreHeaderBlocks tops the download pipeline back up after a block from
// this peer stopped being outstanding, whether it was committed or parked.
// Without it a parked block is a silent loss of one in-flight slot, and the
// pipeline drains one block at a time until nothing is outstanding at all.
//
// The sweep ticker's resume does NOT come through here, because the per-peer
// question this asks is the wrong one for it — it calls fetchHeaderBlocks
// directly instead.
// perPeerDepth is how many blocks one peer may be asked for at once:
// streamingPeerDepth, whatever the block size.
func (sm *SyncManager) perPeerDepth() int {
	return sm.streamingPeerDepth()
}

func (sm *SyncManager) fetchMoreHeaderBlocks(peer *peerpkg.Peer) {
	sm.topUpHeaderBlocks(func() bool {
		return sm.blockDownloads.CountForPeer(peer) < sm.perPeerDepth()
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
// free here, because a reply the peer's branch already holds changes
// nothing (headerCache.FillFrom), so an early reply discards no usable height.
//
// The read-ahead depth (legacy_blockDownloadWindow, 1024) is still the
// wrong quantity to reach for, for the reason it always was: it bounds how
// far downloads may run ahead of the commit frontier, not how much header
// lookahead a getheaders round trip needs.
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
// wanted range? wantedBlocksFromCache stops at that depth (1024 by default)
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

	peers := sm.headerRequestPeers(best)
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

	peer := sm.nextHeaderRefillPeer(peers)

	var (
		locator blockchain.BlockLocator
		err     error
	)

	// A walk continues from the asked peer's own branch tip when it has one,
	// so each peer's walk grows its own branch (see continueCheckpointWalkIfNeeded),
	// and from the active branch's tip for a peer with none yet, so a peer that
	// holds the same chain answers where the walk already is.
	extendFrom, extend := chainhash.Hash{}, false

	if walkIncomplete {
		if _, own, ok := sm.headerCache.PeerTop(sm.headerOwner(peer)); ok {
			extendFrom, extend = own, true
		} else if haveTop {
			extendFrom, extend = sm.headerCache.At(top)
		}
	}

	if extend {
		locator, err = sm.extendingHeadersLocator(extendFrom)
	} else {
		locator, err = sm.headersRoundLocator(&tipHash, uint32(best)) //nolint:gosec // a chain height
	}

	if err != nil {
		sm.logger.Warnf("[assignWantedBlocks] could not build a getheaders locator to refill the header cache past height %d: %v", last, err)

		return
	}

	// This call only ever runs because the header cache came up short, so
	// every request from here is a deliberate retry: peer rotation closes
	// the multi-peer case, but with one eligible peer the (locator, stop
	// hash) pair is unchanged from last time — the stop hash is always the
	// zero hash and the locator cannot advance until a block commits — and
	// PushGetHeadersMsg's own dedup filter would otherwise discard it as an
	// accidental duplicate while logging success.
	peer.ForgetLastHeadersRequest()

	if err := sm.requestHeaders(peer, best, locator, &zeroHash); err != nil {
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

// mayAskForHeaders reports whether this node may send peer a getheaders while
// its committed height is best. Outside headers-first mode and above the last
// checkpoint it always may. Below the last checkpoint, or while headers-first
// mode is on, it may ask a preferred download peer, outbound or whitelisted,
// and an inbound peer that is not whitelisted only when that peer is the sync
// peer and no preferred peer is ahead of best. Judged on the primary peer a
// stream connection belongs to.
//
// The mode counts as well as the height because the mode outlives the last
// checkpoint: the commit that reaches it does not leave the mode, which waits
// for parkedBlockCommitted (block_park_drain.go), and handleHeadersMsg reads
// headers into the cache for as long as the mode is on.
//
// This is SV Node's rule. A peer is a preferred download peer when it is
// outbound or whitelisted (net_processing.cpp:120), and headers and blocks are
// fetched from a preferred peer, or from any peer when none is preferred
// (net_processing.cpp:5057 and :5516). SV Node counts every preferred peer;
// this counts only the ones ahead of the committed height, because the
// election here takes only peers ahead and a preferred peer that is not ahead
// has nothing to give.
//
// In headers-first mode handleHeadersMsg reads headers only from a peer this
// node asked and may still ask, and each such peer holds at most one branch of
// up to branchCap headers. So the branches are bounded by the outbound
// connections this node makes (the automatic outbound target, 8 by default,
// plus addnode or connect peers) and the whitelisted inbound peers the operator
// allows, plus one inbound fallback peer, never by how many peers connect to
// it. The fallback's branch is dropped when it is demoted (demoteSyncPeer) and
// stops growing once a preferred peer is ahead.
func (sm *SyncManager) mayAskForHeaders(peer *peerpkg.Peer, best int32) bool {
	return sm.mayAskForHeadersAs(peer, best, sm.loadSyncPeer())
}

// mayAskForHeadersAs is mayAskForHeaders with syncPeer standing as the sync
// peer, for startSync, which asks the peer it elects before it stores it.
func (sm *SyncManager) mayAskForHeadersAs(peer *peerpkg.Peer, best int32, syncPeer *peerpkg.Peer) bool {
	if sm.chainParams == nil || (!sm.headersFirstMode.Load() && sm.findNextHeaderCheckpoint(best) == nil) {
		return true
	}

	_, primary, _ := sm.peerStateResolvingPrimary(peer)

	if preferredDownloadPeer(primary) {
		return true
	}

	return syncPeer != nil && primary == syncPeer && !sm.preferredPeerAhead(best)
}

// preferredDownloadPeer is SV Node's fPreferredDownload
// (net_processing.cpp:120): the peer is outbound, or its address is
// whitelisted.
func preferredDownloadPeer(peer *peerpkg.Peer) bool {
	return !peer.Inbound() || peer.Whitelisted()
}

// preferredPeerAhead reports whether a connected sync candidate that is a
// preferred download peer has announced a height above best.
func (sm *SyncManager) preferredPeerAhead(best int32) bool {
	if sm.peerStates == nil {
		return false
	}

	for p, state := range sm.peerStates.Range() {
		if state != nil && state.syncCandidate && p.Connected() && preferredDownloadPeer(p) && p.LastBlock() > best {
			return true
		}
	}

	return false
}

// requestHeaders sends peer a getheaders for locator and stopHash, and records
// on the peer's state that this node asked it, which is what lets its headers
// create or extend a branch in headers-first mode (handleHeadersMsg). The
// record is made before the send, so a reply can never arrive ahead of it; a
// send that then fails leaves an outbound peer marked, which only lets an
// unrequested reply from it be read. best is the committed height the caller
// read. It refuses, sending and recording nothing, when mayAskForHeaders does.
//
// Every getheaders this package sends goes through here.
func (sm *SyncManager) requestHeaders(peer *peerpkg.Peer, best int32, locator blockchain.BlockLocator, stopHash *chainhash.Hash) error {
	return sm.requestHeadersAs(peer, best, locator, stopHash, sm.loadSyncPeer())
}

// requestHeadersAs is requestHeaders with syncPeer standing as the sync peer
// (see mayAskForHeadersAs).
func (sm *SyncManager) requestHeadersAs(peer *peerpkg.Peer, best int32, locator blockchain.BlockLocator, stopHash *chainhash.Hash, syncPeer *peerpkg.Peer) error {
	if !sm.mayAskForHeadersAs(peer, best, syncPeer) {
		return errors.NewProcessingError("peer %s is inbound, not whitelisted and not the fallback sync peer, and the node is below the last checkpoint or in headers-first mode (committed height %d), so it is not asked for headers", peer, best)
	}

	if state, _, ok := sm.peerStateResolvingPrimary(peer); ok {
		state.headersAsked.Store(true)
	}

	return peer.PushGetHeadersMsg(locator, stopHash)
}

// headersAskedOf reports whether this node has sent peer, or the primary peer a
// stream connection belongs to, a getheaders.
func (sm *SyncManager) headersAskedOf(peer *peerpkg.Peer) bool {
	state, _, ok := sm.peerStateResolvingPrimary(peer)

	return ok && state.headersAsked.Load()
}

// headerRequestPeers is eligibleBlockPeers less the peers mayAskForHeaders
// refuses at best, the peers maybeRequestMoreHeaders rotates through. Below the
// last checkpoint or in headers-first mode that is the preferred download
// peers, or the inbound fallback sync peer alone when no preferred peer is
// ahead.
func (sm *SyncManager) headerRequestPeers(best int32) []blockPeer {
	peers := sm.eligibleBlockPeers()
	askable := peers[:0]

	for _, p := range peers {
		if sm.mayAskForHeaders(p.peer, best) {
			askable = append(askable, p)
		}
	}

	return askable
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

	msg := hmsg.headers
	numHeaders := len(msg.Headers)

	// Outside headers-first mode nothing reads a headers message: past the last
	// checkpoint blocks are named by announcements. The message is dropped and
	// the sender keeps its connection. Upstream disconnected it as unrequested;
	// SV Node's ProcessHeadersMessage (net_processing.cpp:3338) scores a headers
	// message for its size, linkage and validity, never for arriving unasked.
	//
	// headersFirstMode alone, without upstream's "|| sm.nextCheckpoint == nil": the
	// stored checkpoint field is gone, and startSync — the one place that turns this
	// flag on — already derives the next checkpoint fresh and refuses to enter
	// headers-first mode when there is none ahead. A second, stored copy of that
	// decision is what went stale and left the mode unreachable.
	if !sm.headersFirstMode.Load() {
		sm.logger.Debugf("[handleHeadersMsg] not in headers-first mode, dropping %d headers from %s", numHeaders, peer)

		return
	}

	// Nothing to do for an empty headers message.
	if numHeaders == 0 {
		return
	}

	// The preferred-peer rule. While headers-first mode is on (and this
	// function returned above if it is not), a batch may create or extend a
	// header branch only for a peer this node sent a getheaders to
	// (requestHeaders records it) and may still ask (mayAskForHeaders): a
	// preferred download peer, outbound or whitelisted, or the one inbound
	// fallback sync peer while no preferred peer is ahead. Each such peer holds
	// at most one branch of at most branchCap headers, so the cache is bounded
	// by the peers this node and its operator choose plus one, and not by how
	// many peers connect to it. Anything else is dropped without blame: the
	// cache asks again for whatever it needs.
	//
	// It holds for the whole mode, not only below the last checkpoint. The
	// commit that reaches the last checkpoint does not leave the mode; that
	// waits for parkedBlockCommitted (block_park_drain.go), and in between
	// headers are still read into the cache.
	//
	// The asked check needs no blockchain read, so it runs first and an unasked
	// batch costs nothing. The tip is then read once, and that one read serves
	// both mayAskForHeaders and fillHeaderCacheAt, so a headers message costs at
	// most one GetBestBlockHeader call (an uncached blockchain round trip). A
	// failed read drops the batch: the rule cannot be judged without the
	// committed height, and skipping it would let a later read inside the fill
	// admit a batch the rule refuses.
	if !sm.headersAskedOf(peer) {
		sm.logger.Debugf("[handleHeadersMsg] dropping %d headers from %s, which was not asked for headers in headers-first mode", numHeaders, peer)

		return
	}

	best, tipHash, ok := sm.committedTip()
	if !ok {
		sm.logger.Debugf("[handleHeadersMsg] the committed tip could not be read, dropping %d headers from %s", numHeaders, peer)

		return
	}

	if !sm.mayAskForHeaders(peer, best) {
		sm.logger.Debugf("[handleHeadersMsg] dropping %d headers from %s, which may no longer be asked for headers in headers-first mode (committed height %d)", numHeaders, peer, best)

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
	if !sm.fillHeaderCacheAt(hmsg.peer, msg, best, tipHash) {
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
//   - stronger, because a header is proven only as an ancestor of a held node
//     carrying a pinned checkpoint hash, on branches verified to be internally
//     linked and rooted in this node's own committed tip. Upstream's list could
//     be extended past the point that had actually been verified, which is the
//     hole its second security commit had to go back and close with
//     headerNodeProven.
//   - weaker, because it is read at delivery rather than stamped at request, so a
//     branch dropped while a block is on the wire can lose the proof. That
//     direction costs full validation and nothing else. See headerCache.Proven.
//
// Every route that cannot show the run gets the zero value: peer advertisements,
// blocks adopted from disk by park recovery, and anything re-requested outside the
// wanted range.
func (sm *SyncManager) blockOrigin(blockHash chainhash.Hash) blockRequestOrigin {
	return blockRequestOrigin{headerProven: sm.headerCache.Proven(blockHash)}
}

// fillHeaderCache turns a headers batch into the sender's branch of the header
// cache, and reports whether the sender's branch moved.
//
// The batch must connect to the committed tip or to a header the cache already
// holds; the tip moves under a reply while it is in flight, and Fill keeps
// whatever part of the run still sits above the tip when it arrives. A batch
// that does not connect at all is dropped without blaming the sender: the
// locator steps back to genesis, so a peer answering from an older shared
// ancestor is answering correctly, just about a point this node has passed.
//
// Each peer has its own branch (headerCache), keyed by the peer this message is
// resolved to, so a stream connection's headers count for its primary peer and
// leave with it.
//
// The return value is what handleHeadersMsg gates its assignment pass on, so
// only a batch that moved a branch triggers one.
//
// It reads the committed tip itself; handleHeadersMsg, which has already read
// the tip for its own rule, calls fillHeaderCacheAt with that read instead.
func (sm *SyncManager) fillHeaderCache(peer *peerpkg.Peer, msg *wire.MsgHeaders) bool {
	if len(msg.Headers) == 0 {
		return false
	}

	// Read as one value, height and hash together, so the batch can never be
	// keyed under a height the hash no longer belongs to. It also means this
	// always judges linkage against the chain's real tip, including a tip a
	// same-height reorg just replaced.
	best, tipHash, ok := sm.committedTip()
	if !ok {
		sm.logger.Debugf("[fillHeaderCache] no committed tip recorded yet, dropping %d headers from %s", len(msg.Headers), peer)

		return false
	}

	return sm.fillHeaderCacheAt(peer, msg, best, tipHash)
}

// fillHeaderCacheAt is fillHeaderCache against a committed tip the caller has
// already read: best and tipHash must come from one committedTip call.
func (sm *SyncManager) fillHeaderCacheAt(peer *peerpkg.Peer, msg *wire.MsgHeaders, best int32, tipHash chainhash.Hash) bool {
	if len(msg.Headers) == 0 {
		return false
	}

	owner := sm.headerOwner(peer)
	prevProven := sm.headerCache.ProvenTo()

	result := sm.headerCache.FillFrom(owner, tipHash, best+1, msg.Headers)

	// A header a rule refused. bad-diffbits and checkpoint mismatch are SV
	// Node's DoS 100 (validation.cpp:5789-5793 and 5757-5761): the sender is not
	// describing this chain and the batch was refused whole. Here the sender
	// loses its connection rather than being banned; the ban score is the
	// peer-punishment work's to add. Its branch goes now, not when the
	// disconnect is processed, so nothing it sent is read in between. The
	// other reasons are SV Node's Invalid without DoS: the headers before the
	// refused one were kept and the peer stays.
	if result.rejection.disconnects() {
		sm.headerCache.DropPeer(owner)

		if result.rejection == rejectCheckpointMismatch {
			sm.logger.Warnf("[fillHeaderCache][%s] header branch dropped: %s", peer, result.detail)
			peer.DisconnectWithWarning(fmt.Sprintf("block header at height %d does NOT match the expected checkpoint hash: %s", result.rejectedHeight, result.detail))

			return false
		}

		peer.DisconnectWithWarning(fmt.Sprintf("block header at height %d refused as %s: %s", result.rejectedHeight, result.rejection, result.detail))

		return false
	}

	if result.rejection != headerAccepted {
		sm.logger.Infof("[fillHeaderCache][%s] header at height %d refused as %s: %s", peer, result.rejectedHeight, result.rejection, result.detail)
	}

	if !result.accepted {
		// Proof of work is re-derived because Fill reports a linkage or work
		// failure as one false, and only the second is the peer's fault. Judged
		// with the cache's own ceiling, so a cache built without one never has a
		// refusal explained by a check it did not run. SV Node rejects the
		// header as high-hash with DoS(50) (validation.cpp:5586-5596,
		// CheckBlockHeader); here the peer loses its connection.
		if i := firstHeaderWithoutWork(msg.Headers, sm.headerCache.PowLimit()); i >= 0 {
			peer.DisconnectWithWarning(fmt.Sprintf("block header %s does not meet proof of work (index %d of %d)", msg.Headers[i].BlockHash(), i, len(msg.Headers)))

			return false
		}

		sm.logger.Debugf("[fillHeaderCache] batch of %d headers from %s does not move its branch above the committed tip at height %d, dropping it", len(msg.Headers), peer, best)

		return false
	}

	// low and added are what THIS call made the cache hold for the first time;
	// top is the sender's branch tip. The operator panel parses this line.
	low := result.low
	if result.added == 0 {
		low = result.top + 1
	}

	sm.logger.Infof("[fillHeaderCache] cached %d of %d headers from %s, heights %d to %d", result.added, len(msg.Headers), peer, low, result.top)

	if newProven := sm.headerCache.ProvenTo(); newProven > prevProven {
		sm.logger.Infof("[fillHeaderCache][%s] header walk matched checkpoint at height %d", peer, newProven)

		sm.reofferParkedBlocksProvenBy(newProven)
	}

	sm.continueCheckpointWalkIfNeeded(peer, best, result.top, result.topHash)

	return true
}

// headerOwnerLive reports whether owner, a header cache branch key, is still a
// peer this manager tracks and may still ask for headers at the committed
// height committed (mayAskForHeaders). handleDonePeerMsg removes the peer from
// peerStates before it drops the peer's branch, and demoteSyncPeer clears the
// sync peer before it drops an inbound fallback's branch, so a fill that
// installs after either sees the peer gone or no longer askable and installs
// nothing (headerCache.ownerLive). Checking only peerStates let a fill that was
// running when an inbound fallback was demoted give it its branch back, so one
// inbound branch was not a hard bound.
//
// Called with the header cache's locks held: mayAskForHeaders reads only the
// peer map and the sync peer, and makes no blockchain call.
func (sm *SyncManager) headerOwnerLive(owner any, committed int32) bool {
	peer, ok := owner.(*peerpkg.Peer)
	if !ok || sm.peerStates == nil {
		return true
	}

	if _, exists := sm.peerStates.Get(peer); !exists {
		return false
	}

	return sm.mayAskForHeaders(peer, committed)
}

// headerOwner is the key of peer's branch in the header cache: the primary
// peer a stream connection belongs to, or peer itself.
func (sm *SyncManager) headerOwner(peer *peerpkg.Peer) any {
	if sm.peerStates == nil {
		return peer
	}

	if _, resolved, ok := sm.peerStateResolvingPrimary(peer); ok && resolved != nil {
		return resolved
	}

	return peer
}

// reofferParkedBlocksProvenBy hands the parked blocks sitting directly behind
// the committed tip back to the block-queue consumer once a fill has raised the
// cache's proof to newProven, so a block HandleConvertedBlock refused as
// unproven commits the moment the walk proves it rather than when the park
// sweep next notices it.
//
// It is the other half of the refusal in HandleConvertedBlock. After a restart
// every parked record is unproven, because the header cache starts empty and
// reconcileRecoveredParents runs before any headers reply; each one is refused
// with the blob kept and awaitingProofAt stamped. Nothing on the drain's own path
// changes that answer, and the sweep only looks at a block after
// parkStuckThreshold and then parkSweepRPCBudget at a time, so without this the
// restart dead spot would be the walk plus minutes of sweep cadence. The proof
// arrives here, so the re-offer belongs here.
//
// Only the tip's own children are re-offered: a block parked behind anything
// else cannot commit yet whatever the cache proves, and the drain that follows
// its parent's commit will reach it in order. They are taken through
// TakeChildrenForProof rather than TakeChildren, because the ordinary take
// honours the awaitingProofAt floor and would hand back nothing for exactly the
// block this is for. The stamp is cleared here, on the one copy handed over:
// both consumers of submitParkCommit Restore the entry and then drain, and the
// drain's committableChildLocked would hold a stamped entry for the rest of the
// floor, losing the proof event until the sweep. A child the fill did not prove
// is given back with its stamp untouched, for the same reason in reverse.
//
// This runs on the headers goroutine (handleHeadersMsg is started with go), so
// it posts through submitParkCommit, the cross-goroutine route the sweep uses,
// never through scheduleDrain, which only the consumer may call. The tip is
// re-read rather than taken from the top of fillHeaderCache: the chain moves
// under a fill, and the children of a tip that has since advanced are not the
// blocks held up.
func (sm *SyncManager) reofferParkedBlocksProvenBy(newProven int32) {
	if sm.blockPark == nil {
		return
	}

	best, tipHash, ok := sm.committedTip()
	if !ok || best+1 > newProven {
		return
	}

	for _, entry := range sm.blockPark.TakeChildrenForProof(tipHash) {
		if !sm.blockOrigin(entry.hash).headerProven {
			// Not this fill's doing. Every child of the tip sits at best+1, and
			// the early return above has already placed best+1 within the proof,
			// so the only child the cache does not name is a fork sibling: the
			// proven chain carries a different hash at this height. Back as it
			// was, stamp and all; the floor it was refused under still stands.
			sm.blockPark.Restore(entry)

			continue
		}

		sm.logger.Infof("[fillHeaderCache][%s] header walk proved parked block %s at height %d, re-offering it for commit", tipHash, entry.hash, best+1)

		entry.awaitingProofAt = time.Time{}

		sm.submitParkCommit(parkCommit{entry: entry, parentHeight: uint32(best)}) //nolint:gosec // a committed height, never negative
	}
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
// answer again. The locator leads with that peer's own branch tip (top and
// topHash, from the fill), as SV Node asks for more from pindexLast
// (net_processing.cpp:3452-3464), never with another peer's: a peer asked to
// continue a branch it does not hold answers from the committed tip, and its own
// walk would never get past one reply. Rotation on a stalled walk is still maybeRequestMoreHeaders'
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
func (sm *SyncManager) continueCheckpointWalkIfNeeded(peer *peerpkg.Peer, best, top int32, topHash chainhash.Hash) {
	if !sm.headersFirstMode.Load() || !peer.Connected() {
		return
	}

	next, ok := sm.headerCache.NextCheckpointAbove(best)
	if !ok || top >= next.Height {
		return
	}

	locator, err := sm.extendingHeadersLocator(topHash)
	if err != nil {
		sm.logger.Warnf("[fillHeaderCache][%s] could not build a locator to continue the walk toward checkpoint height %d: %v", peer, next.Height, err)

		return
	}

	peer.ForgetLastHeadersRequest()

	if err := sm.requestHeaders(peer, best, locator, &zeroHash); err != nil {
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
// baseHeight plus its index, and that is only true of a batch whose first header
// names parent, the block at baseHeight-1. A batch that does not is a reply about
// some other point in the chain, and its headers are not at those heights at all,
// so it can contradict no checkpoint and answers nil. The answer decides a
// disconnect: blaming an honest peer for a stale reply cost mainnet its sync peer
// part way through a block on 2026-09-23, and the record that peer had just
// written was stranded and stopped the chain.
//
// Nil checkpoints, or a batch reaching no checkpoint height, both answer nil.
func (sm *SyncManager) contradictedCheckpoint(baseHeight int32, parent chainhash.Hash, headers []*wire.BlockHeader) *chaincfg.Checkpoint {
	if sm.chainParams == nil || len(headers) == 0 || headers[0].PrevBlock != parent {
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

// haveInventory returns whether the inventory represented by the passed
// inventory vector is known.  This includes checking all the various places
// inventory can be when it is in different states such as blocks that are part
// of the main chain, on a side chain, in the orphan pool, and transactions that
// are in the memory pool (either the main pool or orphan pool).
func (sm *SyncManager) haveInventory(invVect *wire.InvVect) (bool, error) {
	switch invVect.Type {
	case wire.InvTypeBlock:
		// Parked, being validated, arriving or converting: in none of those
		// states is the block in the chain, and in all of them asking a peer for
		// it downloads it again.
		//
		// The park case is what makes the park save bandwidth outside
		// headers-first mode, which is every mainnet node: the getblocks a park
		// sends brings back an inv for the block being held, and without this
		// the block was downloaded all over again once every
		// blockRequestRetryInterval for as long as it stayed parked. The
		// dispatcher case is the gap between leaving the park and committing,
		// which every block above the last checkpoint passes through while peers
		// are still announcing it; on mainnet from 2026-09-28 about one block in
		// twelve was downloaded and converted twice through it. The arriving and
		// converting cases are the ones the wanted-range pass has checked since
		// the 2026-09-24 incident (unownedBlocksUpTo) and this path did not: a multi-gigabyte
		// block still streaming past the retry window was fetched again from
		// every peer that announced it, each copy streamed whole into a side
		// file and drained.
		if sm.blockHeldLocally(invVect.Hash) {
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
			announced := invVects[lastBlock].Hash

			// Gated on headers-first mode being ON, which SV Node does not do —
			// it acts on a block inv in every state. The gate is forced by our
			// own code: handleHeadersMsg drops every headers message while
			// headersFirstMode is false, so a getheaders sent outside the mode
			// would be answered into nothing. It goes through requestHeaders,
			// so in headers-first mode an inbound announcer is asked only if it is
			// whitelisted or is the inbound fallback sync peer.
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
				if tipHeight, tipHash, ok := sm.committedTip(); ok && sm.mayAskForHeaders(peer, tipHeight) {
					if locator, lerr := sm.headersRoundLocator(&tipHash, uint32(tipHeight)); lerr != nil { //nolint:gosec // a chain height
						sm.logger.Warnf("[handleInvMsg] could not build repair getheaders locator for announced block %s: %v", announced, lerr)
					} else if len(locator) > 0 {
						if err := sm.requestHeaders(peer, tipHeight, locator, &announced); err != nil {
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
			// Held or on its way since this item was queued: nothing to ask for.
			// The bare break leaves the switch and falls through to the Shift
			// below, so the item is consumed, as it would have been had
			// processInvMsg seen the block held. It guards a caller that does
			// not go through processInvMsg, and a stream that starts between
			// the Append and this drain.
			if sm.blockHeldLocally(iv.Hash) {
				break
			}

			// An owner still sending block bytes has not gone quiet, however
			// long ago the block was asked for: it is delivering what was queued
			// ahead of this one, and nothing is arriving for THIS block yet, so
			// blockHeldLocally above is false and the retry window below has
			// passed. Asking another peer then downloads the block twice. The
			// wanted-range pass has made the same check since the 447 MB block
			// at height 705,000 (unownedBlocksUpTo); SV Node does not re-ask a
			// block from a peer that is still delivering. Consumed like the
			// RequestedWithin case under it.
			//
			// What this widens, handed to the tip walk (step 10 of the 2026-10-02
			// re-review): suppression now lasts the whole stream and the whole
			// busy delivery, not sixty seconds, so a stream that fails above the
			// checkpoint has by then consumed every peer's one-shot inv for the
			// block. Recovery today is the next block's park sending
			// requestMissingBlocks (handleBlockOnDiskMsg), one block interval.
			// The tip walk's quiet re-ask owns that gap, and must call
			// blockHeldLocally before re-asking or it brings this bug back for
			// the owed-block case.
			if sm.blockDownloads.AnyOwner(iv.Hash, func(p *peerpkg.Peer) bool {
				return time.Since(sm.streams.lastBlockBytes(p)) < blockRequestRetryInterval
			}) {
				break
			}

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

	// A block this node holds but the chain does not: no getdata, because the
	// bytes are here or on their way; and no getblocks either, because the
	// branch at the bottom anchors its locator on the announced block, and
	// GetBlockLocator fails for a hash the chain does not have (the real
	// client walks the chain from it), which logged one Error per duplicate
	// inv at the tip. Answered from memory, before the chain round trip
	// haveInventory makes.
	if iv.Type == wire.InvTypeBlock && sm.blockHeldLocally(iv.Hash) {
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

		// The disk, after memory and the chain have both said no. holdsBlock
		// reads the converted record back and covers the hand-off that none of
		// blockHeldLocally's four states does, and that hand-off is on EVERY
		// block, not a corner: handleBlockOnDiskMsg forgives the owners at
		// intake, which back-dates the ledger so RequestedWithin no longer
		// suppresses a re-ask, then reads the record's size from disk and asks
		// the chain whether the parent is reachable before AdoptWritten parks
		// it; and blockDispatcher.dispatch takes the entry out of the park
		// before it appends it to the frontier. In both windows the record on
		// disk is the only sign the block is here. Placed after haveInventory
		// so an inv for a chain-held block, the common case at the tip where
		// every peer announces every block, pays no blob read.
		//
		// Held on disk but not in the park is a record nothing announced: its
		// peer dropped between the sink writing it and the on-disk message
		// reaching the consumer (adoptStranded's doc has the incident, mainnet
		// stopped at 650,021 on 2026-09-23). Above the checkpoint the
		// wanted-range pass never visits the height, so this is the only path
		// that can adopt it; swallowing the inv without adopting would leave the
		// record unadopted until a restart's Recover. Same guard as
		// unownedBlocksUpTo: a block being committed has left the park but not
		// the disk, which is in flight, not stranded, and putting it back would
		// have it read again after its files are gone. adoptStranded itself
		// refuses a record younger than strandedRecordAge, so the hand-off above
		// is never adopted under the consumer.
		if iv.Type == wire.InvTypeBlock && sm.holdsBlock(sm.ctx, iv.Hash) {
			if !sm.blockPark.Has(iv.Hash) && !sm.blockCommitting(iv.Hash) &&
				sm.blockPark.adoptStranded(sm.ctx, iv.Hash, sm.subtreeStore) {
				sm.logger.Warnf("[handleInvMsg][%s] adopted a complete record that was on disk but not in the park", iv.Hash)
			}

			return
		}

		// Add it to the request queue.
		state.requestQueue.Append(iv)

		return
	}

	if iv.Type == wire.InvTypeBlock {
		// We already have the final block advertised by this inventory message, so force a request for more.  This
		// should only happen if we're on a really long side chain.
		//
		// Only a block the chain holds reaches here now, so GetBlockLocator can
		// anchor on it. The height passed is 0, so the locator it builds is
		// genesis-only (computeLocatorHeights(0) is [0]); that is a pre-existing
		// defect of this branch, left in place here, not something the early
		// returns above made correct.
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

// blockHandler is the main handler for the sync manager.  It must be run as a
// goroutine.  It processes block and inv messages in a separate goroutine
// from the peer handlers so the block (MsgBlock) messages are handled by a
// single thread without needing to lock memory data structures.  This is
// important because the sync manager controls which blocks are needed and how
// the fetching should proceed.
func (sm *SyncManager) blockHandler() {
	ticker := time.NewTicker(syncPeerTickerInterval)
	defer ticker.Stop()

	// The dispatcher commits parked blocks one at a time on the consumer
	// goroutine. Nil-guarded because tests build SyncManager as a struct literal
	// that bypasses New().
	if sm.dispatcher == nil {
		sm.dispatcher = newBlockDispatcher(sm)
	}

	// start the block queue handler
	sm.consumerDone = make(chan struct{})

	go func() {
		defer close(sm.consumerDone)

		sm.dispatchBlocks()
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

	go sm.runFrontierRace()

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

// AcquireBlockPrefetch reserves one admission slot for blockHash, which the
// caller MUST later hand back to ReleaseBlockPrefetch exactly once. Every
// admitted block costs exactly one slot: whenever the park is enabled,
// streaming (and so conversion) is unconditional, and the block's bytes are
// gone by the time this runs — the wire layer streamed them through the
// subtree builder and out to files, so there is nothing left to charge by
// size. One slot per in-flight block is what this path can honestly pay, and
// it is the unit SV Node bounds by.
//
// It returns an error only if ctx is cancelled while waiting (shutdown), in
// which case nothing was reserved, OR the benign ErrDuplicateBlockInFlight
// sentinel when blockHash is already in flight (dedup — again nothing
// reserved). While blocked waiting for a slot it increments
// blockPrefetchWaiters so the stall detector can tell self-backpressure apart
// from a genuinely stalled peer.
//
// The caller MUST hand blockHash back to ReleaseBlockPrefetch on success: the
// hash lives in the in-flight set for exactly the same lifetime as the
// reserved slot (inserted here, deleted on release), so the dedup half and the
// slot half of this admission gate never drift.
func (sm *SyncManager) AcquireBlockPrefetch(ctx context.Context, blockHash chainhash.Hash) error {
	// Dedup: reserve the hash BEFORE reserving a slot. Inserting ahead of the
	// (possibly blocking) Acquire is deliberate — it bounds duplicates even while
	// a copy is parked waiting for a slot, so N copies of one requested block
	// cannot each grab a slot and fill the gate. A hash already present is a
	// duplicate: drop it (nothing reserved, nothing inserted).
	sm.inFlightBlocksMu.Lock()
	if _, dup := sm.inFlightBlocks[blockHash]; dup {
		sm.inFlightBlocksMu.Unlock()
		return ErrDuplicateBlockInFlight
	}
	sm.inFlightBlocks[blockHash] = &inFlightBlock{}
	sm.inFlightBlocksMu.Unlock()

	// removeInFlight undoes the reservation above. It runs only when the budget
	// Acquire fails (ctx cancel): nothing was reserved, so the hash must not
	// linger. On success the hash stays until ReleaseBlockPrefetch deletes it.
	removeInFlight := func() {
		sm.inFlightBlocksMu.Lock()
		delete(sm.inFlightBlocks, blockHash)
		sm.inFlightBlocksMu.Unlock()
	}

	// Fast path: a slot available right now, no waiter accounting needed.
	if sm.blockPrefetchBudget.TryAcquire(1) {
		sm.blockPrefetchReserved.Add(1)

		return nil
	}

	// Slow path: we must wait for an in-flight conversion to finish. Flag that
	// this read-loop is backpressured by our own processing so the stall
	// detector does not mistake the resulting read stall for a slow peer.
	sm.blockPrefetchWaiters.Add(1)
	defer sm.blockPrefetchWaiters.Add(-1)

	if err := sm.blockPrefetchBudget.Acquire(ctx, 1); err != nil {
		// Nothing reserved: drop the hash we inserted before parking so a
		// cancelled acquire never leaks a slot in the dedup set.
		removeInFlight()
		return err
	}

	sm.blockPrefetchReserved.Add(1)

	return nil
}

// conversionInFlight reports whether a copy of this block holds an admission,
// which on the streaming path means it is being converted right now.
func (sm *SyncManager) conversionInFlight(blockHash chainhash.Hash) bool {
	sm.inFlightBlocksMu.Lock()
	defer sm.inFlightBlocksMu.Unlock()

	_, ok := sm.inFlightBlocks[blockHash]

	return ok
}

// blockHeldLocally reports whether this node already holds block h or is in the
// middle of taking it in: parked on disk, taken off the park by the dispatcher to
// validate, its bytes arriving from a peer now, or being converted. It is the
// in-memory half of "do we need to ask a peer for this block"; the chain is the
// other half, and haveInventory asks it. SV Node asks an inv the same two
// questions, AlreadyHave (IsBlockKnown for MSG_BLOCK) and
// BlockDownloadTracker::IsInFlight (net/net_processing.cpp, the inv handler).
// Its IsInFlight covers getdata to receipt (MarkBlockAsInFlight to
// MarkBlockAsReceived); here RequestedWithin covers the first retry window of
// that and arriving starts at the first byte, so the requested-but-silent window
// is drainRequestQueue's busy-owner guard, not this.
//
// One answer for the inv path and the wanted-range pass, which has skipped
// arriving and converting blocks since the 2026-09-24 incident (unownedBlocksUpTo)
// while the inv path checked parked and validating only, and drifted. Every term
// guards a nil receiver (blockPark.Has, blockDispatcher.inFlight,
// streamRegistry.arriving) or reads a nil map under a zero mutex
// (conversionInFlight), so a struct-literal manager in a test may call it.
func (sm *SyncManager) blockHeldLocally(h chainhash.Hash) bool {
	return sm.blockPark.Has(h) || sm.blockCommitting(h) || sm.streams.arriving(h) || sm.conversionInFlight(h)
}

// blockCommitting reports whether this block is out of the park and in use: taken by
// any route (the drain step for the dispatcher, the serial drain, the sweep or the
// header walk's re-offer) and not yet given back or settled. Its record is still on disk
// and still in use until its taker's disposition deletes it or puts it back, so nothing
// may park it again or treat it as stranded. The park owns that answer (blockPark.out).
//
// The dispatcher's own in-flight list is asked as well. Every block it dispatches is one
// the drain step took from the park, so in production this adds nothing; it is kept
// because inFlight is the dispatcher's own contract (parentIsReachable reads it too), and
// a frontier entry is in flight whoever built it. Nil-safe on both terms.
func (sm *SyncManager) blockCommitting(h chainhash.Hash) bool {
	return sm.blockPark.HandedOut(h) || sm.dispatcher.inFlight(h)
}

// inFlightBlock marks that a block's hash currently holds the dedup half of
// the admission gate. It carries no other state: with the gate down to one
// slot per block there is nothing left to release in two steps (see
// ReleaseBlockPrefetch).
type inFlightBlock struct{}

// ReleaseBlockPrefetch returns blockHash's admission slot and drops its hash
// from the in-flight dedup set, together: with every admitted block costing
// exactly one slot, there is no earlier moment to hand the slot to a different
// budget the way a decoded block's serialized bytes once were, so the two
// halves of the gate share one release as well as one acquire.
//
// A second release of a hash already released (or one never acquired) is a
// no-op rather than a panic: admitPipelineSink's caller may release on both
// the hand-off and its own deferred cleanup, and the underlying semaphore
// panics on an unbalanced Release.
func (sm *SyncManager) ReleaseBlockPrefetch(blockHash chainhash.Hash) {
	sm.inFlightBlocksMu.Lock()
	_, ok := sm.inFlightBlocks[blockHash]
	delete(sm.inFlightBlocks, blockHash)
	sm.inFlightBlocksMu.Unlock()

	if !ok {
		return
	}

	sm.blockPrefetchBudget.Release(1)
	sm.blockPrefetchReserved.Add(-1)
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

// noteChainProgress records that a block joined the chain, for the commit rate.
func (sm *SyncManager) noteChainProgress() {
	sm.commitRate.note(time.Now())
}

// localReadBackpressured reports whether the node is currently throttling its
// own network reads because local block processing cannot keep up. The stall
// detector skips its checks while this holds, since zero throughput then
// reflects our validation speed, not the sync peer's health: a read loop is
// parked in AcquireBlockPrefetch waiting for a conversion slot.
func (sm *SyncManager) localReadBackpressured() bool {
	return sm.blockPrefetchWaiters.Load() > 0
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

	sm.savePeerRates()

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

	sm.orphanTxs.Stop()
	sm.requestedTxns.Stop()

	if sm.recentlyFailedBlocks != nil {
		sm.recentlyFailedBlocks.Stop()
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
func (sm *SyncManager) announceTx(item *TxHashAndFee, parents ...chainhash.Hash) {
	sm.txAnnounceMu.RLock()
	defer sm.txAnnounceMu.RUnlock()

	if !sm.txAnnounceClosed && sm.txAnnounceBatcher != nil {
		if len(parents) > 0 && sm.announceParents != nil {
			sm.announceParents.Set(item.TxHash, parents)
		}

		sm.txAnnounceBatcher.Put(item)
	}
}

// newTxAnnounceBatcher builds the batcher that announces new txs to peers,
// parents first within each batch.
//
// It runs with background=false, so the batcher's worker flushes one batch
// at a time, in order. With background=true go-batcher starts a goroutine
// per flush, so a child in one batch could reach peers before its parent in
// the previous one. The callback does not block: AnnounceNewTransactions
// hands each batch to an ordered sender and returns.
func (sm *SyncManager) newTxAnnounceBatcher(size int, timeout time.Duration) *batcher.BatcherWithDedup[TxHashAndFee] {
	return batcher.NewWithDeduplicationAndPool[TxHashAndFee](size, timeout, func(batch []*TxHashAndFee) {
		sm.logger.Debugf("announcing %d transactions to peers", len(batch))

		// process the batch, parents first
		sm.peerNotifier.AnnounceNewTransactions(sm.orderAnnounceBatch(batch))
	}, false,
		batcher.WithName("netsync_tx_announce"),
		batcher.WithLogger(sm.logger),
		batcher.WithMetrics(batchermetrics.Provider()),
		batcher.WithTracer(tracing.Tracer("SyncManager").OTelTracer()),
	)
}

// orderAnnounceBatch reorders a batch from txAnnounceBatcher so every tx
// comes after any of its parents in the same batch. The txmeta topic is
// spread over partitions, so a child can be read, and batched, before its
// parent; SV Node only accepts a child once it has the parent. A child whose
// parent is in a later batch is not held back: the peer's orphan pool and the
// rebroadcast queue cover that. Returns a new slice, since the batcher reuses
// the one it passes in.
func (sm *SyncManager) orderAnnounceBatch(batch []*TxHashAndFee) []*TxHashAndFee {
	hashes := make([]chainhash.Hash, len(batch))
	parents := make([][]chainhash.Hash, len(batch))

	for i, item := range batch {
		hashes[i] = item.TxHash

		if sm.announceParents != nil {
			if p, ok := sm.announceParents.Get(item.TxHash); ok {
				parents[i] = p
				sm.announceParents.Delete(item.TxHash)
			}
		}
	}

	ordered := make([]*TxHashAndFee, 0, len(batch))
	for _, i := range ParentsFirst(hashes, func(i int) []chainhash.Hash { return parents[i] }) {
		ordered = append(ordered, batch[i])
	}

	return ordered
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

	// SV Node's ContextualCheckBlockHeader for the header cache, judged against
	// this chain's parameters and reading the committed chain through the
	// blockchain client. Built here, before anything in New starts a goroutine,
	// because it is the one step that can fail.
	headerRules, err := newHeaderRules(logger, tSettings, config.ChainParams, blockchainClient)
	if err != nil {
		return nil, err
	}

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
		commitRate:       newCommitRateTracker(),
		streams:          newStreamRegistry(),
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

	// Where every downloaded block is converted to and committed from. Not
	// optional: a node without a usable park refuses to start.
	park, err := newBlockPark(logger, tSettings, tempStore)
	if err != nil {
		return nil, err
	}

	sm.blockPark = park

	if config.DataDir != "" {
		sm.peerRatesPath = filepath.Join(config.DataDir, peerRatesFile)

		if rates, err := loadPeerRates(sm.peerRatesPath); err != nil {
			sm.logger.Warnf("[legacy] could not read %s, starting with no remembered peer rates: %v", sm.peerRatesPath, err)
		} else {
			sm.streams.remember(rates)
			sm.logger.Infof("[legacy] remembered the download rates of %d peer addresses from %s", len(rates), sm.peerRatesPath)
		}
	}

	// Before the first download pass: the backstop and the watcher need the block sizes of this part of the
	// chain, not only of the blocks that complete after the start.
	sm.seedBlockSizes(ctx)

	// Now the park exists, the wire layer can be told where to put a block body
	// it reads straight off the socket. Before this call the streaming handler
	// was registered but inert: it checks for a sink and a gate and found
	// neither, so every block took the decoding path. See streaming_install.go
	// for why that mattered beyond the allocation.
	sm.installStreamingBlockPath(peerpkg.SetBlockBodyStreaming)

	// Bounded async block prefetch: admitPipelineSink admits a block against
	// this global weighted semaphore and returns, so the read-loop reads the
	// next block while the current one converts. Every block costs one slot,
	// whatever its size — the streaming pipeline is what receives every block,
	// and the bytes are gone by the time admission runs (see
	// AcquireBlockPrefetch) — so the budget is sized as a block count derived
	// from the per-peer queue-depth setting: see pipelineBlockSlotPeerAllowance
	// for the multiplier's reasoning.
	capacity := int64(tSettings.Legacy.MaxBlocksInTransitPerPeer) * pipelineBlockSlotPeerAllowance
	if capacity < 1 {
		capacity = 1
	}

	sm.blockPrefetchBudgetSlots = capacity
	sm.blockPrefetchBudget = semaphore.NewWeighted(capacity)
	logger.Infof("[legacy] streaming pipeline active: download admission budget sized as %d block slots (maxBlocksInTransitPerPeer=%d x %d)",
		capacity, tSettings.Legacy.MaxBlocksInTransitPerPeer, pipelineBlockSlotPeerAllowance)

	// Dedup half of the same admission gate as the budget semaphore, created
	// in lockstep with it: paired 1:1 with each budget reservation so at most
	// one copy of a block hash is ever admitted/queued at a time.
	sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)

	// create the transaction announcement batcher
	sm.announceParents = txmap.NewSyncedMap[chainhash.Hash, []chainhash.Hash](2 * maxRequestedTxns)
	sm.txAnnounceBatcher = sm.newTxAnnounceBatcher(maxRequestedTxns, 1*time.Second)

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
	// a nil list simply means no proof is ever granted. The proof-of-work
	// ceiling goes in the same way, so Fill refuses a header nobody paid for
	// before it names a height (SV Node's CheckProofOfWork on every header).
	// The contextual header rules go in the same way (built at the top of New,
	// before any goroutine starts, because building them can fail).
	sm.headerCache = newHeaderCache().
		WithCheckpoints(config.ChainParams.Checkpoints).
		WithPowLimit(model.PowLimitCeiling(config.ChainParams)).
		WithHeaderRules(headerRules).
		WithMinimumChainWork(minimumChainWork(config.ChainParams)).
		WithOwnerLive(sm.headerOwnerLive)

	// Tracks recently-failed block hashes so descendants of an unstored/rejected
	// block are short-circuited rather than triggering a NOT_FOUND ERROR cascade
	// (#1333). This starts a background eviction goroutine stopped only via
	// Stop(), so build it after the last fallible step above: constructing it
	// before an early error return would leak that goroutine, since the caller
	// receives a nil SyncManager and can never call Stop().
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](recentlyFailedBlocksTTL).WithMaxSize(recentlyFailedBlocksMaxTracked)

	// The dispatcher holds a pointer to the manager returned below, so it must be
	// built from &sm, not from the local value.
	sm.dispatcher = newBlockDispatcher(&sm)
	// Below the last fallible step above for the same reason the two maps are: a
	// goroutine started before it leaks when that step returns an error, because
	// the caller receives a nil SyncManager and can never call Stop.
	// One slot per commit a sweep tick can post, so a tick never waits on the
	// consumer for room and the consumer never waits on the sweep for anything.
	sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

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
	kafka.StartKafkaListener(ctx, sm.logger, kafkaURL, groupID, true, sm.processBlocksFinalMessage, &sm.settings.Kafka)
}

// processBlocksFinalMessage announces a block from the blocks_final topic to
// peers and signals the rebroadcast queue that a new block has been added.
// Malformed messages are logged and skipped; it never returns an error, so
// the listener does not retry them.
func (sm *SyncManager) processBlocksFinalMessage(msg *kafka.KafkaMessage) error {
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
	sm.peerNotifier.BlockConnected()

	return nil
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
			}, txMeta.TxInpoints.ParentTxHashes...)
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

// suppressBlockRejects reports whether invalid blocks must not be rejected to the
// serving peer. Only RUNNING proves the node is caught up; IDLE is included
// because an operator STOP can land while blocks are still syncing.
func suppressBlockRejects(state *teranodeblockchain.FSMStateType) bool {
	return state == nil || *state != teranodeblockchain.FSMStateRUNNING
}
