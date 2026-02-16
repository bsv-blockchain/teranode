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

	"github.com/bsv-blockchain/go-batcher"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
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
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/go-utils/expiringmap"
	"google.golang.org/protobuf/proto"
)

const (
	// defaultMaxInFlightBlocks is the default maximum number of blocks that
	// should be in the request queue for headers-first mode. This is the
	// starting value for small blocks, and will be dynamically adjusted down
	// based on observed block sizes to avoid memory issues with large blocks.
	defaultMaxInFlightBlocks = 20

	// maxNetworkViolations is the max number of network violations a
	// sync peer can have before a new sync peer is found.
	maxNetworkViolations = 3

	// maxRejectedTxns is the maximum number of rejected transactions
	// hashes to store in memory.
	maxRejectedTxns = 10_000

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
)

// zeroHash is the zero-value hash (all zeros).  It is defined as a convenience.
var zeroHash chainhash.Hash

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
}

// peerSyncState stores additional information that the SyncManager tracks
// about a peer.
type peerSyncState struct {
	syncCandidate   bool
	requestQueue    *txmap.SyncedSlice[wire.InvVect]
	requestedTxns   *expiringmap.ExpiringMap[chainhash.Hash, struct{}]
	requestedBlocks *expiringmap.ExpiringMap[chainhash.Hash, struct{}]
}

// syncPeerState stores additional info about the sync peer.
type syncPeerState struct {
	mu                sync.RWMutex // Protects all fields
	recvBytes         uint64
	recvBytesLastTick uint64
	lastBlockTime     time.Time
	violations        int
	ticks             uint64
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
	txAnnounceBatcher *batcher.BatcherWithDedup[TxHashAndFee]

	// These fields should only be accessed from the blockHandler thread
	// (except syncPeer/syncPeerState which are protected by syncPeerMu).
	rejectedTxns    *txmap.SyncedMap[chainhash.Hash, struct{}]
	requestedTxns   *expiringmap.ExpiringMap[chainhash.Hash, struct{}]
	requestedBlocks *expiringmap.ExpiringMap[chainhash.Hash, struct{}]
	syncPeerMu      sync.RWMutex // protects syncPeer and syncPeerState
	syncPeer        *peerpkg.Peer
	syncPeerState   *syncPeerState
	peerStates      *txmap.SyncedMap[*peerpkg.Peer, *peerSyncState]

	// The following fields are used for headers-first mode.
	headersFirstMode atomic.Bool // accessed from multiple goroutines, must be atomic
	headerList       *list.List
	startHeader      *list.Element
	nextCheckpoint   *chaincfg.Checkpoint
	blockSizeTracker *blockSizeTracker // tracks block sizes for dynamic in-flight adjustment

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

// storeSyncPeer sets the sync peer and its state, safe for concurrent access.
func (sm *SyncManager) storeSyncPeer(peer *peerpkg.Peer, state *syncPeerState) {
	sm.syncPeerMu.Lock()
	defer sm.syncPeerMu.Unlock()
	sm.syncPeer = peer
	sm.syncPeerState = state
}

// resetHeaderState sets the headers-first mode state to values appropriate for
// syncing from a new peer.
func (sm *SyncManager) resetHeaderState(newestHash *chainhash.Hash, newestHeight int32) {
	sm.headersFirstMode.Store(false)
	sm.headerList.Init()
	sm.startHeader = nil

	// When there is a next checkpoint, add an entry for the latest known
	// block into the header pool.  This allows the next downloaded header
	// to prove it links to the chain properly.
	if sm.nextCheckpoint != nil {
		node := headerNode{height: newestHeight, hash: newestHash}
		sm.headerList.PushBack(&node)
	}
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
		sm.logger.Errorf("Failed to get best block header: %v", err)
		return
	}

	bestPeers := make([]*peerpkg.Peer, 0)

	okPeers := make([]*peerpkg.Peer, 0)

	sm.logger.Debugf("[startSync] selecting sync peer from %d candidates", sm.peerStates.Length())

	for peer, state := range sm.peerStates.Range() {
		if !state.syncCandidate {
			sm.logger.Debugf("[startSync] peer %v is not a sync candidate", peer.String())

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

	// Clear the requestedBlocks if the sync peer changes, otherwise
	// we may ignore blocks we need that the last sync peer failed
	// to send.
	sm.requestedBlocks.Clear()

	locator, err := sm.blockchainClient.GetBlockLocator(sm.ctx, bestBlockHeader.Hash(), bestBlockHeaderMeta.Height)
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
	if sm.nextCheckpoint != nil &&
		bestBlockHeightInt32 < sm.nextCheckpoint.Height &&
		sm.chainParams != &chaincfg.RegressionNetParams {
		if err = bestPeer.PushGetHeadersMsg(locator, sm.nextCheckpoint.Hash); err != nil {
			sm.logger.Warnf("[startSync] Failed to send getheaders message to peer %s: %v", bestPeer.String(), err)

			return
		}

		sm.headersFirstMode.Store(true)

		sm.logger.Infof("[startSync] Downloading headers for blocks %d to %d from peer %s", bestBlockHeaderMeta.Height+1, sm.nextCheckpoint.Height, bestPeer.String())
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

	sm.logger.Infof("New valid peer %s (%s)", peer, peer.UserAgent())

	// Initialize the peer state
	isSyncCandidate := sm.isSyncCandidate(peer)

	state, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
	if err != nil {
		sm.logger.Debugf("Error getting FSM current state: %v", err)
	}

	if *state == teranodeblockchain.FSMStateLEGACYSYNCING && sm.currentFeeFilter.Load() != bsvutil.SatoshiPerBitcoin {
		// Set fee filter to inform peers that we don't want to be notified of transactions while we're syncing
		feeFilter := wire.NewMsgFeeFilter(bsvutil.SatoshiPerBitcoin)

		peer.QueueMessage(feeFilter, nil)

		sm.currentFeeFilter.Store(bsvutil.SatoshiPerBitcoin)
	}

	sm.peerStates.Set(peer, &peerSyncState{
		syncCandidate:   isSyncCandidate,
		requestQueue:    txmap.NewSyncedSlice[wire.InvVect](maxRequestedBlocks),
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second), // allow the node 10 seconds to respond to the tx request
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](60 * time.Minute), // allow the node 1 hour to respond to the requested blocks, needed for legacy sync/checkpoints
	})

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

	validNetworkSpeed := sps.validNetworkSpeed(sm.minSyncPeerNetworkSpeed)
	lastBlockSince := time.Since(sps.getLastBlockTime())

	sm.logger.Debugf("[CheckSyncPeer] sync peer %s check, network violations: %v (limit %v), time since last block: %v (limit %v), headers-first mode: %v", sp.String(), validNetworkSpeed, maxNetworkViolations, lastBlockSince, maxLastBlockTime, sm.headersFirstMode.Load())

	// Don't check network speed during headers-first mode, as we're intentionally
	// downloading small headers (80 bytes each) rather than full blocks. The peer
	// may appear slow because we're not requesting much data, not because it's actually slow.
	isNetworkSpeedViolation := !sm.headersFirstMode.Load() && (validNetworkSpeed >= maxNetworkViolations)

	// Check network speed of the sync peer and its last block time. If we're currently
	// flushing the cache skip this round.
	if !isNetworkSpeedViolation && (lastBlockSince <= maxLastBlockTime) {
		return
	}

	var reason string
	if isNetworkSpeedViolation {
		reason = "network speed violation"
	} else if lastBlockSince > maxLastBlockTime {
		reason = "last block time out of range"
	}
	sm.logger.Debugf("[CheckSyncPeer] sync peer %s is stalled due to %s, updating sync peer", sp.String(), reason)

	state, exists := sm.peerStates.Get(sp)
	if !exists {
		return
	}

	sm.logger.Debugf("[CheckSyncPeer] removing sync peer %s", sp.String())

	sm.clearRequestedState(state)
	sm.updateSyncPeer(state)
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
	sm.clearRequestedState(state)

	// Fetch a new sync peer if this is the sync peer.
	if peer == sm.loadSyncPeer() {
		sm.updateSyncPeer(state)
	}
}

// clearRequestedState removes requested transactions
// and blocks from the global map.
func (sm *SyncManager) clearRequestedState(state *peerSyncState) {
	// Remove requested transactions from the global map so that they will
	// be fetched from elsewhere next time we get an inv.
	state.requestedTxns.Clear()

	// Remove requested blocks from the global map so that they will be
	// fetched from elsewhere next time we get an inv.
	state.requestedBlocks.Clear()
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
			sm.logger.Debugf("Current header sync state - headerList length: %d, startHeader exists: %v",
				sm.headerList.Len(), sm.startHeader != nil)
		}

		sp.SetSyncPeer(false)
		sp.DisconnectWithInfo("updateSyncPeer - disconnect old sync peer")
	}

	// Reset sync peer state
	sm.storeSyncPeer(nil, nil)

	bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		// TODO we should return an error here to the caller
		sm.logger.Errorf("Failed to get best block header: %v", err)
		return
	}

	bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
	if err != nil {
		sm.logger.Errorf("failed to convert block height to int32: %v", err)
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
		if errors.Is(err, errors.ErrTxMissingParent) || errors.Is(err, errors.ErrTxLocked) {
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
			if errors.Is(err, errors.ErrTxMissingParent) || errors.Is(err, errors.ErrTxLocked) {
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
			sm.logger.Errorf("failed to convert block height to int32: %v", err)
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
// still have blocks to check
func (sm *SyncManager) current() bool {
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
		sm.logger.Errorf("failed to convert block height to int32: %v", err)
	}

	// No matter what the chain thinks, if we are below the block we are syncing to we are not current.
	if bestBlockHeightInt32 < sp.LastBlock() {
		return false
	}

	return true
}

// handleBlockMsg handles block messages from all peers.
func (sm *SyncManager) handleBlockMsg(bmsg *blockQueueMsg) error {
	sm.logger.Debugf("[handleBlockMsg][%s] received block height %d from %s", bmsg.blockHash, bmsg.blockHeight, bmsg.peer)
	peer := bmsg.peer

	state, exists := sm.peerStates.Get(peer)
	if !exists {
		sm.logger.Errorf("[handleBlockMsg][%s] Received block message from unknown peer %s", bmsg.blockHash, peer)
		return errors.NewServiceError("[handleBlockMsg] Received block message from unknown peer %s", peer)
	}

	legacySyncMode := false
	catchingBlocks := false

	sm.logger.Debugf("[handleBlockMsg][%s] checking current FSM state", bmsg.blockHash)

	fsmState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
	if err != nil {
		return errors.NewProcessingError("[handleBlockMsg] failed to get current FSM state", err)
	}

	if fsmState != nil {
		switch *fsmState {
		case teranodeblockchain.FSMStateLEGACYSYNCING:
			legacySyncMode = true
		case teranodeblockchain.FSMStateCATCHINGBLOCKS:
			catchingBlocks = true
		}
	}

	// If we didn't ask for this block then the peer is misbehaving.
	if _, exists = state.requestedBlocks.Get(bmsg.blockHash); !exists {
		// The regression test intentionally sends some blocks twice
		// to test duplicate block insertion fails.  Don't disconnect
		// the peer or ignore the block when we're in regression test
		// mode, in this case, so the chain code is actually fed the
		// duplicate blocks.
		if sm.chainParams != &chaincfg.RegressionNetParams {
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
	isCheckpointBlock := false

	if sm.headersFirstMode.Load() {
		sm.logger.Debugf("[handleBlockMsg][%s] headers-first mode, checking block", bmsg.blockHash)

		firstNodeEl := sm.headerList.Front()
		if firstNodeEl != nil {
			firstNode := firstNodeEl.Value.(*headerNode)

			if bmsg.blockHash.IsEqual(firstNode.hash) {
				if firstNode.hash.IsEqual(sm.nextCheckpoint.Hash) {
					isCheckpointBlock = true
				} else {
					sm.headerList.Remove(firstNodeEl)
				}
			}
		}
	}

	// Remove block from request maps. Either chain will know about it, and
	// so we shouldn't have any more instances of trying to fetch it, or we
	// will fail the insert, and thus we'll retry next time we get an inv.
	state.requestedBlocks.Delete(bmsg.blockHash)
	sm.requestedBlocks.Delete(bmsg.blockHash)

	// Track block size for dynamic in-flight adjustment during headers-first mode.
	// This allows us to start aggressive (20 blocks) and automatically reduce
	// to 1 block when encountering large (>2GB) blocks on mainnet.
	if sm.headersFirstMode.Load() && bmsg.block != nil {
		blockSize := int64(bmsg.block.SerializeSize())
		sm.blockSizeTracker.addBlockSize(blockSize)

		dynamicMax := sm.blockSizeTracker.calculateMaxInFlightBlocks()
		avgSize := sm.blockSizeTracker.getAverageSize()
		sm.logger.Debugf("[handleBlockMsg][%s] Block size: %d bytes, avg: %d bytes, dynamic max in-flight: %d",
			bmsg.blockHash, blockSize, avgSize, dynamicMax)
	}

	sm.logger.Debugf("[handleBlockMsg][%s] calling HandleBlockDirect", bmsg.blockHash)

	// if not in Legacy Sync mode, we need to potentially download the block,
	// promote block to the block validation via kafka (p2p -> blockvalidation message),
	// without calling HandleBlockDirect. Such that it doesn't interfere with the operation of block validation.
	if err = sm.HandleBlockDirect(sm.ctx, bmsg.peer, bmsg.blockHash, bmsg.block); err != nil {
		if (legacySyncMode || catchingBlocks) && errors.Is(err, errors.ErrBlockNotFound) {
			// previous block not found? Probably a new block message from our syncPeer while we are still syncing
			sm.logger.Errorf("Failed to process new block in legacy mode %v: %v", bmsg.blockHash, err)
		} else if errors.Is(err, errors.ErrBlockNotFound) {
			// We don't have the parent of this block/header, so we'll request it.
			sm.logger.Infof("Block %v has missing parent %v, requesting missing blocks",
				bmsg.blockHash, bmsg.block.Header.PrevBlock)

			bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
			if err != nil {
				sm.logger.Errorf("Failed to get best block header: %v", err)
				return nil
			}
			// Create a block locator starting from the parent hash
			locator, err := sm.blockchainClient.GetBlockLocator(sm.ctx, bestBlockHeader.Hash(), bestBlockHeaderMeta.Height)
			if err != nil {
				sm.logger.Errorf("Failed to get block locator for the block hash %s: %v",
					bmsg.blockHash, err)
				return nil
			}

			// Send a getblocks message to request missing blocks
			zeroHash := chainhash.Hash{}
			if err = peer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
				sm.logger.Errorf("Failed to send getblocks message: %v", err)

				return nil
			}

			return nil
		} else if errors.Is(err, context.Canceled) {
			return nil
		} else {
			serviceError := errors.Is(err, errors.ErrServiceError) || errors.Is(err, errors.ErrStorageError)
			if !legacySyncMode && !catchingBlocks && !serviceError {
				peer.PushRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", &bmsg.blockHash, false)
			}

			sm.logger.Errorf("Failed to process new block in service blockQueueMsg %v: %v", bmsg.blockHash, err)

			// TODO TEMPORARY: we should not panic here, but return the error
			panic(err)
		}
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

	if sp, sps := sm.loadSyncPeerAndState(); peer == sp && sps != nil {
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
				sm.logger.Errorf("failed to convert block height to int32: %v", err)
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
		if sm.startHeader != nil && state.requestedBlocks.Len() < dynamicMax {
			sm.fetchHeaderBlocks()
		} else if !sm.current() && state.requestedBlocks.Len() == 0 {
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

	// This is headers-first mode and the block is a checkpoint.  When
	// there is a next checkpoint, get the next round of headers by asking
	// for headers starting from the block after this one up to the next
	// checkpoint.
	prevHeight := sm.nextCheckpoint.Height
	prevHash := sm.nextCheckpoint.Hash

	sm.nextCheckpoint = sm.findNextHeaderCheckpoint(prevHeight)
	if sm.nextCheckpoint != nil {
		locator := blockchain.BlockLocator([]*chainhash.Hash{prevHash})

		err = peer.PushGetHeadersMsg(locator, sm.nextCheckpoint.Hash)
		if err != nil {
			return errors.NewServiceError("failed to send getheaders message to peer %s", peer.String(), err)
		}

		if sp := sm.loadSyncPeer(); sp != nil {
			sm.logger.Infof(
				"handleBlockMsg - Downloading headers for blocks %d to %d from peer %s",
				prevHeight+1,
				sm.nextCheckpoint.Height,
				sp.String(),
			)
		}

		return nil
	}

	// This is headers-first mode, the block is a checkpoint, and there are
	// no more checkpoints, so switch to normal mode by requesting blocks
	// from the block after this one up to the end of the chain (zero hash).
	sm.headersFirstMode.Store(false)
	sm.headerList.Init()
	sm.logger.Infof("Reached the final checkpoint -- switching to normal mode")

	locator := blockchain.BlockLocator([]*chainhash.Hash{&bmsg.blockHash})
	if err = peer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
		return errors.NewServiceError("Failed to send getblocks message to peer %s", peer.String(), err)
	}

	return nil
}

// fetchHeaderBlocks creates and sends a request to the syncPeer for the next
// list of blocks to be downloaded based on the current list of headers.
func (sm *SyncManager) fetchHeaderBlocks() {
	// Nothing to do if there is no sync peer.
	sp := sm.loadSyncPeer()
	if sp == nil {
		sm.logger.Warnf("fetchHeaderBlocks called with no sync peer")
		return
	}

	// Nothing to do if there is no start header.
	if sm.startHeader == nil {
		sm.logger.Warnf("fetchHeaderBlocks called with no start header")
		return
	}

	// Calculate how many blocks to request to reach the dynamic max limit.
	// The limit adjusts based on observed block sizes (20 for small, down to 1 for >2GB).
	peerState, exists := sm.peerStates.Get(sp)
	if !exists {
		sm.logger.Warnf("[fetchHeaderBlocks] sync peer state not found")
		return
	}

	currentInFlight := peerState.requestedBlocks.Len()
	dynamicMaxInFlight := sm.blockSizeTracker.calculateMaxInFlightBlocks()
	maxBlocks := dynamicMaxInFlight - currentInFlight
	if maxBlocks <= 0 {
		sm.logger.Debugf("[fetchHeaderBlocks] Already at max in-flight blocks (%d/%d), not requesting more", currentInFlight, dynamicMaxInFlight)
		return
	}

	headerListLen := sm.headerList.Len()
	avgBlockSize := sm.blockSizeTracker.getAverageSize()
	sm.logger.Debugf("[fetchHeaderBlocks] Header list: %d blocks, in-flight: %d/%d, avg size: %d bytes, requesting: %d more",
		headerListLen, currentInFlight, dynamicMaxInFlight, avgBlockSize, maxBlocks)

	// Build up a getdata request for the list of blocks the headers
	// describe.  The size hint will be limited to wire.MaxInvPerMsg by
	// the function, so no need to double check it here.
	getDataMessage := wire.NewMsgGetDataSizeHint(uint(sm.headerList.Len())) // nolint:gosec
	numRequested := 0

	for e := sm.startHeader; e != nil; e = e.Next() {
		node, ok := e.Value.(*headerNode)
		if !ok {
			sm.logger.Warnf("Header list node type is not a headerNode")
			continue
		}

		iv := wire.NewInvVect(wire.InvTypeBlock, node.hash)

		haveInv, err := sm.haveInventory(iv)
		if err != nil {
			sm.logger.Warnf("Unexpected failure when checking for "+
				"existing inventory during header block "+
				"fetch: %v", err)
		}

		if !haveInv {
			if err = getDataMessage.AddInvVect(iv); err != nil {
				sm.logger.Warnf("Unexpected failure when adding inventory to getdata message: %v", err)
				break
			}

			sm.requestedBlocks.Set(*node.hash, struct{}{})
			peerState, _ := sm.peerStates.Get(sp)
			peerState.requestedBlocks.Set(*node.hash, struct{}{})

			numRequested++
		}

		sm.startHeader = e.Next()

		if numRequested >= maxBlocks {
			sm.logger.Debugf("[fetchHeaderBlocks] Limiting to %d block(s) from %s", numRequested, sp)
			break
		}
	}

	if len(getDataMessage.InvList) > 0 {
		sp.QueueMessage(getDataMessage, nil)
	}
}

// handleHeadersMsg handles block header messages from all peers.  Headers are
// requested when performing a headers-first sync.
func (sm *SyncManager) handleHeadersMsg(hmsg *headersMsg) {
	sm.logger.Debugf("[handleHeadersMsg] received headers message with %d headers from %s", len(hmsg.headers.Headers), hmsg.peer)
	peer := hmsg.peer

	_, exists := sm.peerStates.Get(peer)
	if !exists {
		sm.logger.Warnf("Received headers message from unknown peer %s", peer)
		return
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

	// ensure we have a valid starting point for header validation
	prevNodeEl := sm.headerList.Back()
	if prevNodeEl == nil {
		sm.logger.Warnf("Header list is empty, attempting to recover sync state")

		bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
		if err != nil {
			peer.DisconnectWithWarning(fmt.Sprintf("Failed to get best block header: %v", err))
			return
		}

		bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
		if err != nil {
			peer.DisconnectWithWarning(fmt.Sprintf("Failed to convert block height: %v", err))
			return
		}

		sm.resetHeaderState(bestBlockHeader.Hash(), bestBlockHeightInt32)

		prevNodeEl = sm.headerList.Back()
		if prevNodeEl == nil {
			peer.DisconnectWithWarning("Failed to initialize header sync state")
			return
		}
	}

	// Process all the received headers ensuring each one connects to the
	// previous and that checkpoints match.
	receivedCheckpoint := false

	var finalHash *chainhash.Hash

	for _, blockHeader := range msg.Headers {
		blockHash := blockHeader.BlockHash()
		finalHash = &blockHash

		// Ensure there is a previous header to compare against.
		prevNodeEl := sm.headerList.Back()
		if prevNodeEl == nil {
			peer.DisconnectWithWarning("Header list does not contain a previous element as expected")

			return
		}

		// Ensure the header properly connects to the previous one and
		// add it to the list of headers.
		node := headerNode{hash: &blockHash}

		prevNode := prevNodeEl.Value.(*headerNode)
		if prevNode.hash.IsEqual(&blockHeader.PrevBlock) {
			node.height = prevNode.height + 1
			e := sm.headerList.PushBack(&node)

			if sm.startHeader == nil {
				sm.startHeader = e
			}
		} else {
			peer.DisconnectWithWarning("Received block header that does not properly connect to the chain")

			return
		}

		// Verify the header at the next checkpoint height matches.
		if node.height == sm.nextCheckpoint.Height {
			if node.hash.IsEqual(sm.nextCheckpoint.Hash) {
				receivedCheckpoint = true

				sm.logger.Infof("Verified downloaded block "+
					"header against checkpoint at height "+
					"%d/hash %s", node.height, node.hash)
			} else {
				reason := fmt.Sprintf("Block header at height %d/hash "+
					"%s does NOT match expected checkpoint hash of %s",
					node.height, node.hash,
					sm.nextCheckpoint.Hash)
				peer.DisconnectWithWarning(reason)

				return
			}

			break
		}
	}

	// When this header is a checkpoint, switch to fetching the blocks for
	// all the headers since the last checkpoint.
	if receivedCheckpoint {
		// Since the first entry of the list is always the final block
		// that is already in the database and is only used to ensure
		// the next header links properly, it must be removed before
		// fetching the blocks.
		sm.headerList.Remove(sm.headerList.Front())
		sm.logger.Infof("Received %v block headers: Fetching blocks", sm.headerList.Len())
		sm.fetchHeaderBlocks()

		return
	}

	// This header is not a checkpoint, so request the next batch of
	// headers starting from the latest known header and ending with the
	// next checkpoint.
	locator := blockchain.BlockLocator([]*chainhash.Hash{finalHash})

	if err := peer.PushGetHeadersMsg(locator, sm.nextCheckpoint.Hash); err != nil {
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
		// check whether this block exists in the blockchain service
		return sm.blockchainClient.GetBlockExists(sm.ctx, &invVect.Hash)

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

	state, exists := sm.peerStates.Get(peer)
	if !exists {
		sm.logger.Warnf("[handleInvMsg] Received inv message from unknown peer %s", peer)
		return
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
				sm.logger.Errorf("failed to convert block height to int32: %v", err)
			}

			peer.UpdateLastBlockHeight(blockHeightInt32)
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
	numRequested := 0
	gdmsg := wire.NewMsgGetData()

outside:
	for state.requestQueue.Length() != 0 {
		// shift the first items from the request queue until we have enough to send in a single message
		iv, found := state.requestQueue.Shift()
		if !found {
			break
		}

		switch iv.Type {
		case wire.InvTypeBlock:
			// Request the block if there is not already a pending request.
			if _, exists = sm.requestedBlocks.Get(iv.Hash); !exists {
				if err = gdmsg.AddInvVect(iv); err != nil {
					sm.logger.Warnf("Unexpected failure when adding inventory to getdata message: %v", err)
					break outside
				}

				sm.requestedBlocks.Set(iv.Hash, struct{}{})
				state.requestedBlocks.Set(iv.Hash, struct{}{})

				numRequested++
			}

		case wire.InvTypeTx:
			// Request the transaction if there is not already a pending request.
			if _, exists = sm.requestedTxns.Get(iv.Hash); !exists {
				if err = gdmsg.AddInvVect(iv); err != nil {
					sm.logger.Warnf("Unexpected failure when adding inventory to getdata message: %v", err)
					break outside
				}

				sm.requestedTxns.Set(iv.Hash, struct{}{})
				state.requestedTxns.Set(iv.Hash, struct{}{})

				numRequested++
			}
		}

		if numRequested >= maxRequestedBlocks {
			sm.logger.Debugf("[handleInvMsg] Limiting to %d item(s) from %s", numRequested, peer)
			break
		}
	}

	if len(gdmsg.InvList) > 0 {
		sm.logger.Debugf("[handleInvMsg] Requesting %d items from %s", len(gdmsg.InvList), peer)
		peer.QueueMessage(gdmsg, nil)
	}
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

	// TODO make this configurable
	maxBlockQueue := 10_000

	// create a block queue to handle block messages in a separate goroutine, in order
	blockQueue := make(chan *blockQueueMsg, maxBlockQueue)

	// start the block queue handler
	go func() {
		for {
			select {
			case <-sm.quit:
				return
			case msg := <-blockQueue:
				sm.logger.Debugf("[blockHandler][%s] processing block queue message into handleBlockMsg", msg.blockHash)

				err := sm.handleBlockMsg(msg)
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

				blockQueue <- &blockQueueMsg{
					block:       msg.block.MsgBlock(),
					blockHash:   *msg.block.Hash(),
					blockHeight: msg.block.Height(),
					peer:        msg.peer,
					reply:       msg.reply,
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
		done <- struct{}{}
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
		done <- struct{}{}
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
			sm.msgChan <- &netsyncInvMsg
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
			sm.legacyKafkaInvCh <- &kafka.Message{
				Value: value,
			}
		}
	} else {
		netsyncInvMsg := invMsg{inv: inv, peer: peer}
		sm.msgChan <- &netsyncInvMsg
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

// DonePeer informs the blockmanager that a peer has disconnected.
func (sm *SyncManager) DonePeer(peer *peerpkg.Peer, done chan struct{}) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		done <- struct{}{}
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

	return nil
}

// SyncPeerID returns the ID of the current sync peer, or 0 if there is none.
func (sm *SyncManager) SyncPeerID() int32 {
	reply := make(chan int32)
	sm.msgChan <- getSyncPeerMsg{reply: reply}

	return <-reply
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
	validationClient validator.Interface, utxoStore utxostore.Store, subtreeStore blob.Store,
	subtreeValidation subtreevalidation.Interface, blockValidation blockvalidation.Interface,
	blockAssembly blockassembly.ClientI, config *Config) (*SyncManager, error) {
	initPrometheusMetrics()

	sm := SyncManager{
		ctx:          ctx,
		settings:     tSettings,
		peerNotifier: config.PeerNotifier,
		// txMemPool:     config.TxMemPool,
		orphanTxs:       expiringmap.New[chainhash.Hash, *orphanTxAndParents](tSettings.Legacy.OrphanEvictionDuration),
		chainParams:     config.ChainParams,
		rejectedTxns:    txmap.NewSyncedMap[chainhash.Hash, struct{}](maxRejectedTxns), // limit map size to maxRejectedTxns
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),   // give peers 10 seconds to respond
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](60 * time.Second),   // give peers 60 seconds to respond
		peerStates:      txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		// progressLogger:  newBlockProgressLogger("Processed", log),
		msgChan:          make(chan interface{}, maxMsgQueueSize),
		headerList:       list.New(),
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

	// create the transaction announcement batcher
	sm.txAnnounceBatcher = batcher.NewWithDeduplication[TxHashAndFee](maxRequestedTxns, 1*time.Second, func(batch []*TxHashAndFee) {
		sm.logger.Debugf("announcing %d transactions to peers", len(batch))

		// process the batch
		sm.peerNotifier.AnnounceNewTransactions(batch)
	}, true)

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

	if !config.DisableCheckpoints {
		bestBlockHeightInt32, err := safeconversion.Uint32ToInt32(bestBlockHeaderMeta.Height)
		if err != nil {
			sm.logger.Errorf("failed to convert block height to int32: %v", err)
		}

		// Initialize the next checkpoint based on the current height.
		sm.nextCheckpoint = sm.findNextHeaderCheckpoint(bestBlockHeightInt32)
		if sm.nextCheckpoint != nil {
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
	// Block-related listeners (INV, blocks final): enabled when NOT in LEGACYSYNCING state
	// Transaction-related listeners (txmeta): enabled only when in RUNNING state
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				// Block-related listeners: enable when NOT in LEGACYSYNCING
				isLegacySyncing, _ := sm.blockchainClient.IsFSMCurrentState(sm.ctx, teranodeblockchain.FSMStateLEGACYSYNCING)
				blockEnabled := !isLegacySyncing

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

		// start a go routine to start the kafka producer
		go func() {
			producer.Start(sm.ctx, sm.legacyKafkaInvCh)
		}()

		// INV listener receives inventory messages from other nodes
		// Disabled during LEGACYSYNCING to reduce processing load during catch-up
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

	// Listen to blockchain notifications for subtree announcements
	go func() {
		// will never return an error
		blockchainSubscription, _ := sm.blockchainClient.Subscribe(ctx, "legacy/manager")

		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-blockchainSubscription:
				if notification == nil {
					continue
				}

				// check if the notification is a new subtree
				if notification.Type == model.NotificationType_Subtree {
					// we just got notified of a new subtree internally, announce all the transactions to our peers
					sm.logger.Debugf("[Legacy Manager] received new subtree notification: %v", notification)

					subtreeReader, err := sm.subtreeStore.GetIoReader(ctx, notification.Hash, fileformat.FileTypeSubtree)
					if err != nil {
						sm.logger.Errorf("[Legacy Manager] failed to get subtree from store: %v", err)
						continue
					}

					subtree, err := subtreepkg.NewSubtreeFromReader(subtreeReader)
					_ = subtreeReader.Close()
					if err != nil {
						sm.logger.Errorf("[Legacy Manager] failed to create subtree from bytes: %v", err)
						continue
					}

					// announce all the transactions in the subtree
					// the batcher should de-duplicate the transactions that have already been sent in the last minute
					for _, subtreeNode := range subtree.Nodes {
						if subtreeNode.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
							continue
						}

						sm.txAnnounceBatcher.Put(&TxHashAndFee{
							TxHash: subtreeNode.Hash,
							Fee:    subtreeNode.Fee,
							Size:   subtreeNode.SizeInBytes,
						})
					}
				}
			}
		}
	}()

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
	const (
		txmetaActionADD    = byte(0)
		txmetaActionDELETE = byte(1)
	)

	kafka.StartKafkaListener(ctx, sm.logger, kafkaURL, groupID, true, func(msg *kafka.KafkaMessage) error {
		data := msg.Value
		if len(data) < 4 {
			return nil
		}

		offset := 0

		// Read entry count
		entryCount := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		// Process each entry
		for i := uint32(0); i < entryCount; i++ {
			// Check minimum bytes for hash + action + length
			if offset+32+1+4 > len(data) {
				sm.logger.Errorf("[kafkaTXmetaListener] truncated message at entry %d", i)
				return nil
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

			if action == txmetaActionADD {
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

				sm.txAnnounceBatcher.Put(&TxHashAndFee{
					TxHash: hash,
					Fee:    txMeta.Fee,
					Size:   txMeta.SizeInBytes,
				})
			} else {
				// Skip DELETE entries (no action needed for peer announcements)
				continue
			}
		}

		return nil
	}, &sm.settings.Kafka)
}
