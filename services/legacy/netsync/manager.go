// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"bytes"
	"container/list"
	"context"
	"math/rand/v2"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	ubsvblockchain "github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/legacy/blockchain"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	peerpkg "github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
)

const (
	// minInFlightBlocks is the minimum number of blocks that should be
	// in the request queue for headers-first mode before requesting
	// more.
	minInFlightBlocks = 10

	// maxNetworkViolations is the max number of network violations a
	// sync peer can have before a new sync peer is found.
	maxNetworkViolations = 3

	// maxRejectedTxns is the maximum number of rejected transactions
	// hashes to store in memory.
	maxRejectedTxns = 1000

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

// processBlockResponse is a response sent to the reply channel of a
// processBlockMsg.
type processBlockResponse struct {
	isOrphan bool
	err      error
}

// processBlockMsg is a message type to be sent across the message channel
// for requested a block is processed.  Note this call differs from blockMsg
// above in that blockMsg is intended for blocks that came from peers and have
// extra handling, whereas this message essentially is just a concurrent safe
// way to call ProcessBlock on the internal blockchain instance.
type processBlockMsg struct {
	block *bsvutil.Block
	flags blockchain.BehaviorFlags
	reply chan processBlockResponse
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
	requestQueue    []*wire.InvVect
	requestedTxns   map[chainhash.Hash]struct{}
	requestedBlocks map[chainhash.Hash]struct{}
}

// syncPeerState stores additional info about the sync peer.
type syncPeerState struct {
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
	parents map[chainhash.Hash]struct{}
	addedAt time.Time
}

// updateNetwork updates the received bytes. Just tracks 2 ticks
// worth of network bandwidth.
func (sps *syncPeerState) updateNetwork(syncPeer *peerpkg.Peer) {
	sps.ticks++
	sps.recvBytesLastTick = sps.recvBytes
	sps.recvBytes = syncPeer.BytesReceived()
}

// SyncManager is used to communicate block related messages with peers. The
// SyncManager is started as by executing Start() in a goroutine. Once started,
// it selects peers to sync from and starts the initial block download. Once the
// chain is in sync, the SyncManager handles incoming block and header
// notifications and relays announcements of new blocks to peers.
type SyncManager struct {
	ctx          context.Context
	logger       ulogger.Logger
	peerNotifier PeerNotifier
	started      int32
	shutdown     int32
	chain        *blockchain.BlockChain
	orphanTxs    *expiringmap.ExpiringMap[chainhash.Hash, *orphanTxAndParents]
	chainParams  *chaincfg.Params
	msgChan      chan interface{}
	wg           sync.WaitGroup
	quit         chan struct{}

	// UBSV services
	blockchainClient  ubsvblockchain.ClientI
	validationClient  validator.Interface
	utxoStore         utxostore.Store
	subtreeStore      blob.Store
	subtreeValidation subtreevalidation.Interface
	blockValidation   blockvalidation.Interface
	blockAssembly     *blockassembly.Client
	legacyKafkaInvCh  chan *kafka.Message

	// These fields should only be accessed from the blockHandler thread.
	rejectedTxns    map[chainhash.Hash]struct{}
	requestedTxns   map[chainhash.Hash]struct{}
	requestedBlocks map[chainhash.Hash]struct{}
	syncPeer        *peerpkg.Peer
	syncPeerState   *syncPeerState
	peerStates      map[*peerpkg.Peer]*peerSyncState

	// The following fields are used for headers-first mode.
	headersFirstMode bool
	headerList       *list.List
	startHeader      *list.Element
	nextCheckpoint   *chaincfg.Checkpoint

	// An optional fee estimator.
	// feeEstimator *mempool.FeeEstimator

	// minSyncPeerNetworkSpeed is the minimum speed allowed for
	// a sync peer.
	minSyncPeerNetworkSpeed uint64
}

// resetHeaderState sets the headers-first mode state to values appropriate for
// syncing from a new peer.
func (sm *SyncManager) resetHeaderState(newestHash *chainhash.Hash, newestHeight int32) {
	sm.headersFirstMode = false
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
	if sm.syncPeer != nil {
		return
	}

	sm.logger.Debugf("startSync - Syncing from %v", sm.syncPeer)

	bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		sm.logger.Errorf("Failed to get best block header: %v", err)
		return
	}

	bestPeers := make([]*peerpkg.Peer, 0)

	okPeers := make([]*peerpkg.Peer, 0)

	for peer, state := range sm.peerStates {
		if !state.syncCandidate {
			continue
		}

		// Add any peers on the same block to okPeers. These should
		// only be used as a last resort.
		if peer.LastBlock() == int32(bestBlockHeaderMeta.Height) {
			okPeers = append(okPeers, peer)
			continue
		}

		// Remove sync candidate peers that are no longer candidates due
		// to passing their latest known block.
		if peer.LastBlock() < int32(bestBlockHeaderMeta.Height) {
			state.syncCandidate = false
			continue
		}

		// Append each good peer to bestPeers for selection later.
		bestPeers = append(bestPeers, peer)
	}

	var bestPeer *peerpkg.Peer

	// Try to select a random peer that is at a higher block height,
	// if that is not available, then use a random peer at the same
	// height and hope they find blocks.
	if len(bestPeers) > 0 {
		// #nosec G404
		bestPeer = bestPeers[rand.IntN(len(bestPeers))]
	} else if len(okPeers) > 0 {
		// #nosec G404
		bestPeer = okPeers[rand.IntN(len(okPeers))]
	}

	// Start syncing from the best peer if one was selected.
	if bestPeer != nil {
		// check whether we are in sync with this peer and send RUNNING FSM state
		// nolint:gosec // the height will never exceed int32.Max
		if bestPeer.LastBlock() == int32(bestBlockHeaderMeta.Height) {
			sm.logger.Infof("peer %v is at the same height %d as us, sending RUNNING", bestPeer.Addr(), bestPeer.LastBlock())

			if err = sm.blockchainClient.Run(sm.ctx, "legacy/netsync/manager/startSync"); err != nil {
				sm.logger.Errorf("failed to set blockchain state to running: %v", err)
			}
		}

		// Clear the requestedBlocks if the sync peer changes, otherwise
		// we may ignore blocks we need that the last sync peer failed
		// to send.
		sm.requestedBlocks = make(map[chainhash.Hash]struct{})

		locator, err := sm.blockchainClient.GetBlockLocator(sm.ctx, bestBlockHeader.Hash(), bestBlockHeaderMeta.Height)
		if err != nil {
			sm.logger.Errorf("Failed to get block locator for the latest block: %v", err)
			return
		}

		sm.logger.Infof("Syncing to block height %d from peer %v", bestPeer.LastBlock(), bestPeer.Addr())

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
			int32(bestBlockHeaderMeta.Height) < sm.nextCheckpoint.Height && // nolint:gosec
			sm.chainParams != &chaincfg.RegressionNetParams {
			if err = bestPeer.PushGetHeadersMsg(locator, sm.nextCheckpoint.Hash); err != nil {
				sm.logger.Warnf("Failed to send getheaders message to peer %s: %v", bestPeer.Addr(), err)
				return
			}

			sm.headersFirstMode = true

			sm.logger.Infof("startSync - Downloading headers for blocks %d to %d from peer %s", bestBlockHeaderMeta.Height+1, sm.nextCheckpoint.Height, bestPeer.Addr())
		} else {
			if err = bestPeer.PushGetBlocksMsg(locator, &zeroHash); err != nil {
				sm.logger.Warnf("Failed to send getblocks message to peer %s: %v", bestPeer.Addr(), err)
				return
			}
		}

		bestPeer.SetSyncPeer(true)
		sm.syncPeer = bestPeer
		sm.syncPeerState = &syncPeerState{
			lastBlockTime:     time.Now(),
			recvBytes:         bestPeer.BytesReceived(),
			recvBytesLastTick: uint64(0),
		}
	} else {
		sm.logger.Warnf("No sync peer candidates available")
	}
}

// SyncHeight returns latest known block being synced to.
func (sm *SyncManager) SyncHeight() uint64 {
	if sm.syncPeer == nil {
		return 0
	}

	return uint64(sm.topBlock())
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
		host, _, err := net.SplitHostPort(peer.Addr())
		if err != nil {
			return false
		}

		if host != "127.0.0.1" && host != "localhost" {
			return false
		}
	} else {
		// The peer is not a candidate for sync if it's not a full
		// node.
		nodeServices := peer.Services()
		if nodeServices&wire.SFNodeNetwork != wire.SFNodeNetwork {
			return false
		}
	}

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

	sm.peerStates[peer] = &peerSyncState{
		syncCandidate:   isSyncCandidate,
		requestedTxns:   make(map[chainhash.Hash]struct{}),
		requestedBlocks: make(map[chainhash.Hash]struct{}),
	}

	// Start syncing by choosing the best candidate if needed.
	if isSyncCandidate && sm.syncPeer == nil {
		sm.startSync()
	}
}

// handleCheckSyncPeer selects a new sync peer.
func (sm *SyncManager) handleCheckSyncPeer() {
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	// If we don't have a sync peer, then there is nothing to do.
	if sm.syncPeer == nil {
		return
	}

	// Update network stats at the end of this tick.
	defer sm.syncPeerState.updateNetwork(sm.syncPeer)

	validNetworkSpeed := sm.syncPeerState.validNetworkSpeed(sm.minSyncPeerNetworkSpeed)
	lastBlockSince := time.Since(sm.syncPeerState.lastBlockTime)

	// Check network speed of the sync peer and its last block time. If we're currently
	// flushing the cache skip this round.
	if (validNetworkSpeed < maxNetworkViolations) && (lastBlockSince <= maxLastBlockTime) {
		return
	}

	sm.logger.Debugf("sync peer %s is slow, network speed: %v, last block time: %v", sm.syncPeer.Addr(), validNetworkSpeed, lastBlockSince)

	// Don't update sync peers if you have all the available blocks.
	_, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		sm.logger.Errorf("failed to get best block header: %v", err)
		return
	}

	if sm.topBlock() == int32(bestBlockHeaderMeta.Height) { // nolint:gosec
		// Update the time and violations to prevent disconnects.
		sm.syncPeerState.lastBlockTime = time.Now()
		sm.syncPeerState.violations = 0

		return
	}

	sm.logger.Debugf("sync peer %s is not at the same height (%d) as us (%d), updating sync peer", sm.syncPeer.Addr(), sm.topBlock(), bestBlockHeaderMeta.Height)

	state, exists := sm.peerStates[sm.syncPeer]
	if !exists {
		return
	}

	sm.logger.Debugf("removing sync peer %s", sm.syncPeer.Addr())

	sm.clearRequestedState(state)
	sm.updateSyncPeer(state)
}

// topBlock returns the best chains top block height
func (sm *SyncManager) topBlock() int32 {
	if sm.syncPeer.LastBlock() > sm.syncPeer.StartingHeight() {
		return sm.syncPeer.LastBlock()
	}

	return sm.syncPeer.StartingHeight()
}

// handleDonePeerMsg deals with peers that have signalled they are done.  It
// removes the peer as a candidate for syncing and in the case where it was
// the current sync peer, attempts to select a new best peer to sync from.  It
// is invoked from the syncHandler goroutine.
func (sm *SyncManager) handleDonePeerMsg(peer *peerpkg.Peer) {
	state, exists := sm.peerStates[peer]
	if !exists {
		sm.logger.Debugf("Received done peer message for unknown peer %s", peer)
		return
	}

	// Remove the peer from the list of candidate peers.
	delete(sm.peerStates, peer)

	sm.logger.Infof("Lost peer %s", peer)

	// Cleanup state of requested items.
	sm.clearRequestedState(state)

	// Fetch a new sync peer if this is the sync peer.
	if peer == sm.syncPeer {
		sm.updateSyncPeer(state)
	}
}

// clearRequestedState removes requested transactions
// and blocks from the global map.
func (sm *SyncManager) clearRequestedState(state *peerSyncState) {
	// Remove requested transactions from the global map so that they will
	// be fetched from elsewhere next time we get an inv.
	for txHash := range state.requestedTxns {
		delete(sm.requestedTxns, txHash)
	}

	// Remove requested blocks from the global map so that they will be
	// fetched from elsewhere next time we get an inv.
	for blockHash := range state.requestedBlocks {
		delete(sm.requestedBlocks, blockHash)
	}
}

// updateSyncPeer picks a new peer to sync from.
func (sm *SyncManager) updateSyncPeer(_ *peerSyncState) {
	sm.logger.Infof("Updating sync peer, last block: %v, violations: %v", sm.syncPeerState.lastBlockTime, sm.syncPeerState.violations)

	// Disconnect from the misbehaving peer.
	sm.syncPeer.Disconnect()

	// Attempt to find a new peer to sync from
	// Also, reset the headers-first state.
	sm.syncPeer.SetSyncPeer(false)
	sm.syncPeer = nil
	sm.syncPeerState = nil

	bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	if err != nil {
		// TODO we should return an error here to the caller
		sm.logger.Errorf("Failed to get best block header: %v", err)
		return
	}

	if sm.headersFirstMode {
		sm.resetHeaderState(bestBlockHeader.Hash(), int32(bestBlockHeaderMeta.Height)) // nolint:gosec
	}

	sm.startSync()
}

// handleTxMsg handles transaction messages from all peers.
func (sm *SyncManager) handleTxMsg(tmsg *txMsg) {
	ctx, _, _ := tracing.StartTracing(sm.ctx, "handleTxMsg",
		tracing.WithHistogram(prometheusLegacyNetsyncHandleTxMsg),
		tracing.WithDebugLogMessage(sm.logger, "handling transaction message for %s from %s", tmsg.tx.Hash(), tmsg.peer),
	)

	peer := tmsg.peer

	state, exists := sm.peerStates[peer]
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
	if _, exists = sm.rejectedTxns[*txHash]; exists {
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

	// TODO should we be sending these transactions to the propagation service (Kafka), instead of Validation?
	timeStart := time.Now()
	// nolint:gosec
	err = sm.validationClient.Validate(ctx, btTx, uint32(sm.topBlock()))

	prometheusLegacyNetsyncHandleTxMsgValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	// Remove transaction from request maps. Either the mempool/chain
	// already knows about it and as such we shouldn't have any more
	// instances of trying to fetch it, or we failed to insert and thus
	// we'll retry next time we get an inv.
	delete(state.requestedTxns, *txHash)
	delete(sm.requestedTxns, *txHash)

	if err != nil {
		if errors.Is(err, errors.ErrTxMissingParent) {
			// this is an orphan transaction, we will accept it when the parent comes in
			// first check if the transaction already exists in the orphan pool, otherwise add it
			if _, orphanTxExists := sm.orphanTxs.Get(*txHash); !orphanTxExists {
				sm.logger.Debugf("orphan transaction %v added from %s", txHash, peer)

				// create a map of the parents of the transaction for faster lookups
				txParents := make(map[chainhash.Hash]struct{})
				for _, input := range tmsg.tx.MsgTx().TxIn {
					txParents[input.PreviousOutPoint.Hash] = struct{}{}
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
			sm.rejectedTxns[*txHash] = struct{}{}
			sm.limitMap(sm.rejectedTxns, maxRejectedTxns)

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
	acceptedTxs := []*chainhash.Hash{btTx.TxIDChainHash()}

	// process any orphan transactions that were waiting for this transaction to be accepted
	// this is a recursive call, but the orphan pool should be limited in size
	sm.processOrphanTransactions(ctx, btTx.TxIDChainHash(), &acceptedTxs)

	if len(acceptedTxs) > 0 {
		sm.peerNotifier.AnnounceNewTransactions(acceptedTxs)
	}
}

// processOrphanTransactions recursively processes orphan transactions that were waiting for a transaction to be accepted
func (sm *SyncManager) processOrphanTransactions(ctx context.Context, txHash *chainhash.Hash, acceptedTxs *[]*chainhash.Hash) {
	// check whether any transaction in the orphan pool has this transaction as a parent
	ctx, _, deferFn := tracing.StartTracing(ctx, "processOrphanTransactions",
		tracing.WithHistogram(prometheusLegacyNetsyncProcessOrphanTransactions),
	)
	defer deferFn()

	// first we get all the orphan transactions, this will not block the orphan tx pool while processing
	orphanTxs := sm.orphanTxs.Items()

	for _, orphanTx := range orphanTxs {
		// check if the orphan transaction has this transaction as a parent
		if _, ok := orphanTx.parents[*txHash]; !ok {
			continue
		}

		// validate the orphan transaction
		// nolint:gosec
		err := sm.validationClient.Validate(ctx, orphanTx.tx, uint32(sm.topBlock()))
		if err != nil {
			if errors.Is(err, errors.ErrTxMissingParent) {
				// silently exit, we will accept this transaction when the parent comes in
				continue
			}

			// if the transaction was rejected, we will not process any of the orphan transactions that were waiting for it
			sm.logger.Errorf("Failed to process orphan transaction %v: %v", txHash, err)

			continue
		}

		// add the orphan transaction to the list of accepted transactions
		*acceptedTxs = append(*acceptedTxs, orphanTx.tx.TxIDChainHash())

		// remove the orphan transaction from the orphan pool
		sm.orphanTxs.Delete(*txHash)

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
	checkpoint := &sm.chainParams.Checkpoints[len(sm.chainParams.Checkpoints)-1]
	if checkpoint != nil && int32(bestBlockHeaderMeta.Height) < checkpoint.Height {
		sm.logger.Debugf("[isCurrent] chain is below the latest checkpoint: %v < %v", bestBlockHeaderMeta.Height, checkpoint.Height)
		return false
	}

	// Not current if the latest best block has a timestamp before 24 hours ago.
	//
	// The chain appears to be current if none of the checks reported otherwise.
	// minus24Hours := b.timeSource.AdjustedTime().Add(-24 * time.Hour).Unix()
	minus24Hours := time.Now().Add(-24 * time.Hour).Unix()

	current := int64(bestBlockHeaderMeta.BlockTime) >= minus24Hours
	sm.logger.Debugf("[isCurrent] chain is current based on time: %v (%d >= %d)", current, bestBlockHeaderMeta.BlockTime, minus24Hours)

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
		sm.logger.Debugf("[current] chain is not current: %v", bestBlockHeaderMeta.Height)
		return false
	}

	// if blockChain thinks we are current, and we have no syncPeer, it is probably right.
	if sm.syncPeer == nil {
		sm.logger.Debugf("[current] no sync peer, chain is current")
		return true
	}

	// No matter what the chain thinks, if we are below the block we are syncing to we are not current.
	if int32(bestBlockHeaderMeta.Height) < sm.syncPeer.LastBlock() {
		sm.logger.Debugf("[current] chain is not current, lower than sync peer (%s) block height: %v < %v", bestBlockHeaderMeta.Height, sm.syncPeer, sm.syncPeer.LastBlock())
		return false
	}

	sm.logger.Debugf("[current] chain is current at %v, sync peer: %s (last block %d)", bestBlockHeaderMeta.Height, sm.syncPeer, sm.syncPeer.LastBlock())
	return true
}

// handleBlockMsg handles block messages from all peers.
func (sm *SyncManager) handleBlockMsg(bmsg *blockMsg) error {
	sm.logger.Debugf("[handleBlockMsg][%s] received block from %s", bmsg.block.Hash(), bmsg.peer)
	peer := bmsg.peer

	state, exists := sm.peerStates[peer]
	if !exists {
		return errors.NewServiceError("Received block message from unknown peer %s", peer)
	}

	legacySyncMode := false

	fsmState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
	if err != nil {
		return errors.NewProcessingError("failed to get current FSM state: %v", err)
	} else if fsmState != nil && *fsmState == ubsvblockchain.FSMStateLEGACYSYNCING {
		legacySyncMode = true
	}

	// If we didn't ask for this block then the peer is misbehaving.
	blockHash := bmsg.block.Hash()
	if _, exists = state.requestedBlocks[*blockHash]; !exists {
		// The regression test intentionally sends some blocks twice
		// to test duplicate block insertion fails.  Don't disconnect
		// the peer or ignore the block when we're in regression test
		// mode, in this case, so the chain code is actually fed the
		// duplicate blocks.
		if sm.chainParams != &chaincfg.RegressionNetParams {
			peer.Disconnect()
			return errors.NewServiceError("Got unrequested block %v from %s -- disconnected", blockHash, peer)
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
	behaviorFlags := blockchain.BFNone

	if sm.headersFirstMode {
		firstNodeEl := sm.headerList.Front()
		if firstNodeEl != nil {
			firstNode := firstNodeEl.Value.(*headerNode)

			if blockHash.IsEqual(firstNode.hash) {
				behaviorFlags |= blockchain.BFFastAdd

				if firstNode.hash.IsEqual(sm.nextCheckpoint.Hash) {
					isCheckpointBlock = true
				} else {
					sm.headerList.Remove(firstNodeEl)
				}
			}
		}
	}

	// Remove block from request maps. Either chain will know about it and
	// so we shouldn't have any more instances of trying to fetch it, or we
	// will fail the insert, and thus we'll retry next time we get an inv.
	delete(state.requestedBlocks, *blockHash)
	delete(sm.requestedBlocks, *blockHash)

	// TODO: this should be only done when Legacy Sync mode is active
	// if not in Legacy Sync mode, we need to potentially download the block,
	// promote block to the block validation via kafka (p2p -> blockvalidation message),
	// without calling HandleBlockDirect. Such that it doesn't interfere with the operation of block validation.
	if err = sm.HandleBlockDirect(sm.ctx, bmsg.peer, bmsg.block); err != nil {
		if legacySyncMode && errors.Is(err, errors.ErrBlockNotFound) {
			// previous block not found?
			return err
		}

		serviceError := errors.Is(err, errors.ErrServiceError) || errors.Is(err, errors.ErrStorageError)
		if !legacySyncMode && !serviceError {
			peer.PushRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", blockHash, false)
		}

		panic(err)
		// return err
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

	if peer == sm.syncPeer {
		sm.syncPeerState.lastBlockTime = time.Now()
	}

	// When the block is not an orphan, log information about it and update the chain state.
	sm.logger.Infof("accepted block %v", blockHash)

	// Update this peer's latest block height, for future potential sync node candidacy.
	// bestBlockHeader, bestBlockHeaderMeta, err := sm.blockchainClient.GetBestBlockHeader(sm.ctx)
	// if err != nil {
	//	return errors.NewServiceError("failed to get best block header", err)
	// }

	heightUpdate = bmsg.block.Height()
	blkHashUpdate = bmsg.block.Hash()

	// Clear the rejected transactions.
	sm.rejectedTxns = make(map[chainhash.Hash]struct{})

	// Update the block height for this peer. But only send a message to
	// the server for updating peer heights if this is an orphan or our
	// chain is "current". This avoids sending a spammy amount of messages
	// if we're syncing the chain from scratch.
	if blkHashUpdate != nil && heightUpdate != 0 {
		// only update the height, if the peer is not the current syncPeer
		if peer != sm.syncPeer {
			peer.UpdateLastBlockHeight(heightUpdate)
			sm.logger.Infof("peer %s reports new height %d, current %v", peer.Addr(), heightUpdate, sm.current())
		}

		if sm.current() { // used to check for isOrphan || sm.current()
			go sm.peerNotifier.UpdatePeerHeights(blkHashUpdate, heightUpdate, peer)

			// Since we are current, we can tell FSM to transition to RUN
			// Blockchain client will check if miner is registered, if so it will send Mine event, and FSM will transition to Mine
			if err = sm.blockchainClient.Run(sm.ctx, "legacy/netsync/manager/handleBlockMsg"); err != nil {
				sm.logger.Errorf("[Sync Manager] failed to send FSM RUN event %v", err)
			}
		}
	}

	// This is headers-first mode, so if the block is not a checkpoint
	// request more blocks using the header list when the request queue is
	// getting short.
	if !isCheckpointBlock {
		if sm.startHeader != nil &&
			len(state.requestedBlocks) < minInFlightBlocks {
			sm.fetchHeaderBlocks()
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
			return errors.NewServiceError("failed to send getheaders message to peer %s", peer.Addr(), err)
		}

		if sm.syncPeer != nil {
			sm.logger.Infof(
				"handleBlockMsg - Downloading headers for blocks %d to %d from peer %s",
				prevHeight+1,
				sm.nextCheckpoint.Height,
				sm.syncPeer.Addr(),
			)
		}

		return nil
	}

	// This is headers-first mode, the block is a checkpoint, and there are
	// no more checkpoints, so switch to normal mode by requesting blocks
	// from the block after this one up to the end of the chain (zero hash).
	sm.headersFirstMode = false
	sm.headerList.Init()
	sm.logger.Infof("Reached the final checkpoint -- switching to normal mode")

	locator := blockchain.BlockLocator([]*chainhash.Hash{blockHash})
	err = peer.PushGetBlocksMsg(locator, &zeroHash)

	if err != nil {
		return errors.NewServiceError("Failed to send getblocks message to peer %s", peer.Addr(), err)
	}

	return nil
}

// fetchHeaderBlocks creates and sends a request to the syncPeer for the next
// list of blocks to be downloaded based on the current list of headers.
func (sm *SyncManager) fetchHeaderBlocks() {
	// Nothing to do if there is no sync peer.
	if sm.syncPeer == nil {
		sm.logger.Warnf("fetchHeaderBlocks called with no sync peer")
		return
	}

	// Nothing to do if there is no start header.
	if sm.startHeader == nil {
		sm.logger.Warnf("fetchHeaderBlocks called with no start header")
		return
	}

	// Build up a getdata request for the list of blocks the headers
	// describe.  The size hint will be limited to wire.MaxInvPerMsg by
	// the function, so no need to double check it here.
	getDataMessage := wire.NewMsgGetDataSizeHint(uint(sm.headerList.Len()))
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
			peerState := sm.peerStates[sm.syncPeer]

			sm.requestedBlocks[*node.hash] = struct{}{}
			peerState.requestedBlocks[*node.hash] = struct{}{}

			_ = getDataMessage.AddInvVect(iv)
			numRequested++
		}

		sm.startHeader = e.Next()

		if numRequested >= wire.MaxInvPerMsg {
			break
		}
	}

	if len(getDataMessage.InvList) > 0 {
		sm.syncPeer.QueueMessage(getDataMessage, nil)
	}
}

// handleHeadersMsg handles block header messages from all peers.  Headers are
// requested when performing a headers-first sync.
func (sm *SyncManager) handleHeadersMsg(hmsg *headersMsg) {
	sm.logger.Debugf("[handleHeadersMsg] received headers message with %d headers from %s", len(hmsg.headers.Headers), hmsg.peer)
	peer := hmsg.peer

	_, exists := sm.peerStates[peer]
	if !exists {
		sm.logger.Warnf("Received headers message from unknown peer %s", peer)
		return
	}

	// The remote peer is misbehaving if we didn't request headers.
	msg := hmsg.headers
	numHeaders := len(msg.Headers)

	if !sm.headersFirstMode {
		sm.logger.Warnf("Got %d unrequested headers from %s -- disconnecting", numHeaders, peer.Addr())
		peer.Disconnect()

		return
	}

	// Nothing to do for an empty headers message.
	if numHeaders == 0 {
		return
	}

	// Process all of the received headers ensuring each one connects to the
	// previous and that checkpoints match.
	receivedCheckpoint := false

	var finalHash *chainhash.Hash

	for _, blockHeader := range msg.Headers {
		blockHash := blockHeader.BlockHash()
		finalHash = &blockHash

		// Ensure there is a previous header to compare against.
		prevNodeEl := sm.headerList.Back()
		if prevNodeEl == nil {
			sm.logger.Warnf("Header list does not contain a previous element as expected -- disconnecting peer")
			peer.Disconnect()

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
			sm.logger.Warnf("Received block header that does not "+
				"properly connect to the chain from peer %s "+
				"-- disconnecting", peer.Addr())
			peer.Disconnect()

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
				sm.logger.Warnf("Block header at height %d/hash "+
					"%s from peer %s does NOT match "+
					"expected checkpoint hash of %s -- "+
					"disconnecting", node.height,
					node.hash, peer.Addr(),
					sm.nextCheckpoint.Hash)

				peer.Disconnect()

				return
			}

			break
		}
	}

	// When this header is a checkpoint, switch to fetching the blocks for
	// all of the headers since the last checkpoint.
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

	err := peer.PushGetHeadersMsg(locator, sm.nextCheckpoint.Hash)
	if err != nil {
		sm.logger.Warnf("Failed to send getheaders message to peer %s: %v", peer.Addr(), err)
		return
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
		utxo, err := sm.utxoStore.Get(sm.ctx, &invVect.Hash, []string{"fee"})
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

	state, exists := sm.peerStates[peer]
	if !exists {
		sm.logger.Warnf("Received inv message from unknown peer %s", peer)
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
	// our current sync peer or we're current, then update the last
	// announced block for this peer. We'll use this information later to
	// update the heights of peers based on blocks we've accepted that they
	// previously announced.
	if lastBlock != -1 && (peer != sm.syncPeer || sm.current()) {
		peer.UpdateLastAnnouncedBlock(&invVects[lastBlock].Hash)
	}

	// Ignore invs from peers that aren't the sync if we are not current.
	// Helps prevent fetching a mass of orphans.
	if peer != sm.syncPeer && !sm.current() {
		return
	}

	// If our chain is current and a peer announces a block we already
	// know of, then update their current block height.
	if lastBlock != -1 && sm.current() {
		_, blockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &invVects[lastBlock].Hash)
		if err == nil {
			peer.UpdateLastBlockHeight(int32(blockHeaderMeta.Height))
		}
	}

	// by default, we do not process transactions / blocks
	// only when we are in the running state we process transaction and new block messages
	processInvs := false

	fsmState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
	if err != nil {
		sm.logger.Errorf("Failed to get current FSM state: %v", err)
	} else if fsmState != nil && *fsmState == ubsvblockchain.FSMStateRUNNING {
		processInvs = true
	}

	// Request the advertised inventory if we don't already have it.  Also,
	// request parent blocks of orphans if we receive one we already have.
	// Finally, attempt to detect potential stalls due to long side chains
	// we already have and request more blocks to prevent them.
	for i, iv := range invVects {
		// Ignore unsupported inventory types.
		switch iv.Type {
		case wire.InvTypeBlock:
		case wire.InvTypeTx:
			if !processInvs {
				// If we are not in running state, we are not interested in new transaction or block messages
				sm.logger.Debugf("Ignoring inv message from %s, not in running state", peer)
				continue
			}
		default:
			continue
		}

		// Add the inventory to the cache of known inventory
		// for the peer.
		peer.AddKnownInventory(iv)

		// Ignore inventory when we're in headers-first mode.
		if sm.headersFirstMode {
			continue
		}

		// Request the inventory if we don't already have it.
		haveInv, err := sm.haveInventory(iv)
		if err != nil {
			sm.logger.Warnf("Unexpected failure when checking for "+
				"existing inventory during inv message "+
				"processing: %v", err)

			continue
		}

		if !haveInv {
			if iv.Type == wire.InvTypeTx {
				// Skip the transaction if it has already been rejected.
				if _, exists := sm.rejectedTxns[iv.Hash]; exists {
					continue
				}
			}

			// Add it to the request queue.
			state.requestQueue = append(state.requestQueue, iv)

			continue
		}

		if iv.Type == wire.InvTypeBlock {
			// We already have the final block advertised by this inventory message, so force a request for more.  This
			// should only happen if we're on a really long side chain.
			if i == lastBlock {
				// Request blocks after this one up to the final one the remote peer knows about (zero stop hash).
				locator, err := sm.blockchainClient.GetBlockLocator(sm.ctx, &iv.Hash, 0)
				if err != nil {
					sm.logger.Errorf("Failed to get block locator for the block hash %s, %v", iv.Hash.String(), err)
				} else {
					_ = peer.PushGetBlocksMsg(locator, &zeroHash)
				}
			}
		}
	}

	// Request as much as possible at once.  Anything that won't fit into
	// the request will be requested on the next inv message.
	numRequested := 0
	gdmsg := wire.NewMsgGetData()
	requestQueue := state.requestQueue

	for len(requestQueue) != 0 {
		iv := requestQueue[0]
		requestQueue[0] = nil
		requestQueue = requestQueue[1:]

		switch iv.Type {
		case wire.InvTypeBlock:
			// Request the block if there is not already a pending
			// request.
			if _, exists = sm.requestedBlocks[iv.Hash]; !exists {
				sm.requestedBlocks[iv.Hash] = struct{}{}
				sm.limitMap(sm.requestedBlocks, maxRequestedBlocks)

				state.requestedBlocks[iv.Hash] = struct{}{}

				_ = gdmsg.AddInvVect(iv)
				numRequested++
			}

		case wire.InvTypeTx:
			// Request the transaction if there is not already a
			// pending request.
			if _, exists = sm.requestedTxns[iv.Hash]; !exists {
				sm.requestedTxns[iv.Hash] = struct{}{}

				sm.limitMap(sm.requestedTxns, maxRequestedTxns)

				state.requestedTxns[iv.Hash] = struct{}{}

				_ = gdmsg.AddInvVect(iv)
				numRequested++
			}
		}

		if numRequested >= wire.MaxInvPerMsg {
			break
		}
	}

	state.requestQueue = requestQueue

	if len(gdmsg.InvList) > 0 {
		peer.QueueMessage(gdmsg, nil)
	}
}

// limitMap is a helper function for maps that require a maximum limit by
// evicting a random transaction if adding a new value would cause it to
// overflow the maximum allowed.
func (sm *SyncManager) limitMap(m map[chainhash.Hash]struct{}, limit int) {
	if len(m)+1 > limit {
		// Remove a random entry from the map.  For most compilers, Go's
		// range statement iterates starting at a random item although
		// that is not 100% guaranteed by the spec.  The iteration order
		// is not important here because an adversary would have to be
		// able to pull off preimage attacks on the hashing function in
		// order to target eviction of specific entries anyways.
		for txHash := range m {
			delete(m, txHash)
			return
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

out:
	for {
		select {
		case <-ticker.C:
			sm.handleCheckSyncPeer()
		case m := <-sm.msgChan:
			// whenever legacy receives a message, check if we are current
			if sm.current() {
				currentState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
				if err != nil {
					sm.logger.Errorf("[SyncManager] failed to get fsm current state")
				}

				// we reached current in legacy, and current FSM state is not Running, send RUN event
				if currentState != nil && *currentState != ubsvblockchain.FSMStateRUNNING {
					sm.logger.Infof("[SyncManager] Legacy reached current, sending RUN event to FSM")
					if err = sm.blockchainClient.Run(sm.ctx, "legacy/netsync/manager/blockHandler"); err != nil {
						sm.logger.Infof("[Sync Manager] failed to send FSM RUN event %v", err)
					}
				}
			}

			switch msg := m.(type) {
			case *newPeerMsg:
				sm.handleNewPeerMsg(msg.peer)
				if msg.reply != nil {
					msg.reply <- struct{}{}
				}

			case *txMsg:
				// TODO - if we are in Legacy Sync mode, should we ignore this message?
				sm.handleTxMsg(msg)
				if msg.reply != nil {
					msg.reply <- struct{}{}
				}

			case *blockMsg:
				err := sm.handleBlockMsg(msg)
				if msg.reply != nil {
					msg.reply <- err
				}

			case *invMsg:
				sm.handleInvMsg(msg)

			case *headersMsg:
				sm.handleHeadersMsg(msg)

			case *donePeerMsg:
				sm.handleDonePeerMsg(msg.peer)
				if msg.reply != nil {
					msg.reply <- struct{}{}
				}

			case getSyncPeerMsg:
				var peerID int32

				if sm.syncPeer != nil {
					peerID = sm.syncPeer.ID()
				}
				msg.reply <- peerID

			case isCurrentMsg:
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

	sm.wg.Done()
	sm.logger.Infof("Block handler done")
}

// handleBlockchainNotification handles notifications from blockchain.  It does
// things such as request orphan block parents and relay accepted blocks to
// connected peers.
func (sm *SyncManager) handleBlockchainNotification(notification *ubsvblockchain.Notification) {
	if notification.Type == model.NotificationType_Block {
		// A block has been accepted into the blockchain.  Relay it to other peers.
		// Don't relay if we are not current. Other peers that are
		// current should already know about it.
		if !sm.current() {
			return
		}

		hash, err := chainhash.NewHash(notification.Hash)
		if err != nil {
			sm.logger.Errorf("Failed to create hash from block: %v", err)
			return
		}

		blockHeader, _, err := sm.blockchainClient.GetBlockHeader(sm.ctx, hash)
		if err != nil {
			sm.logger.Errorf("Failed to get block %v: %v", notification.Hash, err)
			return
		}

		// Generate the inventory vector and relay it.
		iv := wire.NewInvVect(wire.InvTypeBlock, hash)
		sm.peerNotifier.RelayInventory(iv, blockHeader.ToWireBlockHeader())
	}
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
			sm.msgChan <- invBlockMsg
		}

		if len(invTxMsg.InvList) > 0 {
			wireInvMsg := invMsg{inv: invTxMsg, peer: peer}

			// write to Kafka
			sm.logger.Debugf("writing INV message to Kafka from peer %s, length: %d", peer.Addr(), len(wireInvMsg.inv.InvList))
			sm.legacyKafkaInvCh <- &kafka.Message{
				Value: wireInvMsg.Bytes(),
			}
		}
	} else {
		wireInvMsg := invMsg{inv: inv, peer: peer}
		sm.msgChan <- &wireInvMsg
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

	sm.msgChan <- &donePeerMsg{peer: peer, reply: done}
}

// Start begins the core block handler which processes block and inv messages.
func (sm *SyncManager) Start() {
	// Already started?
	if atomic.AddInt32(&sm.started, 1) != 1 {
		return
	}

	sm.logger.Infof("Starting sync manager")
	sm.wg.Add(1)

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
	sm.wg.Wait()

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
	reply := make(chan bool)
	sm.msgChan <- isCurrentMsg{reply: reply}

	return <-reply
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
func New(ctx context.Context, logger ulogger.Logger, blockchainClient ubsvblockchain.ClientI,
	validationClient validator.Interface, utxoStore utxostore.Store, subtreeStore blob.Store,
	subtreeValidation subtreevalidation.Interface, blockValidation blockvalidation.Interface,
	blockAssembly *blockassembly.Client, config *Config) (*SyncManager, error) {
	initPrometheusMetrics()

	orphanEvictionDuration, err, _ := gocore.Config().GetDuration("legacy_orphanEvictionDuration", 10*time.Minute)
	if err != nil {
		return nil, err
	}

	sm := SyncManager{
		ctx:          ctx,
		peerNotifier: config.PeerNotifier,
		chain:        config.Chain,
		//txMemPool:     config.TxMemPool,
		orphanTxs:       expiringmap.New[chainhash.Hash, *orphanTxAndParents](orphanEvictionDuration),
		chainParams:     config.ChainParams,
		rejectedTxns:    make(map[chainhash.Hash]struct{}),
		requestedTxns:   make(map[chainhash.Hash]struct{}),
		requestedBlocks: make(map[chainhash.Hash]struct{}),
		peerStates:      make(map[*peerpkg.Peer]*peerSyncState),
		//progressLogger:  newBlockProgressLogger("Processed", log),
		msgChan:    make(chan interface{}, config.MaxPeers*3),
		headerList: list.New(),
		quit:       make(chan struct{}),
		//feeEstimator:            config.FeeEstimator,
		minSyncPeerNetworkSpeed: config.MinSyncPeerNetworkSpeed,

		// ubsv stores etc.
		logger:            logger,
		blockchainClient:  blockchainClient,
		validationClient:  validationClient,
		utxoStore:         utxoStore,
		subtreeStore:      subtreeStore,
		subtreeValidation: subtreeValidation,
		blockValidation:   blockValidation,
		blockAssembly:     blockAssembly,
	}

	// set an eviction function for orphan transactions
	// this will be called when an orphan transaction is evicted from the map
	sm.orphanTxs.WithEvictionFunction(func(txHash chainhash.Hash, orphanTx *orphanTxAndParents) bool {
		// try to process one last time
		// nolint:gosec
		if err = sm.validationClient.Validate(sm.ctx, orphanTx.tx, uint32(sm.topBlock())); err != nil {
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
		// Initialize the next checkpoint based on the current height.
		sm.nextCheckpoint = sm.findNextHeaderCheckpoint(int32(bestBlockHeaderMeta.Height))
		if sm.nextCheckpoint != nil {
			sm.resetHeaderState(bestBlockHeader.Hash(), int32(bestBlockHeaderMeta.Height))
		}
	} else {
		sm.logger.Infof("Checkpoints are disabled")
	}

	// sm.chain.Subscribe(sm.handleBlockchainNotification)
	// subscribe to blockchain notifications
	blockchainSubscriptionCh, err := sm.blockchainClient.Subscribe(ctx, "legacy-sync-manager")
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case notification := <-blockchainSubscriptionCh:
				sm.handleBlockchainNotification(notification)
			case <-sm.quit:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// Kafka for INV messages
	legacyInvConfigURL, err, ok := gocore.Config().GetURL("kafka_legacyInvConfig")
	if err == nil && ok {
		sm.legacyKafkaInvCh = make(chan *kafka.Message, 10_000)

		sm.logger.Infof("[Legacy Manager] starting kafka producer for INV messages at %s", legacyInvConfigURL)

		// start a go routine to start the kafka producer
		go func() {
			if _, err = kafka.NewKafkaAsyncProducerFromURL(sm.logger, legacyInvConfigURL, sm.legacyKafkaInvCh); err != nil {
				sm.logger.Errorf("[Legacy Manager] error starting kafka producer: %v", err)
				return
			}
		}()

		kafkaControlChan := make(chan bool) // true = start, false = stop

		// start a go routine to control the kafka listener, using the FSM state of the node
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// get the FSM state, only turn on the listener if we are in RUN mode
					fsmState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
					if err != nil {
						sm.logger.Errorf("[Legacy Manager] failed to get current FSM state: %v", err)
					}

					if fsmState != nil && *fsmState == ubsvblockchain.FSMStateRUNNING {
						kafkaControlChan <- true // start or continue the listener
					} else {
						kafkaControlChan <- false // stop the listener
					}

					// wait 2 seconds before checking again
					// TODO it would be better to be able to listen somehow to state changes in the FSM
					time.Sleep(2 * time.Second)
				}
			}
		}()

		go func() {
			var (
				kafkaCtx    context.Context
				kafkaCancel context.CancelFunc
			)

			for control := range kafkaControlChan {
				if control { // Start signal
					if kafkaCancel != nil {
						// Listener is already running, no need to start
						continue
					}

					kafkaCtx, kafkaCancel = context.WithCancel(ctx)

					sm.logger.Infof("[Legacy Manager] starting Kafka listener for INV messages on %s", legacyInvConfigURL.String())
					go sm.startKafkaListener(kafkaCtx, legacyInvConfigURL, "legacyInv", 1)
				} else if kafkaCancel != nil {
					sm.logger.Infof("[Legacy Manager] stopping Kafka listener for INV messages on %s", legacyInvConfigURL.String())
					kafkaCancel() // Stop the listener
					kafkaCancel = nil
				}
			}

			kafkaCancel()
		}()
	}

	return &sm, nil
}

func (sm *SyncManager) startKafkaListener(ctx context.Context, kafkaURL *url.URL, groupID string, consumerCount int) {
	client, err := kafka.NewKafkaConsumeGroup(ctx, kafka.KafkaConsumerConfig{
		Logger:            sm.logger,
		URL:               kafkaURL,
		ConsumerGroupID:   groupID,
		ConsumerCount:     consumerCount,
		AutoCommitEnabled: true,
	})
	if err != nil {
		sm.logger.Errorf("failed to start Kafka listener for %s: %v", kafkaURL.String(), err)
	}

	client.Start(ctx, func(msg kafka.KafkaMessage) error {
		wireInvMsg, err := sm.newInvFromBytes(msg.Message.Value)
		if err != nil {
			sm.logger.Errorf("failed to create INV message from Kafka message: %v", err)
			return nil // ignore any errors, the message might be old and/or the peer is already disconnected
		}

		sm.logger.Debugf("Received INV message from Kafka: %v", wireInvMsg)

		// Queue the INV message on the internal message channel
		sm.msgChan <- wireInvMsg

		return nil
	})
}
