// Copyright (c) 2013-2017 The btcsuite developers
// Copyright (c) 2015-2018 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package legacy implements a Bitcoin SV legacy protocol server that handles peer-to-peer communication
// and blockchain synchronization using the traditional Bitcoin network protocol.
package legacy

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	blockchain2 "github.com/bsv-blockchain/teranode/services/legacy/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil/bloom"
	"github.com/bsv-blockchain/teranode/services/legacy/connmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/netsync"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/txscript"
	"github.com/bsv-blockchain/teranode/services/legacy/version"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_options "github.com/bsv-blockchain/teranode/stores/blob/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

const (
	// defaultServices describes the default services that are supported by
	// the server.
	defaultServices = wire.SFNodeNetwork | wire.SFNodeBloom |
		wire.SFNodeCF | wire.SFNodeBitcoinCash

	// defaultRequiredServices describes the default services that are
	// required to be supported by outbound peers.
	defaultRequiredServices = wire.SFNodeNetwork

	// connectionRetryInterval is the base amount of time to wait in between
	// retries when connecting to persistent peers.  It is adjusted by the
	// number of retries such that there is a retry backoff.
	connectionRetryInterval = time.Second * 5

	// maxKnownAddresses is the maximum number of known addresses to
	// store in the peer.
	maxKnownAddresses = 10000
)

var (
	cfg *config
)

var (
	// userAgentName is the user agent name and is used to help identify
	// ourselves to other bitcoin peers.
	userAgentName = "/teranode-legacy-p2p"

	// userAgentVersion is the user agent version and is used to help
	// identify ourselves to other bitcoin peers.
	userAgentVersion = fmt.Sprintf("%d.%d.%d", version.AppMajor, version.AppMinor, version.AppPatch)
)

// addrMe specifies the server address to send peers.
var addrMe *wire.NetAddress

// zeroHash is the zero value hash (all zeros).  It is defined as a convenience.
var zeroHash chainhash.Hash

// onionAddr implements the net.Addr interface and represents a tor address.
type onionAddr struct {
	addr string
}

// String returns the onion address.
//
// This is part of the net.Addr interface.
func (oa *onionAddr) String() string {
	return oa.addr
}

// Network returns "onion".
//
// This is part of the net.Addr interface.
func (oa *onionAddr) Network() string {
	return "onion"
}

// Ensure onionAddr implements the net.Addr interface.
var _ net.Addr = (*onionAddr)(nil)

// simpleAddr implements the net.Addr interface with two struct fields
type simpleAddr struct {
	net, addr string
}

// String returns the address.
//
// This is part of the net.Addr interface.
func (a simpleAddr) String() string {
	return a.addr
}

// Network returns the network.
//
// This is part of the net.Addr interface.
func (a simpleAddr) Network() string {
	return a.net
}

// Ensure simpleAddr implements the net.Addr interface.
var _ net.Addr = simpleAddr{}

// broadcastMsg provides the ability to house a bitcoin message to be broadcast
// to all connected peers except specified excluded peers.
//
// This structure is used by the server's message broadcasting system to efficiently
// distribute protocol messages (blocks, transactions, inventory announcements) to
// multiple peers while allowing selective exclusion of specific peers.
//
// Fields:
//   - message: The wire protocol message to broadcast (MsgBlock, MsgTx, MsgInv, etc.)
//   - excludePeers: List of peers that should not receive this message, typically
//     used to avoid sending a message back to the peer that originally sent it
type broadcastMsg struct {
	message      wire.Message
	excludePeers []*serverPeer
}

// broadcastInventoryAdd is a type used to declare that the InvVect it contains
// needs to be added to the rebroadcast map
type broadcastInventoryAdd relayMsg

// broadcastInventoryDel is a type used to declare that the InvVect it contains
// needs to be removed from the rebroadcast map
type broadcastInventoryDel *wire.InvVect

// relayMsg packages an inventory vector along with the newly discovered
// inventory so the relay has access to that information.
type relayMsg struct {
	invVect *wire.InvVect
	data    interface{}
}

// updatePeerHeightsMsg is a message sent from the blockmanager to the server
// after a new block has been accepted. The purpose of the message is to update
// the heights of peers that were known to announce the block before we
// connected it to the main chain or recognized it as an orphan. With these
// updates, peer heights will be kept up to date, allowing for fresh data when
// selecting sync peer candidacy.
type updatePeerHeightsMsg struct {
	newHash    *chainhash.Hash
	newHeight  int32
	originPeer *peer.Peer
}

type banPeerForDurationMsg struct {
	peer  *serverPeer
	until int64
}

// a custom type for banned peer addresses
type bannedPeerAddr string

// a struct to represent an unban request
type unbanPeerReq struct {
	addr bannedPeerAddr
}

// a struct to represent a query to check if a peer is banned
type isPeerBannedMsg struct {
	addr  string
	reply chan bool
}

// peerState maintains state of inbound, persistent, outbound peers as well
// as banned peers and outbound groups.
type peerState struct {
	inboundPeers    *txmap.SyncedMap[int32, *serverPeer]
	outboundPeers   *txmap.SyncedMap[int32, *serverPeer]
	persistentPeers *txmap.SyncedMap[int32, *serverPeer]
	banned          *txmap.SyncedMap[string, time.Time]
	outboundGroups  *txmap.SyncedMap[string, int]
	connectionCount *txmap.SyncedMap[string, int]
}

// Count returns the count of all known peers.
func (ps *peerState) Count() int {
	return ps.inboundPeers.Length() + ps.outboundPeers.Length() + ps.persistentPeers.Length()
}

// CountIP returns the count of all peers matching the IP.
func (ps *peerState) CountIP(host string) int {
	count, found := ps.connectionCount.Get(host)
	if !found {
		return 0
	}

	return count
}

// forAllOutboundPeers is a helper function that runs closure on all outbound
// peers known to peerState.
func (ps *peerState) forAllOutboundPeers(closure func(sp *serverPeer)) {
	ps.outboundPeers.Iterate(func(i int32, peer *serverPeer) bool {
		closure(peer)
		peer.server.logger.Debugf("outboundPeers: %+v", peer)

		return true // continue always
	})

	ps.persistentPeers.Iterate(func(i int32, peer *serverPeer) bool {
		closure(peer)
		peer.server.logger.Debugf("persistentPeers: %+v", peer)

		return true // continue always
	})
}

// forAllPeers is a helper function that runs closure on all peers known to
// peerState.
func (ps *peerState) forAllPeers(closure func(sp *serverPeer)) {
	ps.inboundPeers.Iterate(func(i int32, peer *serverPeer) bool {
		closure(peer)
		peer.server.logger.Debugf("inboundPeers: %+v", peer)

		return true
	})

	ps.forAllOutboundPeers(closure)
}

// cfHeaderKV is a tuple of a filter header and its associated block hash. The
// struct is used to cache cfcheckpt responses.
type cfHeaderKV struct {
	blockHash    chainhash.Hash
	filterHeader chainhash.Hash
}

// server provides a bitcoin server for handling communications to and from
// bitcoin peers.
type server struct {
	ctx      context.Context
	settings *settings.Settings
	// The following variables must only be used atomically.
	// Putting the uint64s first makes them 64-bit aligned for 32-bit systems.
	bytesReceived uint64 // Total bytes received from all peers since start.
	bytesSent     uint64 // Total bytes sent by all peers since start.
	started       int32
	shutdown      int32
	shutdownSched int32
	startupTime   int64

	addrManager          *addrmgr.AddrManager
	connManager          *connmgr.ConnManager
	sigCache             *txscript.SigCache
	hashCache            *txscript.HashCache
	syncManager          *netsync.SyncManager
	modifyRebroadcastInv chan interface{}
	newPeers             chan *serverPeer
	donePeers            chan *serverPeer
	banPeers             chan *serverPeer
	banPeerForDuration   chan *banPeerForDurationMsg
	unbanPeer            chan unbanPeerReq
	query                chan interface{}
	relayInv             chan relayMsg
	broadcast            chan broadcastMsg
	peerHeightsUpdate    chan updatePeerHeightsMsg
	wg                   sync.WaitGroup
	quit                 chan struct{}
	nat                  NAT
	timeSource           blockchain2.MedianTimeSource
	services             wire.ServiceFlag

	// cfCheckptCaches stores a cached slice of filter headers for cfcheckpt
	// messages for each filter type.
	cfCheckptCaches    map[wire.FilterType][]cfHeaderKV
	cfCheckptCachesMtx sync.RWMutex

	// teranode additions
	logger            ulogger.Logger
	blockchainClient  blockchain.ClientI
	utxoStore         utxostore.Store
	subtreeStore      blob.Store
	tempStore         blob.Store
	concurrentStore   *blob.ConcurrentBlob[chainhash.Hash]
	subtreeValidation subtreevalidation.Interface
	blockValidation   blockvalidation.Interface
	blockAssembly     *blockassembly.Client
	assetHTTPAddress  string
	banList           *p2p.BanList
	banChan           chan p2p.BanEvent
}

// serverPeer extends the peer to maintain state shared by the server and
// the blockmanager.
type serverPeer struct {
	ctx context.Context
	// The following variables must only be used atomically
	feeFilter int64

	// protoconf fields
	numberOfFields       atomic.Uint64
	maxRecvPayloadLength atomic.Uint32

	*peer.Peer

	connReq        *connmgr.ConnReq
	server         *server
	persistent     bool
	continueHash   *chainhash.Hash
	relayMtx       sync.Mutex
	disableRelayTx bool
	sentAddrs      bool
	isWhitelisted  bool
	filter         *bloom.Filter
	addrMtx        sync.RWMutex
	knownAddresses map[string]struct{}
	banScore       connmgr.DynamicBanScore
	quit           chan struct{}
	// The following chans are used to sync blockmanager and server.
	txProcessed    chan struct{}
	blockProcessed chan error
}

// newServerPeer returns a new serverPeer instance. The peer needs to be set by
// the caller.
func newServerPeer(s *server, isPersistent bool) *serverPeer {
	return &serverPeer{
		ctx:            s.ctx, // set the context to the server, if server dies, all peers die
		server:         s,
		persistent:     isPersistent,
		filter:         bloom.LoadFilter(nil),
		knownAddresses: make(map[string]struct{}),
		quit:           make(chan struct{}),
		txProcessed:    make(chan struct{}, 1),
		blockProcessed: make(chan error, 1),
	}
}

// newestBlock returns the current best block hash and height using the format
// required by the configuration for the peer package.
func (sp *serverPeer) newestBlock() (*chainhash.Hash, int32, error) {
	blockHeader, blockHeaderMeta, err := sp.server.blockchainClient.GetBestBlockHeader(sp.ctx)
	if err != nil {
		return nil, 0, err
	}

	return blockHeader.Hash(), int32(blockHeaderMeta.Height), nil
}

// addKnownAddresses adds the given addresses to the set of known addresses to
// the peer to prevent sending duplicate addresses.
func (sp *serverPeer) addKnownAddresses(addresses []*wire.NetAddress) {
	sp.addrMtx.Lock()
	defer sp.addrMtx.Unlock()

	for _, addr := range addresses {
		if len(sp.knownAddresses) >= maxKnownAddresses {
			sp.cleanupKnownAddresses() // Remove old entries when limit is reached
		}

		sp.knownAddresses[addrmgr.NetAddressKey(addr)] = struct{}{}
	}
}

func (sp *serverPeer) cleanupKnownAddresses() {
	for addr := range sp.knownAddresses {
		delete(sp.knownAddresses, addr)

		if len(sp.knownAddresses) <= maxKnownAddresses/2 {
			break // Stop after clearing half to prevent performance issues
		}
	}
}

// addressKnown true if the given address is already known to the peer.
func (sp *serverPeer) addressKnown(na *wire.NetAddress) bool {
	sp.addrMtx.RLock()
	defer sp.addrMtx.RUnlock()
	_, exists := sp.knownAddresses[addrmgr.NetAddressKey(na)]

	return exists
}

// setDisableRelayTx toggles relaying of transactions for the given peer.
// It is safe for concurrent access.
func (sp *serverPeer) setDisableRelayTx(disable bool) {
	sp.relayMtx.Lock()
	sp.disableRelayTx = disable
	sp.relayMtx.Unlock()
}

// relayTxDisabled returns whether relaying of transactions for the given
// peer is disabled.
// It is safe for concurrent access.
func (sp *serverPeer) relayTxDisabled() bool {
	sp.relayMtx.Lock()
	isDisabled := sp.disableRelayTx
	sp.relayMtx.Unlock()

	return isDisabled
}

// pushAddrMsg sends an addr message to the connected peer using the provided
// addresses.
func (sp *serverPeer) pushAddrMsg(addresses []*wire.NetAddress) {
	// Filter addresses already known to the peer.
	addrs := make([]*wire.NetAddress, 0, len(addresses))

	for _, addr := range addresses {
		if !sp.addressKnown(addr) {
			addrs = append(addrs, addr)
		}
	}

	known, err := sp.PushAddrMsg(addrs)
	if err != nil {
		reason := fmt.Sprintf("Can't push address message to peer: %v", err)
		sp.DisconnectWithWarning(reason)

		return
	}

	sp.addKnownAddresses(known)
}

// addBanScore increases the persistent and decaying ban score fields by the
// values passed as parameters. If the resulting score exceeds half of the ban
// threshold, a warning is logged including the reason provided. Further, if
// the score is above the ban threshold, the peer will be banned and
// disconnected.
func (sp *serverPeer) addBanScore(persistent, transient uint32, reason string) {
	// No warning is logged and no score is calculated if banning is disabled.
	// if cfg.DisableBanning {
	//	return
	// }
	if sp.isWhitelisted {
		sp.server.logger.Debugf("Misbehaving whitelisted peer %s: %s", sp, reason)
		return
	}

	warnThreshold := cfg.BanThreshold >> 1

	if transient == 0 && persistent == 0 {
		// The score is not being increased, but a warning message is still
		// logged if the score is above the warn threshold.
		score := sp.banScore.Int()
		if score > warnThreshold {
			sp.server.logger.Warnf("Misbehaving peer %s: %s -- ban score is %d, "+
				"it was not increased this time", sp, reason, score)
		}

		return
	}

	score := sp.banScore.Increase(persistent, transient)
	if score > warnThreshold {
		sp.server.logger.Warnf("Misbehaving peer %s: %s -- ban score increased to %d",
			sp, reason, score)

		if score > cfg.BanThreshold {
			sp.server.BanPeer(sp)
			sp.DisconnectWithWarning("Misbehaving peer -- banning and disconnecting")
		}
	}
}

// hasServices returns whether or not the provided advertised service flags have
// all of the provided desired service flags set.
func hasServices(advertised, desired wire.ServiceFlag) bool {
	return advertised&desired == desired
}

// OnVersion is invoked when a peer receives a version bitcoin message
// and is used to negotiate the protocol version details as well as kick start
// the communications.
func (sp *serverPeer) OnVersion(p *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnVersion",
		tracing.WithHistogram(peerServerMetrics["OnVersion"]),
		tracing.WithLogMessage(sp.server.logger, "OnVersion from %s", p),
	)

	// Update the address manager with the advertised services for outbound
	// connections in case they have changed.  This is not done for inbound
	// connections to help prevent malicious behavior and is skipped when
	// running on the simulation test network since it is only intended to
	// connect to specified peers and actively avoids advertising and
	// connecting to discovered peers.
	//
	// NOTE: This is done before rejecting peers that are too old to ensure
	// it is updated regardless in the case a new minimum protocol version is
	// enforced and the remote node has not upgraded yet.
	isInbound := sp.Inbound()
	remoteAddr := sp.NA()
	addrManager := sp.server.addrManager

	if !isInbound {
		addrManager.SetServices(remoteAddr, msg.Services)
	}

	// Ignore peers that have a protocol version that is too old.  The peer
	// negotiation logic will disconnect it after this callback returns.
	if msg.ProtocolVersion < int32(peer.MinAcceptableProtocolVersion) {
		return nil
	}

	// Only allow connections from peers running Bitcoin SV
	// This prevents connections from BCH/BTC/BTG and other incompatible forks
	userAgent := msg.UserAgent
	if !strings.Contains(userAgent, "Bitcoin SV") && !strings.Contains(userAgent, "BSV") {
		sp.server.logger.Warnf("Rejecting and banning peer %s with non-Bitcoin SV user agent: %s", sp.Peer, userAgent)

		// Ban the peer to prevent repeated connection attempts from incompatible clients
		sp.server.BanPeer(sp)

		reason := "Only Bitcoin SV clients are supported"

		return wire.NewMsgReject(msg.Command(), wire.RejectNonstandard, reason)
	}

	// Update the address manager and request known addresses from the
	// remote peer for outbound connections.  This is skipped when running
	// on the simulation test network since it is only intended to connect
	// to specified peers and actively avoids advertising and connecting to
	// discovered peers.
	if !isInbound {
		// Advertise the local address when the server accepts incoming
		// connections and it believes itself to be close to the best known tip.
		if !cfg.DisableListen && sp.server.syncManager.IsCurrent() {
			// Get address that best matches.
			lna := addrManager.GetBestLocalAddress(remoteAddr)
			if addrmgr.IsRoutable(lna) {
				// Filter addresses the peer already knows about.
				addresses := []*wire.NetAddress{lna}
				sp.pushAddrMsg(addresses)
			}
		}

		// Request known addresses if the server address manager needs
		// more and the peer has a protocol version new enough to
		// include a timestamp with addresses.
		hasTimestamp := sp.ProtocolVersion() >= wire.NetAddressTimeVersion
		if addrManager.NeedMoreAddresses() && hasTimestamp {
			sp.QueueMessage(wire.NewMsgGetAddr(), nil)
		}

		// Mark the address as a known good address.
		addrManager.Good(remoteAddr)
	}

	// Add the remote peer time as a sample for creating an offset against
	// the local clock to keep the network time in sync.
	sp.server.timeSource.AddTimeSample(sp.Addr(), msg.Timestamp)

	// Signal the sync manager this peer is a new sync candidate.
	sp.server.syncManager.NewPeer(sp.Peer, nil)

	// Choose whether to relay transactions before a filter command
	// is received.
	sp.setDisableRelayTx(msg.DisableRelayTx)

	// Add valid peer to the server.
	sp.server.AddPeer(sp)

	return nil
}

// OnProtoconf is invoked when a peer receives a protoconf bitcoin message.
func (sp *serverPeer) OnProtoconf(p *peer.Peer, msg *wire.MsgProtoconf) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnProtoconf",
		tracing.WithHistogram(peerServerMetrics["OnProtoconf"]),
	)

	sp.numberOfFields.Store(msg.NumberOfFields)

	if msg.NumberOfFields > 0 {
		sp.maxRecvPayloadLength.Store(msg.MaxRecvPayloadLength)

		sp.server.logger.Debugf("Peer %v sent a valid protoconf '%v'", sp.String(), msg)
	}
}

// OnMemPool is invoked when a peer receives a mempool bitcoin message.
// It creates and sends an inventory message with the contents of the memory
// pool up to the maximum inventory allowed per message.  When the peer has a
// bloom filter loaded, the contents are filtered accordingly.
func (sp *serverPeer) OnMemPool(_ *peer.Peer, _ *wire.MsgMemPool) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnMemPool",
		tracing.WithHistogram(peerServerMetrics["OnMemPool"]),
	)

	// we do not support onMempool requests
	// normally this would only be sent with bloom filtering on, which we do not support
	sp.DisconnectWithWarning("Ignoring mempool request from peer -- bloom filtering is not supported")
}

// OnTx is invoked when a peer receives a tx bitcoin message.  It blocks
// until the bitcoin transaction has been fully processed.  Unlock the block
// handler this does not serialize all transactions through a single thread
// transactions don't rely on the previous one in a linear fashion like blocks.
func (sp *serverPeer) OnTx(_ *peer.Peer, msg *wire.MsgTx) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnTx",
		tracing.WithHistogram(peerServerMetrics["OnTx"]),
		tracing.WithDebugLogMessage(sp.server.logger, "[serverPeer.OnTx][%s] OnTx from %s", msg.TxHash(), sp),
	)

	if cfg.BlocksOnly {
		sp.server.logger.Infof("Ignoring tx %v from %v - blocksonly enabled", msg.TxHash(), sp)
		return
	}

	// Add the transaction to the known inventory for the peer.
	// Convert the raw MsgTx to a bsvutil.Tx which provides some convenience
	// methods and things such as hash caching.
	tx := bsvutil.NewTx(msg)
	iv := wire.NewInvVect(wire.InvTypeTx, tx.Hash())
	sp.AddKnownInventory(iv)

	// Queue the transaction up to be handled by the sync manager and
	// intentionally block further receives until the transaction is fully
	// processed and known good or bad.  This helps prevent a malicious peer
	// from queuing up a bunch of bad transactions before disconnecting (or
	// being disconnected) and wasting memory.
	sp.server.syncManager.QueueTx(tx, sp.Peer, nil)
}

// OnBlock is invoked when a peer receives a block bitcoin message. It
// blocks until the bitcoin block has been fully processed.
func (sp *serverPeer) OnBlock(_ *peer.Peer, msg *wire.MsgBlock, buf []byte) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnBlock",
		tracing.WithHistogram(peerServerMetrics["OnBlock"]),
	)

	// Check if this peer is banned
	host, _, err := net.SplitHostPort(sp.Addr())
	if err == nil {
		// Use a channel to get the response from the query
		respChan := make(chan bool)

		// Create a proper query message to check if the peer is banned
		sp.server.query <- &isPeerBannedMsg{
			addr:  host,
			reply: respChan,
		}

		// Wait for the response
		isBanned := <-respChan
		if isBanned {
			sp.server.logger.Warnf("Ignoring block %s from banned legacy peer %s",
				msg.BlockHash().String(), sp.Addr())
			return
		}
	} else {
		sp.server.logger.Errorf("Error splitting host and port in OnBlock: %v", err)
	}

	// Convert the raw MsgBlock to a bsvutil.Block which provides some
	// convenience methods and things such as hash caching.
	block := bsvutil.NewBlockFromBlockAndBytes(msg, buf)

	// Add the block to the known inventory for the peer.
	iv := wire.NewInvVect(wire.InvTypeBlock, block.Hash())
	sp.AddKnownInventory(iv)

	exists, err := sp.server.blockchainClient.GetBlockExists(sp.ctx, block.Hash())
	if err != nil {
		sp.server.logger.Errorf("Block exists check error: %v", err)
		return
	}

	if !exists {
		// Queue the block up to be handled by the block
		// manager and intentionally block further receives
		// until the bitcoin block is fully processed and known
		// good or bad.  This helps prevent a malicious peer
		// from queuing up a bunch of bad blocks before
		// disconnecting (or being disconnected) and wasting
		// memory.  Additionally, this behavior is depended on
		// by at least the block acceptance test tool as the
		// reference implementation processes blocks in the same
		// thread and therefore blocks further messages until
		// the bitcoin block has been fully processed.
		sp.server.syncManager.QueueBlock(block, sp.Peer, sp.blockProcessed)

		err = <-sp.blockProcessed
		if err != nil {
			sp.server.logger.Errorf("block processing failed: %v", err)
		}
	}
}

// OnInv is invoked when a peer receives an inv bitcoin message and
// is used to examine the inventory being advertised by the remote peer and react
// accordingly.  We pass the message down to blockmanager which will call
// QueueMessage with any appropriate responses.
func (sp *serverPeer) OnInv(_ *peer.Peer, msg *wire.MsgInv) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnInv",
		tracing.WithHistogram(peerServerMetrics["OnInv"]),
	)

	if !cfg.BlocksOnly {
		if len(msg.InvList) > 0 {
			sp.server.syncManager.QueueInv(msg, sp.Peer)
		}

		return
	}

	newInv := wire.NewMsgInvSizeHint(uint(len(msg.InvList)))

	for _, invVect := range msg.InvList {
		if invVect.Type == wire.InvTypeTx {
			sp.server.logger.Infof("[OnInv] Ignoring tx %v in inv from %v -- blocksonly enabled", invVect.Hash, sp)

			if sp.ProtocolVersion() >= wire.BIP0037Version {
				sp.DisconnectWithWarning("Ignoring tx in inv from peer")

				return
			}

			continue
		}

		if invVect.Type == wire.InvTypeBlock {
			sp.server.logger.Infof("[OnInv] Got block inventory from %s: %s", sp, invVect.Hash)
		}

		err := newInv.AddInvVect(invVect)
		if err != nil {
			sp.server.logger.Errorf("[OnInv] Failed to add inventory vector: %v", err)
			break
		}
	}

	if len(newInv.InvList) > 0 {
		sp.server.syncManager.QueueInv(newInv, sp.Peer)
	}
}

// OnHeaders is invoked when a peer receives a headers bitcoin
// message.  The message is passed down to the sync manager.
func (sp *serverPeer) OnHeaders(_ *peer.Peer, msg *wire.MsgHeaders) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnHeaders",
		tracing.WithHistogram(peerServerMetrics["OnHeaders"]),
	)

	sp.server.syncManager.QueueHeaders(msg, sp.Peer)
}

// OnGetData is invoked when a peer receives a getdata bitcoin message and
// is used to deliver block and transaction information.
func (sp *serverPeer) OnGetData(_ *peer.Peer, msg *wire.MsgGetData) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnGetData",
		tracing.WithHistogram(peerServerMetrics["OnGetData"]),
	)

	numAdded := 0
	notFound := wire.NewMsgNotFound()

	length := len(msg.InvList)
	// A decaying ban score increase is applied to prevent exhausting resources
	// with unusually large inventory queries.
	// Requesting more than the maximum inventory vector length within a short
	// period of time yields a score above the default ban threshold. Sustained
	// bursts of small requests are not penalized as that would potentially ban
	// peers performing IBD.
	// This incremental score decays each minute to half of its value.
	sp.addBanScore(0, uint32(length)*99/wire.MaxInvPerMsg, "getdata")

	// We wait on this wait channel periodically to prevent queuing
	// far more data than we can send in a reasonable time, wasting memory.
	// The waiting occurs after the database fetch for the next one to
	// provide a little pipelining.
	var waitChan chan struct{}

	doneChan := make(chan struct{}, 1)

	for i, iv := range msg.InvList {
		var c chan struct{}
		// If this will be the last message we send.
		if i == length-1 && len(notFound.InvList) == 0 {
			c = doneChan
		} else if (i+1)%3 == 0 {
			// Buffered so as to not make the send goroutine block.
			c = make(chan struct{}, 1)
		}

		var err error

		switch iv.Type {
		case wire.InvTypeTx:
			err = sp.server.pushTxMsg(sp, &iv.Hash, c, waitChan, wire.BaseEncoding)
		case wire.InvTypeBlock:
			err = sp.server.pushBlockMsg(sp, &iv.Hash, c, waitChan, wire.BaseEncoding)
		case wire.InvTypeFilteredBlock:
			err = sp.server.pushMerkleBlockMsg(sp, &iv.Hash, c, waitChan, wire.BaseEncoding)
		default:
			sp.server.logger.Warnf("Unknown type in inventory request %d",
				iv.Type)
			continue
		}

		if err != nil {
			notFound.AddInvVect(iv)

			// When there is a failure fetching the final entry
			// and the done channel was sent in due to there
			// being no outstanding not found inventory, consume
			// it here because there is now not found inventory
			// that will use the channel momentarily.
			if i == len(msg.InvList)-1 && c != nil {
				<-c
			}
		}

		numAdded++
		waitChan = c
	}

	if len(notFound.InvList) != 0 {
		sp.QueueMessage(notFound, doneChan)
	}

	// Wait for messages to be sent. We can send quite a lot of data at this
	// point and this will keep the peer busy for a decent amount of time.
	// We don't process anything else by them in this time so that we
	// have an idea of when we should hear back from them - else the idle
	// timeout could fire when we were only half done sending the blocks.
	if numAdded > 0 {
		<-doneChan
	}
}

// OnGetBlocks is invoked when a peer receives a getblocks bitcoin
// message.
func (sp *serverPeer) OnGetBlocks(_ *peer.Peer, msg *wire.MsgGetBlocks) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnGetBlocks",
		tracing.WithHistogram(peerServerMetrics["OnGetBlocks"]),
	)

	// Find the most recent known block in the best chain based on the block
	// locator and fetch all of the block hashes after it until either
	// wire.MaxBlocksPerMsg have been fetched or the provided stop hash is
	// encountered.
	//
	// Use the block after the genesis block if no other blocks in the
	// provided locator are known.  This does mean the client will start
	// over with the genesis block if unknown block locators are provided.
	//
	// This mirrors the behavior in the reference implementation.
	blockHeaders, err := sp.server.blockchainClient.LocateBlockHeaders(sp.ctx, msg.BlockLocatorHashes, &msg.HashStop, wire.MaxBlocksPerMsg)
	if err != nil {
		sp.server.logger.Errorf("Failed to fetch locator block hashes: %v", err)
	}

	hashList := make([]*chainhash.Hash, 0, len(blockHeaders))
	for _, blockHeader := range blockHeaders {
		hashList = append(hashList, blockHeader.Hash())
	}

	// Generate inventory message.
	invMsg := wire.NewMsgInv()

	for i := range hashList {
		iv := wire.NewInvVect(wire.InvTypeBlock, hashList[i])
		_ = invMsg.AddInvVect(iv)
	}

	// Send the inventory message if there is anything to send.
	if len(invMsg.InvList) > 0 {
		invListLen := len(invMsg.InvList)
		if invListLen == wire.MaxBlocksPerMsg {
			// Intentionally use a copy of the final hash so there
			// is not a reference into the inventory slice which
			// would prevent the entire slice from being eligible
			// for GC as soon as it's sent.
			continueHash := invMsg.InvList[invListLen-1].Hash
			sp.continueHash = &continueHash
		}

		sp.QueueMessage(invMsg, nil)
	} else { // if sp.server.chain.PruneMode() {
		// Typically nodes will just not respond to a GetBlocks message
		// if they don't have any blocks. However, since we are implementing
		// NodeNetworkLimited we need a better way to communicate to the remote
		// peer that we don't have that portion of the chain than just letting
		// the request timeout. So for this purpose we will respond with a
		// not found message.
		notFound := wire.NewMsgNotFound()
		for _, inv := range invMsg.InvList {
			_ = notFound.AddInvVect(inv)
		}

		sp.QueueMessage(notFound, nil)
	}
}

// OnGetHeaders is invoked when a peer receives a getheaders bitcoin
// message.
func (sp *serverPeer) OnGetHeaders(_ *peer.Peer, msg *wire.MsgGetHeaders) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnGetHeaders",
		tracing.WithHistogram(peerServerMetrics["OnGetHeaders"]),
	)

	// Don't serve headers to other peers while we're actively syncing in headers-first mode.
	// Serving headers during checkpoint sync causes significant delays (18s+ per batch) due to:
	// - Database query contention (querying 2000 headers per peer)
	// - Multiple peers requesting headers simultaneously
	// - Competition with our own header processing
	// This prevents the sync from timing out (3-minute lastBlockTime limit).
	if sp.server.syncManager.IsHeadersFirstMode() {
		sp.server.logger.Debugf("Ignoring getheaders request from %s: node is syncing in headers-first mode", sp)
		return
	}

	// Find the most recent known block in the best chain based on the block
	// locator and fetch all the headers after it until either
	// wire.MaxBlockHeadersPerMsg have been fetched or the provided stop
	// hash is encountered.
	//
	// Use the block after the genesis block if no other blocks in the
	// provided locator are known.  This does mean the client will start
	// over with the genesis block if unknown block locators are provided.
	//
	// This mirrors the behavior in the reference implementation.

	bestHeader, _, err := sp.server.blockchainClient.GetBestBlockHeader(sp.ctx)
	if err != nil {
		sp.server.logger.Errorf("Failed to fetch best block header: %v", err)
		return
	}

	blockHeaders, _, err := sp.server.blockchainClient.GetBlockHeadersFromCommonAncestor(sp.ctx, bestHeader.Hash(), util.HashPointersToValues(msg.BlockLocatorHashes), wire.MaxBlockHeadersPerMsg)
	if err != nil {
		sp.server.logger.Errorf("Failed to fetch block headers from common ancestor: %v", err)
	}

	// Send found headers to the requesting peer.
	wireBlockHeaders := make([]*wire.BlockHeader, 0, len(blockHeaders))

	for _, blockHeader := range blockHeaders {
		// Note: We now include genesis block for proper header chain continuity
		// SVNode needs genesis to validate the chain from block 0
		// if blockHeader.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
		// 	// skip genesis block
		// 	continue
		// }
		// continue for regtest only
		if sp.server.settings.ChainCfgParams.Net == chaincfg.RegressionNetParams.Net {
			if blockHeader.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
				// skip genesis block
				continue
			}
		}

		wireBlockHeaders = append(wireBlockHeaders, blockHeader.ToWireBlockHeader())

		if len(wireBlockHeaders) >= wire.MaxBlockHeadersPerMsg {
			break
		}
	}

	if len(wireBlockHeaders) > 0 {
		sp.QueueMessage(&wire.MsgHeaders{Headers: wireBlockHeaders}, nil)
	}
}

// OnGetCFilters is invoked when a peer receives a getcfilters bitcoin message.
func (sp *serverPeer) OnGetCFilters(_ *peer.Peer, msg *wire.MsgGetCFilters) {
	sp.DisconnectWithWarning("Ignoring getcfilters request from peer")
}

// OnGetCFHeaders is invoked when a peer receives a getcfheader bitcoin message.
func (sp *serverPeer) OnGetCFHeaders(_ *peer.Peer, msg *wire.MsgGetCFHeaders) {
	sp.DisconnectWithWarning("Ignoring getcfheaders request from peer")
}

// OnGetCFCheckpt is invoked when a peer receives a getcfcheckpt bitcoin message.
func (sp *serverPeer) OnGetCFCheckpt(_ *peer.Peer, msg *wire.MsgGetCFCheckpt) {
	sp.DisconnectWithWarning("Ignoring getcfcheckpt request from peer")
}

// enforceNodeBloomFlag disconnects the peer if the server is not configured to
// allow bloom filters.  Additionally, if the peer has negotiated to a protocol
// version  that is high enough to observe the bloom filter service support bit,
// it will be banned since it is intentionally violating the protocol.
func (sp *serverPeer) enforceNodeBloomFlag(cmd string) bool {
	if sp.server.services&wire.SFNodeBloom != wire.SFNodeBloom {
		// Ban the peer if the protocol version is high enough that the
		// peer is knowingly violating the protocol and banning is
		// enabled.  This is done to prevent fingerprinting attacks.
		//
		// NOTE: Even though the addBanScore function already examines
		// whether or not banning is enabled, it is checked here as well
		// to ensure the violation is logged and the peer is
		// disconnected regardless.
		if sp.ProtocolVersion() >= wire.BIP0111Version &&
			!cfg.DisableBanning {
			// Disconnect the peer regardless of whether it was
			// banned.
			sp.addBanScore(100, 0, cmd)
			sp.DisconnectWithWarning("Ignoring unsupported request protocol version")

			return false
		}

		// Disconnect the peer regardless of protocol version or banning
		// state.
		sp.DisconnectWithWarning("Ignoring unsupported " + cmd + " request from peer")

		return false
	}

	return true
}

// OnFeeFilter is invoked when a peer receives a feefilter bitcoin message and
// is used by remote peers to request that no transactions which have a fee rate
// lower than provided value are inventoried to them.  The peer will be
// disconnected if an invalid fee filter value is provided.
func (sp *serverPeer) OnFeeFilter(_ *peer.Peer, msg *wire.MsgFeeFilter) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnFeeFilter",
		tracing.WithHistogram(peerServerMetrics["OnFeeFilter"]),
	)

	// Check that the passed minimum fee is a valid amount.
	if msg.MinFee < 0 || msg.MinFee > bsvutil.MaxSatoshi {
		reason := fmt.Sprintf("Peer sent an invalid feefilter '%v'", bsvutil.Amount(msg.MinFee))
		sp.DisconnectWithWarning(reason)

		return
	}

	atomic.StoreInt64(&sp.feeFilter, msg.MinFee)
}

// OnFilterAdd is invoked when a peer receives a filteradd bitcoin
// message and is used by remote peers to add data to an already loaded bloom
// filter.  The peer will be disconnected if a filter is not loaded when this
// message is received or the server is not configured to allow bloom filters.
func (sp *serverPeer) OnFilterAdd(_ *peer.Peer, msg *wire.MsgFilterAdd) {
	sp.DisconnectWithWarning("Ignoring filteradd request from peer")
}

// OnFilterClear is invoked when a peer receives a filterclear bitcoin
// message and is used by remote peers to clear an already loaded bloom filter.
// The peer will be disconnected if a filter is not loaded when this message is
// received  or the server is not configured to allow bloom filters.
func (sp *serverPeer) OnFilterClear(_ *peer.Peer, msg *wire.MsgFilterClear) {
	sp.DisconnectWithWarning("Ignoring filterclear request from peer")
}

// OnFilterLoad is invoked when a peer receives a filterload bitcoin
// message and it used to load a bloom filter that should be used for
// delivering merkle blocks and associated transactions that match the filter.
// The peer will be disconnected if the server is not configured to allow bloom
// filters.
func (sp *serverPeer) OnFilterLoad(_ *peer.Peer, msg *wire.MsgFilterLoad) {
	sp.DisconnectWithWarning("Ignoring filterload request from peer")
}

// OnGetAddr is invoked when a peer receives a getaddr bitcoin message
// and is used to provide the peer with known addresses from the address
// manager.
func (sp *serverPeer) OnGetAddr(_ *peer.Peer, msg *wire.MsgGetAddr) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnGetAddr",
		tracing.WithHistogram(peerServerMetrics["OnGetAddr"]),
	)

	// Do not accept getaddr requests from outbound peers.  This reduces
	// fingerprinting attacks.
	if !sp.Inbound() {
		sp.server.logger.Debugf("Ignoring getaddr request from outbound peer %v", sp)
		return
	}

	// Only allow one getaddr request per connection to discourage
	// address stamping of inv announcements.
	if sp.sentAddrs {
		sp.server.logger.Debugf("Ignoring repeated getaddr request from peer %v", sp)
		return
	}

	sp.sentAddrs = true

	// Get the current known addresses from the address manager.
	addrCache := sp.server.addrManager.AddressCache()

	// Add our best net address for peers to discover us. If the port
	// is 0 that indicates no worthy address was found, therefore
	// we do not broadcast it. We also must trim the cache by one
	// entry if we insert a record to prevent sending past the max
	// send size.
	bestAddress := sp.server.addrManager.GetBestLocalAddress(sp.NA())
	if bestAddress.Port != 0 {
		if len(addrCache) > 0 {
			addrCache = addrCache[1:]
		}

		addrCache = append(addrCache, bestAddress)
	}

	// Push the addresses.
	sp.pushAddrMsg(addrCache)
}

// OnAddr is invoked when a peer receives an addr bitcoin message and is
// used to notify the server about advertised addresses.
func (sp *serverPeer) OnAddr(_ *peer.Peer, msg *wire.MsgAddr) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnAddr",
		tracing.WithHistogram(peerServerMetrics["OnAddr"]),
	)

	// Ignore old style addresses which don't include a timestamp.
	if sp.ProtocolVersion() < wire.NetAddressTimeVersion {
		return
	}

	// A message that has no addresses is invalid.
	if len(msg.AddrList) == 0 {
		reason := fmt.Sprintf("Command [%s] from peer does not contain any addresses", msg.Command())
		sp.DisconnectWithWarning(reason)

		return
	}

	for _, na := range msg.AddrList {
		// Don't add more address if we're disconnecting.
		if !sp.Connected() {
			return
		}

		// Set the timestamp to 5 days ago if it's more than 24 hours
		// in the future so this address is one of the first to be
		// removed when space is needed.
		now := time.Now()
		if na.Timestamp.After(now.Add(time.Minute * 10)) {
			na.Timestamp = now.Add(-1 * time.Hour * 24 * 5)
		}

		// Add address to known addresses for this peer.
		sp.addKnownAddresses([]*wire.NetAddress{na})
	}

	// Add addresses to server address manager.  The address manager handles
	// the details of things such as preventing duplicate addresses, max
	// addresses, and last seen updates.
	// XXX bitcoind gives a 2-hour time penalty here, do we want to do the
	// same?
	sp.server.addrManager.AddAddresses(msg.AddrList, sp.NA())
}

// OnReject logs all reject messages received from the remote peer.
func (sp *serverPeer) OnReject(p *peer.Peer, msg *wire.MsgReject) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnReject",
		tracing.WithHistogram(peerServerMetrics["OnReject"]),
	)

	sp.server.logger.Warnf("Received reject message from peer %s, cmd: %s, code: %s, reason: %s, hash: %s", p, msg.Cmd, msg.Code.String(), msg.Reason, msg.Hash.String())
}

// OnNotFound logs all not found messages received from the remote peer.
func (sp *serverPeer) OnNotFound(p *peer.Peer, msg *wire.MsgNotFound) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnNotFound",
		tracing.WithHistogram(peerServerMetrics["OnNotFound"]),
	)

	sp.server.logger.Warnf("Received not found message from peer %s, %d not found invs", p, len(msg.InvList))
}

// OnRead is invoked when a peer receives a message and it is used to update
// the bytes received by the server.
func (sp *serverPeer) OnRead(_ *peer.Peer, bytesRead int, msg wire.Message, err error) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnRead",
		tracing.WithHistogram(peerServerMetrics["OnRead"]),
	)

	sp.server.AddBytesReceived(uint64(bytesRead))
}

// OnWrite is invoked when a peer sends a message and it is used to update
// the bytes sent by the server.
func (sp *serverPeer) OnWrite(_ *peer.Peer, bytesWritten int, msg wire.Message, err error) {
	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.OnWrite",
		tracing.WithHistogram(peerServerMetrics["OnWrite"]),
	)

	sp.server.AddBytesSent(uint64(bytesWritten))
}

// randomUint16Number returns a random uint16 in a specified input range.  Note
// that the range is in zeroth ordering; if you pass it 1800, you will get
// values from 0 to 1800.
func randomUint16Number(max uint16) uint16 {
	// In order to avoid modulo bias and ensure every possible outcome in
	// [0, max) has equal probability, the random number must be sampled
	// from a random source that has a range limited to a multiple of the
	// modulus.
	var randomNumber uint16

	var limitRange = (math.MaxUint16 / max) * max

	for {
		binary.Read(rand.Reader, binary.LittleEndian, &randomNumber)

		if randomNumber < limitRange {
			return randomNumber % max
		}
	}
}

// AddRebroadcastInventory adds 'iv' to the list of inventories to be
// rebroadcasted at random intervals until they show up in a block.
func (s *server) AddRebroadcastInventory(iv *wire.InvVect, data interface{}) {
	// Ignore if shutting down.
	if atomic.LoadInt32(&s.shutdown) != 0 {
		return
	}

	s.modifyRebroadcastInv <- broadcastInventoryAdd{invVect: iv, data: data}
}

// RemoveRebroadcastInventory removes 'iv' from the list of items to be
// rebroadcasted if present.
func (s *server) RemoveRebroadcastInventory(iv *wire.InvVect) {
	// Ignore if shutting down.
	if atomic.LoadInt32(&s.shutdown) != 0 {
		return
	}

	s.modifyRebroadcastInv <- broadcastInventoryDel(iv)
}

// relayTransactions generates and relays inventory vectors for all of the
// passed transactions to all connected peers.
func (s *server) relayTransactions(txns []*netsync.TxHashAndFee) {
	for _, txHashAndFee := range txns {
		iv := wire.NewInvVect(wire.InvTypeTx, &txHashAndFee.TxHash)
		s.RelayInventory(iv, txHashAndFee)
	}
}

// AnnounceNewTransactions generates and relays inventory vectors and notifies
// both websocket and getblocktemplate long poll clients of the passed
// transactions.  This function should be called whenever new transactions
// are added to the mempool.
func (s *server) AnnounceNewTransactions(txns []*netsync.TxHashAndFee) {
	// check listen mode - if listen_only, don't announce new transactions
	if s.settings.P2P.ListenMode == settings.ListenModeListenOnly {
		return
	}

	// Generate and relay inventory vectors for all newly accepted
	// transactions.
	s.relayTransactions(txns)
}

// TransactionConfirmed has one confirmation on the main chain. Now we can mark it as no
// longer needing rebroadcasting.
func (s *server) TransactionConfirmed(tx *bsvutil.Tx) {
	// Rebroadcasting is only necessary when the RPC server is active.
}

// pushTxMsg sends a tx message for the provided transaction hash to the
// connected peer.  An error is returned if the transaction hash is not known.
func (s *server) pushTxMsg(sp *serverPeer, hash *chainhash.Hash, doneChan chan<- struct{},
	waitChan <-chan struct{}, encoding wire.MessageEncoding) error {
	defer func() {
		// Temporary code to hunt down why a transaction is being requested that we do not have in our store fully
		r := recover()
		if r != nil {
			sp.server.logger.Errorf("Recovered in pushTxMsg for %s: %v", hash.String(), r)
		}
	}()

	// Attempt to fetch the requested transaction from the pool.  A
	// call could be made to check for existence first, but simply trying
	// to fetch a missing transaction results in the same behavior.
	txMeta, err := s.utxoStore.Get(s.ctx, hash, fields.Tx)
	if err != nil {
		sp.server.logger.Warnf("[pushTxMsg] Unable to fetch tx %v from transaction pool: %v", hash, err)

		if doneChan != nil {
			doneChan <- struct{}{}
		}

		return err
	}

	if txMeta == nil || txMeta.Tx == nil {
		err = fmt.Errorf("[pushTxMsg] tx %v is nil from transaction pool", hash)

		sp.server.logger.Warnf(err.Error())

		if doneChan != nil {
			doneChan <- struct{}{}
		}

		return err
	}

	tx, err := bsvutil.NewTxFromBytes(txMeta.Tx.Bytes())
	if err != nil {
		sp.server.logger.Warnf("[pushTxMsg] Unable to deserialize tx %v: %v", hash, err)

		if doneChan != nil {
			doneChan <- struct{}{}
		}

		return err
	}

	// Once we have fetched data wait for any previous operation to finish.
	if waitChan != nil {
		<-waitChan
	}

	sp.QueueMessageWithEncoding(tx.MsgTx(), doneChan, encoding)

	return nil
}

func (s *server) getTxFromStore(hash *chainhash.Hash) (*bsvutil.Tx, int64, error) {
	txMeta, err := s.utxoStore.Get(s.ctx, hash, fields.Tx)
	if err != nil {
		return nil, 0, err
	}

	fee, err := util.GetFees(txMeta.Tx)
	if err != nil {
		return nil, 0, err
	}

	tx, err := bsvutil.NewTxFromBytes(txMeta.Tx.Bytes())
	if err != nil {
		return nil, 0, err
	}

	return tx, int64(fee), nil // nolint:gosec
}

// pushBlockMsg sends a block message for the provided block hash to the
// connected peer.  An error is returned if the block hash is not known.
func (s *server) pushBlockMsg(sp *serverPeer, hash *chainhash.Hash, doneChan chan<- struct{},
	waitChan <-chan struct{}, encoding wire.MessageEncoding) error {
	// Stream directly from Asset Server without disk caching.
	// For large blocks (4GB+), the disk I/O overhead of ConcurrentBlob causes timeouts.
	// The Asset Server handles caching internally, so we stream directly to avoid:
	// 1. Writing the entire block to disk (slow, causes context deadline exceeded)
	// 2. Reading it back from disk (additional I/O overhead)
	url := fmt.Sprintf("%s/block_legacy/%s?wire=1", s.assetHTTPAddress, hash.String())
	reader, err := util.DoHTTPRequestBodyReader(s.ctx, url)
	if err != nil {
		sp.server.logger.Errorf("Unable to fetch requested block %v: %v", hash, err)

		if doneChan != nil {
			doneChan <- struct{}{}
		}

		return err
	}

	defer func() {
		_ = reader.Close()
	}()

	// Use RawBlockMessage to avoid deserialize/serialize overhead for large blocks.
	// This reads raw bytes directly and writes them to the wire, bypassing the
	// expensive process of creating Go structs for millions of transactions.
	rawBlockMsg, err := NewRawBlockMessage(reader)
	if err != nil {
		sp.server.logger.Errorf("Unable to read requested block hash %v: %v", hash, err)

		if doneChan != nil {
			doneChan <- struct{}{}
		}

		return err
	}

	// Once we have fetched data wait for any previous operation to finish.
	if waitChan != nil {
		<-waitChan
	}

	// We only send the channel for this message if we aren't sending
	// an inv straight after.
	var dc chan<- struct{}

	continueHash := sp.continueHash

	sendInv := continueHash != nil && continueHash.IsEqual(hash)
	if !sendInv {
		dc = doneChan
	}

	sp.QueueMessageWithEncoding(rawBlockMsg, dc, encoding)

	// When the peer requests the final block that was advertised in
	// response to a getblocks message which requested more blocks than
	// would fit into a single message, send it a new inventory message
	// to trigger it to issue another getblocks message for the next
	// batch of inventory.
	if sendInv {
		bestBlockHeader, _, err := s.blockchainClient.GetBestBlockHeader(s.ctx)
		if err != nil {
			s.logger.Errorf("unable to fetch best block header: %v", err)
		} else {
			invMsg := wire.NewMsgInvSizeHint(1)
			iv := wire.NewInvVect(wire.InvTypeBlock, bestBlockHeader.Hash())
			_ = invMsg.AddInvVect(iv)
			sp.QueueMessage(invMsg, doneChan)
			sp.continueHash = nil
		}
	}

	return nil
}

// pushMerkleBlockMsg sends a merkleblock message for the provided block hash to
// the connected peer.  Since a merkle block requires the peer to have a filter
// loaded, this call will simply be ignored if there is no filter loaded.  An
// error is returned if the block hash is not known.
func (s *server) pushMerkleBlockMsg(sp *serverPeer, _ *chainhash.Hash,
	doneChan chan<- struct{}, waitChan <-chan struct{}, encoding wire.MessageEncoding) error {
	// Do not send a response if the peer doesn't have a filter loaded.
	if !sp.filter.IsLoaded() {
		if doneChan != nil {
			doneChan <- struct{}{}
		}

		return nil
	}

	// Fetch the raw block bytes from the database.
	// blk, err := sp.server.chain.BlockByHash(hash)
	// if err != nil {
	//	sp.server.logger.Infof("Unable to fetch requested block hash %v: %v",
	//		hash, err)
	//
	//	if doneChan != nil {
	//		doneChan <- struct{}{}
	//	}
	//	return err
	// }
	blk := &bsvutil.Block{}

	// Generate a merkle block by filtering the requested block according
	// to the filter for the peer.
	merkle, matchedTxIndices := bloom.NewMerkleBlock(blk, sp.filter)

	// Once we have fetched data wait for any previous operation to finish.
	if waitChan != nil {
		<-waitChan
	}

	// Send the merkleblock.  Only send the done channel with this message
	// if no transactions will be sent afterwards.
	var dc chan<- struct{}
	if len(matchedTxIndices) == 0 {
		dc = doneChan
	}

	sp.QueueMessage(merkle, dc)

	// Finally, send any matched transactions.
	blkTransactions := blk.MsgBlock().Transactions

	for i, txIndex := range matchedTxIndices {
		// Only send the done channel on the final transaction.
		var dc chan<- struct{}
		if i == len(matchedTxIndices)-1 {
			dc = doneChan
		}

		if txIndex < uint32(len(blkTransactions)) {
			sp.QueueMessageWithEncoding(blkTransactions[txIndex], dc,
				encoding)
		}
	}

	return nil
}

// handleUpdatePeerHeight updates the heights of all peers who were known to
// announce a block we recently accepted.
func (s *server) handleUpdatePeerHeights(state *peerState, umsg updatePeerHeightsMsg) {
	state.forAllPeers(func(sp *serverPeer) {
		// The origin peer should already have the updated height.
		if sp.Peer == umsg.originPeer {
			return
		}

		// This is a pointer to the underlying memory which doesn't
		// change.
		latestBlkHash := sp.LastAnnouncedBlock()

		// Skip this peer if it hasn't recently announced any new blocks.
		if latestBlkHash == nil {
			return
		}

		// If the peer has recently announced a block, and this block
		// matches our newly accepted block, then update their block
		// height.
		if *latestBlkHash == *umsg.newHash {
			sp.UpdateLastBlockHeight(umsg.newHeight)
			sp.UpdateLastAnnouncedBlock(nil)
		}
	})
}

// handleAddPeerMsg deals with adding new peers.  It is invoked from the
// peerHandler goroutine.
func (s *server) handleAddPeerMsg(state *peerState, sp *serverPeer) bool {
	if sp == nil {
		return false
	}

	_, _, _ = tracing.Tracer("legacy").Start(sp.ctx, "serverPeer.handleAddPeerMsg",
		tracing.WithHistogram(peerServerMetrics["handleAddPeerMsg"]),
		tracing.WithLogMessage(sp.server.logger, "handleAddPeerMsg from %s", sp),
	)

	// ignore new peers if in banlist
	if s.banList.IsBanned(sp.Addr()) {
		sp.DisconnectWithWarning("Ignoring banned peer")

		return false
	}

	// Ignore new peers if we're shutting down.
	if atomic.LoadInt32(&s.shutdown) != 0 {
		sp.DisconnectWithInfo("Ignoring new peer - server is shutting down")

		return false
	}

	// Disconnect banned peers.
	host, _, err := net.SplitHostPort(sp.Addr())
	if err != nil {
		reason := fmt.Sprintf("can't split host port %v", err)
		sp.DisconnectWithInfo(reason)

		return false
	}

	if banEnd, ok := state.banned.Get(host); ok {
		if time.Now().Before(banEnd) {
			reason := fmt.Sprintf("Peer %s is banned for another %v", host, time.Until(banEnd))
			sp.DisconnectWithWarning(reason)

			return false
		}

		s.logger.Infof("Peer %s is no longer banned", host)
		state.banned.Delete(host)
	}

	// check whether we are already connected to this peer
	for _, outboundPeer := range state.outboundPeers.Range() {
		if outboundPeer.Addr() == sp.Addr() {
			outboundPeer.DisconnectWithInfo("Already connected to outbound peer, disconnecting in favor of new connection")
			// remove from state.outboundPeers
			state.outboundPeers.Delete(outboundPeer.ID())
		}
	}

	for _, inboundPeer := range state.inboundPeers.Range() {
		if inboundPeer.Addr() == sp.Addr() {
			inboundPeer.DisconnectWithInfo("Already connected to inbound peer, disconnecting in favor of new connection")
			// remove from state.inboundPeers
			state.inboundPeers.Delete(inboundPeer.ID())
		}
	}

	// Limit max number of total peers per ip.
	if state.CountIP(host) >= cfg.MaxPeersPerIP {
		reason := fmt.Sprintf("Max peers per IP reached [%d] - disconnecting peer", cfg.MaxPeersPerIP)
		sp.DisconnectWithInfo(reason)

		return false
	}

	// Limit max number of total peers.
	if state.Count() >= cfg.MaxPeers {
		reason := fmt.Sprintf("Max peers reached [%d] - disconnecting peer", cfg.MaxPeers)
		sp.DisconnectWithInfo(reason)
		// TODO: how to handle permanent peers here?
		// they should be rescheduled.
		return false
	}

	// Add the new peer and start it.
	sp.server.logger.Debugf("New peer %s", sp)

	if sp.Inbound() {
		state.inboundPeers.Set(sp.ID(), sp)

		count, _ := state.connectionCount.Get(host)
		state.connectionCount.Set(host, count+1)
	} else {
		count, _ := state.outboundGroups.Get(addrmgr.GroupKey(sp.NA()))
		state.outboundGroups.Set(addrmgr.GroupKey(sp.NA()), count+1)

		if sp.persistent {
			state.persistentPeers.Set(sp.ID(), sp)
		} else {
			state.outboundPeers.Set(sp.ID(), sp)

			count, _ = state.connectionCount.Get(host)
			state.connectionCount.Set(host, count+1)
		}
	}

	// Signal the sync manager this peer is a new sync candidate.
	s.syncManager.NewPeer(sp.Peer, nil)

	return true
}

// handleDonePeerMsg deals with peers that have signalled they are done.  It is
// invoked from the peerHandler goroutine.
func (s *server) handleDonePeerMsg(state *peerState, sp *serverPeer) {
	var list *txmap.SyncedMap[int32, *serverPeer]

	switch {
	case sp.persistent:
		list = state.persistentPeers
	case sp.Inbound():
		list = state.inboundPeers
	default:
		list = state.outboundPeers
	}

	if _, ok := list.Get(sp.ID()); ok {
		if !sp.Inbound() && sp.VersionKnown() {
			count, _ := state.outboundGroups.Get(addrmgr.GroupKey(sp.NA()))
			state.outboundGroups.Set(addrmgr.GroupKey(sp.NA()), count-1)
		}

		if !sp.Inbound() && sp.connReq != nil {
			s.connManager.Disconnect(sp.connReq.ID())
		}

		list.Delete(sp.ID())

		host, _, err := net.SplitHostPort(sp.Addr())
		if err == nil && !sp.persistent {
			count, _ := state.connectionCount.Get(host)
			state.connectionCount.Set(host, count-1)
		}

		sp.server.logger.Debugf("Removed peer %s", sp)

		return
	}

	if sp.connReq != nil {
		s.connManager.Disconnect(sp.connReq.ID())
	}

	// Update the address' last seen time if the peer has acknowledged
	// our version and has sent us its version as well.
	if sp.VerAckReceived() && sp.VersionKnown() && sp.NA() != nil {
		s.addrManager.Connected(sp.NA())
	}
	// If we get here it means that either we didn't know about the peer
	// or we purposefully deleted it.
}

// handleBanPeerMsg deals with banning peers.  It is invoked from the
// peerHandler goroutine.
func (s *server) handleBanPeerMsg(state *peerState, sp *serverPeer) {
	host, _, err := net.SplitHostPort(sp.Addr())
	if err != nil {
		sp.server.logger.Debugf("can't split ban peer %s %v", sp.Addr(), err)
		return
	}

	direction := directionString(sp.Inbound())

	s.logger.Infof("Banned peer %s (%s) for %v", host, direction, cfg.BanDuration)

	state.banned.Set(host, time.Now().Add(cfg.BanDuration))
	s.banChan <- p2p.BanEvent{
		Action: "add",
		IP:     host,
	}

	err = s.banList.Add(sp.ctx, host, time.Now().Add(cfg.BanDuration))
	if err != nil {
		s.logger.Errorf("Failed to add ban for peer %s: %v", host, err)
	}
}

// handleBanPeerForDurationMsg deals with banning peers for a specific duration.  It is invoked from the
// peerHandler goroutine.
func (s *server) handleBanPeerForDurationMsg(state *peerState, sp *serverPeer, banUntil int64) {
	host, _, err := net.SplitHostPort(sp.Addr())
	if err != nil {
		sp.server.logger.Debugf("can't split ban peer %s %v", sp.Addr(), err)
		return
	}

	direction := directionString(sp.Inbound())

	s.logger.Infof("Banned peer %s (%s) for %v", host, direction, banUntil)

	// convert int64 to time.Time
	banUntilTime := time.Unix(banUntil, 0)

	state.banned.Set(host, banUntilTime)
	s.banChan <- p2p.BanEvent{
		Action: "add",
		IP:     host,
	}

	err = s.banList.Add(sp.ctx, host, banUntilTime)
	if err != nil {
		s.logger.Errorf("Failed to add ban for peer %s: %v", host, err)
	}
}

// handleUnbanPeerMsg deals with un-banning peers.  It is invoked from the
// peerHandler goroutine.
func (s *server) handleUnbanPeerMsg(state *peerState, spAddr bannedPeerAddr) {
	host, _, err := net.SplitHostPort(string(spAddr))
	if err != nil {
		s.logger.Errorf("can't split ban peer %s %v", spAddr, err)
		return
	}

	s.logger.Infof("Un-banned peer %s ", host)

	state.banned.Delete(host)

	err = s.banList.Remove(s.ctx, host)
	if err != nil {
		s.logger.Errorf("Failed to remove ban for peer %s: %v", host, err)
	}
}

// handleRelayInvMsg deals with relaying inventory to peers that are not already
// known to have it.  It is invoked from the peerHandler goroutine.
func (s *server) handleRelayInvMsg(state *peerState, msg relayMsg) {
	state.forAllPeers(func(sp *serverPeer) {
		if !sp.Connected() {
			return
		}

		// If the inventory is a block and the peer prefers headers,
		// generate and send a headers message instead of an inventory
		// message.
		if msg.invVect.Type == wire.InvTypeBlock && sp.WantsHeaders() {
			s.handleRelayBlockMsg(sp, msg)
			return
		}

		if msg.invVect.Type == wire.InvTypeTx {
			// Don't relay the transaction to the peer when it has
			// transaction relaying disabled.
			if sp.relayTxDisabled() {
				return
			}

			feeFilter := atomic.LoadInt64(&sp.feeFilter)

			// Don't relay the transaction if there is a bloom
			// filter loaded and the transaction doesn't match it.
			//
			// if sp.filter.IsLoaded() {
			// 	if !sp.filter.MatchTxAndUpdate(tx) {
			// 		return
			// 	}
			// }

			s.handleRelayTxMsg(sp, msg, feeFilter)
			return
		}
	})
}

type serverPeerQueueInventory interface {
	QueueInventory(*wire.InvVect)
}

func (s *server) handleRelayTxMsg(sp serverPeerQueueInventory, msg relayMsg, feeFilter int64) {
	// Don't relay the transaction if the transaction fee-per-kb
	// is less than the peer's feeFilter.
	if feeFilter > 0 {
		var (
			err      error
			fee      int64
			size     int64
			feePerKB = int64(math.MaxInt64)
		)

		txHashAndFee, ok := msg.data.(*netsync.TxHashAndFee)
		if ok {
			fee, err = safeconversion.Uint64ToInt64(txHashAndFee.Fee)
			if err != nil {
				s.logger.Errorf("Failed to convert tx fee %v to int64: %v", txHashAndFee.Fee, err)
			} else {
				size, err = safeconversion.Uint64ToInt64(txHashAndFee.Size)
				if err != nil {
					s.logger.Errorf("Failed to convert tx size %v to int64: %v", txHashAndFee.Size, err)
				} else if size > 0 {
					// Calculate the fee per 1000 bytes, rounding up
					feePerKB = fee * 1000 / size
				}
			}
		}

		if feePerKB < feeFilter {
			return
		}
	}

	// Queue the inventory to be relayed with the next batch.
	// It will be ignored if the peer is already known to
	// have the inventory.
	sp.QueueInventory(msg.invVect)
}

func (s *server) handleRelayBlockMsg(sp *serverPeer, msg relayMsg) {
	blockHeader, ok := msg.data.(*wire.BlockHeader)
	if !ok {
		sp.server.logger.Warnf("[handleRelayBlockMsg] Underlying data for headers is not a block header")
		return
	}

	msgHeaders := wire.NewMsgHeaders()
	if err := msgHeaders.AddBlockHeader(blockHeader); err != nil {
		sp.server.logger.Errorf("[handleRelayBlockMsg] Failed to add block header: %v", err)
		return
	}

	sp.QueueMessage(msgHeaders, nil)
}

// handleBroadcastMsg deals with broadcasting messages to peers.  It is invoked
// from the peerHandler goroutine.
func (s *server) handleBroadcastMsg(state *peerState, bmsg *broadcastMsg) {
	state.forAllPeers(func(sp *serverPeer) {
		if !sp.Connected() {
			return
		}

		for _, ep := range bmsg.excludePeers {
			if sp == ep {
				return
			}
		}

		sp.QueueMessage(bmsg.message, nil)
	})
}

type getConnCountMsg struct {
	reply chan int32
}

type getPeersMsg struct {
	reply chan []*serverPeer
}

type getOutboundGroup struct {
	key   string
	reply chan int
}

type getAddedNodesMsg struct {
	reply chan []*serverPeer
}

type disconnectNodeMsg struct {
	cmp   func(*serverPeer) bool
	reply chan error
}

type connectNodeMsg struct {
	addr      string
	permanent bool
	reply     chan error
}

type removeNodeMsg struct {
	cmp   func(*serverPeer) bool
	reply chan error
}

// handleQuery is the central handler for all queries and commands from other
// goroutines related to peer state.
func (s *server) handleQuery(state *peerState, querymsg interface{}) {
	switch msg := querymsg.(type) {
	case *isPeerBannedMsg:
		// Check if the peer is banned
		banEnd, ok := state.banned.Get(msg.addr)
		if ok && time.Now().Before(banEnd) {
			// Peer is banned
			msg.reply <- true
		} else {
			// Peer is not banned
			msg.reply <- false
		}
	case getConnCountMsg:
		nconnected := int32(0)

		state.forAllPeers(func(sp *serverPeer) {
			if sp.Connected() {
				nconnected++
			}
		})
		msg.reply <- nconnected

	case getPeersMsg:
		s.logger.Debugf("getPeers: Query message received successfully")

		peers := make([]*serverPeer, 0, state.Count())
		state.forAllPeers(func(sp *serverPeer) {
			if sp.Connected() {
				peers = append(peers, sp)
			}
		})

		s.logger.Debugf("getPeers: Sending reply with %d peers", len(peers))
		s.logger.Debugf("getPeers: %+v", peers)
		msg.reply <- peers
		// peers := make([]*serverPeer, 0, state.Count())
		// state.forAllPeers(func(sp *serverPeer) {
		// 	if sp.Connected() {
		// 		peers = append(peers, sp)
		// 	}
		// })
	case connectNodeMsg:
		// TODO: duplicate oneshots?
		// Limit max number of total peers.
		if state.Count() >= cfg.MaxPeers {
			msg.reply <- errors.New("max peers reached")
			return
		}

		for _, persistentPeer := range state.persistentPeers.Range() {
			if persistentPeer.Addr() == msg.addr {
				if msg.permanent {
					msg.reply <- errors.New("peer already connected")
				} else {
					msg.reply <- errors.New("peer exists as a permanent peer")
				}

				return
			}
		}

		netAddr, err := addrStringToNetAddr(msg.addr)
		if err != nil {
			msg.reply <- err
			return
		}

		connReq := &connmgr.ConnReq{
			Permanent: msg.permanent,
		}
		connReq.SetAddr(netAddr)

		// TODO: if too many, nuke a non-perm peer.
		go s.connManager.Connect(connReq)

		msg.reply <- nil
	case removeNodeMsg:
		found := disconnectPeer(state.persistentPeers, msg.cmp, func(sp *serverPeer) {
			// Keep group counts ok since we remove from
			// the list now.
			count, _ := state.outboundGroups.Get(addrmgr.GroupKey(sp.NA()))
			state.outboundGroups.Set(addrmgr.GroupKey(sp.NA()), count-1)
		})

		if found {
			msg.reply <- nil
		} else {
			msg.reply <- errors.New("peer not found")
		}
	case getOutboundGroup:
		count, ok := state.outboundGroups.Get(msg.key)
		if ok {
			msg.reply <- count
		} else {
			msg.reply <- 0
		}
	// Request a list of the persistent (added) peers.
	case getAddedNodesMsg:
		// Respond with a slice of the relevant peers.
		peers := make([]*serverPeer, 0, state.persistentPeers.Length())
		for _, sp := range state.persistentPeers.Range() {
			peers = append(peers, sp)
		}
		msg.reply <- peers
	case disconnectNodeMsg:
		// Check inbound peers. We pass a nil callback since we don't
		// require any additional actions on disconnect for inbound peers.
		found := disconnectPeer(state.inboundPeers, msg.cmp, nil)
		if found {
			msg.reply <- nil
			return
		}

		// Check outbound peers.
		found = disconnectPeer(state.outboundPeers, msg.cmp, func(sp *serverPeer) {
			// Keep group counts ok since we remove from
			// the list now.
			count, _ := state.outboundGroups.Get(addrmgr.GroupKey(sp.NA()))
			state.outboundGroups.Set(addrmgr.GroupKey(sp.NA()), count-1)
		})
		if found {
			// If there are multiple outbound connections to the same
			// ip:port, continue disconnecting them all until no such
			// peers are found.
			for found {
				found = disconnectPeer(state.outboundPeers, msg.cmp, func(sp *serverPeer) {
					count, _ := state.outboundGroups.Get(addrmgr.GroupKey(sp.NA()))
					state.outboundGroups.Set(addrmgr.GroupKey(sp.NA()), count-1)
				})
			}
			msg.reply <- nil

			return
		}

		msg.reply <- errors.New("peer not found")
	}
}

// disconnectPeer attempts to drop the connection of a targeted peer in the
// passed peer list. Targets are identified via usage of the passed
// `compareFunc`, which should return `true` if the passed peer is the target
// peer. This function returns true on success and false if the peer is unable
// to be located. If the peer is found, and the passed callback: `whenFound'
// isn't nil, we call it with the peer as the argument before it is removed
// from the peerList, and is disconnected from the server.
func disconnectPeer(peerList *txmap.SyncedMap[int32, *serverPeer], compareFunc func(*serverPeer) bool, whenFound func(*serverPeer)) bool {
	for addr, peer := range peerList.Range() {
		if compareFunc(peer) {
			if whenFound != nil {
				whenFound(peer)
			}

			// This is ok because we are not continuing
			// to iterate so won't corrupt the loop.
			peerList.Delete(addr)
			peer.DisconnectWithInfo("disconnectPeer")

			return true
		}
	}

	return false
}

// newPeerConfig returns the configuration for the given serverPeer.
func newPeerConfig(sp *serverPeer) *peer.Config {
	return &peer.Config{
		// This is a complete list including ignored messages.
		Listeners: peer.MessageListeners{
			OnVersion:      sp.OnVersion,
			OnProtoconf:    sp.OnProtoconf,
			OnMemPool:      sp.OnMemPool,
			OnTx:           sp.OnTx,
			OnBlock:        sp.OnBlock,
			OnInv:          sp.OnInv,
			OnHeaders:      sp.OnHeaders,
			OnGetData:      sp.OnGetData,
			OnGetBlocks:    sp.OnGetBlocks,
			OnGetHeaders:   sp.OnGetHeaders,
			OnGetCFilters:  sp.OnGetCFilters,  // not implemented, just logs a warning
			OnGetCFHeaders: sp.OnGetCFHeaders, // not implemented, just logs a warning
			OnGetCFCheckpt: sp.OnGetCFCheckpt, // not implemented, just logs a warning
			OnFeeFilter:    sp.OnFeeFilter,    // being set, but not being enforced, could cause peer to disconnect
			OnFilterAdd:    sp.OnFilterAdd,    // not implemented, just logs a warning
			OnFilterClear:  sp.OnFilterClear,  // not implemented, just logs a warning
			OnFilterLoad:   sp.OnFilterLoad,   // not implemented, just logs a warning
			OnGetAddr:      sp.OnGetAddr,
			OnAddr:         sp.OnAddr,
			OnRead:         sp.OnRead,
			OnWrite:        sp.OnWrite,
			OnReject:       sp.OnReject,
			OnNotFound:     sp.OnNotFound,
		},
		AddrMe:            addrMe,
		NewestBlock:       sp.newestBlock,
		HostToNetAddress:  sp.server.addrManager.HostToNetAddress,
		Proxy:             cfg.Proxy,
		UserAgentName:     userAgentName,
		UserAgentVersion:  userAgentVersion,
		UserAgentComments: cfg.UserAgentComments,
		ChainParams:       sp.server.settings.ChainCfgParams,
		Services:          sp.server.services,
		DisableRelayTx:    cfg.BlocksOnly,
		ProtocolVersion:   peer.MaxProtocolVersion,
		TrickleInterval:   cfg.TrickleInterval,
	}
}

// inboundPeerConnected is invoked by the connection manager when a new inbound
// connection is established.  It initializes a new inbound server peer
// instance, associates it with the connection, and starts a goroutine to wait
// for disconnection.
func (s *server) inboundPeerConnected(conn net.Conn) {
	var err error

	sp := newServerPeer(s, false)

	addr := conn.RemoteAddr().String()
	if s.banList.IsBanned(addr) {
		s.logger.Infof("Rejecting banned inbound peer %s", addr)
		conn.Close()

		return
	}

	sp.isWhitelisted, err = isWhitelisted(conn.RemoteAddr())
	if err != nil {
		s.logger.Warnf("Cannot whitelist peer %v: %v", conn.RemoteAddr(), err)
	}

	sp.Peer = peer.NewInboundPeer(s.logger, s.settings, newPeerConfig(sp))
	sp.AssociateConnection(conn)

	go s.peerDoneHandler(sp)
}

// outboundPeerConnected is invoked by the connection manager when a new
// outbound connection is established.  It initializes a new outbound server
// peer instance, associates it with the relevant state such as the connection
// request instance and the connection itself, and finally notifies the address
// manager of the attempt.
func (s *server) outboundPeerConnected(c *connmgr.ConnReq, conn net.Conn) {
	sp := newServerPeer(s, c.Permanent)

	addr := conn.RemoteAddr().String()
	if s.banList.IsBanned(addr) {
		s.logger.Infof("Rejecting banned outbound peer %s", addr)
		conn.Close()

		return
	}

	p, err := peer.NewOutboundPeer(s.logger, s.settings, newPeerConfig(sp), c.GetAddr().String())
	if err != nil {
		sp.server.logger.Debugf("Cannot create outbound peer %s: %v", c.GetAddr(), err)
		s.connManager.Disconnect(c.ID())
	}

	sp.Peer = p
	sp.connReq = c

	sp.isWhitelisted, err = isWhitelisted(conn.RemoteAddr())
	if err != nil {
		s.logger.Warnf("Cannot whitelist peer %v: %v", conn.RemoteAddr(), err)
	}

	sp.AssociateConnection(conn)

	go s.peerDoneHandler(sp)
	s.addrManager.Attempt(sp.NA())
}

// peerDoneHandler handles peer disconnects by notifiying the server that it's
// done along with other performing other desirable cleanup.
func (s *server) peerDoneHandler(sp *serverPeer) {
	sp.WaitForDisconnect()
	s.donePeers <- sp

	// Only tell sync manager we are gone if we ever told it we existed.
	if sp.VersionKnown() {
		s.syncManager.DonePeer(sp.Peer, nil)
		// Evict any remaining orphans that were sent by the peer.
		// numEvicted := s.txMemPool.RemoveOrphansByTag(mempool.Tag(sp.ID()))
		//
		//	if numEvicted > 0 {
		//		s.logger.Debugf("Evicted %d %s from peer %v (id %d)",
		//			numEvicted, pickNoun(numEvicted, "orphan",
		//				"orphans"), sp, sp.ID())
		//	}
	}

	close(sp.quit)
}

// peerHandler is used to handle peer operations such as adding and removing
// peers to and from the server, banning peers, and broadcasting messages to
// peers.  It must be run in a goroutine.
func (s *server) peerHandler() {
	// Start the address manager and sync manager, both of which are needed
	// by peers.  This is done here since their lifecycle is closely tied
	// to this handler and rather than adding more channels to sychronize
	// things, it's easier and slightly faster to simply start and stop them
	// in this handler.
	s.addrManager.Start()
	s.syncManager.Start()

	s.logger.Infof("[Peer Handler] Starting...")
	defer s.wg.Done()

	state := &peerState{
		inboundPeers:    txmap.NewSyncedMap[int32, *serverPeer](),
		outboundPeers:   txmap.NewSyncedMap[int32, *serverPeer](),
		persistentPeers: txmap.NewSyncedMap[int32, *serverPeer](),
		banned:          txmap.NewSyncedMap[string, time.Time](),
		outboundGroups:  txmap.NewSyncedMap[string, int](),
		connectionCount: txmap.NewSyncedMap[string, int](),
	}

	if !cfg.DisableDNSSeed {
		// Add peers discovered through DNS to the address manager.
		connmgr.SeedFromDNS(activeNetParams.Params, defaultRequiredServices,
			bsvdLookup, func(addrs []*wire.NetAddress) {
				// Bitcoind uses a lookup of the dns seeder here. This
				// is rather strange since the values looked up by the
				// DNS seed lookups will vary quite a lot.
				// to replicate this behaviour we put all addresses as
				// having come from the first one.
				s.addrManager.AddAddresses(addrs, addrs[0])
			})
	}

	go s.connManager.Start()

out:
	for {
		select {
		// New peers connected to the server.
		case p := <-s.newPeers:
			s.handleAddPeerMsg(state, p)

		// Disconnected peers.
		case p := <-s.donePeers:
			s.handleDonePeerMsg(state, p)

		// Block accepted in mainchain or orphan, update peer height.
		case umsg := <-s.peerHeightsUpdate:
			s.handleUpdatePeerHeights(state, umsg)

		// Peer to ban.
		case p := <-s.banPeers:
			s.handleBanPeerMsg(state, p)
		// Peer to ban for duration.
		case p := <-s.banPeerForDuration:
			s.handleBanPeerForDurationMsg(state, p.peer, p.until)

		// Peer to unban.
		case p := <-s.unbanPeer:
			s.handleUnbanPeerMsg(state, p.addr)

		// New inventory to potentially be relayed to other peers.
		case invMsg := <-s.relayInv:
			s.handleRelayInvMsg(state, invMsg)

		// Message to broadcast to all connected peers except those
		// which are excluded by the message.
		case bmsg := <-s.broadcast:
			s.handleBroadcastMsg(state, &bmsg)

		case qmsg := <-s.query:
			s.handleQuery(state, qmsg)

		case <-s.quit:
			// Disconnect all peers on server shutdown.
			state.forAllPeers(func(sp *serverPeer) {
				sp.DisconnectWithInfo("Shutdown peer")
			})
			break out
		}
	}

	s.connManager.Stop()
	_ = s.syncManager.Stop()
	_ = s.addrManager.Stop()

	// Drain channels before exiting so nothing is left waiting around
	// to send.
cleanup:
	for {
		select {
		case <-s.newPeers:
		case <-s.donePeers:
		case <-s.peerHeightsUpdate:
		case <-s.relayInv:
		case <-s.broadcast:
		case <-s.query:
		default:
			break cleanup
		}
	}
	s.logger.Infof("Peer handler done")
}

// AddPeer adds a new peer that has already been connected to the server.
func (s *server) AddPeer(sp *serverPeer) {
	s.newPeers <- sp
}

// BanPeer bans a peer that has already been connected to the server by ip.
func (s *server) BanPeer(sp *serverPeer) {
	s.banPeers <- sp
}

// RelayInventory relays the passed inventory vector to all connected peers
// that are not already known to have it.
func (s *server) RelayInventory(invVect *wire.InvVect, data interface{}) {
	// check listen mode - if listen_only, don't relay inventory
	if s.settings.P2P.ListenMode == settings.ListenModeListenOnly {
		return
	}

	// dont' block on inv relay, losing invs on restart is fine.
	go func(invVect *wire.InvVect, data interface{}) {
		s.relayInv <- relayMsg{invVect: invVect, data: data}
	}(invVect, data)
}

// BroadcastMessage sends msg to all peers currently connected to the server
// except those in the passed peers to exclude.
func (s *server) BroadcastMessage(msg wire.Message, exclPeers ...*serverPeer) {
	// check listen mode - if listen_only, don't broadcast messages
	if s.settings.P2P.ListenMode == settings.ListenModeListenOnly {
		return
	}

	// dont' block on broadcast, losing messages on restart is fine.
	go func(msg wire.Message, exclPeers ...*serverPeer) {
		s.broadcast <- broadcastMsg{message: msg, excludePeers: exclPeers}
	}(msg, exclPeers...)
}

// ConnectedCount returns the number of currently connected peers.
func (s *server) ConnectedCount() int32 {
	replyChan := make(chan int32)

	s.query <- getConnCountMsg{reply: replyChan}

	return <-replyChan
}

// OutboundGroupCount returns the number of peers connected to the given
// outbound group key.
func (s *server) OutboundGroupCount(key string) int {
	replyChan := make(chan int)
	s.query <- getOutboundGroup{key: key, reply: replyChan}

	return <-replyChan
}

// AddBytesSent adds the passed number of bytes to the total bytes sent counter
// for the server.  It is safe for concurrent access.
func (s *server) AddBytesSent(bytesSent uint64) {
	atomic.AddUint64(&s.bytesSent, bytesSent)
}

// AddBytesReceived adds the passed number of bytes to the total bytes received
// counter for the server.  It is safe for concurrent access.
func (s *server) AddBytesReceived(bytesReceived uint64) {
	atomic.AddUint64(&s.bytesReceived, bytesReceived)
}

// NetTotals returns the sum of all bytes received and sent across the network
// for all peers.  It is safe for concurrent access.
func (s *server) NetTotals() (uint64, uint64) {
	return atomic.LoadUint64(&s.bytesReceived),
		atomic.LoadUint64(&s.bytesSent)
}

// UpdatePeerHeights updates the heights of all peers who have have announced
// the latest connected main chain block, or a recognized orphan. These height
// updates allow us to dynamically refresh peer heights, ensuring sync peer
// selection has access to the latest block heights for each peer.
func (s *server) UpdatePeerHeights(latestBlkHash *chainhash.Hash, latestHeight int32, updateSource *peer.Peer) {
	s.peerHeightsUpdate <- updatePeerHeightsMsg{
		newHash:    latestBlkHash,
		newHeight:  latestHeight,
		originPeer: updateSource,
	}
}

// rebroadcastHandler keeps track of user submitted inventories that we have
// sent out but have not yet made it into a block. We periodically rebroadcast
// them in case our peers restarted or otherwise lost track of them.
func (s *server) rebroadcastHandler() {
	// Wait 5 min before first tx rebroadcast.
	timer := time.NewTimer(5 * time.Minute)
	pendingInvs := make(map[wire.InvVect]interface{})

out:
	for {
		select {
		case riv := <-s.modifyRebroadcastInv:
			switch msg := riv.(type) {
			// Incoming InvVects are added to our map of RPC txs.
			case broadcastInventoryAdd:
				pendingInvs[*msg.invVect] = msg.data

			// When an InvVect has been added to a block, we can
			// now remove it, if it was present.
			case broadcastInventoryDel:
				delete(pendingInvs, *msg)
			}

		case <-timer.C:
			// Any inventory we have has not made it into a block
			// yet. We periodically resubmit them until they have.
			for iv, data := range pendingInvs {
				ivCopy := iv
				s.RelayInventory(&ivCopy, data)
			}

			// Process at a random time up to 30mins (in seconds)
			// in the future.
			timer.Reset(time.Second *
				time.Duration(randomUint16Number(1800)))

		case <-s.quit:
			break out
		}
	}

	timer.Stop()

	// Drain channels before exiting so nothing is left waiting around
	// to send.
cleanup:
	for {
		select {
		case <-s.modifyRebroadcastInv:
		default:
			break cleanup
		}
	}
	s.wg.Done()
}

// Start begins accepting connections from peers.
func (s *server) Start() {
	// Already started?
	if atomic.AddInt32(&s.started, 1) != 1 {
		return
	}

	s.wg.Add(1)
	s.peerHandler()

	if s.nat != nil {
		s.wg.Add(1)
		go s.upnpUpdateThread()
	}

	// Start listening for ban events
	go s.listenForBanEvents(s.ctx)

	s.WaitForShutdown()
}

// Stop gracefully shuts down the server by stopping and disconnecting all
// peers and the main listener.
func (s *server) Stop() error {
	// Make sure this only happens once.
	if atomic.AddInt32(&s.shutdown, 1) != 1 {
		s.logger.Infof("Server is already in the process of shutting down")
		return nil
	}

	s.logger.Warnf("Server shutting down")

	// Signal the remaining goroutines to quit.
	close(s.quit)

	return nil
}

// WaitForShutdown blocks until the main listener and peer handlers are stopped.
func (s *server) WaitForShutdown() {
	s.wg.Wait()
}

// ScheduleShutdown schedules a server shutdown after the specified duration.
// It also dynamically adjusts how often to warn the server is going down based
// on remaining duration.
func (s *server) ScheduleShutdown(duration time.Duration) {
	// Don't schedule shutdown more than once.
	if atomic.AddInt32(&s.shutdownSched, 1) != 1 {
		return
	}

	s.logger.Warnf("Server shutdown in %v", duration)

	go func() {
		remaining := duration
		tickDuration := dynamicTickDuration(remaining)
		done := time.After(remaining)
		ticker := time.NewTicker(tickDuration)
	out:
		for {
			select {
			case <-done:
				ticker.Stop()
				s.Stop()
				break out
			case <-ticker.C:
				remaining -= tickDuration
				if remaining < time.Second {
					continue
				}

				// Change tick duration dynamically based on remaining time.
				newDuration := dynamicTickDuration(remaining)
				if tickDuration != newDuration {
					tickDuration = newDuration
					ticker.Stop()
					ticker = time.NewTicker(tickDuration)
				}
				s.logger.Warnf("Server shutdown in %v", remaining)
			}
		}
	}()
}

// parseListeners determines whether each listen address is IPv4 and IPv6 and
// returns a slice of appropriate net.Addrs to listen on with TCP. It also
// properly detects addresses which apply to "all interfaces" and adds the
// address as both IPv4 and IPv6.
func parseListeners(addrs []string) ([]net.Addr, error) {
	netAddrs := make([]net.Addr, 0, len(addrs)*2)

	for _, addr := range addrs {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			// Shouldn't happen due to already being normalized.
			return nil, err
		}

		// Empty host or host of * on plan9 is both IPv4 and IPv6.
		if host == "" || (host == "*" && runtime.GOOS == "plan9") {
			netAddrs = append(netAddrs, simpleAddr{net: "tcp4", addr: addr})
			netAddrs = append(netAddrs, simpleAddr{net: "tcp6", addr: addr})

			continue
		}

		// Strip IPv6 zone id if present since net.ParseIP does not
		// handle it.
		zoneIndex := strings.LastIndex(host, "%")
		if zoneIndex > 0 {
			host = host[:zoneIndex]
		}

		// Parse the IP.
		ip := net.ParseIP(host)
		if ip == nil {
			return nil, fmt.Errorf("'%s' is not a valid IP address", host)
		}

		// To4 returns nil when the IP is not an IPv4 address, so use
		// this determine the address type.
		if ip.To4() == nil {
			netAddrs = append(netAddrs, simpleAddr{net: "tcp6", addr: addr})
		} else {
			netAddrs = append(netAddrs, simpleAddr{net: "tcp4", addr: addr})
		}
	}

	return netAddrs, nil
}
func (s *server) getPeers() []*serverPeer {
	replyChan := make(chan []*serverPeer, 1)

	select {
	case s.query <- getPeersMsg{reply: replyChan}:
		s.logger.Debugf("getPeers: Query message sent successfully")
	default:
		s.logger.Errorf("getPeers: Failed to send query message, channel full")
		return nil
	}

	select {
	case peers := <-replyChan:
		return peers
	case <-time.After(5 * time.Second):
		s.logger.Warnf("getPeers: Timeout waiting for peer list")
		return nil
	}
}

func (s *server) upnpUpdateThread() {
	// Go off immediately to prevent code duplication, thereafter we renew
	// lease every 15 minutes.
	timer := time.NewTimer(0 * time.Second)
	lport, _ := strconv.ParseInt(activeNetParams.DefaultPort, 10, 16)
	first := true
out:
	for {
		select {
		case <-timer.C:
			// TODO: pick external port  more cleverly
			// TODO: know which ports we are listening to on an external net.
			// TODO: if specific listen port doesn't work then ask for wildcard
			// listen port?
			// XXX this assumes timeout is in seconds.
			listenPort, err := s.nat.AddPortMapping("tcp", int(lport), int(lport), "teranode listen port", 20*60)
			if err != nil {
				s.logger.Warnf("can't add UPnP port mapping: %v", err)
			}
			if first && err == nil {
				// TODO: look this up periodically to see if upnp domain changed
				// and so did ip.
				externalip, err := s.nat.GetExternalAddress()
				if err != nil {
					s.logger.Warnf("UPnP can't get external address: %v", err)
					continue out
				}
				na := wire.NewNetAddressIPPort(externalip, uint16(listenPort),
					s.services)
				err = s.addrManager.AddLocalAddress(na, addrmgr.UpnpPrio)
				if err != nil {
					// XXX DeletePortMapping?
				}
				s.logger.Warnf("Successfully bound via UPnP to %s", addrmgr.NetAddressKey(na))
				first = false
			}
			timer.Reset(time.Minute * 15)
		case <-s.quit:
			break out
		}
	}

	timer.Stop()

	if err := s.nat.DeletePortMapping("tcp", int(lport), int(lport)); err != nil {
		s.logger.Warnf("unable to remove UPnP port mapping: %v", err)
	} else {
		s.logger.Debugf("successfully disestablished UPnP port mapping")
	}

	s.wg.Done()
}

// newServer returns a new bsvd server configured to listen on addr for the
// bitcoin network type specified by chainParams.  Use start to begin accepting
// connections from peers.
func newServer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, config Config, blockchainClient blockchain.ClientI,
	validationClient validator.Interface, utxoStore utxostore.Store, subtreeStore blob.Store, tempStore blob.Store,
	subtreeValidation subtreevalidation.Interface, blockValidation blockvalidation.Interface,
	blockAssembly *blockassembly.Client,
	listenAddrs []string, assetHTTPAddress string) (*server, error) {
	// init config
	c, _, err := loadConfig(logger)
	if err != nil {
		return nil, err
	}
	// c.Upnp = true // TODO set from settings

	cfg = c

	// This is normally only done from file in bsvd, but we need to do it here, also happens inside loadConfig

	if tSettings.ChainCfgParams.Name == "testnet" {
		activeNetParams = &testNetParams
	} else if tSettings.ChainCfgParams.Name == "teratestnet" {
		activeNetParams = &teraTestNetParams
	} else if tSettings.ChainCfgParams.Name == "regtest" {
		activeNetParams = &regressionNetParams
	}

	// overwrite any config options from settings, if applicable
	setConfigValuesFromSettings(logger, config.GetAll(), cfg)

	// If Port was set via settings, update activeNetParams
	if cfg.Port != "" {
		activeNetParams.DefaultPort = cfg.Port
	}

	workingDir, _ := config.Get("legacy_workingDir", "../../data")

	// get the full path to the data directory
	cfg.DataDir, err = filepath.Abs(workingDir)
	if err != nil {
		return nil, err
	}

	// make sure the data directory exists
	if err = os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}

	if tSettings.Legacy.Upnp {
		cfg.Upnp = true
	}

	// /TERANODE CONFIG

	services := defaultServices
	// cfg.NoPeerBloomFilters
	services &^= wire.SFNodeBloom
	// cfg.NoCFilters
	services &^= wire.SFNodeCF

	// Determine node type (full vs pruned) based on block persister status
	// This uses the same logic as the P2P service to ensure consistent advertising
	var bestHeight uint32
	var blockPersisterHeight uint32

	// Get current best height and block persister height
	if blockchainClient != nil {
		if _, bestBlockMeta, err := blockchainClient.GetBestBlockHeader(ctx); err == nil && bestBlockMeta != nil {
			bestHeight = bestBlockMeta.Height
		}

		// Query block persister height from blockchain state
		if stateData, err := blockchainClient.GetState(ctx, "BlockPersisterHeight"); err == nil && len(stateData) >= 4 {
			blockPersisterHeight = binary.LittleEndian.Uint32(stateData)
		}
	}

	retentionWindow := uint32(0)
	if tSettings.GlobalBlockHeightRetention > 0 {
		retentionWindow = tSettings.GlobalBlockHeightRetention
	}

	prunerBlockTrigger := ""
	if tSettings != nil {
		prunerBlockTrigger = tSettings.Pruner.BlockTrigger
	}

	storage := util.DetermineStorageMode(blockPersisterHeight, bestHeight, retentionWindow, prunerBlockTrigger)
	logger.Infof("Legacy service determined storage mode: %s (persisterHeight=%d, bestHeight=%d, retention=%d)",
		storage, blockPersisterHeight, bestHeight, retentionWindow)

	if storage == "full" {
		// Advertise as full node
		services |= wire.SFNodeNetwork
		services &^= wire.SFNodeNetworkLimited
	} else {
		// Advertise as pruned node
		services &^= wire.SFNodeNetwork
		services |= wire.SFNodeNetworkLimited
	}

	peersDir := cfg.DataDir
	if !tSettings.Legacy.SavePeers {
		peersDir = ""
	}

	amgr := addrmgr.New(logger, peersDir, bsvdLookup)

	var listeners []net.Listener

	var nat NAT

	if !cfg.DisableListen {
		var err error

		listeners, nat, err = initListeners(logger, amgr, listenAddrs, services)
		if err != nil {
			return nil, err
		}

		if len(listeners) == 0 {
			return nil, errors.New("no valid listen address")
		}
	}

	banList, banChan, err := p2p.GetBanList(ctx, logger, tSettings)
	if err != nil {
		return nil, errors.New("can't get banList")
	}

	s := server{
		ctx:                  ctx,
		settings:             tSettings,
		startupTime:          time.Now().Unix(),
		addrManager:          amgr,
		newPeers:             make(chan *serverPeer, cfg.MaxPeers),
		donePeers:            make(chan *serverPeer, cfg.MaxPeers),
		banPeers:             make(chan *serverPeer, cfg.MaxPeers),
		banPeerForDuration:   make(chan *banPeerForDurationMsg, cfg.MaxPeers),
		unbanPeer:            make(chan unbanPeerReq, cfg.MaxPeers),
		query:                make(chan interface{}),
		relayInv:             make(chan relayMsg, cfg.MaxPeers),
		broadcast:            make(chan broadcastMsg, cfg.MaxPeers),
		quit:                 make(chan struct{}),
		modifyRebroadcastInv: make(chan interface{}),
		peerHeightsUpdate:    make(chan updatePeerHeightsMsg),
		nat:                  nat,
		timeSource:           blockchain2.NewMedianTime(),
		services:             services,
		sigCache:             txscript.NewSigCache(cfg.SigCacheMaxSize),
		hashCache:            txscript.NewHashCache(cfg.SigCacheMaxSize),
		cfCheckptCaches:      make(map[wire.FilterType][]cfHeaderKV),
		logger:               logger,
		blockchainClient:     blockchainClient,
		utxoStore:            utxoStore,
		subtreeStore:         subtreeStore,
		tempStore:            tempStore,
		concurrentStore: blob.NewConcurrentBlob[chainhash.Hash](
			tempStore,
			blob_options.WithDeleteAt(10),
			blob_options.WithSubDirectory("blocks"),
			blob_options.WithAllowOverwrite(true),
		),
		subtreeValidation: subtreeValidation,
		blockValidation:   blockValidation,
		blockAssembly:     blockAssembly,
		assetHTTPAddress:  assetHTTPAddress,
		banList:           banList,
		banChan:           banChan,
	}

	s.syncManager, err = netsync.New(
		ctx,
		logger,
		tSettings,
		blockchainClient,
		validationClient,
		utxoStore,
		subtreeStore,
		subtreeValidation,
		blockValidation,
		blockAssembly,
		&netsync.Config{
			PeerNotifier:            &s,
			ChainParams:             s.settings.ChainCfgParams,
			DisableCheckpoints:      cfg.DisableCheckpoints,
			MaxPeers:                cfg.MaxPeers,
			MinSyncPeerNetworkSpeed: cfg.MinSyncPeerNetworkSpeed,
		},
	)
	if err != nil {
		return nil, err
	}

	// Add the peers that are defined in teranode settings...
	// also retrieved in services/legacy/Server.go:118
	addresses := s.settings.Legacy.ConnectPeers
	if len(addresses) > 0 {
		c.ConnectPeers = append(c.ConnectPeers, addresses...)
		// set max peers to the number of connect peers
		// this forces the server to only connect to the peers defined in the settings
		c.MaxPeers = len(c.ConnectPeers)
	}

	// Only set up a function to return new addresses to connect to when
	// not running in connect-only mode.  The simulation network is always
	// in connect-only mode since it is only intended to connect to
	// specified peers and actively avoid advertising and connecting to
	// discovered peers in order to prevent it from becoming a public test
	// network.
	var newAddressFunc func() (net.Addr, error)
	if len(cfg.ConnectPeers) == 0 {
		newAddressFunc = func() (net.Addr, error) {
			for tries := 0; tries < 100; tries++ {
				addr := s.addrManager.GetAddress()
				if addr == nil {
					break
				}

				// Address will not be invalid, local or unroutable
				// because addrmanager rejects those on addition.
				// Just check that we don't already have an address
				// in the same group so that we are not connecting
				// to the same network segment at the expense of
				// others.
				key := addrmgr.GroupKey(addr.NetAddress())
				if s.OutboundGroupCount(key) != 0 {
					continue
				}

				// only allow recent nodes (10mins) after we failed 30
				// times
				if tries < 30 && time.Since(addr.LastAttempt()) < 10*time.Minute {
					continue
				}

				// allow nondefault ports after 50 failed tries.
				if tries < 50 && fmt.Sprintf("%d", addr.NetAddress().Port) !=
					activeNetParams.DefaultPort {
					continue
				}

				addrString := addrmgr.NetAddressKey(addr.NetAddress())

				return addrStringToNetAddr(addrString)
			}

			return nil, errors.New("no valid connect address")
		}
	}

	// Create a connection manager.
	targetOutbound := cfg.TargetOutboundPeers
	if cfg.MaxPeers < int(targetOutbound) {
		targetOutbound = uint32(cfg.MaxPeers)
	}

	cmgr, err := connmgr.New(logger, &connmgr.Config{
		Listeners:      listeners,
		OnAccept:       s.inboundPeerConnected,
		RetryDuration:  connectionRetryInterval,
		TargetOutbound: targetOutbound,
		Dial:           bsvdDial,
		OnConnection:   s.outboundPeerConnected,
		GetNewAddress:  newAddressFunc,
	})
	if err != nil {
		return nil, err
	}

	s.connManager = cmgr

	// Start up persistent peers.
	permanentPeers := cfg.ConnectPeers

	if len(permanentPeers) == 0 {
		permanentPeers = cfg.AddPeers
	}

	for _, addr := range permanentPeers {
		netAddr, err := addrStringToNetAddr(addr)
		if err != nil {
			return nil, err
		}

		connReq := &connmgr.ConnReq{
			Permanent: true,
		}
		connReq.SetAddr(netAddr)

		go s.connManager.Connect(connReq)
	}

	go func() {
		// wait for the ctx to be done
		<-ctx.Done()
		// stop the servers
		_ = s.Stop()
		_ = s.syncManager.Stop()
	}()

	return &s, nil
}

// initListeners initializes the configured net listeners and adds any bound
// addresses to the address manager. Returns the listeners and a NAT interface,
// which is non-nil if UPnP is in use.
func initListeners(logger ulogger.Logger, amgr *addrmgr.AddrManager, listenAddrs []string, services wire.ServiceFlag) ([]net.Listener, NAT, error) {
	// Listen for TCP connections at the configured addresses
	netAddrs, err := parseListeners(listenAddrs)
	if err != nil {
		return nil, nil, err
	}

	listeners := make([]net.Listener, 0, len(netAddrs))

	for _, addr := range netAddrs {
		listener, err := net.Listen(addr.Network(), addr.String())
		if err != nil {
			logger.Warnf("Can't listen on %s: %v", addr, err)
			continue
		}

		listeners = append(listeners, listener)
	}

	var nat NAT

	defaultPort, err := strconv.ParseUint(activeNetParams.DefaultPort, 10, 16)
	if err != nil {
		logger.Errorf("Can not parse default port %s for active chain: %v", activeNetParams.DefaultPort, err)
		return nil, nil, err
	}

	if len(cfg.ExternalIPs) != 0 {
		for _, sip := range cfg.ExternalIPs {
			eport := uint16(defaultPort)

			host, portstr, err := net.SplitHostPort(sip)
			if err != nil {
				// no port, use default.
				host = sip
			} else {
				port, err := strconv.ParseUint(portstr, 10, 16)
				if err != nil {
					logger.Warnf("Can not parse port from %s for "+
						"externalip: %v", sip, err)
					continue
				}

				eport = uint16(port)
			}

			na, err := amgr.HostToNetAddress(host, eport, services)
			if err != nil {
				logger.Warnf("Not adding %s as externalip: %v", sip, err)
				continue
			}

			// Found a valid external IP, make sure we use these details
			// so peers get the correct IP information. Since we can only
			// advertise one IP, use the first seen.
			if addrMe == nil {
				addrMe = na
			}

			err = amgr.AddLocalAddress(na, addrmgr.ManualPrio)
			if err != nil {
				logger.Warnf("Skipping specified external IP: %v", err)
			}
		}
	} else {
		if cfg.Upnp {
			var err error

			nat, err = Discover()
			if err != nil {
				logger.Warnf("Can't discover upnp: %v", err)
			}
			// nil nat here is fine, just means no upnp on network.

			// Found a valid external IP, make sure we use these details
			// so peers get the correct IP information.
			if nat != nil {
				addr, err := nat.GetExternalAddress()
				if err == nil {
					eport := uint16(defaultPort)

					na, err := amgr.HostToNetAddress(addr.String(), eport, services)
					if err == nil {
						if addrMe == nil {
							addrMe = na
						}
					}
				}
			}
		}

		// Add bound addresses to address manager to be advertised to peers.
		for _, listener := range listeners {
			addr := listener.Addr().String()
			err := addLocalAddress(amgr, addr, services)

			if err != nil {
				logger.Warnf("Skipping bound address %s: %v", addr, err)
			}
		}
	}

	return listeners, nat, nil
}

// addrStringToNetAddr takes an address in the form of 'host:port' and returns
// a net.Addr which maps to the original address with any host names resolved
// to IP addresses.  It also handles tor addresses properly by returning a
// net.Addr that encapsulates the address.
func addrStringToNetAddr(addr string) (net.Addr, error) {
	host, strPort, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return nil, err
	}

	// Skip if host is already an IP address.
	if ip := net.ParseIP(host); ip != nil {
		return &net.TCPAddr{
			IP:   ip,
			Port: port,
		}, nil
	}

	// Attempt to look up an IP address associated with the parsed host.
	ips, err := bsvdLookup(host)
	if err != nil {
		return nil, err
	}

	if len(ips) == 0 {
		return nil, fmt.Errorf("no addresses found for %s", host)
	}

	return &net.TCPAddr{
		IP:   ips[0],
		Port: port,
	}, nil
}

// addLocalAddress adds an address that this node is listening on to the
// address manager so that it may be relayed to peers.
func addLocalAddress(addrMgr *addrmgr.AddrManager, addr string, services wire.ServiceFlag) error {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return err
	}

	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		// If bound to unspecified address, advertise all local interfaces
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return err
		}

		for _, addr := range addrs {
			ifaceIP, _, err := net.ParseCIDR(addr.String())
			if err != nil {
				continue
			}

			// If bound to 0.0.0.0, do not add IPv6 interfaces and if bound to
			// ::, do not add IPv4 interfaces.
			if (ip.To4() == nil) != (ifaceIP.To4() == nil) {
				continue
			}

			netAddr := wire.NewNetAddressIPPort(ifaceIP, uint16(port), services)
			addrMgr.AddLocalAddress(netAddr, addrmgr.BoundPrio)
		}
	} else {
		netAddr, err := addrMgr.HostToNetAddress(host, uint16(port), services)
		if err != nil {
			return err
		}

		addrMgr.AddLocalAddress(netAddr, addrmgr.BoundPrio)
	}

	return nil
}

// dynamicTickDuration is a convenience function used to dynamically choose a
// tick duration based on remaining time.  It is primarily used during
// server shutdown to make shutdown warnings more frequent as the shutdown time
// approaches.
func dynamicTickDuration(remaining time.Duration) time.Duration {
	switch {
	case remaining <= time.Second*5:
		return time.Second
	case remaining <= time.Second*15:
		return time.Second * 5
	case remaining <= time.Minute:
		return time.Second * 15
	case remaining <= time.Minute*5:
		return time.Minute
	case remaining <= time.Minute*15:
		return time.Minute * 5
	case remaining <= time.Hour:
		return time.Minute * 15
	}

	return time.Hour
}

// isWhitelisted returns whether the IP address is included in the whitelisted
// networks and IPs.
func isWhitelisted(addr net.Addr) (bool, error) {
	if len(cfg.whitelists) == 0 {
		return false, nil
	}

	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return false, fmt.Errorf("Unable to SplitHostPort on '%s': %v", addr, err)
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false, fmt.Errorf("Unable to parse IP '%s'", addr)
	}

	for _, ipnet := range cfg.whitelists {
		if ipnet.Contains(ip) {
			return true, nil
		}
	}

	return false, nil
}

// checkpointSorter implements sort.Interface to allow a slice of checkpoints to
// be sorted.
type checkpointSorter []chaincfg.Checkpoint

// Len returns the number of checkpoints in the slice.  It is part of the
// sort.Interface implementation.
func (s checkpointSorter) Len() int {
	return len(s)
}

// Swap swaps the checkpoints at the passed indices.  It is part of the
// sort.Interface implementation.
func (s checkpointSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less returns whether the checkpoint with index i should sort before the
// checkpoint with index j.  It is part of the sort.Interface implementation.
func (s checkpointSorter) Less(i, j int) bool {
	return s[i].Height < s[j].Height
}

// mergeCheckpoints returns two slices of checkpoints merged into one slice
// such that the checkpoints are sorted by height.  In the case the additional
// checkpoints contain a checkpoint with the same height as a checkpoint in the
// default checkpoints, the additional checkpoint will take precedence and
// overwrite the default one.
func mergeCheckpoints(defaultCheckpoints, additional []chaincfg.Checkpoint) []chaincfg.Checkpoint {
	// Create a map of the additional checkpoints to remove duplicates while
	// leaving the most recently-specified checkpoint.
	extra := make(map[int32]chaincfg.Checkpoint)
	for _, checkpoint := range additional {
		extra[checkpoint.Height] = checkpoint
	}

	// Add all default checkpoints that do not have an override in the
	// additional checkpoints.
	numDefault := len(defaultCheckpoints)
	checkpoints := make([]chaincfg.Checkpoint, 0, numDefault+len(extra))

	for _, checkpoint := range defaultCheckpoints {
		if _, exists := extra[checkpoint.Height]; !exists {
			checkpoints = append(checkpoints, checkpoint)
		}
	}

	// Append the additional checkpoints and return the sorted results.
	for _, checkpoint := range extra {
		checkpoints = append(checkpoints, checkpoint)
	}

	sort.Sort(checkpointSorter(checkpoints))

	return checkpoints
}

// directionString is a helper function that returns a string that represents
// the direction of a connection (inbound or outbound).
func directionString(inbound bool) string {
	if inbound {
		return "inbound"
	}

	return "outbound"
}

// pickNoun returns the singular or plural form of a noun depending
// on the count n.
func pickNoun(n uint64, singular, plural string) string {
	if n == 1 {
		return singular
	}

	return plural
}

// listenForBanEvents continuously listens for ban events from the P2P service
// and handles them by disconnecting banned peers. This method runs in a goroutine
// and will exit when the context is canceled.
func (s *server) listenForBanEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-s.banChan:
			s.handleBanEvent(ctx, event)
		}
	}
}

// handleBanEvent processes a ban event from the P2P service by disconnecting
// peers that match the banned IP address or subnet. Only "add" ban actions
// are processed; other actions are ignored.
func (s *server) handleBanEvent(_ context.Context, event p2p.BanEvent) {
	if event.Action != "add" {
		return // We only care about new bans
	}

	s.logger.Infof("Received ban event for %s", event.IP)

	// Create a message to send to the peer handler
	disconnectMsg := disconnectNodeMsg{
		cmp: func(sp *serverPeer) bool {
			peerIP := net.ParseIP(sp.Addr())
			return sp.Addr() == event.IP || (event.Subnet != nil && event.Subnet.Contains(peerIP))
		},
		reply: make(chan error),
	}

	// Send the message to the peer handler
	s.query <- disconnectMsg

	// Wait for the reply
	err := <-disconnectMsg.reply
	if err != nil {
		s.logger.Errorf("Error disconnecting banned peers: %v", err)
	}
}
