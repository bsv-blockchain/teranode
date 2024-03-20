package p2p

import (
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"
)

const defaultExcessiveBlockSize = 4000000000

type PeerManager struct {
	mu         sync.RWMutex
	peers      []PeerI
	network    wire.BitcoinNet
	batchDelay time.Duration
	logger     *slog.Logger
	ebs        int64
}

// NewPeerManager creates a new PeerManager
// messageCh is a channel that will be used to send messages from peers to the parent process
// this is used to pass INV messages from the bitcoin network peers to the parent process
// at the moment this is only used for Inv tx message for "seen", "sent" and "rejected" transactions
func NewPeerManager(logger *slog.Logger, network wire.BitcoinNet, options ...PeerManagerOptions) PeerManagerI {

	pm := &PeerManager{
		peers:   make([]PeerI, 0),
		network: network,
		logger:  logger,
		ebs:     defaultExcessiveBlockSize,
	}

	for _, option := range options {
		option(pm)
	}

	logger.Info("Excessive block size set to", slog.Int64("block size", pm.ebs))
	wire.SetLimits(uint64(pm.ebs))

	return pm
}

func (pm *PeerManager) AddPeer(peer PeerI) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if peer.Network() != pm.network {
		return ErrPeerNetworkMismatch
	}

	pm.peers = append(pm.peers, peer)

	return nil
}

func (pm *PeerManager) GetPeers() []PeerI {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	peers := make([]PeerI, 0, len(pm.peers))
	peers = append(peers, pm.peers...)

	return peers
}

// AnnounceTransaction will send an INV message to the provided peers or to selected peers if peers is nil
// it will return the peers that the transaction was actually announced to
func (pm *PeerManager) AnnounceTransaction(txHash *chainhash.Hash, peers []PeerI) []PeerI {
	if len(peers) == 0 {
		peers = pm.GetAnnouncedPeers()
	}

	for _, peer := range peers {
		peer.AnnounceTransaction(txHash)
	}

	return peers
}

func (pm *PeerManager) RequestTransaction(txHash *chainhash.Hash) PeerI {
	// send to the first found peer that is connected
	var sendToPeer PeerI
	for _, peer := range pm.GetAnnouncedPeers() {
		if peer.Connected() {
			sendToPeer = peer
			break
		}
	}

	// we don't have any connected peers
	if sendToPeer == nil {
		return nil
	}

	sendToPeer.RequestTransaction(txHash)

	return sendToPeer
}

func (pm *PeerManager) AnnounceBlock(blockHash *chainhash.Hash, peers []PeerI) []PeerI {
	if len(peers) == 0 {
		peers = pm.GetAnnouncedPeers()
	}

	for _, peer := range peers {
		peer.AnnounceBlock(blockHash)
	}

	return peers
}

func (pm *PeerManager) RequestBlock(blockHash *chainhash.Hash) PeerI {
	// send to the first found peer that is connected
	var sendToPeer PeerI
	for _, peer := range pm.GetAnnouncedPeers() {
		if peer.Connected() {
			sendToPeer = peer
			break
		}
	}

	// we don't have any connected peers
	if sendToPeer == nil {
		return nil
	}

	sendToPeer.RequestBlock(blockHash)

	return sendToPeer
}

func (pm *PeerManager) GetAnnouncedPeers() []PeerI {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Get a list of peers that are connected
	connectedPeers := make([]PeerI, 0, len(pm.peers))
	for _, peer := range pm.peers {
		if peer.Connected() {
			connectedPeers = append(connectedPeers, peer)
		}
	}

	// sort peers by address
	sort.SliceStable(connectedPeers, func(i, j int) bool {
		return connectedPeers[i].String() < connectedPeers[j].String()
	})

	// send to a subset of peers to be able to listen on the rest
	sendToPeers := make([]PeerI, 0, len(connectedPeers))
	for _, peer := range connectedPeers {

		if len(connectedPeers) > 1 && len(sendToPeers) >= (len(connectedPeers)+1)/2 {
			break
		}
		sendToPeers = append(sendToPeers, peer)
	}

	return sendToPeers
}
