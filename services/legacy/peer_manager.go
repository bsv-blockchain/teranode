package legacy

import (
	"context"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type PeerManager struct {
	logger   ulogger.Logger
	height   uint32
	lastHash *chainhash.Hash
	peers    []*Peer
	cond     *sync.Cond

	// store
	blockchainStore blockchain.Store
	subtreeStore    blob.Store
	utxoStore       utxo.Store
}

// NewPeerManager creates a new instance of the PeerManager
func NewPeerManager(logger ulogger.Logger, blockchainStore blockchain.Store, subtreeStore blob.Store, utxoStore utxo.Store) *PeerManager {
	return &PeerManager{
		logger:          logger,
		blockchainStore: blockchainStore,
		subtreeStore:    subtreeStore,
		utxoStore:       utxoStore,
		peers:           make([]*Peer, 0),
	}
}

// Start starts the PeerManager
// TODO do we allow inbound connections?
func (pm *PeerManager) Start(ctx context.Context) error {

	// Create a new Bitcoin peer
	addresses, _ := gocore.Config().GetMulti("legacy_connect_peers", "|", []string{"54.169.45.196:8333"})
	addr := addresses[0]

	var mutex sync.Mutex
	pm.cond = sync.NewCond(&mutex)

	for {
		// TODO connect as pruned node
		peer, err := NewPeer(pm, addr)
		if err != nil {
			pm.logger.Fatalf("Failed to create peer: %v", err)
		}
		pm.peers = append(pm.peers, peer)

		// Wait for connection
		time.Sleep(time.Second * 5)

		bestBlockHeader, bestBlockMeta, err := pm.blockchainStore.GetBestBlockHeader(ctx)
		if err != nil {
			pm.logger.Fatalf("Failed to get best block header: %v", err)
		}

		pm.height = bestBlockMeta.Height
		pm.lastHash = bestBlockHeader.Hash()

		if pm.height > 0 {
			pm.height--
			pm.lastHash = bestBlockHeader.HashPrevBlock
		}

		invMsg := wire.NewMsgGetHeaders()
		_ = invMsg.AddBlockLocatorHash(pm.lastHash) // First time this is Genesis block hash
		invMsg.HashStop = chainhash.Hash{}

		if pm.height == 0 {
			pm.logger.Infof("Requesting headers starting from genesis\n")
		} else {
			pm.logger.Infof("Requesting headers starting from %s\n", pm.lastHash)
		}

		// Send the getheaders message
		pm.peers[0].QueueMessage(invMsg, nil)

		// Keep the program running to receive headers
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-pm.WaitForPeerQuitChannel():
				break
			}
		}
	}
}

func (pm *PeerManager) Stop(_ context.Context) error {
	return nil
}

// WaitForPeerQuitChannel is needed because the peer.quit channel is not exported
func (pm *PeerManager) WaitForPeerQuitChannel() <-chan struct{} {
	ch := make(chan struct{})

	go func() {
		// do this for all peers
		pm.peers[0].WaitForDisconnect()
		close(ch)
	}()

	return ch
}
