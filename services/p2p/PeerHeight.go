package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-p2p"
)

// PeerHeight manages peer height tracking and synchronization in the P2P network.
// This component monitors the blockchain height of connected peers to ensure network
// synchronization and coordinate operations that depend on peer consensus about
// blockchain state. It's particularly useful for testing and validation scenarios
// where specific height requirements must be met across the network.
//
// The struct maintains a mapping of peer messages and provides functionality to:
//   - Track the latest block height reported by each peer
//   - Wait for all peers to reach a minimum height threshold
//   - Coordinate network-wide synchronization operations
//   - Handle timeout scenarios when peers fail to synchronize
//
// All operations are thread-safe for concurrent access from multiple goroutines.
type PeerHeight struct {
	logger                ulogger.Logger     // Logger instance for peer height operations
	settings              *settings.Settings // Global Teranode configuration settings
	P2PNode               p2p.Node           // P2P node instance for network communication
	numberOfExpectedPeers int                // Expected number of peers for synchronization operations
	lastMsgByPeerID       sync.Map           // Thread-safe map of last messages received from each peer
	defaultTimeout        time.Duration      // Default timeout for synchronization operations
}

// NewPeerHeight creates a new PeerHeight instance with the specified configuration.
// This constructor initializes a peer height tracker with P2P networking capabilities,
// allowing it to monitor and coordinate blockchain synchronization across connected peers.
//
// The function sets up a P2P node with private DHT configuration for secure peer
// communication and configures it specifically for height tracking operations.
//
// Parameters:
//   - logger: Logger instance for tracking operations and debugging
//   - tSettings: Global Teranode settings containing P2P configuration
//   - processName: Identifier for this peer height tracker instance
//   - numberOfExpectedPeers: Expected number of peers for synchronization operations
//   - defaultTimeout: Default timeout duration for synchronization operations
//   - p2pPort: Port number for P2P communication
//   - staticPeers: List of static peer addresses to connect to
//   - privateKey: Private key for P2P node identity
//
// Returns:
//   - Configured PeerHeight instance ready for use
//   - Error if configuration is invalid or P2P node creation fails
func NewPeerHeight(logger ulogger.Logger, tSettings *settings.Settings, processName string, numberOfExpectedPeers int, defaultTimeout time.Duration, p2pPort int, staticPeers []string, privateKey string) (*PeerHeight, error) {
	p2pListenAddresses := tSettings.P2P.ListenAddresses
	if len(p2pListenAddresses) == 0 {
		return nil, errors.NewConfigurationError("[PeerHeight] p2p_listen_addresses not set in config")
	}

	sharedKey := tSettings.P2P.SharedKey
	if sharedKey == "" {
		return nil, errors.NewConfigurationError("[PeerHeight] error getting p2p_shared_key")
	}

	usePrivateDht := tSettings.P2P.DHTUsePrivate

	optimiseRetries := tSettings.P2P.OptimiseRetries

	// Merge bootstrap addresses into static peers if persistent bootstrap is enabled
	if tSettings.P2P.BootstrapPersistent {
		logger.Infof("Bootstrap persistent mode enabled - adding %d bootstrap addresses to static peers", len(tSettings.P2P.BootstrapAddresses))
		staticPeers = append(staticPeers, tSettings.P2P.BootstrapAddresses...)
	}

	config := p2p.Config{
		ProcessName:        processName,
		ListenAddresses:    p2pListenAddresses,
		Port:               p2pPort,
		PrivateKey:         privateKey,
		SharedKey:          sharedKey,
		UsePrivateDHT:      usePrivateDht,
		OptimiseRetries:    optimiseRetries,
		Advertise:          false, // no one need to discover or connect to us, we just listen
		StaticPeers:        staticPeers,
		BootstrapAddresses: tSettings.P2P.BootstrapAddresses,
		DHTProtocolID:      tSettings.P2P.DHTProtocolID,
	}

	peerConnection, err := p2p.NewNode(context.Background(), logger, config)
	if err != nil {
		return nil, errors.NewServiceError("[PeerHeight] Error creating P2PNode", err)
	}

	peerStatus := &PeerHeight{
		logger:                logger,
		settings:              tSettings,
		P2PNode:               *peerConnection, //nolint:govet // this needs to be refactored to pass
		numberOfExpectedPeers: numberOfExpectedPeers,
		lastMsgByPeerID:       sync.Map{},
		defaultTimeout:        defaultTimeout,
	}

	return peerStatus, nil
}

// Start initializes the PeerHeight service by starting the P2P node and setting up
func (p *PeerHeight) Start(ctx context.Context) error {
	topicPrefix := p.settings.ChainCfgParams.TopicPrefix
	if topicPrefix == "" {
		return errors.NewConfigurationError("[PeerHeight] missing config ChainCfgParams.TopicPrefix")
	}

	topic := p.settings.P2P.BlockTopic
	if topic == "" {
		topic = "block"

		p.logger.Warnf("[PeerHeight] p2p_block_topic not set in config, using default value")
	}

	topicName := fmt.Sprintf("%s-%s", topicPrefix, topic)

	err := p.P2PNode.Start(ctx, nil, topicName)
	if err != nil {
		return err
	}

	err = p.P2PNode.SetTopicHandler(ctx, topicName, p.blockHandler)
	if err != nil {
		return err
	}

	return nil
}

// Stop gracefully stops the PeerHeight service by shutting down the P2P node.
func (p *PeerHeight) Stop(ctx context.Context) error {
	err := p.P2PNode.Stop(ctx)
	return err
}

// blockHandler processes incoming block messages from peers.
func (p *PeerHeight) blockHandler(ctx context.Context, msg []byte, from string) {
	blockMessage := p2p.BlockMessage{}

	err := json.Unmarshal(msg, &blockMessage)
	if err != nil {
		p.logger.Errorf("[PeerHeight] Received block message from %s: %v", from, err)
	} else {
		p.logger.Debugf("[PeerHeight] Received block message from %s: %v", from, blockMessage)
	}

	before := 0

	p.lastMsgByPeerID.Range(func(key, value interface{}) bool {
		before++
		return true // continue iterating
	})

	previousBlockMessage, ok := p.lastMsgByPeerID.Load(blockMessage.PeerID)
	if ok && previousBlockMessage.(p2p.BlockMessage).Height > blockMessage.Height {
		p.logger.Debugf("[PeerHeight] Ignoring block message from %s for block height %d as we are already at %d", from, blockMessage.Height, previousBlockMessage.(p2p.BlockMessage).Height)
	} else {
		p.lastMsgByPeerID.Store(blockMessage.PeerID, blockMessage)
	}

	after := 0

	p.lastMsgByPeerID.Range(func(key, value interface{}) bool {
		after++
		return true // continue iterating
	})

	// log if we have received a block message from all expected peers. but only once
	if after > before && after >= p.numberOfExpectedPeers {
		p.logger.Infof("[PeerHeight] Received a block message from %d peers. Startup complete for checking things are at the same block height.", after)
	}
}

// HaveAllPeersReachedMinHeight checks if all peers have a block at the given block height or higher very crude implementation,
// we need to allow for natural forks and reorgs.
func (p *PeerHeight) HaveAllPeersReachedMinHeight(height uint32, testAllPeers bool, first bool) bool {
	size := 0

	p.lastMsgByPeerID.Range(func(key, value interface{}) bool {
		size++
		return true // continue iterating
	})

	if size < p.numberOfExpectedPeers {
		if first {
			p.logger.Infof("[PeerHeight] Not enough peers to check if at same block height %d/%d", size, p.numberOfExpectedPeers)
			p.lastMsgByPeerID.Range(func(key, value interface{}) bool {
				block := value.(p2p.BlockMessage)
				p.logger.Infof("[PeerHeight] peer=%s %s=%d", block.PeerID, block.DataHubURL, block.Height)

				return true
			})
		}

		return false
	}

	result := true

	p.lastMsgByPeerID.Range(func(key, value interface{}) bool {
		block := value.(p2p.BlockMessage)

		/* we need the other nodes to be at least at the same height as us, it's ok if they are ahead */
		if height > block.Height {
			p.logger.Infof("[PeerHeight][%s] Not at same block height, %s=%d vs %d", block.PeerID, block.DataHubURL, block.Height, height)

			result = false

			if !testAllPeers {
				return false
			}
		}

		return true // continue .Range() iteration
	})

	return result
}

// WaitForAllPeers waits for all peers to reach a minimum block height.
func (p *PeerHeight) WaitForAllPeers(ctx context.Context, height uint32, testAllPeers bool) error {
	_, ok := ctx.Deadline()
	if !ok {
		// there is no timeout or deadline context passed in so we will
		// create a default timeout, we can't have this sitting there forever
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.defaultTimeout)
		defer cancel()
	}

	first := true

	for {
		select {
		case <-ctx.Done():
			return errors.NewContextCanceledError("[PeerHeight] WaitForAllPeers cancelled due to timeout or context cancellation")
		default:
			if p.HaveAllPeersReachedMinHeight(height, testAllPeers, first) {
				if !first {
					// only log the success if there was a previous logging of a failure
					p.logger.Infof("[PeerHeight] Peers are all at block height %d or higher", height)
				}

				return nil
			}

			first = false

			time.Sleep(1 * time.Second)
		}
	}
}
