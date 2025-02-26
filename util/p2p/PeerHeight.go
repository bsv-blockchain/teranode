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
)

type PeerHeight struct {
	logger                ulogger.Logger
	settings              *settings.Settings
	P2PNode               P2PNode
	numberOfExpectedPeers int
	lastMsgByPeerID       sync.Map
	defaultTimeout        time.Duration
}

func NewPeerHeight(logger ulogger.Logger, tSettings *settings.Settings, processName string, numberOfExpectedPeers int, defaultTimeout time.Duration, p2pPort int, staticPeers []string, privateKey string) (*PeerHeight, error) {
	p2pIP := tSettings.P2P.IP
	if p2pIP == "" {
		return nil, errors.NewConfigurationError("[PeerHeight] p2p_ip not set in config")
	}

	sharedKey := tSettings.P2P.SharedKey
	if sharedKey == "" {
		return nil, errors.NewConfigurationError("[PeerHeight] error getting p2p_shared_key")
	}

	usePrivateDht := tSettings.P2P.DHTUsePrivate

	optimiseRetries := tSettings.P2P.OptimiseRetries

	config := P2PConfig{
		ProcessName:     processName,
		IP:              p2pIP,
		Port:            p2pPort,
		PrivateKey:      privateKey,
		SharedKey:       sharedKey,
		UsePrivateDHT:   usePrivateDht,
		OptimiseRetries: optimiseRetries,
		Advertise:       false, // no one need to discover or connect to us, we just listen
		StaticPeers:     staticPeers,
	}

	peerConnection, err := NewP2PNode(logger, tSettings, config, nil)
	if err != nil {
		return nil, errors.NewServiceError("[PeerHeight] Error creating P2PNode", err)
	}

	peerStatus := &PeerHeight{
		logger:                logger,
		settings:              tSettings,
		P2PNode:               *peerConnection,
		numberOfExpectedPeers: numberOfExpectedPeers,
		lastMsgByPeerID:       sync.Map{},
		defaultTimeout:        defaultTimeout,
	}

	return peerStatus, nil
}

func (p *PeerHeight) Start(ctx context.Context) error {
	topicPrefix := p.settings.P2P.TopicPrefix
	if topicPrefix == "" {
		return errors.NewConfigurationError("[PeerHeight] p2p_topic_prefix not set in config")
	}

	topic := p.settings.P2P.BlockTopic
	if topic == "" {
		topic = "block"

		p.logger.Warnf("[PeerHeight] p2p_block_topic not set in config, using default value")
	}

	topicName := fmt.Sprintf("%s-%s", topicPrefix, topic)

	err := p.P2PNode.Start(ctx, topicName)
	if err != nil {
		return err
	}

	err = p.P2PNode.SetTopicHandler(ctx, topicName, p.blockHandler)
	if err != nil {
		return err
	}

	return nil
}

func (p *PeerHeight) Stop(ctx context.Context) error {
	err := p.P2PNode.Stop(ctx)
	return err
}

func (p *PeerHeight) blockHandler(ctx context.Context, msg []byte, from string) {
	blockMessage := BlockMessage{}

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
	if ok && previousBlockMessage.(BlockMessage).Height > blockMessage.Height {
		p.logger.Debugf("[PeerHeight] Ignoring block message from %s for block height %d as we are already at %d", from, blockMessage.Height, previousBlockMessage.(BlockMessage).Height)
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

/*
 * HaveAllPeersReachedMinHeight checks if all peers have a block at the given block height or higher.
 * Very crude implementation, we need to allow for natural forks and reorgs.
 */
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
				block := value.(BlockMessage)
				p.logger.Infof("[PeerHeight] peer=%s %s=%d", block.PeerID, block.DataHubURL, block.Height)

				return true
			})
		}

		return false
	}

	result := true

	p.lastMsgByPeerID.Range(func(key, value interface{}) bool {
		block := value.(BlockMessage)

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
