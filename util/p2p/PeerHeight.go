package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

type PeerHeight struct {
	logger                ulogger.Logger
	P2PNode               P2PNode
	numberOfExpectedPeers int
	lastMsgByPeerId       sync.Map
	defaultTimeout        time.Duration
}

func NewPeerHeight(logger ulogger.Logger, processName string, numberOfExpectedPeers int, defaultTimeout time.Duration) *PeerHeight {

	p2pIp, ok := gocore.Config().Get("p2p_ip")
	if !ok {
		panic("[PeerHeight] p2p_ip not set in config")
	}
	p2pPort, ok := gocore.Config().GetInt(fmt.Sprintf("p2p_port_%s", processName))
	if !ok {
		panic(fmt.Sprintf("[PeerHeight] p2p_port_%s not set in config", processName))
	}
	sharedKey, ok := gocore.Config().Get("p2p_shared_key")
	if !ok {
		panic(errors.NewConfigurationError("[PeerHeight] error getting p2p_shared_key"))
	}
	usePrivateDht := gocore.Config().GetBool("p2p_dht_use_private", false)
	optimiseRetries := gocore.Config().GetBool("p2p_optimise_retries", false)

	staticPeers, _ := gocore.Config().GetMulti(fmt.Sprintf("%s_p2p_static_peers", processName), "|")
	privateKey, _ := gocore.Config().Get(fmt.Sprintf("%s_p2p_private_key", processName))

	config := P2PConfig{
		ProcessName:     processName,
		IP:              p2pIp,
		Port:            p2pPort,
		PrivateKey:      privateKey,
		SharedKey:       sharedKey,
		UsePrivateDHT:   usePrivateDht,
		OptimiseRetries: optimiseRetries,
		Advertise:       false, // no one need to discover or connect to us, we just listen
		StaticPeers:     staticPeers,
	}
	peerConnection := NewP2PNode(logger, config)

	peerStatus := &PeerHeight{
		logger:                logger,
		P2PNode:               *peerConnection,
		numberOfExpectedPeers: numberOfExpectedPeers,
		lastMsgByPeerId:       sync.Map{},
		defaultTimeout:        defaultTimeout,
	}

	return peerStatus

}

func (p *PeerHeight) Start(ctx context.Context) error {
	topicPrefix, ok := gocore.Config().Get("p2p_topic_prefix")
	if !ok {
		panic("[PeerHeight] p2p_topic_prefix not set in config")
	}
	topic, _ := gocore.Config().Get("p2p_block_topic", "block")

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
	p.lastMsgByPeerId.Range(func(key, value interface{}) bool {
		before++
		return true // continue iterating
	})

	previousBlockMessage, ok := p.lastMsgByPeerId.Load(blockMessage.PeerId)
	if ok && previousBlockMessage.(BlockMessage).Height > blockMessage.Height {
		p.logger.Debugf("[PeerHeight] Ignoring block message from %s for block height %d as we are already at %d", from, blockMessage.Height, previousBlockMessage.(BlockMessage).Height)
	} else {
		p.lastMsgByPeerId.Store(blockMessage.PeerId, blockMessage)
	}

	after := 0
	p.lastMsgByPeerId.Range(func(key, value interface{}) bool {
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
	p.lastMsgByPeerId.Range(func(key, value interface{}) bool {
		size++
		return true // continue iterating
	})
	if size < p.numberOfExpectedPeers {
		if first {
			p.logger.Infof("[PeerHeight] Not enough peers to check if at same block height %d/%d", size, p.numberOfExpectedPeers)
			p.lastMsgByPeerId.Range(func(key, value interface{}) bool {
				block := value.(BlockMessage)
				p.logger.Infof("[PeerHeight] peer=%s %s=%d", block.PeerId, block.DataHubUrl, block.Height)
				return true
			})
		}
		return false
	}

	result := true

	p.lastMsgByPeerId.Range(func(key, value interface{}) bool {
		block := value.(BlockMessage)

		/* we need the other nodes to be at least at the same height as us, it's ok if they are ahead */
		if height > block.Height {

			p.logger.Infof("[PeerHeight][%s] Not at same block height, %s=%d vs %d", block.PeerId, block.DataHubUrl, block.Height, height)
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
