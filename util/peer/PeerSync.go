package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

type PeerSync struct {
	logger                ulogger.Logger
	PeerConnection        PeerConnection
	numberOfExpectedPeers int
	lastMsgByPeerId       map[string]MiningOnMessage
	defaultTimeout        time.Duration
}

func NewPeerSync(logger ulogger.Logger, processName string, numberOfExpectedPeers int, defaultTimeout time.Duration) *PeerSync {

	p2pIp, ok := gocore.Config().Get("p2p_ip")
	if !ok {
		panic("[PeerSync] p2p_ip not set in config")
	}
	p2pPort, ok := gocore.Config().GetInt(fmt.Sprintf("p2p_port_%s", processName))
	if !ok {
		panic(fmt.Sprintf("[PeerSync] p2p_port_%s not set in config", processName))
	}
	sharedKey, ok := gocore.Config().Get("p2p_shared_key")
	if !ok {
		panic(fmt.Errorf("[PeerSync] error getting p2p_shared_key"))
	}
	usePrivateDht := gocore.Config().GetBool("p2p_dht_use_private", false)

	config := PeerConfig{
		ProcessName:   processName,
		IP:            p2pIp,
		Port:          p2pPort,
		SharedKey:     sharedKey,
		UsePrivateDHT: usePrivateDht,
	}
	peerConnection := NewPeerConnection(logger, config)

	peerStatus := &PeerSync{
		logger:                logger,
		PeerConnection:        *peerConnection,
		numberOfExpectedPeers: numberOfExpectedPeers,
		lastMsgByPeerId:       make(map[string]MiningOnMessage),
		defaultTimeout:        defaultTimeout,
	}

	return peerStatus

}

func (p *PeerSync) Start(ctx context.Context) error {
	topicPrefix, ok := gocore.Config().Get("p2p_topic_prefix")
	if !ok {
		panic("[PeerSync] p2p_topic_prefix not set in config")
	}
	topic, _ := gocore.Config().Get("p2p_mining_on_topic", "miningon")

	topicName := fmt.Sprintf("%s-%s", topicPrefix, topic)

	err := p.PeerConnection.Start(ctx, topicName)
	if err != nil {
		return err
	}

	err = p.PeerConnection.SetTopicHandler(ctx, topicName, p.miningOnHandler)
	if err != nil {
		return err
	}

	return nil
}

func (p *PeerSync) Stop(ctx context.Context) error {
	err := p.PeerConnection.Stop(ctx)
	return err
}

func (p *PeerSync) miningOnHandler(msg []byte, from string) {
	miningOnMessage := MiningOnMessage{}
	err := json.Unmarshal(msg, &miningOnMessage)
	if err != nil {
		p.logger.Errorf("[PeerSync] Received miningon message from %s: %v", from, err)
	} else {
		p.logger.Debugf("[PeerSync] Received miningon message from %s: %v", from, miningOnMessage)
	}

	p.lastMsgByPeerId[from] = miningOnMessage
}

/*
 * HaveAllPeersReachedMinHeight checks if all peers are mining at the given block height or higher.
 * Very crude implementation, we need to allow for natural forks and reorgs.
 */
func (p *PeerSync) HaveAllPeersReachedMinHeight(height uint32, testAllPeers bool) bool {
	if len(p.lastMsgByPeerId) == 0 {
		// no peers to check
		return true
	}

	if len(p.lastMsgByPeerId) < p.numberOfExpectedPeers {
		p.logger.Infof("[PeerSync] Not enough peers to check if in sync %d/%d", len(p.lastMsgByPeerId), p.numberOfExpectedPeers)
		return false
	}

	result := true

	for _, miningon := range p.lastMsgByPeerId {

		/* we need the other nodes to be at least at the same height as us, it's ok if they are ahead */
		if height > miningon.Height {
			p.logger.Infof("[PeerSync][%s] Not in sync, %s=%d vs %d", miningon.PeerId, miningon.Miner, miningon.Height, height)
			result = false

			if !testAllPeers {
				return false
			}
		}

	}

	p.logger.Debugf("[PeerSync] Peers are all at block height %d or higher", height)
	return result
}

func (p *PeerSync) WaitForAllPeers(ctx context.Context, height uint32, testAllPeers bool) error {
	_, ok := ctx.Deadline()
	if !ok {
		// there is no timeout or deadline context passed in so we will
		// create a default timeout, we can't have this sitting there forever
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.defaultTimeout)
		defer cancel()
	}

	for {
		select {
		case <-ctx.Done():
			return errors.New("[PeerSync] WaitForAllPeers cancelled due to timeout or context cancellation")
		default:
			if p.HaveAllPeersReachedMinHeight(height, testAllPeers) {
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}
