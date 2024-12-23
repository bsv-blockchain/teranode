//go:build e2eTest

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	model "github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	p2p "github.com/bitcoin-sv/teranode/util/p2p"
)

var (
	blockTopicName      string
	bestBlockTopicName  string
	subtreeTopicName    string
	miningOnTopicName   string
	rejectedTxTopicName string
)

type BlockMessage struct {
	Hash       string
	Height     uint32
	DataHubUrl string
	PeerId     string
}

func TestTwoP2PNodes(t *testing.T) {
	ctx := context.Background()
	tSettings := settings.NewSettings()

	var logLevelStr = tSettings.LogLevel
	logger := ulogger.New("p2p", ulogger.WithLevel(logLevelStr))

	assetHTTPAddress := tSettings.Asset.HTTPAddress
	if assetHTTPAddress == "" {
		panic("asset_http_address not set in config")
	}
	assetHTTPAddressURL, err := url.Parse(assetHTTPAddress)
	if err != nil {
		panic("error parsing asset_httpAddress", err)
	}
	p2pIp := tSettings.P2P.IP
	if p2pIp == "" {
		panic("p2p_ip_prefix not set in config")
	}

	p2pPort := tSettings.P2P.Port
	if p2pPort == 0 {
		panic("p2p_port not set in config")
	}

	topicPrefix := tSettings.P2P.TopicPrefix
	if topicPrefix == "" {
		panic("p2p_topic_prefix not set in config")
	}

	btn := tSettings.P2P.BlockTopic
	if btn == "" {
		panic("p2p_block_topic not set in config")
	}

	stn := tSettings.P2P.SubtreeTopic
	if stn == "" {
		panic("p2p_subtree_topic not set in config")
	}

	bbtn := tSettings.P2P.BestBlockTopic
	if bbtn == "" {
		panic("p2p_bestblock_topic not set in config")
	}

	miningOntn := tSettings.P2P.MiningOnTopic
	if miningOntn == "" {
		panic("p2p_mining_on_topic not set in config")
	}

	rtn := tSettings.P2P.RejectedTxTopic
	if rtn == "" {
		panic("p2p_rejected_tx_topic not set in config")
	}

	sharedKey := tSettings.P2P.SharedKey
	if sharedKey == "" {
		panic("p2p_shared_key not set in config")
	}

	usePrivateDht := tSettings.P2P.DHTUsePrivate
	optimiseRetries := tSettings.P2P.OptimiseRetries

	blockTopicName = fmt.Sprintf("%s-%s", topicPrefix, btn)
	subtreeTopicName = fmt.Sprintf("%s-%s", topicPrefix, stn)
	bestBlockTopicName = fmt.Sprintf("%s-%s", topicPrefix, bbtn)
	miningOnTopicName = fmt.Sprintf("%s-%s", topicPrefix, miningOntn)
	rejectedTxTopicName = fmt.Sprintf("%s-%s", topicPrefix, rtn)

	staticPeers := tSettings.P2P.StaticPeers
	privateKey, _ := tSettings.P2P.PrivateKey

	p2pPortPrefix1 := "1"
	p2pPortPrefix2 := "2"

	port1 := fmt.Sprintf("%s%d", p2pPortPrefix1, p2pPort)
	port2 := fmt.Sprintf("%s%d", p2pPortPrefix2, p2pPort)

	port1Int, err := strconv.Atoi(port1)
	if err != nil {
		logger.Errorf("Error converting port1 to int: %v", err)
		return
	}

	port2Int, err := strconv.Atoi(port2)
	if err != nil {
		logger.Errorf("Error converting port2 to int: %v", err)
		return
	}

	//TODO - Make a map of test nodes
	configPeer1 := p2p.P2PConfig{
		ProcessName:     "peer",
		IP:              p2pIp,
		Port:            port1Int,
		PrivateKey:      privateKey,
		SharedKey:       sharedKey,
		UsePrivateDHT:   usePrivateDht,
		OptimiseRetries: optimiseRetries,
		Advertise:       true,
		StaticPeers:     staticPeers,
	}

	configPeer2 := p2p.P2PConfig{
		ProcessName:     "peer",
		IP:              p2pIp,
		Port:            port2Int,
		PrivateKey:      privateKey,
		SharedKey:       sharedKey,
		UsePrivateDHT:   usePrivateDht,
		OptimiseRetries: optimiseRetries,
		Advertise:       true,
		StaticPeers:     staticPeers,
	}

	p2pNode1 := p2p.NewP2PNode(logger, configPeer1)
	err = p2pNode1.Start(ctx, blockTopicName, subtreeTopicName, bestBlockTopicName, miningOnTopicName, rejectedTxTopicName)
	if err != nil {
		logger.Errorf("Error starting P2P node 1: %v", err)
		return
	}
	time.Sleep(5 * time.Second)

	p2pNode2 := p2p.NewP2PNode(logger, configPeer2)
	err = p2pNode2.Start(ctx, blockTopicName, subtreeTopicName, bestBlockTopicName, miningOnTopicName, rejectedTxTopicName)
	if err != nil {
		logger.Errorf("Error starting P2P node 2: %v", err)
		return
	}
	time.Sleep(5 * time.Second)

	err = p2pNode1.SetTopicHandler(ctx, blockTopicName, func(ctx context.Context, data []byte, peerID string) {
		logger.Infof("Node 1 received data from peer %s: %s", peerID, string(data))
	})
	if err != nil {
		logger.Errorf("Error setting topic handler: %v", err)
		return
	}

	err = p2pNode2.SetTopicHandler(ctx, blockTopicName, func(ctx context.Context, data []byte, peerID string) {
		logger.Infof("Node 2 received data from peer %s: %s", peerID, string(data))
	})
	if err != nil {
		logger.Errorf("Error setting topic handler: %v", err)
		return
	}

	blockchainClient, err := blockchain.NewClient(ctx, logger, "cmd/testUtil/e2e")
	if err != nil {
		t.Errorf("Error creating blockchain client: %v", err)
		return
	}
	header, _, _ := blockchainClient.GetBestBlockHeader(ctx)

	n := model.Notification{
		Type:    model.NotificationType_Block,
		Hash:    header.Hash(),
		BaseURL: assetHTTPAddressURL.String(),
	}

	blockMessage := BlockMessage{
		Hash:       n.Hash.String(),
		DataHubUrl: assetHTTPAddressURL.String(),
		PeerId:     p2pNode1.HostID().String(),
	}

	msgBytes, err := json.Marshal(blockMessage)
	if err != nil {
		logger.Errorf("json mmarshal error: ", err)
	}
	err = p2pNode1.Publish(ctx, blockTopicName, []byte(msgBytes))
	if err != nil {
		t.Errorf("Error publishing to topic: %v", err)
	}

	err = p2pNode1.SendToPeer(ctx, p2pNode2.HostID(), []byte(msgBytes))
	if err != nil {
		t.Errorf("Error publishing to topic: %v", err)
	}

	time.Sleep(2 * time.Second)
}
