//go:build e2eTest

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	model "github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	p2p "github.com/bitcoin-sv/ubsv/util/p2p"
	"github.com/ordishs/gocore"
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
	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("p2p", ulogger.WithLevel(logLevelStr))

	assetHttpAddressURL, _, _ := gocore.Config().GetURL("asset_httpAddress")

	p2pIp, ok := gocore.Config().Get("p2p_ip")
	if !ok {
		panic("p2p_ip_prefix not set in config")
	}
	p2pPort, ok := gocore.Config().GetInt("p2p_port")
	if !ok {
		panic("p2p_port not set in config")
	}

	topicPrefix, ok := gocore.Config().Get("p2p_topic_prefix")
	if !ok {
		panic("p2p_topic_prefix not set in config")
	}
	btn, ok := gocore.Config().Get("p2p_block_topic")
	if !ok {
		panic("p2p_block_topic not set in config")
	}
	stn, ok := gocore.Config().Get("p2p_subtree_topic")
	if !ok {
		panic("p2p_subtree_topic not set in config")
	}
	bbtn, ok := gocore.Config().Get("p2p_bestblock_topic")
	if !ok {
		panic("p2p_bestblock_topic not set in config")
	}

	miningOntn, ok := gocore.Config().Get("p2p_mining_on_topic")
	if !ok {
		panic("p2p_mining_on_topic not set in config")
	}
	rtn, ok := gocore.Config().Get("p2p_rejected_tx_topic")
	if !ok {
		panic("p2p_rejected_tx_topic not set in config")
	}

	sharedKey, ok := gocore.Config().Get("p2p_shared_key")
	if !ok {
		panic(errors.New(errors.ERR_PROCESSING, "error getting p2p_shared_key"))
	}
	usePrivateDht := gocore.Config().GetBool("p2p_dht_use_private", false)
	optimiseRetries := gocore.Config().GetBool("p2p_optimise_retries", false)

	blockTopicName = fmt.Sprintf("%s-%s", topicPrefix, btn)
	subtreeTopicName = fmt.Sprintf("%s-%s", topicPrefix, stn)
	bestBlockTopicName = fmt.Sprintf("%s-%s", topicPrefix, bbtn)
	miningOnTopicName = fmt.Sprintf("%s-%s", topicPrefix, miningOntn)
	rejectedTxTopicName = fmt.Sprintf("%s-%s", topicPrefix, rtn)

	staticPeers, _ := gocore.Config().GetMulti("p2p_static_peers", "|")
	privateKey, _ := gocore.Config().Get("p2p_private_key")

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

	blockchainClient, err := blockchain.NewClient(ctx, logger)
	if err != nil {
		t.Errorf("Error creating blockchain client: %v", err)
		return
	}
	header, _, _ := blockchainClient.GetBestBlockHeader(ctx)

	n := model.Notification{
		Type:    model.NotificationType_Block,
		Hash:    header.Hash(),
		BaseURL: assetHttpAddressURL.String(),
	}

	blockMessage := BlockMessage{
		Hash:       n.Hash.String(),
		DataHubUrl: assetHttpAddressURL.String(),
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
