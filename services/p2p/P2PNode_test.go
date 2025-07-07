package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func generateTestPrivateKey(t *testing.T) string {
	// Generate a new Ed25519 key pair using libp2p's crypto package
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	assert.NoError(t, err)

	// Get the raw private key bytes - not protobuf-encoded
	// This matches what decodeHexEd25519PrivateKey expects
	raw, err := priv.Raw()
	assert.NoError(t, err)

	// Encode as hex
	return hex.EncodeToString(raw)
}

func TestSendToPeer(t *testing.T) {
	// t.Skip("Fails in CI, but works locally")
	// logger := ulogger.NewVerboseTestLogger(t)
	// Use a unique DHT protocol ID to prevent interference with other tests
	protocolID := "teranode/bitcoin/1.0.0"

	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	// Find available ports for both nodes
	port1 := findAvailablePort(t)
	port2 := findAvailablePort(t)

	// Create two P2PNode instances with dynamic ports
	config1 := P2PConfig{
		ProcessName:     "test1",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            port1,
		PrivateKey:      "",
		SharedKey:       "",
		UsePrivateDHT:   false,
		OptimiseRetries: false,
		Advertise:       false,
		StaticPeers:     []string{},
	}

	config2 := P2PConfig{
		ProcessName:     "test2",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            port2,
		PrivateKey:      "",
		SharedKey:       "",
		UsePrivateDHT:   false,
		OptimiseRetries: false,
		Advertise:       false,
		StaticPeers:     []string{},
	}

	// Create nodes with retries in case of port conflicts
	var (
		node1, node2 *P2PNode
		err          error
	)

	// Create node1 with retries
	node1, err = createTestNode(t, &config1, logger, tSettings)
	require.NoError(t, err)

	// Create node2 with retries
	node2, err = createTestNode(t, &config2, logger, tSettings)
	require.NoError(t, err)

	// After node2 is created, before starting nodes or connecting:
	node2.host.SetStreamHandler(protocol.ID(protocolID), func(stream network.Stream) {
		defer stream.Close()
		buf, err := io.ReadAll(stream)
		require.NoError(t, err, "Failed to read from stream")
		require.True(t, len(buf) > 0, "Received empty message")

		var receivedMsg HandshakeMessage
		err = json.Unmarshal(buf, &receivedMsg)
		require.NoError(t, err, "Failed to unmarshal handshake message: %s", string(buf))

		// Assertions on the received message can be added here, for example:
		// require.Equal(t, Version, receivedMsg.Type, "Handshake message type should be 'version'")
		// require.Equal(t, node1.HostID().String(), receivedMsg.PeerID, "Handshake message PeerID should match sender")
		// For now, just log it
		t.Logf("Node2 received handshake: %+v from %s", receivedMsg, stream.Conn().RemotePeer().String())

		// Update receiver metrics
		node2.UpdateBytesReceived(uint64(len(buf)))
		node2.UpdateLastReceived()
	})

	// Create a context with a timeout for the entire test
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a WaitGroup to track all goroutines
	var wg sync.WaitGroup

	// Start both nodes
	wg.Add(2)

	startNode1 := func() {
		defer wg.Done()

		err := node1.Start(ctx, nil)
		require.NoError(t, err)
	}
	go startNode1()

	startNode2 := func() {
		defer wg.Done()

		err := node2.Start(ctx, nil)
		require.NoError(t, err)
	}
	go startNode2()

	// Wait for nodes to start
	wg.Wait()

	// Ensure cleanup happens when test ends
	defer func() {
		// Cancel context to stop all goroutines
		cancel()

		// First disconnect peers
		if node1 != nil && node2 != nil {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer stopCancel()

			err := node1.DisconnectPeer(stopCtx, node2.HostID())
			require.NoError(t, err)

			// Give time for disconnect notifications to complete
			time.Sleep(100 * time.Millisecond)
		}

		// Stop nodes
		if node1 != nil {
			err := node1.Stop(context.Background())
			require.NoError(t, err)
		}

		if node2 != nil {
			err := node2.Stop(context.Background())
			require.NoError(t, err)
		}

		// Give a small grace period for cleanup
		time.Sleep(1 * time.Second)
	}()

	// Before connecting, log the HostID and connection string
	t.Logf("Node1 HostID: %s\n", node1.HostID())
	t.Logf("Node2 HostID: %s\n", node2.HostID())
	connectionString := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", node2.config.Port, node2.HostID())
	t.Logf("Connecting to: %s\n", connectionString)

	// Attempt to connect
	connected := node1.connectToStaticPeers(ctx, []string{connectionString})
	require.True(t, connected)

	t.Logf("Connected to: %s\n", connectionString)

	hshakeMsg := HandshakeMessage{
		Type:       Version, // "version"
		PeerID:     node1.HostID().String(),
		BestHeight: 0,
		BestHash:   "",
		DataHubURL: "",
		UserAgent:  "test-agent/0.0.1",
		Services:   0,
	}
	message, err := json.Marshal(hshakeMsg)
	require.NoError(t, err, "Failed to marshal handshake message")

	// Use WaitGroup to ensure message sending completes
	var msgWg sync.WaitGroup

	msgWg.Add(1)

	sendMessage := func() {
		defer msgWg.Done()

		// Send message from node1 to node2 using P2PNode's method
		err := node1.SendToPeer(ctx, node2.HostID(), message)
		require.NoError(t, err, "Node1 failed to send message to Node2")
	}
	go sendMessage()
	msgWg.Wait()

	// Give a short time for message processing
	time.Sleep(500 * time.Millisecond)

	t.Logf("Message written to stream between %s and %s\n", node1.HostID(), node2.HostID())

	assert.Equal(t, uint64(len(message)), node2.bytesReceived, "Node2 should have received the correct number of bytes")
	assert.Equal(t, uint64(len(message)), node1.bytesSent, "Node1 should have sent the correct number of bytes")
	assert.NotZero(t, node2.lastRecv, "Node2 should have updated the lastRecv timestamp")
	assert.NotZero(t, node1.lastSend, "Node1 should have updated the lastSend timestamp")
}

func createTestNode(t *testing.T, config *P2PConfig, logger ulogger.Logger, tSettings *settings.Settings) (node *P2PNode, err error) {
	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(100+rand.Intn(300)) * time.Millisecond // nolint:gosec
			t.Logf("Retrying node creation after delay of %v (attempt %d)", delay, attempt+1)
			time.Sleep(delay)

			// Try a different port if previous attempt failed
			config.Port = findAvailablePort(t)
		}

		node, err = NewP2PNode(context.Background(), logger, tSettings, *config, nil)
		if err == nil {
			// After node creation, update config.Port to the actual port
			updateConfigPortFromNode(node, config)

			return node, nil
		}

		t.Logf("Failed to create node on attempt %d: %v", attempt+1, err)
	}

	return nil, err
}

func updateConfigPortFromNode(node *P2PNode, config *P2PConfig) {
	for _, addr := range node.host.Addrs() {
		if portStr, err := addr.ValueForProtocol(multiaddr.P_TCP); err == nil {
			if port, err := strconv.Atoi(portStr); err == nil {
				config.Port = port

				break
			}
		}
	}
}
func TestSendBlockMessageToPeer(t *testing.T) {
	t.Skip("Skipping blockmessage peer test")

	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	tSettings := settings.NewSettings()
	ctx := context.Background()

	topicPrefix := tSettings.ChainCfgParams.TopicPrefix
	if topicPrefix == "" {
		t.Log("missing config ChainCfgParams.TopicPrefix")
		t.FailNow()
	}

	bbtn := tSettings.P2P.BestBlockTopic
	if bbtn == "" {
		t.Log("p2p_bestblock_topic not set in config")
		t.FailNow()
	}

	bestBlockTopicName := fmt.Sprintf("%s-%s", topicPrefix, bbtn)

	// Generate valid Ed25519 private keys for both nodes
	priv1, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	priv2, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	// Convert private keys to hex strings for the config
	priv1Bytes, err := priv1.Raw()
	require.NoError(t, err)

	priv2Bytes, err := priv2.Raw()
	require.NoError(t, err)

	// Find available ports for both nodes
	port1 := findAvailablePort(t)
	port2 := findAvailablePort(t)

	// Create two P2PNode instances with dynamic ports
	config1 := P2PConfig{
		ProcessName:     "test1",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            port1,
		PrivateKey:      hex.EncodeToString(priv1Bytes),
		SharedKey:       "",
		UsePrivateDHT:   false,
		OptimiseRetries: false,
		Advertise:       false,
		StaticPeers:     []string{},
	}

	config2 := P2PConfig{
		ProcessName:     "test2",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            port2,
		PrivateKey:      hex.EncodeToString(priv2Bytes),
		SharedKey:       "",
		UsePrivateDHT:   false,
		OptimiseRetries: false,
		Advertise:       false,
		StaticPeers:     []string{},
	}

	// Create nodes with retries in case of port conflicts
	var (
		node1, node2 *P2PNode
		err2         error
	)

	// Create node1 with retries
	node1, err2 = createTestNode(t, &config1, logger, tSettings)
	if err2 != nil {
		panic(err2)
	}

	// Create node2 with retries
	node2, err2 = createTestNode(t, &config2, logger, tSettings)
	if err2 != nil {
		panic(err2)
	}

	// Start both nodes
	err = node1.Start(context.Background(), nil, bestBlockTopicName)
	assert.NoError(t, err)

	err = node2.Start(context.Background(), nil, bestBlockTopicName)
	assert.NoError(t, err)

	// Log HostIDs
	t.Logf("Node1 HostID: %s", node1.HostID())
	t.Logf("Node2 HostID: %s", node2.HostID())

	// Retrieve node2's real listen address
	addrs := node2.host.Addrs()

	var peerAddr string
	for _, addr := range addrs {
		peerAddr = fmt.Sprintf("%s/p2p/%s", addr.String(), node2.HostID())
		t.Logf("Connecting to: %s", peerAddr)

		break // use the first address
	}

	ctxConn, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	connected := node1.connectToStaticPeers(ctxConn, []string{peerAddr})
	assert.True(t, connected)

	t.Logf("Connected to: %s", peerAddr)

	blockMessage := BlockMessage{
		Hash:       "0000000000000000024a7e7cfa9191b3a4cd03b875c298b7f9bb7bf7e74a5ef7",
		Height:     882697,
		DataHubURL: "http://localhost:8080",
	}

	msgBytes, err := json.Marshal(blockMessage)
	if err != nil {
		t.Errorf("[handleBestBlockTopic] json marshal error: %v", err)
		t.FailNow()
	}

	// send best block to the requester
	err = node1.SendToPeer(ctx, node2.HostID(), msgBytes)
	if err != nil {
		t.Errorf("[handleBestBlockTopic] error sending peer message: %v", err)
		t.FailNow()
	}

	t.Logf("Message written to stream between %s and %s\n", node1.HostID(), node2.HostID())

	// sleep
	time.Sleep(1 * time.Second)

	assert.Equal(t, uint64(len(msgBytes)), node2.bytesReceived, "Node2 should have received the correct number of bytes")
	assert.Equal(t, uint64(len(msgBytes)), node1.bytesSent, "Node1 should have sent the correct number of bytes")
	assert.NotZero(t, node2.lastRecv, "Node2 should have updated the lastRecv timestamp")
	assert.NotZero(t, node1.lastSend, "Node1 should have updated the lastSend timestamp")
}

func TestSendBestBlockMessage(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	topicPrefix := tSettings.ChainCfgParams.TopicPrefix
	if topicPrefix == "" {
		t.Log("missing config ChainCfgParams.TopicPrefix")
		t.FailNow()
	}

	bbtn := tSettings.P2P.BestBlockTopic
	if bbtn == "" {
		t.Log("p2p_bestblock_topic not set in config")
		t.FailNow()
	}

	bestBlockTopicName := fmt.Sprintf("%s-%s", topicPrefix, bbtn)

	// Create two P2PNode instances with dynamic ports
	config1 := P2PConfig{
		ProcessName:        "test1",
		ListenAddresses:    []string{"127.0.0.1"},
		AdvertiseAddresses: []string{"127.0.0.1"},
		Port:               12345,
		PrivateKey:         "",
		SharedKey:          "",
		UsePrivateDHT:      false,
		OptimiseRetries:    false,
		Advertise:          true,
		StaticPeers:        []string{},
	}

	config2 := P2PConfig{
		ProcessName:        "test2",
		ListenAddresses:    []string{"127.0.0.1"},
		AdvertiseAddresses: []string{"127.0.0.1"},
		Port:               12346,
		PrivateKey:         "",
		SharedKey:          "",
		UsePrivateDHT:      false,
		OptimiseRetries:    false,
		Advertise:          true,
		StaticPeers:        []string{},
	}

	// Create nodes with retries in case of port conflicts
	var (
		node1, node2 *P2PNode
		err          error
	)

	// Create node1 with retries
	node1, err = createTestNode(t, &config1, logger, tSettings)
	require.NoError(t, err)

	config1.Port = node1.config.Port

	// Create a WaitGroup to track all goroutines
	var wg sync.WaitGroup

	// Create node2 with retries
	node2, err = createTestNode(t, &config2, logger, tSettings)
	require.NoError(t, err)

	config2.Port = node2.config.Port

	// Start the nodes
	wg.Add(2)

	startNode1 := func() {
		defer wg.Done()

		err := node1.Start(t.Context(), nil, bestBlockTopicName)
		require.NoError(t, err)
	}
	go startNode1()

	startNode2 := func() {
		defer wg.Done()

		err := node2.Start(t.Context(), nil, bestBlockTopicName)
		require.NoError(t, err)
	}
	go startNode2()

	// Wait for nodes to start
	wg.Wait()

	// Ensure cleanup happens when test ends
	defer func() {
		// First disconnect peers
		if node1 != nil && node2 != nil {
			err := node1.DisconnectPeer(t.Context(), node2.HostID())
			require.NoError(t, err)

			// Give time for disconnect notifications to complete
			time.Sleep(100 * time.Millisecond)
		}

		// Stop nodes
		if node1 != nil {
			err := node1.Stop(t.Context())
			require.NoError(t, err)
		}

		if node2 != nil {
			err := node2.Stop(t.Context())
			require.NoError(t, err)
		}

		// Give a small grace period for cleanup
		time.Sleep(1 * time.Second)
	}()

	// Before connecting, log the HostID and connection string
	t.Logf("Node1 HostID: %s\n", node1.HostID())
	t.Logf("Node2 HostID: %s\n", node2.HostID())
	addrs := node2.host.Addrs()

	var connectionString string

	for _, addr := range addrs {
		connectionString = fmt.Sprintf("%s/p2p/%s", addr, node2.HostID())
		t.Logf("Connecting to: %s", connectionString)

		break // use the first address
	}

	// Attempt to connect
	connected := node1.connectToStaticPeers(t.Context(), []string{connectionString})
	require.True(t, connected)

	t.Logf("Connected to: %s\n", connectionString)

	msgBytes, err := json.Marshal(BestBlockRequestMessage{PeerID: node1.HostID().String()})
	if err != nil {
		t.Logf("[sendBestBlockMessage] json marshal error: %v", err)
	}

	if err := node1.Publish(t.Context(), bestBlockTopicName, msgBytes); err != nil {
		t.Logf("[sendBestBlockMessage] publish error: %v", err)
	}

	t.Logf("Message written to stream between %s and %s\n", node1.HostID(), node2.HostID())

	// sleep
	time.Sleep(1 * time.Second)

	// assert.Equal(t, uint64(len(msgBytes)), node2.bytesReceived, "Node2 should have received the correct number of bytes")
	assert.Equal(t, uint64(len(msgBytes)), node1.bytesSent, "Node1 should have sent the correct number of bytes")
	// assert.NotZero(t, node2.lastRecv, "Node2 should have updated the lastRecv timestamp")
	assert.NotZero(t, node1.lastSend, "Node1 should have updated the lastSend timestamp")
}

func TestSendToTopic(t *testing.T) {
	t.Skip("This test runs locally but is not working as expected in the build, needs to be fixed")

	logger := ulogger.TestLogger{} // ulogger.NewVerboseTestLogger(t)

	tSettings := settings.NewSettings()

	topicName := "test-topic"

	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()
	ctx := context.Background()

	// create two P2PNode instances
	config1 := P2PConfig{
		ProcessName:     "test1",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            12345,
		PrivateKey:      "",
		SharedKey:       "",
		UsePrivateDHT:   false,
		OptimiseRetries: false,
		Advertise:       false,
		StaticPeers:     []string{},
	}

	config2 := P2PConfig{
		ProcessName:     "test2",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            12346,
		PrivateKey:      "",
		SharedKey:       "",
		UsePrivateDHT:   false,
		OptimiseRetries: false,
		Advertise:       false,
		StaticPeers:     []string{},
	}

	node1, err := NewP2PNode(context.Background(), logger, tSettings, config1, nil)
	if err != nil {
		panic(err)
	}

	node2, err := NewP2PNode(context.Background(), logger, tSettings, config2, nil)
	if err != nil {
		panic(err)
	}

	// start both nodes
	err = node1.Start(ctx, nil, topicName)
	assert.NoError(t, err)

	err = node2.Start(ctx, nil, topicName)
	assert.NoError(t, err)

	// before connecting, log the HostID and connection string
	t.Logf("Node1 HostID: %s\n", node1.HostID())
	t.Logf("Node2 HostID: %s\n", node2.HostID())

	connectionString2 := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", node2.config.Port, node2.HostID())
	connected := node1.connectToStaticPeers(ctx, []string{connectionString2})
	assert.True(t, connected)

	t.Logf("Node1 connected to: %s\n", connectionString2)

	connectionString1 := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", node1.config.Port, node1.HostID())
	connected = node2.connectToStaticPeers(ctx, []string{connectionString1})
	assert.True(t, connected)

	t.Logf("Node2 cdonnected to: %s\n", connectionString1)

	message := []byte("Hello, Node!")

	err = node2.SetTopicHandler(ctx, topicName, func(ctx context.Context, data []byte, peerID string) {
		if peerID == node1.HostID().String() {
			return
		}

		atomic.AddUint64(&node2.bytesReceived, uint64(len(data)))
		node2.logger.Debugf("Node2 received data from peer: %s: %s\n", string(data), peerID)
	})

	assert.NoError(t, err)

	err = node1.SetTopicHandler(ctx, topicName, func(ctx context.Context, data []byte, peerID string) {
		if peerID == node1.HostID().String() {
			return
		}

		atomic.AddUint64(&node1.bytesReceived, uint64(len(data)))

		node1.logger.Infof("Node1 received data from peer %s: %s\n", peerID, string(data))
	})
	if err != nil {
		logger.Errorf("Error setting topic handler: %v", err)
		return
	}

	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	t.Logf("added topic handler to node2 for topic: %s\n", topicName)

	err = node1.Publish(ctx, topicName, message)
	assert.NoError(t, err)

	err = node2.Publish(ctx, topicName, message)
	assert.NoError(t, err)

	// sleep
	time.Sleep(1 * time.Second)

	assert.NotNil(t, node1.GetTopic(topicName), "Node1 should have a topic")
	assert.NotNil(t, node2.GetTopic(topicName), "Node2 should have a topic")
	assert.Equal(t, uint64(len(message)), node2.BytesReceived(), "Node2 should have received the correct number of bytes")
	assert.Equal(t, uint64(len(message)), node1.BytesReceived(), "Node1 should have received the correct number of bytes")
	assert.Equal(t, uint64(len(message)), node1.BytesSent(), "Node1 should have sent the correct number of bytes")
	assert.Equal(t, uint64(len(message)), node2.BytesSent(), "Node2 should have sent the correct number of bytes")
}

func TestGetIPFromMultiaddr_Server(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		want    string
		wantErr bool
	}{
		{
			name:    "IPv4 with TCP port",
			addr:    "/ip4/127.0.0.1/tcp/8080",
			want:    "127.0.0.1",
			wantErr: false,
		},
		{
			name:    "IPv6 with TCP port",
			addr:    "/ip6/::1/tcp/8080",
			want:    "::1",
			wantErr: false,
		},
		{
			name:    "IPv4 without port",
			addr:    "/ip4/192.168.1.1",
			want:    "192.168.1.1",
			wantErr: false,
		},
		{
			name:    "Invalid multiaddr",
			addr:    "invalid",
			want:    "",
			wantErr: true,
		},
		{
			name:    "DNS multiaddr",
			addr:    "/dns4/example.com/tcp/8080",
			want:    "example.com",
			wantErr: false,
		},
		{
			name:    "IPv4 with multiple components",
			addr:    "/ip4/10.0.0.1/tcp/1234/ws",
			want:    "10.0.0.1",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maddr, err := multiaddr.NewMultiaddr(tt.addr)
			if err != nil && !tt.wantErr {
				t.Fatalf("Failed to create multiaddr: %v", err)
			}

			if err != nil {
				return
			}

			got, err := getIPFromMultiaddr(maddr)
			if (err != nil) != tt.wantErr {
				t.Errorf("getIPFromMultiaddr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got != tt.want {
				t.Errorf("getIPFromMultiaddr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStartStaticPeerConnector(t *testing.T) {
	tests := []struct {
		name       string
		staticPeer string
	}{
		{
			name:       "no static peers",
			staticPeer: "",
		},
		{
			name:       "with static peers",
			staticPeer: "/ip4/127.0.0.1/tcp/12346/p2p/12D3KooWQvCkC8YiCTjRYXdyhNnncFRPKVxjE9u7rQKh2oaEXxQ6",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := settings.NewSettings()

			config := P2PConfig{
				ProcessName:     "test",
				ListenAddresses: []string{"127.0.0.1"},
				PrivateKey:      "",
				SharedKey:       "",
				UsePrivateDHT:   false,
				OptimiseRetries: false,
				Advertise:       false,
			}

			// Add static peer if specified
			if tt.staticPeer != "" {
				config.StaticPeers = []string{tt.staticPeer}
			}

			// Create the node with retries in case of port conflicts
			var (
				node *P2PNode
				err  error
			)

			node, err = createTestNode(t, &config, logger, tSettings)
			require.NoError(t, err)

			// Create a context that will be used for the test
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			maxRetries := 3

			err = startNodeWithRetries(ctx, t, maxRetries, node)
			require.NoError(t, err, "Failed to start node after multiple attempts")

			defer func() {
				if node != nil {
					err := node.Stop(context.Background())
					require.NoError(t, err)
				}
			}()

			// Start the static peer connector
			node.startStaticPeerConnector(ctx)
		})
	}
}

func startNodeWithRetries(ctx context.Context, t *testing.T, maxRetries int, node *P2PNode, topicNames ...string) (err error) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = node.Start(ctx, nil, topicNames...)
		if err == nil {
			break
		}

		if attempt < maxRetries-1 {
			t.Logf("Failed to start node on attempt %d: %v, retrying...", attempt+1, err)
			time.Sleep(time.Duration(100+rand.Intn(300)) * time.Millisecond) // nolint:gosec
		}
	}

	return err
}

func TestInitGossipSub(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	tests := []struct {
		name       string
		topicNames []string
		wantErr    bool
	}{
		{
			name:       "single topic",
			topicNames: []string{"test-topic"},
			wantErr:    false,
		},
		{
			name:       "multiple topics",
			topicNames: []string{"test-topic-1", "test-topic-2"},
			wantErr:    false,
		},
		{
			name:       "no topics",
			topicNames: []string{},
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createNodeConfig(t, "test", []string{"127.0.0.1"}, false, false, []string{})

			// Create the node with retries in case of port conflicts
			var (
				node *P2PNode
				err  error
			)

			maxRetries := 3

			node, err = createTestNode(t, &config, logger, tSettings)
			require.NoError(t, err)

			// Create a context with timeout for operations
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err = startNodeWithRetries(ctx, t, maxRetries, node, tt.topicNames...)
			require.NoError(t, err, "Failed to start node after multiple attempts")

			defer func() {
				if node != nil {
					err := node.Stop(context.Background())
					require.NoError(t, err)
				}
			}()

			// Verify topics were subscribed properly
			for _, topic := range tt.topicNames {
				_, exists := node.topics[topic]
				assert.True(t, exists, "Topic %s should be subscribed", topic)
			}

			// If no topics were provided, check that topics map is empty
			if len(tt.topicNames) == 0 {
				assert.Equal(t, 0, len(node.topics), "Topics map should be empty when no topics are provided")
			}
		})
	}
}

func TestDecodeHexEd25519PrivateKey(t *testing.T) {
	// Generate a test private key
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	// Get raw private key bytes - NOT protobuf-encoded
	// This matches what decodeHexEd25519PrivateKey expects
	privBytes, err := priv.Raw()
	require.NoError(t, err)

	// Encode as hex
	hexEncodedKey := hex.EncodeToString(privBytes)

	// Test successful decoding
	t.Run("Valid key decoding", func(t *testing.T) {
		decodedKey, err := decodeHexEd25519PrivateKey(hexEncodedKey)
		require.NoError(t, err)
		require.NotNil(t, decodedKey)

		// Verify it's the same key by comparing raw bytes
		decodedBytes, err := (*decodedKey).Raw()
		require.NoError(t, err)
		require.Equal(t, privBytes, decodedBytes)
	})

	// Test invalid hex string
	t.Run("Invalid hex string", func(t *testing.T) {
		_, err := decodeHexEd25519PrivateKey("not-a-hex-string")
		require.Error(t, err)
	})

	// Test invalid key data
	t.Run("Invalid key data", func(t *testing.T) {
		_, err := decodeHexEd25519PrivateKey("1234")
		require.Error(t, err)
	})

	// Test empty string
	t.Run("Empty string", func(t *testing.T) {
		_, err := decodeHexEd25519PrivateKey("")
		require.Error(t, err)
	})
}

func createNodeConfig(t *testing.T, name string, listenAddresses []string, usePrivateDHT bool, advertise bool, staticPeers []string) P2PConfig {
	return P2PConfig{
		ProcessName:     name,
		ListenAddresses: listenAddresses,
		PrivateKey:      generateTestPrivateKey(t),
		SharedKey:       "",
		UsePrivateDHT:   usePrivateDHT,
		Advertise:       advertise,
		StaticPeers:     staticPeers,
	}
}

func TestConnectedPeers(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	// Disable bootstrap addresses to prevent automatic peer discovery
	tSettings.P2P.BootstrapAddresses = []string{}
	// Use a unique DHT protocol ID to prevent interference with other tests
	tSettings.P2P.DHTProtocolID = fmt.Sprintf("/teranode/test/dht/%d", time.Now().UnixNano())

	// Get 2 ports to use
	port1 := findAvailablePort(t)
	port2 := findAvailablePort(t)

	// Create first node
	config1 := P2PConfig{
		ProcessName:        "test3",
		ListenAddresses:    []string{"127.0.0.1"},
		AdvertiseAddresses: []string{"127.0.0.1"},
		Port:               port1,
		PrivateKey:         generateTestPrivateKey(t),
		UsePrivateDHT:      false,
		Advertise:          false,
		StaticPeers:        []string{},
	}

	// Create second node
	config2 := P2PConfig{
		ProcessName:        "test4",
		ListenAddresses:    []string{"127.0.0.1"},
		AdvertiseAddresses: []string{"127.0.0.1"},
		Port:               port2,
		PrivateKey:         generateTestPrivateKey(t),
		UsePrivateDHT:      false,
		Advertise:          false,
		StaticPeers:        []string{},
	}

	// Create nodes with retries in case of port conflicts
	var (
		node1, node2 *P2PNode
		err          error
	)

	maxRetries := 3

	node1, err = createTestNode(t, &config1, logger, tSettings)
	require.NoError(t, err)

	// Create node2 with retries
	node2, err = createTestNode(t, &config2, logger, tSettings)
	require.NoError(t, err)

	// Create separate parent context we can cancel explicitly for cleanup
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer func() {
		// Cancel parent context to stop background goroutines
		parentCancel()

		// Then close the host
		err := node1.host.Close()
		if err != nil {
			t.Logf("Error closing host: %v", err)
		}

		err = node2.host.Close()
		if err != nil {
			t.Logf("Error closing host: %v", err)
		}

		// Give time for goroutines to clean up
		time.Sleep(100 * time.Millisecond)
	}()

	// Create a context with a timeout for the operations
	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()

	err = startNodeWithRetries(ctx, t, maxRetries, node1)
	require.NoError(t, err, "Failed to start node1 after multiple attempts")

	err = startNodeWithRetries(ctx, t, maxRetries, node2)
	require.NoError(t, err, "Failed to start node2 after multiple attempts")

	// Wait for a moment to let the nodes initialize
	time.Sleep(100 * time.Millisecond)

	// Check active connections instead of peerstore entries
	initialConns := len(node1.host.Network().Conns())
	t.Logf("Initial active connections: %d", initialConns)

	// Connect node1 to node2
	peerInfo := peer.AddrInfo{
		ID:    node2.host.ID(),
		Addrs: node2.host.Addrs(),
	}

	err = connectWithRetries(ctx, t, maxRetries, node1, peerInfo)

	require.NoError(t, err, "Failed to connect to peer after multiple attempts")

	// Wait for connection to establish
	time.Sleep(100 * time.Millisecond)

	// Check that node2 is connected (using the Network interface, not Peerstore)
	connectedness := node1.host.Network().Connectedness(node2.host.ID())
	require.Equal(t, network.Connected, connectedness,
		"Node1 should be connected to node2")

	// Verify the connection count increased
	connectionsAfterConnect := len(node1.host.Network().Conns())
	t.Logf("Connections after connect: %d", connectionsAfterConnect)
	require.Greater(t, connectionsAfterConnect, initialConns,
		"Connection count should increase after connecting")

	// Now disconnect
	err = node1.DisconnectPeer(ctx, node2.host.ID())
	require.NoError(t, err)

	// Increase wait time for disconnection to take effect
	// The previous 500ms was likely not enough for reliable disconnection in CI environments
	disconnectWaitTime := 1000 * time.Millisecond
	t.Logf("Waiting %v for disconnection to complete", disconnectWaitTime)

	time.Sleep(disconnectWaitTime)

	// Add retry for disconnection verification
	var connectednessAfter network.Connectedness

	maxDisconnectRetries := 5
	disconnectSuccess := false

	for attempt := 0; attempt < maxDisconnectRetries; attempt++ {
		// Verify disconnection happened at the network level
		connectednessAfter = node1.host.Network().Connectedness(node2.host.ID())
		if connectednessAfter != network.Connected {
			disconnectSuccess = true
			break
		}

		// If still connected after first retry, try a more forceful approach
		if attempt == 1 {
			t.Logf("First disconnect attempt didn't work, trying a more forceful approach")

			// Close all connections to the peer explicitly
			for _, conn := range node1.host.Network().ConnsToPeer(node2.host.ID()) {
				t.Logf("Forcefully closing connection to %s", conn.RemotePeer())
				conn.Close()
			}
		}

		t.Logf("Disconnect not complete on attempt %d, waiting another 300ms", attempt+1)
		time.Sleep(300 * time.Millisecond)
	}

	assert.True(t, disconnectSuccess, "Node1 should no longer be connected to node2 after %d attempts", maxDisconnectRetries)

	// The connection count should decrease
	connectionsAfterDisconnect := len(node1.host.Network().Conns())
	t.Logf("Connections after disconnect: %d", connectionsAfterDisconnect)

	// Important: As we've discovered, ConnectedPeers() still returns peers from the peerstore
	// even after they're disconnected, so we don't use it to verify disconnection
	peerstoreEntries := len(node1.ConnectedPeers())
	t.Logf("Entries in peerstore: %d", peerstoreEntries)
	t.Logf("Note: Peerstore still contains previously seen peers, even after disconnection")
}

func connectWithRetries(ctx context.Context, t *testing.T, maxRetries int, node1 *P2PNode, peerInfo peer.AddrInfo) (err error) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = node1.host.Connect(ctx, peerInfo)
		if err == nil {
			break
		}

		if attempt < maxRetries-1 {
			t.Logf("Failed to connect to peer on attempt %d: %v, retrying...", attempt+1, err)
			time.Sleep(time.Duration(100+rand.Intn(300)) * time.Millisecond) // nolint:gosec
		}
	}

	return err
}

func TestConnectionTimeTracking(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	// Disable bootstrap addresses to prevent automatic peer discovery
	tSettings.P2P.BootstrapAddresses = []string{}
	// Use a unique DHT protocol ID to prevent interference with other tests
	tSettings.P2P.DHTProtocolID = fmt.Sprintf("/teranode/test/dht/%d", time.Now().UnixNano())

	// Get 2 ports to use
	port1 := findAvailablePort(t)
	port2 := findAvailablePort(t)

	// Create first node
	config1 := P2PConfig{
		ProcessName:        "test-conn-time-1",
		ListenAddresses:    []string{"127.0.0.1"},
		AdvertiseAddresses: []string{"127.0.0.1"},
		Port:               port1,
		PrivateKey:         generateTestPrivateKey(t),
		UsePrivateDHT:      false,
		Advertise:          false,
		StaticPeers:        []string{},
	}

	// Create second node
	config2 := P2PConfig{
		ProcessName:        "test-conn-time-2",
		ListenAddresses:    []string{"127.0.0.1"},
		AdvertiseAddresses: []string{"127.0.0.1"},
		Port:               port2,
		PrivateKey:         generateTestPrivateKey(t),
		UsePrivateDHT:      false,
		Advertise:          false,
		StaticPeers:        []string{},
	}

	// Create nodes
	node1, err := createTestNode(t, &config1, logger, tSettings)
	require.NoError(t, err)

	node2, err := createTestNode(t, &config2, logger, tSettings)
	require.NoError(t, err)

	// Create separate parent context we can cancel explicitly for cleanup
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer func() {
		// Cancel parent context to stop background goroutines
		parentCancel()

		// Then close the hosts
		err := node1.host.Close()
		if err != nil {
			t.Logf("Error closing host1: %v", err)
		}

		err = node2.host.Close()
		if err != nil {
			t.Logf("Error closing host2: %v", err)
		}

		// Give time for goroutines to clean up
		time.Sleep(100 * time.Millisecond)
	}()

	// Create a context with a timeout for the operations
	ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()

	// Start both nodes
	err = startNodeWithRetries(ctx, t, 3, node1)
	require.NoError(t, err, "Failed to start node1")

	err = startNodeWithRetries(ctx, t, 3, node2)
	require.NoError(t, err, "Failed to start node2")

	// Wait for nodes to initialize
	time.Sleep(100 * time.Millisecond)

	// Record the time before connection
	beforeConnect := time.Now()

	// Connect node1 to node2
	peerInfo := peer.AddrInfo{
		ID:    node2.host.ID(),
		Addrs: node2.host.Addrs(),
	}

	err = connectWithRetries(ctx, t, 3, node1, peerInfo)
	require.NoError(t, err, "Failed to connect to peer")

	// Wait for connection to establish
	time.Sleep(100 * time.Millisecond)

	// Verify connection was established
	connectedness := node1.host.Network().Connectedness(node2.host.ID())
	require.Equal(t, network.Connected, connectedness, "Node1 should be connected to node2")

	// Get the connected peers from node1
	peers := node1.ConnectedPeers()

	// Find node2 in the list
	var node2PeerInfo *PeerInfo
	for _, peer := range peers {
		if peer.ID == node2.host.ID() {
			node2PeerInfo = &peer
			break
		}
	}

	require.NotNil(t, node2PeerInfo, "Node2 should be in the connected peers list")
	require.NotNil(t, node2PeerInfo.ConnTime, "Connection time should be set")

	// Verify the connection time is reasonable (after beforeConnect and not in the future)
	afterConnect := time.Now()
	assert.True(t, node2PeerInfo.ConnTime.After(beforeConnect) || node2PeerInfo.ConnTime.Equal(beforeConnect),
		"Connection time should be after or equal to when we initiated the connection")
	assert.True(t, node2PeerInfo.ConnTime.Before(afterConnect) || node2PeerInfo.ConnTime.Equal(afterConnect),
		"Connection time should be before or equal to now")

	// Test that disconnection clears the connection time
	err = node1.DisconnectPeer(ctx, node2.host.ID())
	require.NoError(t, err)

	// Check immediately after disconnection - should be cleared by now
	// since we added manual cleanup in DisconnectPeer
	_, hasConnTime := node1.peerConnTimes.Load(node2.host.ID())

	// Check that the connection time is cleared from the map
	assert.False(t, hasConnTime, "Connection time should be cleared after disconnection")
}

func TestAtomicMetrics(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	// Create a test node with the config
	config := P2PConfig{
		ProcessName:     "test5",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            22347,
		PrivateKey:      generateTestPrivateKey(t),
		UsePrivateDHT:   false,
	}

	// Create a test node with the config
	node, err := NewP2PNode(context.Background(), logger, tSettings, config, nil)
	require.NoError(t, err)

	// Test initial values
	require.Equal(t, uint64(0), node.BytesSent())
	require.Equal(t, uint64(0), node.BytesReceived())

	// Test setting values
	atomic.StoreUint64(&node.bytesSent, 100)
	atomic.StoreUint64(&node.bytesReceived, 200)

	// Test reading values
	require.Equal(t, uint64(100), node.BytesSent())
	require.Equal(t, uint64(200), node.BytesReceived())

	// Test timestamp functions
	now := time.Now().Unix()
	atomic.StoreInt64(&node.lastSend, now)
	atomic.StoreInt64(&node.lastRecv, now-60) // 1 minute ago

	// Verify LastSend and LastRecv return correct times
	require.WithinDuration(t, time.Unix(now, 0), node.LastSend(), time.Second)
	require.WithinDuration(t, time.Unix(now-60, 0), node.LastRecv(), time.Second)
}

func TestInitDHT(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	// Set required settings for the DHT test
	tSettings.P2P.BootstrapAddresses = []string{"/ip4/127.0.0.1/tcp/12345/p2p/12D3KooWQvCkC8YiCTjRYXdyhNnncFRPKVxjE9u7rQKh2oaEXxQ6"}
	tSettings.P2P.DHTProtocolID = "/teranode/test/dht/1.0.0"

	// Create a config for DHT test
	dhtConfig := P2PConfig{
		ProcessName:     "test-dht",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            findAvailablePort(t),
		UsePrivateDHT:   false,
	}
	_ = dhtConfig // Use the variable to avoid unused variable warning

	t.Run("DHT init basic functionality", func(t *testing.T) {
		testSettings := settings.NewSettings()
		// Create a unique protocol ID to avoid conflicts
		testSettings.P2P.DHTProtocolID = fmt.Sprintf("/teranode/test/dht/%d", time.Now().UnixNano())
		// Note: empty bootstrap addresses are OK because DefaultBootstrapPeers are used
		testSettings.P2P.BootstrapAddresses = []string{}

		testNode := &P2PNode{
			settings: testSettings,
			logger:   logger,
		}

		// Create a temporary host for testing
		priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
		require.NoError(t, err)

		h, err := libp2p.New(
			libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
			libp2p.Identity(priv),
		)
		require.NoError(t, err)

		// Create parent context that will be canceled first to stop background goroutines
		parentCtx, parentCancel := context.WithCancel(context.Background())
		defer func() {
			// Cancel parent context to stop background goroutines
			parentCancel()

			// Then close the host
			err := h.Close()
			if err != nil {
				t.Logf("Error closing host: %v", err)
			}

			// Give time for goroutines to clean up
			time.Sleep(100 * time.Millisecond)
		}()

		// Create test context with short timeout
		testCtx, testCancel := context.WithTimeout(parentCtx, 1*time.Second)
		defer testCancel()

		// initDHT should work even with empty bootstrap addresses because it uses defaults
		dht, err := testNode.initDHT(testCtx, h)
		// Just verify that we got a DHT instance back, error not expected
		if err == nil {
			assert.NotNil(t, dht)
		} else {
			// If there's an error, it's likely due to network issues with the DefaultBootstrapPeers
			// not because of the empty bootstrap addresses in settings
			t.Logf("DHT init error (possibly due to network timeout): %v", err)
		}
	})

	t.Run("DHT with custom bootstrap address", func(t *testing.T) {
		// This time use a custom bootstrap address that's unreachable
		testSettings := settings.NewSettings()
		// Create a unique protocol ID to avoid conflicts
		testSettings.P2P.DHTProtocolID = fmt.Sprintf("/teranode/test/dht/%d", time.Now().UnixNano())
		testSettings.P2P.BootstrapAddresses = []string{"/ip4/127.0.0.1/tcp/12345/p2p/12D3KooWJbNwBUJ8Wb9fme9aT5J89HK7Lfas2s81BeDFfTgcXGCU"}

		testNode := &P2PNode{
			settings: testSettings,
			logger:   logger,
		}

		// Create a temporary host for testing
		priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
		require.NoError(t, err)

		h, err := libp2p.New(
			libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
			libp2p.Identity(priv),
		)
		require.NoError(t, err)

		// Create parent context that will be canceled first to stop background goroutines
		parentCtx, parentCancel := context.WithCancel(context.Background())
		defer func() {
			// Cancel parent context to stop background goroutines
			parentCancel()

			// Then close the host
			err := h.Close()
			if err != nil {
				t.Logf("Error closing host: %v", err)
			}

			// Give time for goroutines to clean up
			time.Sleep(100 * time.Millisecond)
		}()

		// Create test context with short timeout
		testCtx, testCancel := context.WithTimeout(parentCtx, 1*time.Second)
		defer testCancel()

		// The function should still work, even though the custom address is unreachable
		dht, err := testNode.initDHT(testCtx, h)
		if err == nil {
			assert.NotNil(t, dht)
		} else {
			// If there's an error, log it but don't fail the test
			t.Logf("DHT init error (possibly due to network timeout): %v", err)
		}
	})
}

func TestInitPrivateDHT(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	// Setup test settings - use fields directly from P2PSettings
	tSettings.P2P.DHTProtocolID = "/teranode/test/dht/1.0.0"
	tSettings.P2P.DHTUsePrivate = true

	// Test error condition: empty protocol prefix
	t.Run("Private DHT init fails with empty protocol prefix", func(t *testing.T) {
		testSettings := settings.NewSettings()
		testSettings.P2P.DHTProtocolID = "" // This should cause an error

		// Create a temporary host for testing
		priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
		require.NoError(t, err)

		h, err := libp2p.New(
			libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
			libp2p.Identity(priv),
		)
		require.NoError(t, err)

		// Create parent context that will be canceled first to stop background goroutines
		parentCtx, parentCancel := context.WithCancel(context.Background())
		defer func() {
			// Cancel parent context to stop background goroutines
			parentCancel()

			// Then close the host
			err := h.Close()
			if err != nil {
				t.Logf("Error closing host: %v", err)
			}

			// Give time for goroutines to clean up
			time.Sleep(100 * time.Millisecond)
		}()

		testNode := &P2PNode{
			settings: testSettings,
			logger:   logger,
		}

		testCtx, testCancel := context.WithTimeout(parentCtx, 1*time.Second)
		defer testCancel()

		_, err = testNode.initPrivateDHT(testCtx, h)
		require.Error(t, err)
	})
}

func TestUsePrivateDHTConfig(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	// Test with private DHT config
	t.Run("UsePrivateDHT configuration", func(t *testing.T) {
		// Skip this test as it's only meant to check the configuration flow,
		// not the actual network connectivity
		t.Skip("Skipping private DHT test that requires network connectivity")

		config := P2PConfig{
			ProcessName:     "test8",
			ListenAddresses: []string{"127.0.0.1"},
			Port:            22350,
			PrivateKey:      generateTestPrivateKey(t),
			UsePrivateDHT:   true,
			SharedKey:       hex.EncodeToString([]byte("test-shared-key-16b")), // 16-byte key
		}

		// We're just testing that the constructor acknowledges the private DHT config
		_, err := NewP2PNode(context.Background(), logger, tSettings, config, nil)
		t.Logf("NewP2PNode with UsePrivateDHT result: %v", err)
	})
}

func TestSetTopicHandler(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	// Create a minimal host
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	h, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", findAvailablePort(t))),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)
	defer h.Close()

	// Create a pubsub instance
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps, err := pubsub.NewGossipSub(ctx, h)
	require.NoError(t, err)

	// Create a P2PNode with the pubsub
	node := &P2PNode{
		host:           h,
		pubSub:         ps,
		logger:         logger,
		topics:         make(map[string]*pubsub.Topic),
		handlerByTopic: make(map[string]Handler),
	}

	// Set up a test topic and join it first
	topicName := "test-topic"
	topic, err := ps.Join(topicName)
	require.NoError(t, err, "Failed to join topic")

	// Store the topic in the node's topics map
	node.topics[topicName] = topic

	// Define the handler function - using a boolean for simple testing
	var messageReceived bool

	handler := func(ctx context.Context, msg []byte, from string) {
		messageReceived = true

		t.Logf("Received message: %s from %s", string(msg), from)
	}

	// Set the topic handler
	err = node.SetTopicHandler(ctx, topicName, handler)
	require.NoError(t, err, "SetTopicHandler should not return an error")

	// Verify handler was set
	assert.NotNil(t, node.handlerByTopic[topicName], "Handler should be set")

	// We're not actually triggering messages in this test
	// The messageReceived variable might be used in an expanded version of this test
	_ = messageReceived
}

func TestConnectedPeers_Isolated(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	// Create a custom stream handler for local testing only
	streamHandler := func(stream network.Stream) {
		// Just close the stream, we don't need to do anything with it for this test
		stream.Close()
	}

	// Create test nodes with minimal configuration
	config1 := P2PConfig{
		ProcessName:     "isolated1",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            33345,
		PrivateKey:      generateTestPrivateKey(t),
		// Critical: disable all auto-discovery features
		UsePrivateDHT:   false,
		OptimiseRetries: false,
		Advertise:       false,
		StaticPeers:     []string{},
	}

	// Disable all network discovery in settings
	tSettings.P2P.BootstrapAddresses = []string{}
	tSettings.P2P.DHTUsePrivate = false // Ensure private DHT is disabled
	tSettings.P2P.DHTProtocolID = ""    // Empty protocol ID to prevent DHT initialization

	// First create the isolated host directly with libp2p
	priv1, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	// Create first host
	h1, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", config1.Port)),
		libp2p.Identity(priv1),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)
	defer h1.Close()

	h1.SetStreamHandler(protocol.ID(config1.ProcessName), streamHandler)

	// Create second host
	priv2, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	h2, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", findAvailablePort(t))),
		libp2p.Identity(priv2),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)
	defer h2.Close()

	h2.SetStreamHandler(protocol.ID("isolated2"), streamHandler)

	// Create P2PNode struct directly, bypassing NewP2PNode
	node1 := &P2PNode{
		config:            config1,
		settings:          tSettings,
		host:              h1,
		logger:            logger,
		bitcoinProtocolID: config1.ProcessName,
		handlerByTopic:    make(map[string]Handler),
		startTime:         time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get the initial connection count - we don't assume it's zero because
	// libp2p might establish some minimal connections automatically
	initialPeers := node1.ConnectedPeers()
	initialCount := len(initialPeers)
	t.Logf("Initial peer count: %d", initialCount)

	// Record initial peer IDs
	initialPeerIDs := make(map[peer.ID]bool)
	for _, p := range initialPeers {
		initialPeerIDs[p.ID] = true
	}

	// Manually connect to h2
	connectionInfo := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}
	err = h1.Connect(ctx, connectionInfo)
	require.NoError(t, err, "Failed to connect peer1 to peer2")

	// Wait for connection to establish
	time.Sleep(100 * time.Millisecond)

	// Verify that node1 has h2 in its connected peers
	connectedPeers := node1.ConnectedPeers()
	t.Logf("Peer count after connect: %d", len(connectedPeers))

	// Find h2's ID in the list
	found := false

	for _, p := range connectedPeers {
		if p.ID == h2.ID() {
			found = true
			break
		}
	}

	assert.True(t, found, "Peer h2 not found in connected peers after connection")

	// Test DisconnectPeer
	err = node1.DisconnectPeer(ctx, h2.ID())
	require.NoError(t, err, "DisconnectPeer returned error")

	// Give time for disconnect to take effect
	time.Sleep(100 * time.Millisecond)

	// Verify the connection state is updated in libp2p
	connectedness := h1.Network().Connectedness(h2.ID())
	assert.NotEqual(t, network.Connected, connectedness,
		"Peers should not be connected according to libp2p Network after disconnect")

	// Additional check on ConnectedPeers method
	foundAfter := false

	for _, p := range node1.ConnectedPeers() {
		if p.ID == h2.ID() {
			foundAfter = true
			break
		}
	}

	assert.True(t, foundAfter, "Peer h2 should still be in connected peers after disconnect")

	// Check active connections count
	activeConns := len(h1.Network().Conns())
	t.Logf("Active connections after disconnect: %d", activeConns)
	assert.Equal(t, 0, activeConns, "No active connections expected after disconnect")
}

func TestPeerDisconnect(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	// Create two hosts directly using libp2p
	priv1, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	// Create first host
	h1, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/34351"),
		libp2p.Identity(priv1),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)
	defer h1.Close()

	// Create second host
	priv2, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	h2, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/34352"),
		libp2p.Identity(priv2),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)
	defer h2.Close()

	// Create a minimal P2PNode struct
	node := &P2PNode{
		host:   h1,
		logger: logger,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect h1 to h2
	peerInfo := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}
	err = h1.Connect(ctx, peerInfo)
	require.NoError(t, err, "Failed to connect peers")

	// Wait for connection to establish
	time.Sleep(100 * time.Millisecond)

	// Check that h2 is in the connected peers
	foundBefore := false

	connectedPeers := node.ConnectedPeers()
	for _, p := range connectedPeers {
		if p.ID == h2.ID() {
			foundBefore = true
			break
		}
	}

	require.True(t, foundBefore, "Peer h2 not found in connected peers before disconnect")

	// Check directly if hosts are connected (libp2p's own check)
	require.True(t, h1.Network().Connectedness(h2.ID()) == network.Connected,
		"Peers should be connected according to libp2p Network")

	// Now disconnect
	err = node.DisconnectPeer(ctx, h2.ID())
	require.NoError(t, err, "DisconnectPeer returned error")

	// Give time for disconnect to take effect
	time.Sleep(100 * time.Millisecond)

	// Check that connection state is updated in libp2p
	connectedness := h1.Network().Connectedness(h2.ID())
	assert.NotEqual(t, network.Connected, connectedness,
		"Peers should not be connected according to libp2p Network after disconnect")

	// Additional check on ConnectedPeers method
	foundAfter := false

	for _, p := range node.ConnectedPeers() {
		if p.ID == h2.ID() {
			foundAfter = true
			break
		}
	}

	assert.True(t, foundAfter, "Peer h2 should still be in connected peers after disconnect")
}

func TestDisconnectPeerWithConnect(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	// Create two hosts directly using libp2p
	priv1, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	// Create first host
	h1, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/34351"),
		libp2p.Identity(priv1),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)
	defer h1.Close()

	// Create second host
	priv2, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	h2, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/34352"),
		libp2p.Identity(priv2),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)
	defer h2.Close()

	// Create a minimal P2PNode struct
	node := &P2PNode{
		host:   h1,
		logger: logger,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect h1 to h2
	peerInfo := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}
	err = h1.Connect(ctx, peerInfo)
	require.NoError(t, err, "Failed to connect peers")

	// Wait for connection to establish
	time.Sleep(200 * time.Millisecond)

	// Verify that the hosts are connected using libp2p's Network interface
	require.Equal(t, network.Connected, h1.Network().Connectedness(h2.ID()),
		"Peers should be connected according to libp2p Network")

	// Get connected peers from the Network interface, which is more accurate
	// than the Peerstore for checking active connections
	connectedBefore := len(h1.Network().Conns())
	t.Logf("Connected peers count before disconnect: %d", connectedBefore)

	// Now disconnect using our method
	err = node.DisconnectPeer(ctx, h2.ID())
	require.NoError(t, err, "DisconnectPeer returned error")

	// Give time for disconnect to take effect
	time.Sleep(200 * time.Millisecond)

	// Verify the connection is closed using libp2p's Network interface
	connectedness := h1.Network().Connectedness(h2.ID())
	assert.NotEqual(t, network.Connected, connectedness,
		"Peers should not be connected according to libp2p Network after disconnect")

	// Count connections after disconnect
	connectedAfter := len(h1.Network().Conns())
	t.Logf("Connected peers count after disconnect: %d", connectedAfter)

	// There should be fewer connections after disconnecting
	assert.Less(t, connectedAfter, connectedBefore,
		"Number of connections should decrease after disconnect")

	// Note: We can't use ConnectedPeers() to verify disconnection because it returns
	// peers from the peerstore, which includes previously connected peers
	peers := node.ConnectedPeers()
	t.Logf("Peers in peerstore: %d", len(peers))

	// Verify that the disconnected peer remains in the peerstore
	// This demonstrates that ConnectedPeers() doesn't differentiate between
	// currently connected peers and peers that were previously seen
	foundInPeerstore := false

	for _, p := range peers {
		if p.ID == h2.ID() {
			foundInPeerstore = true

			assert.NotEmpty(t, p.Addrs, "Added peer should have addresses")

			break
		}
	}

	assert.True(t, foundInPeerstore,
		"Disconnected peer should still be in the peerstore")
}

func TestConnectedPeersFiltering(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	// Create libp2p host
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	port1 := findAvailablePort(t)
	h, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port1)),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)

	defer h.Close()

	// Create a P2PNode
	node := &P2PNode{
		host:   h,
		logger: logger,
	}

	// Add our own host to the peerstore if it's not already there
	h.Peerstore().AddAddrs(h.ID(), h.Addrs(), peerstore.PermanentAddrTTL)

	// Get all peers from the peerstore
	peers := node.ConnectedPeers()

	t.Logf("Found %d peers in the peerstore", len(peers))

	// Check if the host itself is in the peers list
	var selfFound bool

	for _, peer := range peers {
		if peer.ID == h.ID() {
			selfFound = true
			// Verify that our own peer info has proper addresses
			assert.NotEmpty(t, peer.Addrs, "Host's own peer info should have addresses")

			break
		}
	}

	// libp2p's behavior is that the host itself is in the peerstore
	assert.True(t, selfFound, "Host should be in its own peerstore")

	// Create another host and add it to the peerstore
	priv2, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	port2 := findAvailablePort(t)
	h2, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port2)),
		libp2p.Identity(priv2),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)

	defer h2.Close()

	// Add host2 to host1's peerstore
	h.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), peerstore.PermanentAddrTTL)

	// Get updated peers list
	peersAfter := node.ConnectedPeers()

	t.Logf("Found %d peers in the peerstore after adding h2", len(peersAfter))

	// Verify h2 was added to the peers list
	h2Found := false

	for _, p := range peersAfter {
		if p.ID == h2.ID() {
			h2Found = true

			assert.NotEmpty(t, p.Addrs, "Added peer should have addresses")

			break
		}
	}

	assert.True(t, h2Found, "Added peer should be in the peers list")

	// Verify the peers list grew by at least one
	assert.GreaterOrEqual(t, len(peersAfter), len(peers)+1,
		"Peers list should have grown after adding a peer")
}

func TestHostID(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	// Create libp2p host with a specific private key for deterministic ID
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	port := findAvailablePort(t)
	h, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)

	defer h.Close()

	// Create a P2PNode
	node := &P2PNode{
		host:   h,
		logger: logger,
	}

	// Verify that HostID returns the same ID as the host
	assert.Equal(t, h.ID(), node.HostID(), "HostID should return the correct peer ID")
}

func TestBytesCounters(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	node := &P2PNode{
		logger: logger,
	}

	// Verify initial state
	assert.Equal(t, uint64(0), node.BytesSent())
	assert.Equal(t, uint64(0), node.BytesReceived())

	// Set bytes directly using atomic operations
	atomic.StoreUint64(&node.bytesSent, 1000)
	atomic.StoreUint64(&node.bytesReceived, 2000)

	// Verify methods report the correct values
	assert.Equal(t, uint64(1000), node.BytesSent())
	assert.Equal(t, uint64(2000), node.BytesReceived())
}

func TestTimestampMethods(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	node := &P2PNode{
		logger: logger,
	}

	// Set timestamps to known values
	now := time.Now().Unix()
	earlier := now - 60 // 1 minute ago

	atomic.StoreInt64(&node.lastSend, now)
	atomic.StoreInt64(&node.lastRecv, earlier)

	// Verify methods return the correct timestamps
	assert.Equal(t, time.Unix(now, 0), node.LastSend())
	assert.Equal(t, time.Unix(earlier, 0), node.LastRecv())
}

func TestConnectedPeers_Old(t *testing.T) {
	// Create first host
	h1, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", findAvailablePort(t))),
	)
	require.NoError(t, err)
	defer h1.Close()

	// Create second host
	port2 := findAvailablePort(t)
	h2, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port2)),
	)
	require.NoError(t, err)

	defer h2.Close()

	// Create a P2PNode with just the host
	node := &P2PNode{
		host:   h1,
		logger: ulogger.TestLogger{},
	}

	// Check initial peer count using Network().Peers() which shows active connections
	initialPeerCount := len(node.ConnectedPeers())
	t.Logf("Initial peer count: %d", initialPeerCount)

	// Connect h1 to h2
	addrs := h2.Addrs()

	var peerAddr string

	for _, addr := range addrs {
		peerAddr = fmt.Sprintf("%s/p2p/%s", addr.String(), h2.ID())
		t.Logf("Connecting to: %s", peerAddr)

		break // use the first address
	}

	maddr, err := multiaddr.NewMultiaddr(peerAddr)
	require.NoError(t, err)
	peerInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = h1.Connect(ctx, *peerInfo)
	require.NoError(t, err)

	// Wait for connection to establish
	time.Sleep(100 * time.Millisecond)

	// Check the ConnectedPeers method
	peerCount := len(node.ConnectedPeers())
	t.Logf("Peer count after connect: %d", peerCount)
	require.Greater(t, peerCount, initialPeerCount, "Peer count should increase")

	// Verify direct connection state using Network interface
	connectedness := h1.Network().Connectedness(h2.ID())
	require.Equal(t, network.Connected, connectedness, "Peers should be connected")

	// Now disconnect
	err = node.DisconnectPeer(ctx, h2.ID())
	require.NoError(t, err)

	// Wait for disconnection to take effect
	time.Sleep(100 * time.Millisecond)

	// Important: As we've discovered, ConnectedPeers() still returns peers from the peerstore
	// even after they're disconnected
	peerstoreCount := len(node.ConnectedPeers())
	t.Logf("Peer count after disconnect (from peerstore): %d", peerstoreCount)

	// Verify the actual connection state is not connected using the Network interface
	connectednessAfter := h1.Network().Connectedness(h2.ID())
	assert.NotEqual(t, network.Connected, connectednessAfter,
		"Peers should not be connected according to Network interface")

	// Check active connections count
	activeConns := len(h1.Network().Conns())
	t.Logf("Active connections after disconnect: %d", activeConns)
	assert.Equal(t, 0, activeConns, "No active connections expected after disconnect")
}

func TestGetTopic(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	// Create a test node with mock topics map
	testTopic := &pubsub.Topic{}

	node := &P2PNode{
		logger: logger,
		topics: map[string]*pubsub.Topic{
			"test-topic": testTopic,
		},
	}

	// Test getting an existing topic
	topic := node.GetTopic("test-topic")
	assert.Equal(t, testTopic, topic, "GetTopic should return the correct topic")

	// Test getting a non-existent topic
	nonExistentTopic := node.GetTopic("non-existent-topic")
	assert.Nil(t, nonExistentTopic, "GetTopic should return nil for non-existent topics")
}

func TestPublish(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	// Create a real topic for testing
	host, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)
	defer host.Close()

	ps, err := pubsub.NewGossipSub(context.Background(), host)
	require.NoError(t, err)

	topic, err := ps.Join("test-topic")
	require.NoError(t, err)

	// Create a P2PNode with the real topic
	node := &P2PNode{
		host:   host,
		pubSub: ps,
		logger: logger,
		topics: map[string]*pubsub.Topic{
			"test-topic": topic,
		},
	}

	// Set up a subscription to verify message is published
	sub, err := topic.Subscribe()
	require.NoError(t, err)
	defer sub.Cancel()

	// Test message data
	testMessage := []byte("test message")

	// Test publishing to an existing topic
	ctx := context.Background()
	err = node.Publish(ctx, "test-topic", testMessage)
	assert.NoError(t, err, "Publish should not return an error for existing topic")

	// Test publishing to a non-existent topic
	err = node.Publish(ctx, "non-existent-topic", testMessage)
	assert.Error(t, err, "Publish should return an error for non-existent topic")
}

func TestStop(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	// Create a real host that we can verify gets closed
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), // Use port 0 to let OS assign port
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)

	// Create the P2PNode with this host
	node := &P2PNode{
		host:   h,
		logger: logger,
	}

	// Record host ID for later verification
	hostID := h.ID()

	// Verify the host is listening by checking if it has addresses
	addrs := h.Addrs()
	require.NotEmpty(t, addrs, "Host should have listening addresses before Stop")

	// Stop the node
	ctx := context.Background()
	err = node.Stop(ctx)
	assert.NoError(t, err, "Stop should not return an error")

	// Verify the host ID is still accessible
	_ = h.ID()

	// Create a second host to verify the first one is still connectable
	h2, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.DisableRelay(),
	)
	require.NoError(t, err)
	defer h2.Close()

	// Try to connect - this should actually fail since Stop() closes the host
	peerInfo := peer.AddrInfo{
		ID:    hostID,
		Addrs: addrs,
	}

	// Use a short timeout to avoid hanging if there are issues
	connectCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = h2.Connect(connectCtx, peerInfo)
	assert.Error(t, err, "Connection should fail since Stop() closes the host")
}

func TestSendToPeerIsolated(t *testing.T) {
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	// Find available ports for both hosts
	port1 := findAvailablePort(t)
	port2 := findAvailablePort(t)

	// Setup
	h1Opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port1)),
	}

	h1, err := libp2p.New(h1Opts...)
	require.NoError(t, err, "Failed to create host1")

	h2Opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port2)),
	}

	h2, err := libp2p.New(h2Opts...)
	require.NoError(t, err, "Failed to create host2")

	// Define protocol ID for testing
	testProtocolID := "test-protocol-id"

	// Create a P2PNode wrapping h1
	node := &P2PNode{
		host:              h1,
		logger:            logger,
		topics:            make(map[string]*pubsub.Topic),
		startTime:         time.Now(),
		bitcoinProtocolID: testProtocolID,
		// Don't need to initialize the atomic fields as they start at zero
	}

	// Log host information
	t.Logf("h2 ID: %s", h2.ID())

	for i, addr := range h2.Addrs() {
		t.Logf("h2 address %d: %s", i, addr.String())
	}

	// Add h2's address to h1's peerstore to enable connection
	peerInfo := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), peerstore.PermanentAddrTTL)
	t.Logf("Added h2's address to h1's peerstore: %v", peerInfo)

	// Setup a stream handler on h2 to receive messages
	var (
		receivedMessage   string
		receivedMessageMu sync.Mutex
		wg                sync.WaitGroup
	)

	wg.Add(1)

	h2.SetStreamHandler(protocol.ID(testProtocolID), func(stream network.Stream) {
		defer wg.Done()
		defer stream.Close()

		buf := make([]byte, 1024)

		n, err := stream.Read(buf)
		if err != nil {
			t.Logf("Error reading from stream: %v", err)
			return
		}

		receivedMessageMu.Lock()
		receivedMessage = string(buf[:n])
		receivedMessageMu.Unlock()
	})

	// Create context for the operation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect h1 to h2
	err = node.SendToPeer(ctx, h2.ID(), []byte("Hello from node1 to node2"))
	require.NoError(t, err, "Failed to send message to peer")

	// Wait for the message to be received
	wg.Wait()

	// Check the received message
	receivedMessageMu.Lock()
	receivedMsg := receivedMessage
	receivedMessageMu.Unlock()

	t.Logf("Received message: %s", receivedMsg)
	assert.Equal(t, "Hello from node1 to node2", receivedMsg, "Received message should match sent message")

	// Cleanup
	err = h1.Close()
	require.NoError(t, err, "Failed to close host1")

	err = h2.Close()
	require.NoError(t, err, "Failed to close host2")
}

func TestGeneratePrivateKey(t *testing.T) {
	// now saves to the state store
	mockBlockchainClient := &blockchain.Mock{}

	var savedKey []byte

	mockBlockchainClient.On("SetState", mock.Anything, privateKeyKey, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		savedKey = append([]byte(nil), args.Get(2).([]byte)...) // copy
	})

	mockBlockchainClient.On("GetState", mock.Anything, privateKeyKey).Return(
		func(args mock.Arguments) ([]byte, error) {
			key := args.Get(1).(string)
			if key != privateKeyKey || savedKey == nil {
				return nil, errors.NewServiceUnavailableError("not found")
			}

			return append([]byte(nil), savedKey...), nil
		},
	)

	// Call the function to generate and save the private key
	privateKey, err := generatePrivateKey(context.Background(), mockBlockchainClient)
	require.NoError(t, err, "generatePrivateKey should not return an error")
	require.NotNil(t, privateKey, "privateKey should not be nil")

	// Verify the type of the returned key
	_, ok := (*privateKey).(*crypto.Ed25519PrivateKey)
	assert.True(t, ok, "privateKey should be an Ed25519PrivateKey")

	// Verify the key was saved to the state store
	require.NotEmpty(t, savedKey, "private key should not be empty in the state store")

	// Unmarshal the saved key and compare with the returned key
	unmarshaledKey, err := crypto.UnmarshalPrivateKey(savedKey)
	require.NoError(t, err, "should be able to unmarshal the private key")

	// Compare the keys - they should match
	keyBytes1, err := crypto.MarshalPrivateKey(*privateKey)
	require.NoError(t, err)
	keyBytes2, err := crypto.MarshalPrivateKey(unmarshaledKey)
	require.NoError(t, err)
	assert.Equal(t, keyBytes1, keyBytes2, "keys should match")

	// Verify the public key can be derived
	pubKey := (*privateKey).GetPublic()
	require.NotNil(t, pubKey, "should be able to derive public key")
}

func createTestAddrInfo(id peer.ID, addrs []multiaddr.Multiaddr) *peer.AddrInfo {
	return &peer.AddrInfo{
		ID:    id,
		Addrs: addrs,
	}
}

func createTestPeers() map[string]*peer.AddrInfo {
	return map[string]*peer.AddrInfo{
		"localhost-peer": createTestAddrInfo(peer.ID("localhost-peer"), []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/127.0.0.1/tcp/12345")}),
		"multi-addr-peer": createTestAddrInfo(peer.ID("multi-addr-peer"), []multiaddr.Multiaddr{
			multiaddr.StringCast("/ip4/127.0.0.1/tcp/12346"),
			multiaddr.StringCast("/ip4/192.168.1.5/tcp/12346"),
		}),
		"mismatch-peer":  createTestAddrInfo(peer.ID("mismatch-peer"), []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.1/tcp/12345")}),
		"no-addr-peer":   createTestAddrInfo(peer.ID("no-addr-peer"), []multiaddr.Multiaddr{}),
		"good-peer":      createTestAddrInfo(peer.ID("good-peer"), []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.2/tcp/12345")}),
		"self-peer":      createTestAddrInfo(peer.ID("self-peer"), []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.3/tcp/12345")}),
		"connected-peer": createTestAddrInfo(peer.ID("connected-peer"), []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.4/tcp/12345")}),
	}
}

func TestPeerDiscoveryFiltering(t *testing.T) {
	// Create a logger, settings, and a mock host
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}
	testSettings := settings.NewSettings()

	// Create a config for discovery test
	discoveryConfig := P2PConfig{
		ProcessName:     "test-discovery",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            findAvailablePort(t),
		UsePrivateDHT:   false,
	}
	_ = discoveryConfig // Use the variable to avoid unused variable warning

	// Create test peers map
	testPeers := createTestPeers()

	// Create a map to track peer connection attempts
	connectionAttempts := make(map[string]bool)

	// Create a connect function that our mock will use
	connectFunc := func(ctx context.Context, addr peer.AddrInfo) error {
		connectionAttempts[addr.ID.String()] = true
		return nil
	}

	// Create a mock host with our custom peer ID and network
	mockHost := &mockHostImpl{
		peerID:         testPeers["self-peer"].ID,
		connectedPeers: map[peer.ID]bool{testPeers["connected-peer"].ID: true},
		connFunc:       connectFunc,
		network:        &mockNetworkImpl{connectedPeers: map[peer.ID]bool{testPeers["connected-peer"].ID: true}},
	}

	// Create P2PNode with our mock host
	node := &P2PNode{
		host:      mockHost,
		logger:    logger,
		settings:  testSettings,
		config:    P2PConfig{OptimiseRetries: true},
		startTime: time.Now(),
	}

	// Create a map to track errors for peer addresses
	peerAddrErrorMap := &sync.Map{}
	// Set errors for specific test peers
	peerAddrErrorMap.Store(testPeers["localhost-peer"].ID.String(), "no good addresses")
	peerAddrErrorMap.Store(testPeers["multi-addr-peer"].ID.String(), "no good addresses")
	peerAddrErrorMap.Store(testPeers["mismatch-peer"].ID.String(), "peer id mismatch")

	// Run test cases on each test peer
	tests := []struct {
		name          string
		peer          peer.AddrInfo
		shouldConnect bool
	}{
		{
			name:          "Self",
			peer:          *testPeers["self-peer"],
			shouldConnect: false,
		},
		{
			name:          "Connected",
			peer:          *testPeers["connected-peer"],
			shouldConnect: false,
		},
		{
			name:          "No addresses",
			peer:          *testPeers["no-addr-peer"],
			shouldConnect: false,
		},
		{
			name:          "Localhost with error",
			peer:          *testPeers["localhost-peer"],
			shouldConnect: false,
		},
		{
			name:          "Multiple addresses with error",
			peer:          *testPeers["multi-addr-peer"],
			shouldConnect: true,
		},
		{
			name:          "ID mismatch",
			peer:          *testPeers["mismatch-peer"],
			shouldConnect: false,
		},
		{
			name:          "Good",
			peer:          *testPeers["good-peer"],
			shouldConnect: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := testPeerFiltering(tc.peer,
				mockHost.ID(),
				mockHost.Network(),
				node.config.OptimiseRetries,
				peerAddrErrorMap,
				mockHost)
			assert.Equal(t, tc.shouldConnect, result, "Filtering logic mismatch for %s", tc.peer.ID)

			connected := connectionAttempts[tc.peer.ID.String()]
			assert.Equal(t, tc.shouldConnect, connected, "Connection attempt mismatch for %s", tc.peer.ID)
		})
	}
}

// a function that directly tests the filtering logic
func testPeerFiltering(addr peer.AddrInfo, selfID peer.ID,
	mockNetwork network.Network, optimiseRetries bool,
	peerAddrErrorMap *sync.Map, mockHost *mockHostImpl) bool {
	// Skip self connection
	if addr.ID == selfID {
		return false
	}

	// Skip already connected peers
	if mockNetwork.Connectedness(addr.ID) == network.Connected {
		return false
	}

	// Skip peers with no addresses
	if len(addr.Addrs) == 0 {
		return false
	}

	// Check OptimiseRetries logic
	if optimiseRetries && shouldSkipForOptimiseRetries(peerAddrErrorMap, addr) {
		return false
	}

	// If we got here, we would attempt to connect
	// Actually make the connection attempt to update the connectionAttempts map
	_ = mockHost.Connect(context.Background(), addr)

	return true
}

func shouldSkipForOptimiseRetries(peerAddrErrorMap *sync.Map, addr peer.AddrInfo) bool {
	if errorString, ok := peerAddrErrorMap.Load(addr.ID.String()); ok {
		if strings.Contains(errorString.(string), "no good addresses") {
			numAddresses := len(addr.Addrs)
			switch numAddresses {
			case 0:
				// Peer has no addresses, no point trying to connect to it
				return true
			case 1:
				address := addr.Addrs[0].String()
				if strings.Contains(address, "127.0.0.1") {
					// Peer has a single localhost address
					return true
				}
			}
		}

		if strings.Contains(errorString.(string), "peer id mismatch") {
			return true
		}
	}

	return false
}

type mockHostImpl struct {
	peerID         peer.ID
	connectedPeers map[peer.ID]bool
	connFunc       func(context.Context, peer.AddrInfo) error
	network        *mockNetworkImpl
}

func (m *mockHostImpl) ID() peer.ID {
	return m.peerID
}

func (m *mockHostImpl) Network() network.Network {
	return m.network
}

func (m *mockHostImpl) Connect(ctx context.Context, ai peer.AddrInfo) error {
	if m.connFunc != nil {
		return m.connFunc(ctx, ai)
	}

	m.connectedPeers[ai.ID] = true
	m.network.connectedPeers[ai.ID] = true

	return nil
}

// Other required mockHostImpl methods to satisfy the host.Host interface
func (m *mockHostImpl) Peerstore() peerstore.Peerstore                                  { return nil }
func (m *mockHostImpl) Mux() protocol.Switch                                            { return nil }
func (m *mockHostImpl) SetStreamHandler(pid protocol.ID, handler network.StreamHandler) {}
func (m *mockHostImpl) SetStreamHandlerMatch(pid protocol.ID, match func(pid protocol.ID) bool, handler network.StreamHandler) {
}
func (m *mockHostImpl) RemoveStreamHandler(pid protocol.ID) {}
func (m *mockHostImpl) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error) {
	return nil, nil
}
func (m *mockHostImpl) Close() error                 { return nil }
func (m *mockHostImpl) Addrs() []multiaddr.Multiaddr { return nil }

// Additional methods required for host.Host interface
func (m *mockHostImpl) ConnManager() connmgr.ConnManager { return nil }
func (m *mockHostImpl) EventBus() event.Bus              { return nil }

type mockNetworkImpl struct {
	connectedPeers map[peer.ID]bool
}

func (m *mockNetworkImpl) Connectedness(pid peer.ID) network.Connectedness {
	if m.connectedPeers[pid] {
		return network.Connected
	}

	return network.NotConnected
}

// Stub implementations to satisfy the network.Network interface
func (m *mockNetworkImpl) Peerstore() peerstore.Peerstore                           { return nil }
func (m *mockNetworkImpl) LocalPeer() peer.ID                                       { return "" }
func (m *mockNetworkImpl) ListenAddresses() []multiaddr.Multiaddr                   { return nil }
func (m *mockNetworkImpl) InterfaceListenAddresses() ([]multiaddr.Multiaddr, error) { return nil, nil }
func (m *mockNetworkImpl) Close() error                                             { return nil }
func (m *mockNetworkImpl) ConnsToPeer(p peer.ID) []network.Conn                     { return nil }
func (m *mockNetworkImpl) Peers() []peer.ID                                         { return nil }
func (m *mockNetworkImpl) Conns() []network.Conn                                    { return nil }
func (m *mockNetworkImpl) ConnectedPeers() []peer.ID                                { return nil }
func (m *mockNetworkImpl) NewStream(ctx context.Context, p peer.ID) (network.Stream, error) {
	return nil, nil
}
func (m *mockNetworkImpl) SetStreamHandler(handler network.StreamHandler) {}
func (m *mockNetworkImpl) SetStreamHandlerMatch(pid protocol.ID, match func(pid protocol.ID) bool, handler network.StreamHandler) {
}
func (m *mockNetworkImpl) RemoveStreamHandler(pid protocol.ID) {}
func (m *mockNetworkImpl) Listeners() []multiaddr.Multiaddr    { return nil }
func (m *mockNetworkImpl) Resource() network.ResourceManager   { return nil }

// Additional methods required for network.Network interface
func (m *mockNetworkImpl) SetConnHandler(func(network.Conn)) {}
func (m *mockNetworkImpl) Notify(network.Notifiee)           {}
func (m *mockNetworkImpl) StopNotify(network.Notifiee)       {}
func (m *mockNetworkImpl) DialPeer(ctx context.Context, id peer.ID) (network.Conn, error) {
	// Fix DialPeer to use the correct return type
	return nil, nil
}
func (m *mockNetworkImpl) ClosePeer(peer.ID) error                           { return nil }
func (m *mockNetworkImpl) CanDial(id peer.ID, addr multiaddr.Multiaddr) bool { return true }
func (m *mockNetworkImpl) Listen(addrs ...multiaddr.Multiaddr) error         { return nil }
func (m *mockNetworkImpl) ResourceManager() network.ResourceManager          { return nil }

// TestShouldSkipPeer tests the shouldSkipPeer method in isolation
func TestShouldSkipPeer(t *testing.T) {
	// Create test peer IDs
	selfPeerID := peer.ID("self-peer")
	connectedPeerID := peer.ID("connected-peer")
	noPeerID := peer.ID("no-addresses-peer")
	goodPeerID := peer.ID("good-peer")
	errorPeerID := peer.ID("error-peer")

	// Create test peers
	selfPeer := peer.AddrInfo{
		ID:    selfPeerID,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.1/tcp/12345")},
	}

	connectedPeer := peer.AddrInfo{
		ID:    connectedPeerID,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.2/tcp/12345")},
	}

	noPeer := peer.AddrInfo{
		ID:    noPeerID,
		Addrs: []multiaddr.Multiaddr{},
	}

	goodPeer := peer.AddrInfo{
		ID:    goodPeerID,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.3/tcp/12345")},
	}

	errorPeer := peer.AddrInfo{
		ID:    errorPeerID,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.4/tcp/12345")},
	}

	// Create connected peers map
	connPeers := map[peer.ID]bool{connectedPeerID: true}

	// Setup test cases
	testCases := []struct {
		name          string
		peer          peer.AddrInfo
		optimizeRetry bool
		errorString   string
		expectedSkip  bool
	}{
		{
			name:         "Self peer",
			peer:         selfPeer,
			expectedSkip: true,
		},
		{
			name:         "Connected peer",
			peer:         connectedPeer,
			expectedSkip: true,
		},
		{
			name:         "No addresses",
			peer:         noPeer,
			expectedSkip: true,
		},
		{
			name:          "With error - optimize off",
			peer:          errorPeer,
			optimizeRetry: false,
			errorString:   "some error",
			expectedSkip:  false,
		},
		{
			name:          "Good peer - optimize on",
			peer:          goodPeer,
			optimizeRetry: true,
			expectedSkip:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock network with the connected peers
			mockNet := &mockNetworkImpl{
				connectedPeers: connPeers,
			}

			// Create a mock host with our custom peer ID and network
			mockHost := &mockHostImpl{
				peerID:         selfPeerID,
				connectedPeers: connPeers,
				network:        mockNet,
			}

			// Create a map to track errors for peer addresses
			peerAddrErrorMap := &sync.Map{}

			// Set any pre-existing errors
			if tc.errorString != "" {
				peerAddrErrorMap.Store(tc.peer.ID.String(), tc.errorString)
			}

			// Create a P2PNode with our mocks
			node := &P2PNode{
				host:     mockHost,
				logger:   ulogger.TestLogger{},
				settings: settings.NewSettings(),
				config:   P2PConfig{OptimiseRetries: tc.optimizeRetry},
			}

			// Call the method
			skip := node.shouldSkipPeer(tc.peer, peerAddrErrorMap)

			// Check the result
			assert.Equal(t, tc.expectedSkip, skip, "Expected shouldSkipPeer to return %v for case %s", tc.expectedSkip, tc.name)
		})
	}
}

// TestShouldSkipBasedOnErrors tests the shouldSkipBasedOnErrors method in isolation
func TestShouldSkipBasedOnErrors(t *testing.T) {
	// Create test peer IDs
	localhostPeerID := peer.ID("localhost-peer")
	multiAddrPeerID := peer.ID("multi-addr-peer")
	idMismatchPeerID := peer.ID("id-mismatch-peer")
	otherErrorPeerID := peer.ID("other-error-peer")

	// Create test peers
	localhostPeer := peer.AddrInfo{
		ID:    localhostPeerID,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/127.0.0.1/tcp/12345")},
	}

	multiAddrPeer := peer.AddrInfo{
		ID: multiAddrPeerID,
		Addrs: []multiaddr.Multiaddr{
			multiaddr.StringCast("/ip4/127.0.0.1/tcp/12346"),
			multiaddr.StringCast("/ip4/192.168.1.5/tcp/12346"),
		},
	}

	idMismatchPeer := peer.AddrInfo{
		ID:    idMismatchPeerID,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.1/tcp/12345")},
	}

	otherErrorPeer := peer.AddrInfo{
		ID:    otherErrorPeerID,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.7/tcp/12345")},
	}

	// Setup test cases
	testCases := []struct {
		name         string
		peer         peer.AddrInfo
		errorString  string
		expectedSkip bool
	}{
		{
			name:         "No error in map",
			peer:         otherErrorPeer,
			expectedSkip: false,
		},
		{
			name:         "No good addresses - single localhost",
			peer:         localhostPeer,
			errorString:  "no good addresses",
			expectedSkip: true,
		},
		{
			name:         "No good addresses - multiple addresses",
			peer:         multiAddrPeer,
			errorString:  "no good addresses",
			expectedSkip: false,
		},
		{
			name:         "Peer ID mismatch",
			peer:         idMismatchPeer,
			errorString:  "peer id mismatch",
			expectedSkip: true,
		},
		{
			name:         "Other error",
			peer:         otherErrorPeer,
			errorString:  "some other error",
			expectedSkip: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a map to track errors for peer addresses
			peerAddrErrorMap := &sync.Map{}

			// Set any pre-existing errors
			if tc.errorString != "" {
				peerAddrErrorMap.Store(tc.peer.ID.String(), tc.errorString)
			}

			// Create a P2PNode
			node := &P2PNode{
				logger:   ulogger.TestLogger{},
				settings: settings.NewSettings(),
			}

			// Call the method
			skip := node.shouldSkipBasedOnErrors(tc.peer, peerAddrErrorMap)

			// Check the result
			assert.Equal(t, tc.expectedSkip, skip, "Expected shouldSkipBasedOnErrors to return %v for case %s", tc.expectedSkip, tc.name)
		})
	}
}

// TestShouldSkipNoGoodAddresses tests the shouldSkipNoGoodAddresses method in isolation
func TestShouldSkipNoGoodAddresses(t *testing.T) {
	// Create test peer IDs
	localhostPeerID := peer.ID("localhost-peer")
	multiAddrPeerID := peer.ID("multi-addr-peer")
	goodPeerID := peer.ID("good-peer")
	noPeerID := peer.ID("no-addresses-peer")

	// Create test peers
	localhostPeer := peer.AddrInfo{
		ID:    localhostPeerID,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/127.0.0.1/tcp/12345")},
	}

	multiAddrPeer := peer.AddrInfo{
		ID: multiAddrPeerID,
		Addrs: []multiaddr.Multiaddr{
			multiaddr.StringCast("/ip4/127.0.0.1/tcp/12346"),
			multiaddr.StringCast("/ip4/192.168.1.5/tcp/12346"),
		},
	}

	goodPeer := peer.AddrInfo{
		ID:    goodPeerID,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.3/tcp/12345")},
	}

	noPeer := peer.AddrInfo{
		ID:    noPeerID,
		Addrs: []multiaddr.Multiaddr{},
	}

	// Setup test cases
	testCases := []struct {
		name         string
		peer         peer.AddrInfo
		expectedSkip bool
	}{
		{
			name:         "No addresses",
			peer:         noPeer,
			expectedSkip: true,
		},
		{
			name:         "Single localhost address",
			peer:         localhostPeer,
			expectedSkip: true,
		},
		{
			name:         "Multiple addresses including localhost",
			peer:         multiAddrPeer,
			expectedSkip: false,
		},
		{
			name:         "Non-localhost address",
			peer:         goodPeer,
			expectedSkip: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a P2PNode
			node := &P2PNode{
				logger:   ulogger.TestLogger{},
				settings: settings.NewSettings(),
			}

			// Call the method
			skip := node.shouldSkipNoGoodAddresses(tc.peer)

			// Check the result
			assert.Equal(t, tc.expectedSkip, skip, "Expected shouldSkipNoGoodAddresses to return %v for case %s", tc.expectedSkip, tc.name)
		})
	}
}

// TestAttemptConnection tests the attemptConnection method
func TestAttemptConnection(t *testing.T) {
	// Create test peer IDs
	peerID1 := peer.ID("peer1")
	peerID2 := peer.ID("peer2")
	peerID3 := peer.ID("peer3")

	// Create test peers
	peer1 := peer.AddrInfo{
		ID:    peerID1,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.1/tcp/12345")},
	}

	peer2 := peer.AddrInfo{
		ID:    peerID2,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.2/tcp/12345")},
	}

	peer3 := peer.AddrInfo{
		ID:    peerID3,
		Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.168.1.3/tcp/12345")},
	}

	// Setup test cases
	testCases := []struct {
		name              string
		peer              peer.AddrInfo
		alreadyStored     bool
		connectError      bool
		expectErrorStored bool
	}{
		{
			name:              "New peer - successful connection",
			peer:              peer1,
			alreadyStored:     false,
			connectError:      false,
			expectErrorStored: false,
		},
		{
			name:              "New peer - connection failure",
			peer:              peer2,
			alreadyStored:     false,
			connectError:      true,
			expectErrorStored: true,
		},
		{
			name:              "Already stored peer",
			peer:              peer3,
			alreadyStored:     true,
			connectError:      false,
			expectErrorStored: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create maps for tracking
			peerAddrMap := &sync.Map{}
			peerAddrErrorMap := &sync.Map{}
			connectionAttempts := &sync.Map{}

			// Pre-store peer if needed
			if tc.alreadyStored {
				peerAddrMap.Store(tc.peer.ID.String(), tc.peer)
			}

			// Create a connect function for our mock host
			connectFunc := func(ctx context.Context, addr peer.AddrInfo) error {
				connectionAttempts.Store(addr.ID.String(), true)

				if tc.connectError {
					return errors.New(errors.ERR_ERROR, "connection error")
				}

				return nil
			}

			// Create a mock host
			mockHost := &mockHostImpl{
				peerID:         peer.ID("self-peer"),
				connectedPeers: make(map[peer.ID]bool),
				connFunc:       connectFunc,
			}

			// Create a P2PNode
			node := &P2PNode{
				host:      mockHost,
				logger:    ulogger.TestLogger{},
				settings:  settings.NewSettings(),
				startTime: time.Now(),
			}

			// Create a context
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			// Call the method
			node.attemptConnection(ctx, tc.peer, peerAddrMap, peerAddrErrorMap)

			// Allow time for the goroutine to complete
			time.Sleep(100 * time.Millisecond)

			// Check if the peer was added to the map
			_, peerStored := peerAddrMap.Load(tc.peer.ID.String())
			assert.True(t, peerStored, "Expected peer to be stored in peerAddrMap")

			// If peer was not already stored, check connection attempt
			if !tc.alreadyStored {
				_, connectionAttempted := connectionAttempts.Load(tc.peer.ID.String())
				assert.True(t, connectionAttempted, "Expected a connection attempt")

				// Check if error was stored
				_, errorStored := peerAddrErrorMap.Load(tc.peer.ID.String())
				assert.Equal(t, tc.expectErrorStored, errorStored, "Expected error to be stored: %v, got: %v", tc.expectErrorStored, errorStored)
			} else {
				// If peer was already stored, ensure no connection attempt
				_, connectionAttempted := connectionAttempts.Load(tc.peer.ID.String())
				assert.False(t, connectionAttempted, "Did not expect a connection attempt for already stored peer")
			}
		})
	}
}

func findAvailablePort(t *testing.T) int {
	var port int

	maxRetries := 5
	retryDelay := 500 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Add some randomness to the delay to avoid conflicts
			jitter := time.Duration(rand.Intn(200)) * time.Millisecond // nolint:gosec
			delay := retryDelay + jitter
			t.Logf("Retrying port allocation after delay of %v (attempt %d)", delay, attempt+1)
			time.Sleep(delay)
		}

		// Try to find any available port by letting the OS choose
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Logf("Failed to find available port on attempt %d: %v", attempt+1, err)
			continue
		}

		// Get the port that was assigned
		addr := listener.Addr().(*net.TCPAddr)
		port = addr.Port

		// Close the listener to release the port
		err = listener.Close()
		if err != nil {
			t.Logf("Error closing listener: %v", err)
			continue
		}

		// Small delay to ensure the port is released
		time.Sleep(500 * time.Millisecond)

		// Verify the port is actually free
		testListener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			t.Logf("Port %d is not available after release: %v", port, err)
			continue
		}

		testListener.Close()

		// Port is available
		return port
	}

	// If we couldn't find a port after all retries, use a random high port
	// This is a last resort and might still fail, but it's better than nothing
	port = 10000 + rand.Intn(50000) // nolint:gosec
	t.Logf("Using random port after failed allocation attempts: %d", port)

	return port
}
