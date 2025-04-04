package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateTestPrivateKey(t *testing.T) string {
	// Generate a new Ed25519 key pair using libp2p's crypto package
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	assert.NoError(t, err)

	// Marshal the private key using libp2p's marshaler
	privBytes, err := crypto.MarshalPrivateKey(priv)
	assert.NoError(t, err)

	// Encode as hex
	return hex.EncodeToString(privBytes)
}

func TestSendToPeer(t *testing.T) {
	t.Skip("Fails in CI, but works locally")
	logger := ulogger.NewVerboseTestLogger(t)
	tSettings := settings.NewSettings()

	// Create two P2PNode instances
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

	node1, err := NewP2PNode(logger, tSettings, config1, nil)
	require.NoError(t, err)

	node2, err := NewP2PNode(logger, tSettings, config2, nil)
	require.NoError(t, err)

	// Create a context with a timeout for the entire test
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a WaitGroup to track all goroutines
	var wg sync.WaitGroup

	// Start both nodes
	wg.Add(2)

	startNode1 := func() {
		defer wg.Done()

		err := node1.Start(ctx)
		require.NoError(t, err)
	}
	go startNode1()

	startNode2 := func() {
		defer wg.Done()

		err := node2.Start(ctx)
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

	message := []byte("{}")

	// Use WaitGroup to ensure message sending completes
	var msgWg sync.WaitGroup

	msgWg.Add(1)

	sendMessage := func() {
		defer msgWg.Done()

		err := node1.SendToPeer(ctx, node2.HostID(), message)
		require.NoError(t, err)
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

func TestSendBlockMessageToPeer(t *testing.T) {
	t.Skip("Skipping blockmessage peer test")

	logger := ulogger.NewVerboseTestLogger(t)
	tSettings := settings.NewSettings()
	ctx := context.Background()

	topicPrefix := tSettings.ChainCfgParams.TopicPrefix
	if topicPrefix == "" {
		t.Log("p2p_topic_prefix not set in config")
		t.FailNow()
	}

	bbtn := tSettings.P2P.BestBlockTopic
	if bbtn == "" {
		t.Log("p2p_bestblock_topic not set in config")
		t.FailNow()
	}

	bestBlockTopicName := fmt.Sprintf("%s-%s", topicPrefix, bbtn)

	// Generate valid Ed25519 private keys for both nodes
	privKey1 := generateTestPrivateKey(t)
	privKey2 := generateTestPrivateKey(t)

	// Create two P2PNode instances
	config1 := P2PConfig{
		ProcessName:     "test1",
		ListenAddresses: []string{"127.0.0.1"},
		Port:            12345,
		PrivateKey:      privKey1,
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
		PrivateKey:      privKey2,
		SharedKey:       "",
		UsePrivateDHT:   false,
		OptimiseRetries: false,
		Advertise:       false,
		StaticPeers:     []string{},
	}

	node1, err := NewP2PNode(logger, tSettings, config1, nil)
	if err != nil {
		panic(err)
	}

	node2, err := NewP2PNode(logger, tSettings, config2, nil)
	if err != nil {
		panic(err)
	}

	// Start both nodes
	err = node1.Start(ctx, bestBlockTopicName)
	assert.NoError(t, err)

	err = node2.Start(ctx, bestBlockTopicName)
	assert.NoError(t, err)

	// Before connecting, log the HostID and connection string
	t.Logf("Node1 HostID: %s\n", node1.HostID())
	t.Logf("Node2 HostID: %s\n", node2.HostID())
	connectionString := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", node2.config.Port, node2.HostID())
	t.Logf("Connecting to: %s\n", connectionString)

	// Create a context with a 5-second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attempt to connect
	connected := node1.connectToStaticPeers(ctx, []string{connectionString})
	assert.True(t, connected)

	t.Logf("Connected to: %s\n", connectionString)

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
	ctx := context.Background()

	topicPrefix := tSettings.ChainCfgParams.TopicPrefix
	if topicPrefix == "" {
		t.Log("p2p_topic_prefix not set in config")
		t.FailNow()
	}

	bbtn := tSettings.P2P.BestBlockTopic
	if bbtn == "" {
		t.Log("p2p_bestblock_topic not set in config")
		t.FailNow()
	}

	bestBlockTopicName := fmt.Sprintf("%s-%s", topicPrefix, bbtn)

	// Create two P2PNode instances
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

	node1, err := NewP2PNode(logger, tSettings, config1, nil)
	if err != nil {
		panic(err)
	}

	node2, err := NewP2PNode(logger, tSettings, config2, nil)
	if err != nil {
		panic(err)
	}

	// Start both nodes
	err = node1.Start(ctx, bestBlockTopicName)
	assert.NoError(t, err)

	err = node2.Start(ctx, bestBlockTopicName)
	assert.NoError(t, err)

	// Before connecting, log the HostID and connection string
	t.Logf("Node1 HostID: %s\n", node1.HostID())
	t.Logf("Node2 HostID: %s\n", node2.HostID())
	connectionString := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", node2.config.Port, node2.HostID())
	t.Logf("Connecting to: %s\n", connectionString)

	// Create a context with a 5-second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attempt to connect
	connected := node1.connectToStaticPeers(ctx, []string{connectionString})
	assert.True(t, connected)

	t.Logf("Connected to: %s\n", connectionString)

	msgBytes, err := json.Marshal(BestBlockMessage{PeerID: node1.HostID().String()})
	if err != nil {
		t.Logf("[sendBestBlockMessage] json marshal error: %v", err)
	}

	if err := node1.Publish(ctx, bestBlockTopicName, msgBytes); err != nil {
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

	node1, err := NewP2PNode(logger, tSettings, config1, nil)
	if err != nil {
		panic(err)
	}

	node2, err := NewP2PNode(logger, tSettings, config2, nil)
	if err != nil {
		panic(err)
	}

	// start both nodes
	err = node1.Start(ctx, topicName)
	assert.NoError(t, err)

	err = node2.Start(ctx, topicName)
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
	logger := ulogger.NewVerboseTestLogger(t)
	tSettings := settings.NewSettings()

	tests := []struct {
		name        string
		staticPeers []string
		wantLogs    bool
	}{
		{
			name:        "no static peers",
			staticPeers: []string{},
			wantLogs:    true,
		},
		{
			name:        "with static peers",
			staticPeers: []string{"/ip4/127.0.0.1/tcp/12346/p2p/12D3KooWQvCkC8YiCTjRYXdyhNnncFRPKVxjE9u7rQKh2oaEXxQ6"},
			wantLogs:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := P2PConfig{
				ProcessName:     "test",
				ListenAddresses: []string{"127.0.0.1"},
				Port:            12345,
				StaticPeers:     tt.staticPeers,
				UsePrivateDHT:   false,
				Advertise:       false,
			}

			node, err := NewP2PNode(logger, tSettings, config, nil)
			assert.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			// Start the connector
			node.startStaticPeerConnector(ctx)

			// Wait for the context to be done
			<-ctx.Done()
		})
	}
}

func TestInitGossipSub(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
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
			config := P2PConfig{
				ProcessName:     "test",
				ListenAddresses: []string{"127.0.0.1"},
				Port:            12345,
				UsePrivateDHT:   false,
				Advertise:       false,
			}

			node, err := NewP2PNode(logger, tSettings, config, nil)
			assert.NoError(t, err)

			ctx := context.Background()
			err = node.initGossipSub(ctx, tt.topicNames)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, node.pubSub)

				// Verify topics were created
				assert.Equal(t, len(tt.topicNames), len(node.topics))

				for _, topicName := range tt.topicNames {
					topic, exists := node.topics[topicName]
					assert.True(t, exists)
					assert.NotNil(t, topic)
				}
			}
		})
	}
}
