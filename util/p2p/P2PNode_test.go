package p2p

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
)

func TestSendToPeer(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := settings.NewSettings()

	// Create two P2PNode instances
	config1 := P2PConfig{
		ProcessName:     "test1",
		IP:              "127.0.0.1",
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
		IP:              "127.0.0.1",
		Port:            12346,
		PrivateKey:      "",
		SharedKey:       "",
		UsePrivateDHT:   false,
		OptimiseRetries: false,
		Advertise:       false,
		StaticPeers:     []string{},
	}

	node1, err := NewP2PNode(logger, tSettings, config1)
	if err != nil {
		panic(err)
	}

	node2, err := NewP2PNode(logger, tSettings, config2)
	if err != nil {
		panic(err)
	}

	// Start both nodes
	err = node1.Start(context.Background())
	assert.NoError(t, err)

	err = node2.Start(context.Background())
	assert.NoError(t, err)

	// Before connecting, log the HostID and connection string
	fmt.Printf("Node1 HostID: %s\n", node1.HostID())
	fmt.Printf("Node2 HostID: %s\n", node2.HostID())
	connectionString := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", node2.config.Port, node2.HostID())
	fmt.Printf("Connecting to: %s\n", connectionString)

	// Create a context with a 5-second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attempt to connect
	connected := node1.connectToStaticPeers(ctx, []string{connectionString})
	assert.True(t, connected)

	fmt.Printf("Connected to: %s\n", connectionString)

	message := []byte("Hello, Node!")

	err = node1.SendToPeer(ctx, node2.HostID(), message)
	assert.NoError(t, err)

	fmt.Printf("Message written to stream between %s and %s\n", node1.HostID(), node2.HostID())

	// sleep
	time.Sleep(1 * time.Second)

	assert.Equal(t, uint64(len(message)), node2.bytesReceived, "Node2 should have received the correct number of bytes")
	assert.Equal(t, uint64(len(message)), node1.bytesSent, "Node1 should have sent the correct number of bytes")
	assert.NotZero(t, node2.lastRecv, "Node2 should have updated the lastRecv timestamp")
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
		IP:              "127.0.0.1",
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
		IP:              "127.0.0.1",
		Port:            12346,
		PrivateKey:      "",
		SharedKey:       "",
		UsePrivateDHT:   false,
		OptimiseRetries: false,
		Advertise:       false,
		StaticPeers:     []string{},
	}

	node1, err := NewP2PNode(logger, tSettings, config1)
	if err != nil {
		panic(err)
	}

	node2, err := NewP2PNode(logger, tSettings, config2)
	if err != nil {
		panic(err)
	}

	// start both nodes
	err = node1.Start(ctx, topicName)
	assert.NoError(t, err)

	err = node2.Start(ctx, topicName)
	assert.NoError(t, err)

	// before connecting, log the HostID and connection string
	fmt.Printf("Node1 HostID: %s\n", node1.HostID())
	fmt.Printf("Node2 HostID: %s\n", node2.HostID())

	connectionString2 := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", node2.config.Port, node2.HostID())
	connected := node1.connectToStaticPeers(ctx, []string{connectionString2})
	assert.True(t, connected)

	fmt.Printf("Node1 connected to: %s\n", connectionString2)

	connectionString1 := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", node1.config.Port, node1.HostID())
	connected = node2.connectToStaticPeers(ctx, []string{connectionString1})
	assert.True(t, connected)

	fmt.Printf("Node2 cdonnected to: %s\n", connectionString1)

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

	fmt.Printf("added topic handler to node2 for topic: %s\n", topicName)

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
