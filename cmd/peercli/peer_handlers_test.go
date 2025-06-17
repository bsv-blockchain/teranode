package main

import (
	"bytes"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/go-wire"
	"github.com/bitcoin-sv/teranode/services/legacy/peer"
	"github.com/stretchr/testify/assert"
)

// TODO - create tests for main.go when peers can be mocked or simulated.

// TestOnAddr tests the OnAddr function to ensure it correctly handles a MsgAddr message.
func TestOnAddr(t *testing.T) {
	// Mock peer and MsgAddr
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgAddr{
		AddrList: []*wire.NetAddress{
			{
				Timestamp: time.Time{},
				Services:  wire.SFNodeNetwork | wire.SFNodeXthin,
				IP:        net.ParseIP("192.168.1.1"),
				Port:      8333,
			},
			{
				Timestamp: time.Time{},
				Services:  wire.SFNodeNetwork | wire.SFNodeXthin,
				IP:        net.ParseIP("192.168.1.2"),
				Port:      8333,
			},
		},
	}

	// Capture output
	output := captureOutput(func() {
		OnAddr(mockPeer, mockMsg)
	})

	// Verify output
	assert.JSONEq(t, `{"addrList":[{"timestamp":"0001-01-01T00:00:00Z","services":17,"ip":"192.168.1.1","port":8333},{"timestamp":"0001-01-01T00:00:00Z","services":17,"ip":"192.168.1.2","port":8333}]}`, output, "Output did not match expected JSON")
}

// TestOnVersion tests the OnVersion function to ensure it correctly handles a MsgVersion message.
func TestOnVersion(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgVersion{}

	output := captureOutput(func() {
		OnVersion(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received version:", "Output did not match expected value")
}

// TestOnMemPool tests the OnMemPool function to ensure it correctly handles a MsgMemPool message.
func TestOnMemPool(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgMemPool{}

	output := captureOutput(func() {
		OnMemPool(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received mempool", "Output did not match expected value")
}

// TestOnTx tests the OnTx function to ensure it correctly handles a MsgTx message.
func TestOnTx(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgTx{}

	output := captureOutput(func() {
		OnTx(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received tx message:", "Output did not match expected value")
}

// TestOnBlock tests the OnBlock function to ensure it correctly handles a MsgBlock message.
func TestOnBlock(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgBlock{}

	output := captureOutput(func() {
		OnBlock(mockPeer, mockMsg, nil)
	})

	assert.Contains(t, output, "Received block message:", "Output did not match expected value")
}

// TestOnInv tests the OnInv function to ensure it correctly handles a MsgInv message.
func TestOnInv(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgInv{}

	output := captureOutput(func() {
		OnInv(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received inv message:", "Output did not match expected value")
}

// TestOnHeaders tests the OnHeaders function to ensure it correctly handles a MsgHeaders message.
func TestOnHeaders(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgHeaders{}

	output := captureOutput(func() {
		OnHeaders(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received headers", "Output did not match expected value")
}

// TestOnGetData tests the OnGetData function to ensure it correctly handles a MsgGetData message.
func TestOnGetData(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgGetData{}

	output := captureOutput(func() {
		OnGetData(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received getData", "Output did not match expected value")
}

// TestOnGetBlocks tests the OnGetBlocks function to ensure it correctly handles a MsgGetBlocks message.
func TestOnGetBlocks(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgGetBlocks{}

	output := captureOutput(func() {
		OnGetBlocks(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received getBlocks", "Output did not match expected value")
}

// TestOnGetHeaders tests the OnGetHeaders function to ensure it correctly handles a MsgGetHeaders message.
func TestOnGetHeaders(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgGetHeaders{}

	output := captureOutput(func() {
		OnGetHeaders(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received getHeaders", "Output did not match expected value")
}

// TestOnFeeFilter tests the OnFeeFilter function to ensure it correctly handles a MsgFeeFilter message.
func TestOnFeeFilter(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgFeeFilter{}

	output := captureOutput(func() {
		OnFeeFilter(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received feeFilter", "Output did not match expected value")
}

// TestOnGetAddr tests the OnGetAddr function to ensure it correctly handles a MsgGetAddr message.
func TestOnGetAddr(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgGetAddr{}

	output := captureOutput(func() {
		OnGetAddr(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received getAddr", "Output did not match expected value")
}

// TestOnVerAck tests the OnVerAck function to ensure it correctly handles a MsgVerAck message.
func TestOnVerAck(t *testing.T) {
	t.Skip("Skipping OnVerAck as it requires a peer connection")

	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgVerAck{}

	output := captureOutput(func() {
		OnVerAck(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "", "Output did not match expected value")
}

// TestOnPing tests the OnPing function to ensure it correctly handles a MsgPing message.
func TestOnPing(t *testing.T) {
	t.Skip("Skipping OnPing test as it requires a peer connection")

	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgPing{Nonce: 12345}

	output := captureOutput(func() {
		OnPing(mockPeer, mockMsg)
	})

	assert.Contains(t, output, "Received ping: 12345", "Output did not match expected value")
	assert.Contains(t, output, "Sent pong: 12345", "Output did not match expected value")
}

// TestOnWrite tests the OnWrite function to ensure it correctly handles a MsgPing message.
func TestOnWrite(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgPing{} // Using MsgPing as a placeholder

	output := captureOutput(func() {
		OnWrite(mockPeer, 100, mockMsg, nil)
	})

	expectedOutput := "onWrite: bytesWritten: 100\nmsg: &{Nonce:0}\n\n"
	assert.Equal(t, expectedOutput, output, "Output did not match expected value")
}

// TestOnReject tests the OnReject function to ensure it correctly handles a MsgReject message.
func TestOnReject(t *testing.T) {
	mockPeer := &peer.Peer{}

	output := captureOutput(func() {
		OnReject(mockPeer, nil)
	})

	expectedOutput := "Received reject\n"
	assert.Equal(t, expectedOutput, output, "Output did not match expected value")
}

// TestOnNotFound tests the OnNotFound function to ensure it correctly handles a MsgNotFound message.
func TestOnNotFound(t *testing.T) {
	mockPeer := &peer.Peer{}

	output := captureOutput(func() {
		OnNotFound(mockPeer, nil)
	})

	expectedOutput := "Received objectNotFound\n"
	assert.Equal(t, expectedOutput, output, "Output did not match expected value")
}

// TestOnPong tests the OnPong function to ensure it correctly handles a MsgPong message.
func TestOnPong(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgPong{Nonce: 12345}

	output := captureOutput(func() {
		OnPong(mockPeer, mockMsg)
	})

	expectedOutput := "Received pong: 12345\n"
	assert.Equal(t, expectedOutput, output, "Output did not match expected value")
}

// TestOnRead tests the OnRead function to ensure it correctly handles a MsgPing message.
func TestOnRead(t *testing.T) {
	mockPeer := &peer.Peer{}
	mockMsg := &wire.MsgPing{} // Using MsgPing as a placeholder

	output := captureOutput(func() {
		OnRead(mockPeer, 50, mockMsg, nil)
	})

	expectedOutput := "onRead: bytesRead: 50\nmsg: &{Nonce:0}\n\n"
	assert.Equal(t, expectedOutput, output, "Output did not match expected value")

	testError := errors.New(9, "test error")
	output = captureOutput(func() {
		OnRead(mockPeer, 50, mockMsg, testError)
	})

	expectedOutput = "onRead: bytesRead: 50\nmsg: &{Nonce:0}\nerr: ERROR (9): test error\n\n"
	assert.Equal(t, expectedOutput, output, "Output did not match expected value with error")
}

// Helper function to capture output
func captureOutput(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Create a channel to signal completion
	done := make(chan struct{})

	var buf bytes.Buffer

	// Start a goroutine to copy the output
	go func() {
		_, _ = io.Copy(&buf, r)
		done <- struct{}{}
	}()

	// Execute the function
	f()

	// Close the writer and wait for the goroutine to finish
	_ = w.Close()
	// Wait for the goroutine to finish
	<-done

	// Restore the original stdout
	os.Stdout = old

	return buf.String()
}
