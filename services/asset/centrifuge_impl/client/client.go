// Package client provides a WebSocket client implementation for connecting to a Centrifuge server.
// It handles real-time messaging and subscriptions for blockchain-related events.
package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/centrifugal/centrifuge-go"
)

// Client represents a WebSocket client that connects to a Centrifuge server.
// It handles real-time message processing and subscription management.
type Client struct {
	logger ulogger.Logger
	mu     sync.RWMutex
	client *centrifuge.Client
}

// New creates a new Centrifuge client instance with the provided logger.
// The client is initialized but not connected until Start is called.
//
// Parameters:
//   - logger: Logger instance for client operations and event logging
//
// Returns:
//   - *Client: A new client instance ready for configuration and connection
func New(logger ulogger.Logger) *Client {
	return &Client{
		logger: logger,
	}
}

// Start initializes the WebSocket connection to the Centrifuge server and sets up event handlers.
// It automatically subscribes to predefined channels for blockchain events.
//
// The client will attempt to connect to the server and establish subscriptions for:
//   - blocks: Channel for block-related events
//   - subtrees: Channel for Merkle tree subtree events
//
// Parameters:
//   - ctx: Context for the operation (currently unused but maintained for interface consistency)
//   - addr: Server address to connect to (e.g., "localhost:8000")
//
// Returns:
//   - error: Any error encountered during startup or subscription process
//
// Example usage:
//
//	client := New(logger)
//	err := client.Start(context.Background(), "localhost:8000")
//	if err != nil {
//	    log.Fatal(err)
//	}
func (c *Client) Start(_ context.Context, addr string) error {
	c.logger.Infof("[CentrifugeClient] Starting client")

	c.mu.Lock()
	c.client = centrifuge.NewJsonClient(fmt.Sprintf("ws://%s/connection/websocket", addr), centrifuge.Config{})
	c.mu.Unlock()

	c.client.OnConnecting(func(e centrifuge.ConnectingEvent) {
		c.logger.Infof("[CentrifugeClient] connecting - %d (%s)", e.Code, e.Reason)
	})

	c.client.OnConnected(func(e centrifuge.ConnectedEvent) {
		c.logger.Infof("[CentrifugeClient] connected with ID %s", e.ClientID)
	})

	c.client.OnDisconnected(func(e centrifuge.DisconnectedEvent) {
		c.logger.Infof("[CentrifugeClient] disconnected: %d (%s)", e.Code, e.Reason)
	})

	c.client.OnMessage(func(e centrifuge.MessageEvent) {
		c.logger.Infof("[CentrifugeClient] message: %s", string(e.Data))
	})

	c.client.OnPublication(func(e centrifuge.ServerPublicationEvent) {
		c.logger.Infof("[CentrifugeClient] publication: %s", string(e.Data))
	})

	c.client.OnError(func(e centrifuge.ErrorEvent) {
		c.logger.Errorf("[CentrifugeClient] error: %s", e.Error.Error())
	})

	subscriptions := []string{"blocks", "subtrees"}
	for _, channel := range subscriptions {
		c.logger.Infof("[CentrifugeClient] subscribing to %s", channel)

		sub, err := c.client.NewSubscription(channel, centrifuge.SubscriptionConfig{})
		if err != nil {
			return err
		}

		if err = sub.Subscribe(); err != nil {
			return err
		}
	}

	return c.client.Connect()
}

// Stop gracefully disconnects the client from the Centrifuge server.
// It closes all active subscriptions and the WebSocket connection.
//
// Parameters:
//   - ctx: Context for the operation (currently unused but maintained for interface consistency)
//
// Returns:
//   - error: Any error encountered during the shutdown process
func (c *Client) Stop(_ context.Context) error {
	c.logger.Infof("[CentrifugeClient] Stopping client")

	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		return nil
	}
	return client.Disconnect()
}

// IsConnected returns true if the client has been initialized.
// This is a thread-safe method to check if the client connection has been established.
//
// Returns:
//   - bool: True if client is initialized, false otherwise
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.client != nil
}
