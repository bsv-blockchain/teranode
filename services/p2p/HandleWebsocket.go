// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/bitcoin-sv/teranode/services/asset/asset_api"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

type notificationMsg struct {
	Timestamp    string `json:"timestamp,omitempty"`
	Type         string `json:"type"`
	Hash         string `json:"hash,omitempty"`
	BaseURL      string `json:"base_url,omitempty"`
	PeerID       string `json:"peer_id,omitempty"`
	PreviousHash string `json:"previousblockhash,omitempty"`
	TxCount      uint64 `json:"tx_count,omitempty"`
	Height       uint32 `json:"height,omitempty"`
	SizeInBytes  uint64 `json:"size_in_bytes,omitempty"`
	Miner        string `json:"miner,omitempty"`
}

type WebSocketConn interface {
	WriteMessage(messageType int, data []byte) error
	Close() error
}

const (
	isoFormat = "2006-01-02T15:04:05Z"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

// broadcastMessage sends a message to all connected clients
func (s *Server) broadcastMessage(data []byte, clientChannels map[chan []byte]struct{}) {
	if len(clientChannels) == 0 {
		return
	}

	for clientCh := range clientChannels {
		select {
		case clientCh <- data:
			// Data sent successfully
		case <-time.After(time.Second): // Adjust timeout duration as needed
			s.logger.Errorf("Timeout sending data to client")
		}
	}
}

// createPingMessage creates a ping notification message
func createPingMessage(baseURL string) (*notificationMsg, error) {
	return &notificationMsg{
		Timestamp: time.Now().UTC().Format(isoFormat),
		Type:      asset_api.Type_PING.String(),
		BaseURL:   baseURL,
	}, nil
}

// handleClientMessages processes messages for a single websocket client
func (s *Server) handleClientMessages(ws WebSocketConn, ch chan []byte, deadClientCh chan<- chan []byte) {
	for data := range ch {
		err := ws.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			deadClientCh <- ch

			if err.Error() == "write: connection reset by peer" {
				s.logger.Infof("Connection Lost: %v", err)
			} else {
				s.logger.Errorf("Failed to Send notification WS message: %v", err)
			}

			break
		}
	}
}

// startNotificationProcessor starts the goroutine that processes notifications and manages clients
func (s *Server) startNotificationProcessor(
	clientChannels map[chan []byte]struct{},
	newClientCh <-chan chan []byte,
	deadClientCh <-chan chan []byte,
	notificationCh <-chan *notificationMsg,
	baseURL string,
	ctx context.Context,
) {
	pingTimer := time.NewTicker(10 * time.Second)
	defer pingTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case newClient := <-newClientCh:
			clientChannels[newClient] = struct{}{}
		case deadClient := <-deadClientCh:
			delete(clientChannels, deadClient)
		case <-pingTimer.C:
			msg, err := createPingMessage(baseURL)
			if err != nil {
				s.logger.Errorf("Failed to create ping message: %v", err)
				continue
			}

			data, err := json.Marshal(msg)
			if err != nil {
				s.logger.Errorf("Failed to marshal ping message: %v", err)
				continue
			}

			s.broadcastMessage(data, clientChannels)
		case notification := <-notificationCh:
			data, err := json.Marshal(notification)
			if err != nil {
				s.logger.Errorf("Failed to marshal notification: %v", err)
				continue
			}

			s.broadcastMessage(data, clientChannels)
		}
	}
}

func (s *Server) HandleWebSocket(notificationCh chan *notificationMsg, baseURL string) func(c echo.Context) error {
	clientChannels := make(map[chan []byte]struct{})
	newClientCh := make(chan chan []byte, 1_000)
	deadClientCh := make(chan chan []byte, 1_000)

	ctx, cancel := context.WithCancel(context.Background())

	go s.startNotificationProcessor(clientChannels, newClientCh, deadClientCh, notificationCh, baseURL, ctx)

	return func(c echo.Context) error {
		ch := make(chan []byte)

		ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
		if err != nil {
			cancel() // Cancel context if upgrade fails
			return err
		}

		// Add client channel before starting message handling
		newClientCh <- ch

		// Start message handling in a goroutine
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.handleClientMessages(ws, ch, deadClientCh)
		}()

		// Wait for either context cancellation or message handling to complete
		select {
		case <-ctx.Done():
			ws.Close()
		case <-done:
			// Message handling completed normally
		}

		return nil
	}
}
