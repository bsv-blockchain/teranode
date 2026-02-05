// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

// notificationMsg represents a WebSocket notification message sent to connected clients.
// This structure defines the JSON format for real-time notifications about blockchain
// events such as new blocks, mining updates, and peer status changes. The message
// format is designed to provide comprehensive information about blockchain state
// changes to WebSocket subscribers.
//
// All fields are optional (omitempty) except Type, which identifies the notification category.
// Common notification types include block announcements, mining status updates, and peer events.
type notificationMsg struct {
	Timestamp      string `json:"timestamp,omitempty"`         // ISO 8601 timestamp when the event occurred
	Type           string `json:"type"`                        // Required: notification type (e.g., "block", "mining", "peer")
	Hash           string `json:"hash,omitempty"`              // Block hash or transaction hash for blockchain events
	BaseURL        string `json:"base_url,omitempty"`          // Base URL for additional resource access
	PropagationURL string `json:"propagation_url,omitempty"`   // URL for peers to use for propagating txs (defaults to BaseURL if empty)
	PeerID         string `json:"peer_id,omitempty"`           // Peer identifier for peer-related notifications
	PreviousHash   string `json:"previousblockhash,omitempty"` // Previous block hash for block chain continuity
	TxCount        uint64 `json:"tx_count,omitempty"`          // Number of transactions in a block
	Height         uint32 `json:"height,omitempty"`            // Block height in the blockchain
	SizeInBytes    uint64 `json:"size_in_bytes,omitempty"`     // Size of the block or data in bytes
	Miner          string `json:"miner,omitempty"`             // Miner identifier for mining-related notifications
	// Node status fields
	Version       string  `json:"version,omitempty"`         // Node version
	CommitHash    string  `json:"commit_hash,omitempty"`     // Git commit hash
	BestBlockHash string  `json:"best_block_hash,omitempty"` // Best block hash
	BestHeight    uint32  `json:"best_height"`               // Best block height
	SubtreeCount  uint32  `json:"subtree_count,omitempty"`   // Number of subtrees in block assembly
	FSMState      string  `json:"fsm_state,omitempty"`       // FSM state
	StartTime     int64   `json:"start_time,omitempty"`      // Node start time
	Uptime        float64 `json:"uptime,omitempty"`          // Node uptime in seconds
	ClientName    string  `json:"client_name,omitempty"`     // Client name of this node
	MinerName     string  `json:"miner_name,omitempty"`      // Miner name that mined the best block
	ListenMode    string  `json:"listen_mode,omitempty"`     // Listen mode
	ChainWork     string  `json:"chain_work,omitempty"`      // Chain work as hex string
	// Sync peer fields
	SyncPeerID        string `json:"sync_peer_id,omitempty"`         // ID of the peer we're syncing from
	SyncPeerHeight    uint32 `json:"sync_peer_height,omitempty"`     // Height of the sync peer
	SyncPeerBlockHash string `json:"sync_peer_block_hash,omitempty"` // Best block hash of the sync peer
	SyncConnectedAt   int64  `json:"sync_connected_at,omitempty"`    // Unix timestamp when we first connected to this sync peer
	// New fields for enhanced node status
	MinMiningTxFee      *float64 `json:"min_mining_tx_fee,omitempty"`     // Minimum mining transaction fee configured for this node (nil = unknown, 0 = no fee)
	ConnectedPeersCount int      `json:"connected_peers_count,omitempty"` // Number of connected peers
	Storage             string   `json:"storage,omitempty"`               // Storage mode: "full" (block persister running and caught up), "pruned" (no persister or lagging), or empty (old version)
}

// clientChannelMap manages a thread-safe collection of WebSocket client channels.
// This structure maintains a registry of active WebSocket connections, allowing
// the server to broadcast notifications to all connected clients efficiently.
// The map uses channels as keys to uniquely identify each client connection.
//
// All operations on this map are protected by a read-write mutex to ensure
// thread safety when multiple goroutines are adding, removing, or broadcasting
// to client channels concurrently.
type clientChannelMap struct {
	sync.RWMutex                          // Protects concurrent access to the channels map
	channels     map[chan []byte]struct{} // Set of active client channels (using struct{} for memory efficiency)
}

// newClientChannelMap creates a new thread-safe client channel registry.
// This constructor initializes an empty map for tracking WebSocket client
// connections and returns a ready-to-use clientChannelMap instance.
//
// The returned map is safe for concurrent use by multiple goroutines and
// provides methods for adding, removing, and broadcasting to client channels.
//
// Returns:
//   - Pointer to a new clientChannelMap instance with initialized internal map
func newClientChannelMap() *clientChannelMap {
	return &clientChannelMap{
		channels: make(map[chan []byte]struct{}),
	}
}

func (cm *clientChannelMap) add(ch chan []byte) {
	cm.Lock()
	defer cm.Unlock()
	cm.channels[ch] = struct{}{}
}

func (cm *clientChannelMap) remove(ch chan []byte) {
	cm.Lock()
	defer cm.Unlock()
	delete(cm.channels, ch)
}

func (cm *clientChannelMap) broadcast(data []byte, logger ulogger.Logger) {
	// Get a snapshot of channels under the lock
	cm.RLock()
	channels := make([]chan []byte, 0, len(cm.channels))

	for ch := range cm.channels {
		channels = append(channels, ch)
	}
	cm.RUnlock()

	if len(channels) == 0 {
		return
	}

	// Send to all channels in parallel without holding the lock
	// This prevents O(N) delay accumulation from blocking clients
	var wg sync.WaitGroup
	for _, ch := range channels {
		wg.Add(1)
		go func(ch chan []byte) {
			defer wg.Done()
			timer := time.NewTimer(time.Second)
			defer func() {
				// Ensure timer resources are released promptly when the send succeeds.
				if !timer.Stop() {
					// If the timer already fired concurrently, drain to avoid keeping the value queued on timer.C.
					select {
					case <-timer.C:
					default:
					}
				}
			}()
			select {
			case ch <- data:
				// Data sent successfully
			case <-timer.C:
				logger.Errorf("Timeout sending data to client")
				// Remove timed out client
				cm.remove(ch)
			}
		}(ch)
	}
	wg.Wait() // Wait for all sends to complete
}

func (cm *clientChannelMap) contains(ch chan []byte) bool {
	cm.RLock()
	defer cm.RUnlock()
	_, exists := cm.channels[ch]

	return exists
}

func (cm *clientChannelMap) count() int {
	cm.RLock()
	defer cm.RUnlock()

	return len(cm.channels)
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
func (s *Server) broadcastMessage(data []byte, clientChannels *clientChannelMap) {
	clientChannels.broadcast(data, s.logger)
}

// handleClientMessages processes messages for a single websocket client
func (s *Server) handleClientMessages(ws WebSocketConn, ch chan []byte, deadClientCh chan<- chan []byte) {
ClientMessageLoop:
	for {
		select {
		case <-s.gCtx.Done():
			// Global context is done, close the WebSocket connection
			s.logger.Infof("Closing WebSocket connection due to global context cancellation")
			return
		case data := <-ch:
			if data == nil {
				s.logger.Warnf("Received nil data on client channel, closing connection")
				deadClientCh <- ch
				return
			}

			err := ws.WriteMessage(websocket.TextMessage, data)
			if err != nil {
				deadClientCh <- ch

				if err.Error() == "write: connection reset by peer" {
					s.logger.Infof("Connection Lost: %v", err)
				} else {
					s.logger.Errorf("Failed to Send notification WS message: %v", err)
				}

				break ClientMessageLoop
			}
		}
	}
}

// startNotificationProcessor starts the goroutine that processes notifications and manages clients
func (s *Server) startNotificationProcessor(
	clientChannels *clientChannelMap,
	newClientCh <-chan chan []byte,
	deadClientCh <-chan chan []byte,
	notificationCh <-chan *notificationMsg,
	ctx context.Context,
) {
	for {
		select {
		case <-ctx.Done():
			return

		case newClient := <-newClientCh:
			clientChannels.add(newClient)
			// Send initial node_status messages to the new client
			s.sendInitialNodeStatuses(newClient)

		case deadClient := <-deadClientCh:
			clientChannels.remove(deadClient)

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

// sendInitialNodeStatuses sends the current node's status to a newly connected client
// This ensures the UI can identify which node is the current one
func (s *Server) sendInitialNodeStatuses(clientCh chan []byte) {
	// Always generate a fresh node_status message for our node
	ourStatus := s.getNodeStatusMessage(context.Background())
	if ourStatus == nil {
		s.logger.Warnf("[sendInitialNodeStatuses] Failed to get current node status")
		return
	}

	// Send our node's status as the first message
	if data, err := json.Marshal(ourStatus); err == nil {
		select {
		case clientCh <- data:
			s.logger.Debugf("[sendInitialNodeStatuses] Sent current node status (peer_id: %s) to new client", ourStatus.PeerID)
		default:
			s.logger.Warnf("[sendInitialNodeStatuses] Failed to send current node status - channel full")
		}
	} else {
		s.logger.Errorf("[sendInitialNodeStatuses] Failed to marshal current node status: %v", err)
	}
}

func (s *Server) HandleWebSocket(notificationCh chan *notificationMsg) func(c echo.Context) error {
	clientChannels := newClientChannelMap()
	newClientCh := make(chan chan []byte, 1_000)
	deadClientCh := make(chan chan []byte, 1_000)

	ctx, cancel := context.WithCancel(context.Background())

	go s.startNotificationProcessor(clientChannels, newClientCh, deadClientCh, notificationCh, ctx)

	return func(c echo.Context) error {
		ch := make(chan []byte, 100) // Add buffer to help prevent blocking

		ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
		if err != nil {
			cancel() // Cancel context if upgrade fails
			return err
		}

		// Start message handling goroutine FIRST
		// This needs to be ready to process messages from the channel
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.handleClientMessages(ws, ch, deadClientCh)
		}()

		// Add client channel to the notification processor
		newClientCh <- ch

		// Wait for either context cancellation or message handling to complete
		select {
		case <-ctx.Done():
			ws.Close()
		case <-done: // Message handling completed normally
		}

		return nil
	}
}
