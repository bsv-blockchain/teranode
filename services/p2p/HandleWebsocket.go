// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
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
	MinMiningTxFee      *float64   `json:"min_mining_tx_fee,omitempty"`     // Minimum mining transaction fee configured for this node (nil = unknown, 0 = no fee). Prefer FeePolicy.MiningFee.
	FeePolicy           *FeePolicy `json:"fee_policy,omitempty"`            // Full fee policy advertised to peers (nil = unknown/old peer)
	ConnectedPeersCount int        `json:"connected_peers_count,omitempty"` // Number of connected peers
	Storage             string     `json:"storage,omitempty"`               // Storage mode: "full" (block persister running and caught up), "pruned" (no persister or lagging), or empty (old version)
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

// maxConcurrentBroadcasts caps the number of in-flight broadcast goroutines so a
// notification burst with many connected clients can't exhaust goroutines/timers.
// Declared as a var (not const) so tests can override it; not exposed to settings
// because the cap is an internal resource ceiling, not a behavioural knob.
var maxConcurrentBroadcasts = 256

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
	// This prevents O(N) delay accumulation from blocking clients.
	// Clamp poolSize to at least 1 so a misconfigured/test-overridden cap can't
	// deadlock the loop: with capacity 0, sem <- struct{}{} would block forever
	// because the receiving goroutine is launched only after the send returns.
	poolSize := max(maxConcurrentBroadcasts, 1)
	sem := make(chan struct{}, poolSize)

	var wg sync.WaitGroup
	for _, ch := range channels {
		wg.Add(1)
		sem <- struct{}{} // blocks if pool is full — caps in-flight goroutines
		go func(ch chan []byte) {
			defer wg.Done()
			defer func() { <-sem }()
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
	SetWriteDeadline(t time.Time) error
	Close() error
}

const (
	isoFormat = "2006-01-02T15:04:05Z"

	// wsMaxReadBytes caps inbound frame size; clients are not expected to send data.
	wsMaxReadBytes int64 = 1024
	// wsHandshakeTimeout bounds the websocket upgrade handshake so a stalled
	// client cannot hold a connection slot without completing the upgrade.
	wsHandshakeTimeout = 10 * time.Second
	// wsInitialStatusTimeout bounds the blockchain lookup for the initial
	// node_status message so a hung backend cannot wedge connection setup.
	wsInitialStatusTimeout = 5 * time.Second
)

// wsTimeouts groups the /p2p-ws keepalive parameters. They are per-Server so
// tests can shrink them without racing on globals; production always uses
// defaultWSTimeouts. Not exposed as settings because they are internal
// resource-protection ceilings, not behavioural knobs.
type wsTimeouts struct {
	// writeTimeout bounds every websocket write so a client that stops
	// reading cannot wedge its writer goroutine forever.
	writeTimeout time.Duration
	// pongWait is how long a connection may go without any read activity
	// (pong or data) before the read pump gives up and the connection is
	// torn down. Must be greater than pingPeriod.
	pongWait time.Duration
	// pingPeriod is how often the writer pings the client to refresh the
	// read deadline of healthy connections.
	pingPeriod time.Duration
}

func defaultWSTimeouts() wsTimeouts {
	return wsTimeouts{
		writeTimeout: 10 * time.Second,
		pongWait:     60 * time.Second,
		pingPeriod:   54 * time.Second,
	}
}

// websocketTimeouts returns the effective keepalive parameters, enforcing the
// pingPeriod < pongWait invariant so a misconfigured override cannot evict
// every healthy connection each ping cycle.
func (s *Server) websocketTimeouts() wsTimeouts {
	to := defaultWSTimeouts()
	if s.wsTimeouts != nil {
		to = *s.wsTimeouts
	}

	if to.pingPeriod >= to.pongWait {
		to.pingPeriod = to.pongWait * 9 / 10
	}

	return to
}

// originAllowed reports whether a browser Origin header value is acceptable
// given the configured allow-list. An empty list preserves the historical
// allow-all behaviour; "*" matches any origin; other entries match the origin
// exactly (case-insensitively).
func originAllowed(origin string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}

	for _, a := range allowed {
		if a == "*" || strings.EqualFold(a, origin) {
			return true
		}
	}

	return false
}

// broadcastMessage sends a message to all connected clients
func (s *Server) broadcastMessage(data []byte, clientChannels *clientChannelMap) {
	clientChannels.broadcast(data, s.logger)
}

// handleClientMessages processes messages for a single websocket client.
// Every write carries a deadline so a slow or stalled client fails fast
// instead of wedging this goroutine, and periodic pings refresh the read
// deadline of healthy clients (the peer answers each ping with a pong).
// Returning is the only teardown signal needed: the connection handler joins
// this goroutine and deregisters the client channel synchronously.
func (s *Server) handleClientMessages(ctx context.Context, ws WebSocketConn, ch chan []byte) {
	to := s.websocketTimeouts()

	pingTicker := time.NewTicker(to.pingPeriod)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("Closing WebSocket connection due to context cancellation")
			return
		case <-pingTicker.C:
			_ = ws.SetWriteDeadline(time.Now().Add(to.writeTimeout))

			if err := ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				s.logger.Debugf("Failed to ping websocket client, closing connection: %v", err)
				return
			}
		case data := <-ch:
			if data == nil {
				s.logger.Warnf("Received nil data on client channel, closing connection")
				return
			}

			_ = ws.SetWriteDeadline(time.Now().Add(to.writeTimeout))

			err := ws.WriteMessage(websocket.TextMessage, data)
			if err != nil {
				if err.Error() == "write: connection reset by peer" {
					s.logger.Infof("Connection Lost: %v", err)
				} else {
					s.logger.Errorf("Failed to Send notification WS message: %v", err)
				}

				return
			}
		}
	}
}

// startNotificationProcessor starts the goroutine that broadcasts notifications
// to registered clients. Client registration and removal happen synchronously
// in the connection handler (not via channels), so a stalled broadcast can
// never wedge connection setup or teardown.
func (s *Server) startNotificationProcessor(
	clientChannels *clientChannelMap,
	notificationCh <-chan *notificationMsg,
	ctx context.Context,
) {
	for {
		select {
		case <-ctx.Done():
			return

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
func (s *Server) sendInitialNodeStatuses(ctx context.Context, clientCh chan []byte) {
	// Always generate a fresh node_status message for our node
	ourStatus := s.getNodeStatusMessage(ctx)
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

	serverCtx := s.gCtx

	go s.startNotificationProcessor(clientChannels, notificationCh, serverCtx)

	var (
		allowedOrigins []string
		maxConns       int64
	)

	if s.settings != nil {
		allowedOrigins = s.settings.P2P.WebSocketAllowedOrigins
		maxConns = int64(s.settings.P2P.WebSocketMaxConnections)
	}

	upgrader := websocket.Upgrader{
		HandshakeTimeout: wsHandshakeTimeout,
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				// Non-browser clients don't send an Origin header.
				return true
			}

			return originAllowed(origin, allowedOrigins)
		},
	}

	var (
		activeConns   atomic.Int64
		capWarnedOnce sync.Once
	)

	to := s.websocketTimeouts()

	return func(c echo.Context) error {
		// Cap concurrent connections before upgrading so an attacker can't
		// exhaust goroutines/file descriptors by opening sockets. Concurrent
		// upgrades may transiently overshoot the cap and be rejected; that
		// conservative bias is fine for a resource ceiling.
		if n := activeConns.Add(1); maxConns > 0 && n > maxConns {
			activeConns.Add(-1)
			// Warnf once so operators notice; Debugf after that so an
			// attacker hammering the endpoint can't amplify into log spam.
			capWarnedOnce.Do(func() {
				s.logger.Warnf("Rejecting websocket connection from %s: limit of %d concurrent connections reached (further rejections logged at debug)", c.RealIP(), maxConns)
			})
			s.logger.Debugf("Rejecting websocket connection from %s: limit of %d concurrent connections reached", c.RealIP(), maxConns)

			return echo.NewHTTPError(http.StatusServiceUnavailable, "websocket connection limit reached")
		}
		defer activeConns.Add(-1)

		connCtx, connCancel := context.WithCancel(serverCtx)
		defer connCancel()

		ch := make(chan []byte, 100)

		ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
		if err != nil {
			return err
		}

		readDone := make(chan struct{})
		writeDone := make(chan struct{})

		// Runs on every exit path: the client channel is deregistered from
		// the broadcaster synchronously (never blocks), closing the socket
		// unblocks both pumps and releases the fd, and waiting for the pumps
		// guarantees no goroutine outlives the connection slot it is
		// accounted against.
		defer func() {
			clientChannels.remove(ch)
			connCancel()
			_ = ws.Close()
			<-writeDone
			<-readDone
		}()

		// Read pump: enforce a read deadline refreshed by pongs so half-open
		// or silent connections are detected, and process control frames.
		ws.SetReadLimit(wsMaxReadBytes)
		_ = ws.SetReadDeadline(time.Now().Add(to.pongWait))
		ws.SetPongHandler(func(string) error {
			return ws.SetReadDeadline(time.Now().Add(to.pongWait))
		})

		go func() {
			defer close(readDone)

			for {
				if _, _, err := ws.ReadMessage(); err != nil {
					return
				}
			}
		}()

		go func() {
			defer close(writeDone)
			s.handleClientMessages(connCtx, ws, ch)
		}()

		// Queue the initial node_status into the client's buffer before
		// registering for broadcasts, so it is always the first message. The
		// lookup is bounded so a hung blockchain backend cannot wedge setup.
		statusCtx, statusCancel := context.WithTimeout(connCtx, wsInitialStatusTimeout)
		s.sendInitialNodeStatuses(statusCtx, ch)
		statusCancel()

		clientChannels.add(ch)

		select {
		case <-connCtx.Done():
		case <-writeDone:
		case <-readDone:
		}

		return nil
	}
}
