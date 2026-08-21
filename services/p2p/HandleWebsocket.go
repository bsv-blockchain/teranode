// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
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

// wsClient couples a client's notification channel with the cancel function
// for its per-connection context. Dropping a client from the broadcast set
// without cancelling its context used to strand the connection: nothing
// would ever send on the channel again, the handler goroutine parked on it
// forever, and the connection slot it held against wsConnLimiter was never
// released - so a handful of clients that stopped reading could pin the
// global cap at its ceiling until the process restarted. Removal must
// therefore be authoritative over the connection, not just over the
// broadcast set.
type wsClient struct {
	ch     chan []byte
	cancel context.CancelFunc
}

// clientChannelMap manages a thread-safe collection of WebSocket client channels.
// This structure maintains a registry of active WebSocket connections, allowing
// the server to broadcast notifications to all connected clients efficiently.
// The map is keyed by each client's channel and carries the cancel function
// that tears the underlying connection down.
//
// All operations on this map are protected by a read-write mutex to ensure
// thread safety when multiple goroutines are adding, removing, or broadcasting
// to client channels concurrently.
type clientChannelMap struct {
	sync.RWMutex                                    // Protects concurrent access to the channels map
	channels     map[chan []byte]context.CancelFunc // Active client channels -> their connection-cancel func (nil when unmanaged, e.g. in tests)
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
		channels: make(map[chan []byte]context.CancelFunc),
	}
}

// add registers a channel with no associated connection. Used by tests and
// by any caller that has no socket to tear down.
func (cm *clientChannelMap) add(ch chan []byte) {
	cm.addClient(&wsClient{ch: ch})
}

func (cm *clientChannelMap) addClient(client *wsClient) {
	cm.Lock()
	defer cm.Unlock()
	cm.channels[client.ch] = client.cancel

	if prometheusP2PWebSocketConnections != nil {
		prometheusP2PWebSocketConnections.Set(float64(len(cm.channels)))
	}
}

// remove deregisters a client and cancels its connection context, so the
// handler goroutine wakes, closes the socket and releases its connection
// slot. Cancelling outside the lock keeps the broadcast fan-out from
// serialising on connection teardown.
func (cm *clientChannelMap) remove(ch chan []byte) {
	cm.Lock()

	cancel, existed := cm.channels[ch]
	delete(cm.channels, ch)

	if prometheusP2PWebSocketConnections != nil {
		prometheusP2PWebSocketConnections.Set(float64(len(cm.channels)))
	}

	cm.Unlock()

	if existed && cancel != nil {
		cancel()
	}
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
	ReadMessage() (messageType int, p []byte, err error)
	SetWriteDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetReadLimit(limit int64)
	SetPongHandler(h func(appData string) error)
	Close() error
}

const (
	isoFormat = "2006-01-02T15:04:05Z"
)

// Liveness parameters for /p2p-ws. Without them a half-open peer - one that
// blackholes packets, so no RST ever arrives - is undetectable by design:
// the writer blocks forever, the connection slot is never released and the
// per-IP/global caps ratchet towards permanent lockout. Declared as vars so
// tests can shorten them.
var (
	// wsWriteWait bounds a single WriteMessage. A peer that stops reading
	// stalls at the TCP window; without a deadline that stall is unbounded.
	wsWriteWait = 10 * time.Second

	// wsPongWait is how long we wait for a pong before declaring the peer
	// dead. Must be comfortably larger than wsPingPeriod.
	wsPongWait = 60 * time.Second

	// wsPingPeriod is how often we ping an otherwise idle peer.
	wsPingPeriod = 25 * time.Second
)

// wsMaxClientMessageBytes bounds the size of a single frame read from a
// /p2p-ws client. The endpoint is push-only - the read side exists purely as
// a liveness probe (pong frames plus whatever a peer sends unprompted) - so
// there is no legitimate client payload to accommodate. Without a limit,
// gorilla's default of "unlimited" lets a client declare an arbitrarily large
// frame and have the server grow a single buffer to match, which is a
// trivial remote OOM. 4KB is generously larger than an RFC 6455 control
// frame (125 bytes) and anything a conforming client would ever send here.
const wsMaxClientMessageBytes = 4 * 1024

// wsAllowedOrigins returns the operator-configured extra allowed origins for
// /p2p-ws, plus the dashboard's Vite dev-server origins when - and only when -
// the P2P HTTP server binds loopback, so `make dev` keeps working without
// leaving http(s)://localhost:5173/:4173 permanently allowlisted on a
// network-reachable node.
func (s *Server) wsAllowedOrigins() []string {
	if s.settings == nil {
		return nil
	}

	origins := make([]string, 0, len(s.settings.P2P.WSAllowedOrigins))
	origins = append(origins, s.settings.P2P.WSAllowedOrigins...)

	if util.LoopbackListenAddress(s.settings.P2P.HTTPListenAddress) {
		origins = append(origins, util.DevServerOrigins(s.settings.Dashboard.DevServerPorts)...)
	}

	return origins
}

// wsConnKey normalises a source IP for use as a connection-cap/rate-limit
// bucket key. IPv4 is kept full-precision; IPv6 is collapsed to its /64
// prefix so an attacker rotating addresses within a single /64 can't
// trivially evade the per-IP cap.
func wsConnKey(rawIP string) string {
	ip := net.ParseIP(rawIP)
	if ip == nil {
		return rawIP
	}

	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}

	mask := net.CIDRMask(64, 128)

	return ip.Mask(mask).String()
}

// wsConnLimiter enforces a global and a per-IP cap on concurrent /p2p-ws
// connections, checked before the HTTP connection is upgraded to WebSocket
// (and before the per-connection channel/goroutine are allocated), so a
// connection flood can't exhaust file descriptors, memory, or goroutines.
//
// The per-IP counters are a plain map guarded by the same mutex as the
// global counter, with an entry deleted as soon as its refcount returns to
// zero. That bounds the map by the number of *live* connections (i.e. by
// maxGlobal), so it needs no LRU. An LRU here would be actively harmful: its
// eviction is driven by key churn rather than by liveness, so an attacker
// rotating source addresses (a single IPv6 /48 yields 65 536 /64 buckets)
// could evict the counter of an IP that still holds open connections and
// silently reset that IP's cap to zero.
type wsConnLimiter struct {
	maxGlobal int
	maxPerIP  int
	mu        sync.Mutex
	global    int
	perIP     map[string]int
}

// newWSConnLimiter creates a wsConnLimiter. maxGlobal <= 0 disables the
// global cap; maxPerIP <= 0 disables the per-IP cap.
func newWSConnLimiter(maxGlobal, maxPerIP int) *wsConnLimiter {
	return &wsConnLimiter{
		maxGlobal: maxGlobal,
		maxPerIP:  maxPerIP,
		perIP:     make(map[string]int),
	}
}

// acquire reserves a connection slot for ip. If ok is false, the global or
// per-IP cap has been reached and the caller must reject the connection
// without upgrading it. If ok is true, release must be called when the
// connection ends; repeated calls are no-ops, so callers can defer it and
// still let another path (e.g. the broadcast timeout) tear the connection
// down.
//
// Both counters are checked and incremented under one lock, so a burst of
// concurrent first-touch acquires for the same previously-unseen key can
// never collectively exceed the cap.
func (l *wsConnLimiter) acquire(ip string) (release func(), ok bool) {
	key := wsConnKey(ip)

	l.mu.Lock()

	if l.maxGlobal > 0 && l.global >= l.maxGlobal {
		l.mu.Unlock()
		return nil, false
	}

	if l.maxPerIP > 0 && l.perIP[key] >= l.maxPerIP {
		l.mu.Unlock()
		return nil, false
	}

	l.global++

	if l.maxPerIP > 0 {
		l.perIP[key]++
	}

	l.mu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()

			l.global--

			if l.maxPerIP > 0 {
				if remaining := l.perIP[key] - 1; remaining > 0 {
					l.perIP[key] = remaining
				} else {
					delete(l.perIP, key)
				}
			}
		})
	}, true
}

// count returns the number of currently held slots. Test helper.
func (l *wsConnLimiter) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.global
}

// broadcastMessage sends a message to all connected clients
func (s *Server) broadcastMessage(data []byte, clientChannels *clientChannelMap) {
	clientChannels.broadcast(data, s.logger)
}

// handleClientMessages processes messages for a single websocket client.
//
// It is the connection's only writer (gorilla forbids concurrent writes), so
// the keepalive ping is emitted from this loop rather than from a separate
// goroutine. ctx is the per-connection context: cancelling it - which
// clientChannelMap.remove does when a client is dropped, and which
// startReadPump does when the peer stops responding - unblocks this loop so
// the handler returns and the connection slot is released.
func (s *Server) handleClientMessages(ctx context.Context, ws WebSocketConn, ch chan []byte, deadClientCh chan<- chan []byte) {
	pingTicker := time.NewTicker(wsPingPeriod)
	defer pingTicker.Stop()

	// markDead reports the client to the notification processor without
	// blocking forever if the processor has already stopped.
	markDead := func() {
		select {
		case deadClientCh <- ch:
		case <-ctx.Done():
		case <-s.gCtx.Done():
		}
	}

	write := func(messageType int, data []byte) error {
		if err := ws.SetWriteDeadline(time.Now().Add(wsWriteWait)); err != nil {
			return err
		}

		return ws.WriteMessage(messageType, data)
	}

	for {
		select {
		case <-ctx.Done():
			// This connection was torn down (dropped from the broadcast set,
			// peer stopped responding, or the server is shutting down).
			return
		case <-s.gCtx.Done():
			// Global context is done, close the WebSocket connection
			s.logger.Infof("Closing WebSocket connection due to global context cancellation")
			return
		case <-pingTicker.C:
			if err := write(websocket.PingMessage, nil); err != nil {
				s.logger.Infof("Closing WebSocket connection, keepalive ping failed: %v", err)
				markDead()

				return
			}
		case data := <-ch:
			if data == nil {
				s.logger.Warnf("Received nil data on client channel, closing connection")
				markDead()

				return
			}

			if err := write(websocket.TextMessage, data); err != nil {
				markDead()

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

// startReadPump runs the connection's read side. /p2p-ws is push-only, so
// this exists purely as a liveness probe: it refreshes the read deadline on
// every pong (and on any frame the peer happens to send), and cancels the
// connection when the peer goes quiet past wsPongWait or the socket errors.
// Without it a half-open peer is never detected and its connection slot is
// held for the life of the process.
func (s *Server) startReadPump(ws WebSocketConn, cancel context.CancelFunc) {
	defer cancel()

	ws.SetReadLimit(wsMaxClientMessageBytes)

	if err := ws.SetReadDeadline(time.Now().Add(wsPongWait)); err != nil {
		s.logger.Debugf("Failed to set websocket read deadline: %v", err)
		return
	}

	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(wsPongWait))
	})

	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			s.logger.Debugf("Closing WebSocket connection, read side ended: %v", err)
			return
		}

		if err := ws.SetReadDeadline(time.Now().Add(wsPongWait)); err != nil {
			return
		}
	}
}

// startNotificationProcessor starts the goroutine that processes notifications and manages clients.
//
// Registering a new client (and sending its initial node_status) happens
// synchronously on the connecting goroutine, in handleWebSocket, rather than
// here. That serves two invariants at once:
//   - insertion must strictly precede any possible removal, otherwise a
//     connection that ends before this goroutine got around to registering
//     it would leave an orphan entry (buffered channel plus broadcast slot)
//     that nothing would ever clean up;
//   - the initial node_status must reach the client before any broadcast
//     does (see sendInitialNodeStatuses), which is only guaranteed if the
//     client isn't made visible to broadcast (added to clientChannels) until
//     after the initial send - routing both through this single-goroutine
//     select loop would race the new-client case against the notification
//     case whenever both become ready at once.
func (s *Server) startNotificationProcessor(
	clientChannels *clientChannelMap,
	deadClientCh <-chan chan []byte,
	notificationCh <-chan *notificationMsg,
	ctx context.Context,
) {
	for {
		select {
		case <-ctx.Done():
			return

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

// initialNodeStatusTimeout bounds the blockchain gRPC round-trips made when a
// client connects before the first periodic node-status publish has cached one.
const initialNodeStatusTimeout = 5 * time.Second

// sendInitialNodeStatuses sends the current node's status to a newly connected
// client. Consumers (the asset service's centrifuge listener and the dashboard)
// pin the current node's identity to the FIRST node_status they receive, so this
// message must reach the client before any remote peer's node_status broadcast.
// It runs on the shared notification-processor goroutine and must never block:
// the status cached by the periodic publisher (warmed in Start before the HTTP
// surface comes up) is sent directly. The empty-cache fallback exists only for
// servers that never ran Start (tests): it computes a fresh status on a separate
// goroutine, with a bounded context tied to the processor lifecycle, and cannot
// guarantee first-message ordering.
func (s *Server) sendInitialNodeStatuses(ctx context.Context, clientCh chan []byte) {
	if status := s.latestNodeStatus.Load(); status != nil {
		s.sendNodeStatusToClient(clientCh, status)
		return
	}

	go func() {
		fetchCtx, cancel := context.WithTimeout(ctx, initialNodeStatusTimeout)
		defer cancel()

		s.sendNodeStatusToClient(clientCh, s.getNodeStatusMessage(fetchCtx))
	}()
}

// sendNodeStatusToClient marshals a node status and sends it to the client's
// buffered channel without blocking, dropping the message if the channel is full.
func (s *Server) sendNodeStatusToClient(clientCh chan []byte, status *notificationMsg) {
	data, err := json.Marshal(status)
	if err != nil {
		s.logger.Errorf("[sendNodeStatusToClient] Failed to marshal current node status: %v", err)
		return
	}

	select {
	case clientCh <- data:
		s.logger.Debugf("[sendNodeStatusToClient] Sent current node status (peer_id: %s) to new client", status.PeerID)
	default:
		s.logger.Warnf("[sendNodeStatusToClient] Failed to send current node status - channel full")
	}
}

func (s *Server) HandleWebSocket(notificationCh chan *notificationMsg) func(c echo.Context) error {
	handler, _ := s.handleWebSocket(notificationCh)

	return handler
}

// handleWebSocket builds the /p2p-ws handler and also returns the connection
// limiter backing it, so tests can assert that slots are actually released
// when a connection ends.
func (s *Server) handleWebSocket(notificationCh chan *notificationMsg) (func(c echo.Context) error, *wsConnLimiter) {
	clientChannels := newClientChannelMap()
	deadClientCh := make(chan chan []byte, 1_000)

	serverCtx := s.gCtx

	go s.startNotificationProcessor(clientChannels, deadClientCh, notificationCh, serverCtx)

	maxConns, maxConnsPerIP := 0, 0

	if s.settings != nil {
		maxConns = s.settings.P2P.WSMaxConnections
		maxConnsPerIP = s.settings.P2P.WSMaxConnectionsPerIP
	}

	wsUpgrader := websocket.Upgrader{
		CheckOrigin: util.WebsocketOriginChecker(s.wsAllowedOrigins()),
	}
	connLimiter := newWSConnLimiter(maxConns, maxConnsPerIP)

	return func(c echo.Context) error {
		release, ok := connLimiter.acquire(c.RealIP())
		if !ok {
			return c.String(http.StatusServiceUnavailable, "too many websocket connections")
		}
		defer release()

		connCtx, connCancel := context.WithCancel(serverCtx)
		defer connCancel()

		ch := make(chan []byte, 100)

		ws, err := wsUpgrader.Upgrade(c.Response(), c.Request(), nil)
		if err != nil {
			return err
		}

		defer ws.Close()

		// Liveness probe: cancels connCtx when the peer stops responding to
		// pings or the socket errors, so a half-open connection can't hold
		// its slot indefinitely.
		go s.startReadPump(ws, connCancel)

		done := make(chan struct{})
		go func() {
			defer close(done)
			s.handleClientMessages(connCtx, ws, ch, deadClientCh)
		}()

		// Send the initial node_status before the client becomes visible to
		// broadcast (i.e. before addClient), so it is guaranteed to be the
		// first message the client ever receives on this connection - a
		// contract consumers (the asset service's centrifuge listener and
		// the dashboard) rely on. See sendInitialNodeStatuses.
		s.sendInitialNodeStatuses(serverCtx, ch)

		// Registered synchronously - before the connection can possibly be
		// torn down - with its cancel func so dropping the client anywhere
		// (broadcast send timeout, dead-client path, shutdown) tears the
		// connection down and releases its slot. Registering here, on the
		// connecting goroutine, rather than asynchronously on the
		// notification-processor goroutine, guarantees insertion strictly
		// precedes any removal; previously the insert only happened when the
		// processor dequeued a registration message, which could run after
		// teardown had already removed the client, leaving an orphan entry
		// (buffered channel plus broadcast slot) that nothing would ever
		// clean up.
		client := &wsClient{ch: ch, cancel: connCancel}
		clientChannels.addClient(client)

		select {
		case <-connCtx.Done():
			// Unblock the writer if it is parked on a stalled WriteMessage.
			ws.Close()
			<-done
		case <-done:
		}

		// Make sure the client is no longer in the broadcast set once the
		// socket is gone; harmless if it was removed already.
		clientChannels.remove(ch)

		return nil
	}, connLimiter
}
