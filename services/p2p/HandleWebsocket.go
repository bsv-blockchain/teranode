// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
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
// The map uses channels as keys to uniquely identify each client connection;
// each channel carries the cancel func of its connection so eviction can tear
// the connection down rather than orphaning it.
//
// All operations on this map are protected by a read-write mutex to ensure
// thread safety when multiple goroutines are adding, removing, or broadcasting
// to client channels concurrently.
type clientChannelMap struct {
	sync.RWMutex                                    // Protects concurrent access to the channels map
	channels     map[chan []byte]context.CancelFunc // Active client channels mapped to their connection cancel funcs
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

// add registers a client channel together with its connection's cancel func
// (may be nil in tests) so a broadcast eviction can close the connection.
func (cm *clientChannelMap) add(ch chan []byte, cancel context.CancelFunc) {
	cm.Lock()
	defer cm.Unlock()
	cm.channels[ch] = cancel
}

func (cm *clientChannelMap) remove(ch chan []byte) {
	cm.Lock()
	defer cm.Unlock()
	delete(cm.channels, ch)
}

// evict removes a client channel and cancels its connection. Without the
// cancel, an evicted-but-protocol-healthy client (draining its socket and
// answering pings, but too slow to consume broadcasts) would stay connected
// forever - subscribed to nothing yet holding a connection slot, and its
// consumer (e.g. the asset-service bridge) would never learn it went mute.
func (cm *clientChannelMap) evict(ch chan []byte) {
	cm.Lock()
	cancel, exists := cm.channels[ch]
	delete(cm.channels, ch)
	cm.Unlock()

	if exists && cancel != nil {
		cancel()
	}
}

func (cm *clientChannelMap) broadcast(data []byte, logger ulogger.Logger) {
	// Get a snapshot of channels under the lock
	cm.RLock()
	channels := make([]chan []byte, 0, len(cm.channels))

	for ch := range cm.channels {
		channels = append(channels, ch)
	}
	cm.RUnlock()

	// Non-blocking send to every client. This runs inline on the single
	// notification processor goroutine, so it must never wait on a client:
	// any wall-clock spent per slow consumer delays every other client's
	// notifications and backs notificationCh up into producer-side drops
	// (previously a client that kept its buffer full cost up to 1s per
	// broadcast, letting a pool of slow readers stall the fan-out forever).
	// The per-client buffer is the entire grace a slow consumer gets: a full
	// buffer means the client is at least that many notifications behind, so
	// it is evicted and its connection closed rather than waited on.
	evicted := 0

	for _, ch := range channels {
		select {
		case ch <- data:
			// Data sent successfully
		default:
			cm.evict(ch)
			evicted++
		}
	}

	// One summary line per broadcast, not one per client: a mass-eviction
	// event (network blip, attacker at the connection cap) must not turn
	// into thousands of synchronous log writes on this hot loop.
	if evicted > 0 {
		initPrometheusMetrics()
		prometheusP2PWebsocketClientsEvicted.Add(float64(evicted))
		logger.Errorf("Evicted %d websocket clients with full send buffers and closed their connections", evicted)
	}
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

	// wsMaxReadBytes caps inbound message size; a message over the limit makes
	// gorilla close the connection (1009). Clients are not expected to send data.
	wsMaxReadBytes int64 = 1024
	// wsMaxInboundFrames caps how many inbound frames (data AND control:
	// pings, pongs) a client may send per pongWait window; the endpoint is
	// publish-only, so an admitted client must not be able to spend server
	// read-path budget without bound.
	wsMaxInboundFrames = 100
	// wsCapWarnInterval throttles the connection-cap rejection warning: at most
	// one WARN per interval so an ongoing saturation stays visible at default
	// log level, without letting an attacker amplify rejections into log spam.
	wsCapWarnInterval = time.Minute
	// wsHandshakeTimeout bounds writing the 101 upgrade response (gorilla
	// applies HandshakeTimeout server-side only to the response write).
	// Reading the request headers is bounded by the HTTP server's
	// ReadHeaderTimeout, set in setupHTTPServer.
	wsHandshakeTimeout = 10 * time.Second
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

// websocketTimeouts returns the effective keepalive parameters, guarding
// against misconfigured overrides: non-positive durations fall back to the
// defaults (a zero pingPeriod would make time.NewTicker panic in the writer
// goroutine, taking the process down), and pingPeriod is clamped below
// pongWait so an override cannot evict every healthy connection each ping
// cycle.
func (s *Server) websocketTimeouts() wsTimeouts {
	def := defaultWSTimeouts()

	to := def
	if s.wsTimeouts != nil {
		to = *s.wsTimeouts
	}

	if to.writeTimeout <= 0 {
		to.writeTimeout = def.writeTimeout
	}

	if to.pongWait <= 0 {
		to.pongWait = def.pongWait
	}

	if to.pingPeriod <= 0 || to.pingPeriod >= to.pongWait {
		to.pingPeriod = to.pongWait * 9 / 10
	}

	// Integer division can still floor to zero for absurdly small pongWait
	// values; keep the ticker period strictly positive no matter what.
	if to.pingPeriod <= 0 {
		to.pingPeriod = to.pongWait
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

// wsConnLimiter admission-controls /p2p-ws connections: a global cap plus a
// per-source cap for untrusted sources (so one host cannot take the whole
// pool), and a trusted-CIDR bypass so internal consumers such as the asset
// service's websocket bridge can always reconnect even while an attacker
// holds every public slot. Sources are keyed by the transport-level
// RemoteAddr host, never by forwarded-for headers, which are caller-supplied.
type wsConnLimiter struct {
	mu                  sync.Mutex
	total               int64            // live connections from untrusted sources
	perSource           map[string]int64 // live connections per untrusted source host
	trustedLive         int64            // live connections admitted via the trusted bypass
	lastBypassWarn      time.Time        // last trusted-bypass overload warning (throttled to wsCapWarnInterval)
	bypassWarnThreshold int64            // live trusted-bypass connections above this trigger the warning
	maxTotal            int64            // <= 0 disables both caps
	maxPerSource        int64
	trusted             []*net.IPNet
	logger              ulogger.Logger
}

// wsTrustNoneSentinel disables the trusted-source bypass entirely when used as
// the sole value of p2p_websocket_trusted_source_cidrs. Needed because the
// settings loader treats an empty configured value as "use the default", so an
// empty list is otherwise inexpressible - e.g. for a reverse proxy terminating
// on the same host, whose loopback source would silently bypass the caps.
const wsTrustNoneSentinel = "none"

// newWSConnLimiter builds the admission controller. perSourceOverride selects
// the per-source cap: 0 derives max(4, maxTotal/20), a positive value is used
// as-is, and a negative value disables the per-source cap - needed when a
// proxy or NAT in front of /p2p-ws funnels many legitimate clients through
// one RemoteAddr host.
func newWSConnLimiter(maxTotal, perSourceOverride int64, trustedCIDRs []string, logger ulogger.Logger) *wsConnLimiter {
	l := &wsConnLimiter{
		maxTotal:  maxTotal,
		perSource: make(map[string]int64),
		logger:    logger,
	}

	switch {
	case perSourceOverride > 0:
		l.maxPerSource = perSourceOverride
	case perSourceOverride == 0 && maxTotal > 0:
		l.maxPerSource = max(4, maxTotal/20)
	default:
		// Negative override, or auto with the global cap disabled: no per-source cap.
	}

	// Warn about a non-binding cap once trusted-bypass connections exceed what
	// any single untrusted source could hold - far earlier than exhaustion.
	l.bypassWarnThreshold = maxTotal
	if l.maxPerSource > 0 && l.maxPerSource < maxTotal {
		l.bypassWarnThreshold = l.maxPerSource
	}

	for _, cidr := range trustedCIDRs {
		cidr = strings.TrimSpace(cidr)
		if cidr == "" {
			continue
		}

		if strings.EqualFold(cidr, wsTrustNoneSentinel) {
			l.trusted = nil
			return l
		}

		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			logger.Warnf("Ignoring invalid websocket trusted source CIDR %q: %v", cidr, err)
			continue
		}

		l.trusted = append(l.trusted, ipNet)
	}

	return l
}

// acquire admits or rejects a connection from remoteAddr. On success the
// returned release func must be called exactly once at connection teardown.
// Trusted sources bypass and are not counted against either cap; if the live
// trusted-bypass population ever exceeds the global cap, a one-time warning
// flags that the caps are not binding (typically a reverse proxy fronting
// /p2p-ws from a trusted address, hiding the real client IPs).
func (l *wsConnLimiter) acquire(remoteAddr string) (release func(), ok bool) {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}

	if ip := net.ParseIP(host); ip != nil {
		for _, ipNet := range l.trusted {
			if ipNet.Contains(ip) {
				l.mu.Lock()
				l.trustedLive++

				// Throttled (not once-per-process) so a genuine "caps are not
				// binding" condition stays visible for the process lifetime.
				// The threshold is the per-source budget an untrusted host
				// would get, so a fronting proxy surfaces long before it
				// could exhaust the global cap.
				if now := time.Now(); l.bypassWarnThreshold > 0 && l.trustedLive > l.bypassWarnThreshold && now.Sub(l.lastBypassWarn) >= wsCapWarnInterval {
					l.lastBypassWarn = now
					l.logger.Warnf("Live /p2p-ws connections admitted via the trusted-source bypass (%d) exceed the per-source budget an untrusted host would get (%d); if a reverse proxy fronts this endpoint from a trusted address, the connection caps are not protecting it - preserve client source addresses, narrow p2p_websocket_trusted_source_cidrs, or set it to %q", l.trustedLive, l.bypassWarnThreshold, wsTrustNoneSentinel)
				}
				l.mu.Unlock()

				return func() {
					l.mu.Lock()
					defer l.mu.Unlock()
					l.trustedLive--
				}, true
			}
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.maxTotal > 0 && l.total >= l.maxTotal {
		return nil, false
	}

	if l.maxPerSource > 0 && l.perSource[host] >= l.maxPerSource {
		return nil, false
	}

	l.total++
	l.perSource[host]++

	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()

		l.total--
		if l.perSource[host] <= 1 {
			delete(l.perSource, host)
		} else {
			l.perSource[host]--
		}
	}, true
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
			// Debugf: this fires on every normal disconnect (the teardown
			// cancels the per-connection context), not just at shutdown.
			s.logger.Debugf("Closing WebSocket connection due to context cancellation")
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

// initialNodeStatusTimeout bounds the blockchain gRPC round-trips made when a
// client connects before the first periodic node-status publish has cached one.
const initialNodeStatusTimeout = 5 * time.Second

// sendInitialNodeStatuses sends the current node's status to a newly connected
// client. Consumers (the asset service's centrifuge listener and the dashboard)
// pin the current node's identity to the FIRST node_status they receive, so this
// message must reach the client before any remote peer's node_status broadcast.
// It is called synchronously by the connection handler before registering the
// client for broadcasts and must never block: the status cached by the periodic
// publisher (warmed in Start before the HTTP surface comes up) is sent directly.
// The empty-cache fallback exists only for servers that never ran Start (tests):
// it computes a fresh status on a separate goroutine, with a bounded context
// tied to the connection lifecycle, and cannot guarantee first-message ordering.
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
	clientChannels := newClientChannelMap()

	serverCtx := s.gCtx

	go s.startNotificationProcessor(clientChannels, notificationCh, serverCtx)

	var (
		allowedOrigins []string
		maxConns       int64
		perSourceCap   int64
		trustedCIDRs   []string
	)

	if s.settings != nil {
		allowedOrigins = s.settings.P2P.WebSocketAllowedOrigins
		maxConns = int64(s.settings.P2P.WebSocketMaxConnections)
		perSourceCap = int64(s.settings.P2P.WebSocketMaxConnectionsPerSource)
		trustedCIDRs = s.settings.P2P.WebSocketTrustedSourceCIDRs
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

	limiter := newWSConnLimiter(maxConns, perSourceCap, trustedCIDRs, s.logger)

	// Rejections warn at most once per interval so an ongoing saturation event
	// stays visible at default log level for the life of the process, while an
	// attacker hammering the endpoint can't amplify rejections into log spam.
	var lastCapWarnNanos atomic.Int64

	to := s.websocketTimeouts()

	return func(c echo.Context) error {
		// Cap concurrent connections before upgrading so an attacker can't
		// exhaust goroutines/file descriptors by opening sockets. RemoteAddr
		// (not RealIP) on purpose: forwarded-for headers are caller-supplied.
		release, ok := limiter.acquire(c.Request().RemoteAddr)
		if !ok {
			now := time.Now().UnixNano()
			if last := lastCapWarnNanos.Load(); now-last >= wsCapWarnInterval.Nanoseconds() && lastCapWarnNanos.CompareAndSwap(last, now) {
				s.logger.Warnf("Rejecting websocket connection from %s: connection limit reached (max %d total, %d per source; further rejections logged at debug for up to %s)", c.Request().RemoteAddr, limiter.maxTotal, limiter.maxPerSource, wsCapWarnInterval)
			} else {
				s.logger.Debugf("Rejecting websocket connection from %s: connection limit reached", c.Request().RemoteAddr)
			}

			return echo.NewHTTPError(http.StatusServiceUnavailable, "websocket connection limit reached")
		}
		defer release()

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
		// Inbound frames are tolerated (the dashboard's wstest page sends a
		// data frame) but budgeted per pongWait window - the endpoint is
		// publish-only, so a client streaming frames at line rate is abuse.
		// The budget covers control frames too: gorilla answers pings and
		// consumes pongs internally without ever returning from ReadMessage,
		// so without counting them a ping/pong flood would spend read-path
		// budget (and, for pongs, defeat the idle timeout) unmetered.
		//
		// The budget state is mutated only on the read goroutine - gorilla
		// invokes the ping/pong handlers from within ReadMessage - so it
		// needs no locking.
		inbound, windowEnd := 0, time.Now().Add(to.pongWait)
		overBudget := func() bool {
			if now := time.Now(); now.After(windowEnd) {
				inbound, windowEnd = 0, now.Add(to.pongWait)
			}

			inbound++

			return inbound > wsMaxInboundFrames
		}

		ws.SetReadLimit(wsMaxReadBytes)
		_ = ws.SetReadDeadline(time.Now().Add(to.pongWait))

		ws.SetPongHandler(func(string) error {
			if overBudget() {
				return errors.NewProcessingError("websocket inbound frame budget exceeded")
			}

			return ws.SetReadDeadline(time.Now().Add(to.pongWait))
		})

		// Mirrors gorilla's default ping handler (reply with a pong echoing the
		// payload, tolerate a concurrently sent close), plus the budget check.
		ws.SetPingHandler(func(appData string) error {
			if overBudget() {
				return errors.NewProcessingError("websocket inbound frame budget exceeded")
			}

			err := ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(to.writeTimeout))
			if err == websocket.ErrCloseSent {
				return nil
			}

			return err
		})

		go func() {
			defer close(readDone)

			for {
				if _, _, err := ws.ReadMessage(); err != nil {
					return
				}

				if overBudget() {
					s.logger.Debugf("Websocket client exceeded inbound frame budget, closing connection")
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
		// warmed cache makes this synchronous and free of blockchain I/O; the
		// cold-cache fallback bounds its own lookup with a timeout derived
		// from connCtx, so a hung backend cannot wedge connection setup.
		s.sendInitialNodeStatuses(connCtx, ch)

		clientChannels.add(ch, connCancel)

		select {
		case <-connCtx.Done():
		case <-writeDone:
		case <-readDone:
		}

		return nil
	}
}
