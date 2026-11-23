// Package centrifuge_impl provides WebSocket handling capabilities for the Centrifuge server,
// enabling real-time bidirectional communication between clients and the server.
package centrifuge_impl

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/centrifugal/centrifuge"
	"github.com/gorilla/websocket"
)

// Defaults.
const (
	DefaultWebsocketPingInterval     = 25 * time.Second
	DefaultWebsocketWriteTimeout     = 1 * time.Second
	DefaultWebsocketMessageSizeLimit = 65536 // 64KB
)

// WebsocketConfig represents config for WebsocketHandler.
type WebsocketConfig struct {
	// CompressionLevel sets a level for websocket compression.
	// See possible value description at https://golang.org/pkg/compress/flate/#NewWriter
	CompressionLevel int

	// CompressionMinSize allows setting minimal limit in bytes for
	// message to use compression when writing it into client connection.
	// By default, it's 0 - i.e. all messages will be compressed when
	// WebsocketCompression enabled and compression negotiated with client.
	CompressionMinSize int

	// ReadBufferSize is a parameter that is used for raw websocket Upgrader.
	// If set to zero reasonable default value will be used.
	ReadBufferSize int

	// WriteBufferSize is a parameter that is used for raw websocket Upgrader.
	// If set to zero reasonable default value will be used.
	WriteBufferSize int

	// MessageSizeLimit sets the maximum size in bytes of allowed message from client.
	// By default DefaultWebsocketMaxMessageSize will be used.
	MessageSizeLimit int

	// CheckOrigin func to provide custom origin check logic.
	// nil means same-host origins only (see checkOrigin).
	CheckOrigin func(r *http.Request) bool

	// PingInterval sets interval server will send ping messages to clients.
	// By default DefaultPingInterval will be used.
	PingInterval time.Duration

	// WriteTimeout is maximum time of write message operation.
	// Slow client will be disconnected.
	// By default DefaultWebsocketWriteTimeout will be used.
	WriteTimeout time.Duration

	// Compression allows to enable websocket permessage-deflate
	// compression support for raw websocket connections. It does
	// not guarantee that compression will be used - i.e. it only
	// says that server will try to negotiate it with client.
	Compression bool

	// MaxConnections caps the number of concurrently live websocket connections.
	// Zero means unlimited.
	MaxConnections int

	// MaxConnectionsPerIP caps the number of concurrently live websocket connections
	// held by a single client IP, so MaxConnections cannot be exhausted by one
	// client. Zero means unlimited. Ignored when ClientIP is nil and the request
	// has no usable RemoteAddr.
	MaxConnectionsPerIP int

	// ClientIP extracts the client IP from a request for the MaxConnectionsPerIP
	// budget. nil falls back to the request's RemoteAddr.
	ClientIP func(r *http.Request) string

	// UseWriteBufferPool enables using buffer pool for writes.
	UseWriteBufferPool bool
}

// WebsocketHandler handles WebSocket client connections. WebSocket protocol
// is a bidirectional connection between a client an a server for low-latency
// communication.
type WebsocketHandler struct {
	node    *centrifuge.Node
	upgrade *websocket.Upgrader
	config  WebsocketConfig

	liveConnections    atomic.Int64 // connections currently holding a slot
	refusedConnections atomic.Int64 // upgrades refused because the cap was full
	capWarnedAt        atomic.Int64 // UnixNano of the last cap-reached WARN, 0 = never

	perIPMu    sync.Mutex     // guards perIPConns
	perIPConns map[string]int // live connection count per client IP, only touched when MaxConnectionsPerIP > 0
}

// connCapWarnInterval bounds how often a refused upgrade is logged at WARN, so a client
// retrying in a loop cannot flood the log.
const connCapWarnInterval = 1 * time.Minute

var writeBufferPool = &sync.Pool{}

// NewWebsocketHandler creates a new WebSocket handler with the specified configuration.
//
// Parameters:
//   - node: Centrifuge node instance
//   - config: WebSocket configuration options
//
// Returns:
//   - http.Handler: Configured WebSocket handler
func NewWebsocketHandler(n *centrifuge.Node, c WebsocketConfig) *WebsocketHandler {
	upgrade := &websocket.Upgrader{
		ReadBufferSize:    c.ReadBufferSize,
		EnableCompression: c.Compression,
		// Advertise the Centrifuge protobuf subprotocol so a client requesting it
		// (Sec-WebSocket-Protocol: centrifuge-protobuf) negotiates protobuf rather
		// than being silently served JSON. Clients that request no subprotocol
		// (e.g. the dashboard's raw WebSocket) continue to use JSON. Matches the
		// upstream centrifuge WebsocketHandler.
		Subprotocols: []string{"centrifuge-protobuf"},
	}
	if c.UseWriteBufferPool {
		upgrade.WriteBufferPool = writeBufferPool
	} else {
		upgrade.WriteBufferSize = c.WriteBufferSize
	}

	if c.CheckOrigin != nil {
		upgrade.CheckOrigin = c.CheckOrigin
	} else {
		upgrade.CheckOrigin = originCheck(nil)
	}

	return &WebsocketHandler{
		node:       n,
		config:     c,
		upgrade:    upgrade,
		perIPConns: make(map[string]int),
	}
}

// ServeHTTP implements the http.Handler interface for WebSocket connections.
// It handles the WebSocket upgrade process and manages the connection lifecycle.
//
// Parameters:
//   - rw: HTTP response writer
//   - r: HTTP request
func (s *WebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	compression := s.config.Compression
	compressionLevel := s.config.CompressionLevel
	compressionMinSize := s.config.CompressionMinSize

	ip := s.clientIP(r)

	// Take an admission slot before the upgrade: an upgrade that is accepted and then
	// closed still costs a handshake and leaves the client believing it is connected.
	if ok, capSetting, capLimit := s.acquireConnection(ip); !ok {
		s.logConnectionRefused(capSetting, capLimit)
		rw.Header().Set("Retry-After", "1")
		http.Error(rw, "Asset websocket connection limit reached", http.StatusServiceUnavailable)

		return
	}

	conn, err := s.upgrade.Upgrade(rw, r, nil)
	if err != nil {
		s.releaseConnection(ip)
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "[Centrifuge] websocket upgrade error", map[string]any{"error": err.Error()}))

		return
	}

	if compression {
		err := conn.SetCompressionLevel(compressionLevel)
		if err != nil {
			s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "[Centrifuge] websocket error setting compression level", map[string]any{"error": err.Error()}))
		}
	}

	pingInterval := s.config.PingInterval
	if pingInterval == 0 {
		pingInterval = DefaultWebsocketPingInterval
	}

	writeTimeout := s.config.WriteTimeout
	if writeTimeout == 0 {
		writeTimeout = DefaultWebsocketWriteTimeout
	}

	messageSizeLimit := s.config.MessageSizeLimit
	if messageSizeLimit == 0 {
		messageSizeLimit = DefaultWebsocketMessageSizeLimit
	}

	if messageSizeLimit > 0 {
		conn.SetReadLimit(int64(messageSizeLimit))
	}

	if pingInterval > 0 {
		pongWait := pingInterval * 10 / 9
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(pongWait))
			return nil
		})
	}

	// Negotiate the protocol from the subprotocol the upgrader selected. Defaults
	// to JSON (no/other subprotocol); protobuf only when the client requested and
	// the upgrader echoed "centrifuge-protobuf". The transport then reads AND
	// writes in the matching codec — hardcoding JSON here would decode a protobuf
	// client's frames as JSON, disconnecting it with "bad request".
	protoType := centrifuge.ProtocolTypeJSON
	if conn.Subprotocol() == "centrifuge-protobuf" {
		protoType = centrifuge.ProtocolTypeProtobuf
	}

	// Separate goroutine for better GC of caller's data.
	go func() {
		// This goroutine owns the connection lifetime, so it also owns the slot.
		defer s.releaseConnection(ip)

		opts := websocketTransportOptions{
			pingInterval:       pingInterval,
			writeTimeout:       writeTimeout,
			compressionMinSize: compressionMinSize,
			protoType:          protoType,
		}

		graceCh := make(chan struct{})
		transport := newWebsocketTransport(conn, opts, graceCh)

		select {
		case <-s.node.NotifyShutdown():
			_ = transport.Close(centrifuge.DisconnectShutdown)
			return
		default:
		}

		ctxCh := make(chan struct{})
		defer close(ctxCh)

		// Detach cancellation: r.Context() is cancelled when ServeHTTP returns,
		// which is right after this goroutine launches. The centrifuge core
		// checks ctx.Done() inside HandleCommand and aborts every frame if the
		// context is dead — so we keep request-scoped values (notably the
		// Credentials set by readinessMiddleware) but strip the cancellation signal.
		// The connection lifetime is owned by closeFn / the read loop below.
		c, closeFn, err := centrifuge.NewClient(context.WithoutCancel(r.Context()), s.node, transport)
		if err != nil {
			s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "[Centrifuge] error creating client", map[string]any{"transport": transport.Name()}))
			return
		}

		defer func() { _ = closeFn() }()

		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "[Centrifuge] client connection established", map[string]any{"client": c.ID(), "transport": transport.Name()}))

		defer func(started time.Time) {
			s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "[Centrifuge] client connection completed", map[string]any{"client": c.ID(), "transport": transport.Name(), "duration": time.Since(started)}))
		}(time.Now())

		for {
			_, reader, err := conn.NextReader()
			if err != nil {
				break
			}

			if proceed := centrifuge.HandleReadFrame(c, reader); !proceed {
				break
			}
		}

		// https://github.com/gorilla/websocket/issues/448
		conn.SetPingHandler(nil)
		conn.SetPongHandler(nil)
		conn.SetCloseHandler(nil)
		_ = conn.SetReadDeadline(time.Now().Add(closeFrameWait))

		for {
			if _, _, err := conn.NextReader(); err != nil {
				close(graceCh)
				break
			}
		}
	}()
}

// clientIP extracts the client IP for the MaxConnectionsPerIP budget, using
// s.config.ClientIP when set (so it inherits the same trusted-proxy configuration
// as the rest of the Asset HTTP server) and falling back to the request's
// RemoteAddr otherwise.
func (s *WebsocketHandler) clientIP(r *http.Request) string {
	if s.config.ClientIP != nil {
		return s.config.ClientIP(r)
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}

// acquireConnection takes a slot from the concurrent-connection budget and, if
// MaxConnectionsPerIP is set, from ip's own share of it, reporting whether both were
// available and, on refusal, which setting's cap fired and its limit. A
// MaxConnections/MaxConnectionsPerIP of zero means unlimited, which is the shipped
// default and today's behaviour.
func (s *WebsocketHandler) acquireConnection(ip string) (ok bool, capSetting string, capLimit int) {
	limit := s.config.MaxConnections
	if limit > 0 {
		if s.liveConnections.Add(1) > int64(limit) {
			s.liveConnections.Add(-1)
			s.refusedConnections.Add(1)

			return false, "asset_maxWebsocketConnections", limit
		}
	}

	if !s.acquirePerIP(ip) {
		if limit > 0 {
			s.liveConnections.Add(-1)
		}

		s.refusedConnections.Add(1)

		return false, "asset_maxWebsocketConnectionsPerIP", s.config.MaxConnectionsPerIP
	}

	return true, "", 0
}

// acquirePerIP takes a slot from ip's share of MaxConnectionsPerIP, reporting whether
// one was available. A MaxConnectionsPerIP of zero, or an empty ip, means unlimited.
func (s *WebsocketHandler) acquirePerIP(ip string) bool {
	limit := s.config.MaxConnectionsPerIP
	if limit <= 0 || ip == "" {
		return true
	}

	s.perIPMu.Lock()
	defer s.perIPMu.Unlock()

	if s.perIPConns[ip] >= limit {
		return false
	}

	s.perIPConns[ip]++

	return true
}

// releaseConnection returns a slot taken by acquireConnection, including ip's
// per-IP share. It is called exactly once per accepted upgrade, from whichever path
// ends that connection.
func (s *WebsocketHandler) releaseConnection(ip string) {
	if s.config.MaxConnections > 0 {
		s.liveConnections.Add(-1)
	}

	s.releasePerIP(ip)
}

// releasePerIP returns a slot taken by acquirePerIP.
func (s *WebsocketHandler) releasePerIP(ip string) {
	if s.config.MaxConnectionsPerIP <= 0 || ip == "" {
		return
	}

	s.perIPMu.Lock()
	defer s.perIPMu.Unlock()

	if n := s.perIPConns[ip]; n <= 1 {
		delete(s.perIPConns, ip)
	} else {
		s.perIPConns[ip] = n - 1
	}
}

// logConnectionRefused reports a refused upgrade, at WARN at most once per
// connCapWarnInterval and at DEBUG in between, so the cap being reached is always
// visible without a retrying client being able to flood the log.
func (s *WebsocketHandler) logConnectionRefused(capSetting string, capLimit int) {
	fields := map[string]any{"limit": capLimit, "refused": s.refusedConnections.Load()}
	msg := "[Centrifuge] websocket upgrade refused: " + capSetting + " reached"

	last, now := s.capWarnedAt.Load(), time.Now().UnixNano()
	if now-last >= int64(connCapWarnInterval) && s.capWarnedAt.CompareAndSwap(last, now) {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, msg, fields))
		return
	}

	s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, msg, fields))
}

// websocketTransport implements the transport layer for WebSocket connections.
// It handles the low-level WebSocket communication details.
type websocketTransport struct {
	mu        sync.RWMutex
	writeMu   sync.Mutex // sync general write with unidirectional ping write.
	conn      *websocket.Conn
	closed    bool
	closeCh   chan struct{}
	graceCh   chan struct{}
	opts      websocketTransportOptions
	pingTimer *time.Timer
}

type websocketTransportOptions struct {
	protoType          centrifuge.ProtocolType
	pingInterval       time.Duration
	writeTimeout       time.Duration
	compressionMinSize int
}

func newWebsocketTransport(conn *websocket.Conn, opts websocketTransportOptions, graceCh chan struct{}) *websocketTransport {
	transport := &websocketTransport{
		conn:    conn,
		closeCh: make(chan struct{}),
		graceCh: graceCh,
		opts:    opts,
	}
	if opts.pingInterval > 0 {
		transport.addPing()
	}

	return transport
}

func (t *websocketTransport) ping() {
	select {
	case <-t.closeCh:
		return
	default:
		deadline := time.Now().Add(t.opts.pingInterval / 2)

		err := t.conn.WriteControl(websocket.PingMessage, nil, deadline)
		if err != nil {
			_ = t.Close(centrifuge.DisconnectWriteError)
			return
		}

		t.addPing()
	}
}

func (t *websocketTransport) addPing() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}

	t.pingTimer = time.AfterFunc(t.opts.pingInterval, t.ping)
	t.mu.Unlock()
}

// Name returns the transport name ("websocket").
func (t *websocketTransport) Name() string {
	return "websocket"
}

// Protocol returns transport protocol.
func (t *websocketTransport) Protocol() centrifuge.ProtocolType {
	return t.opts.protoType
}

// ProtocolVersion returns transport ProtocolVersion.
func (t *websocketTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	return centrifuge.ProtocolVersion2
}

// Unidirectional returns whether transport is unidirectional. WebSocket is
// inherently bidirectional, so this returns false — the centrifuge core then
// emits connect replies in the command-reply envelope (with the client's
// command Id) rather than as Push frames. See issue #938.
func (t *websocketTransport) Unidirectional() bool {
	return false
}

// Emulation ...
func (t *websocketTransport) Emulation() bool {
	return false
}

// DisabledPushFlags ...
func (t *websocketTransport) DisabledPushFlags() uint64 {
	return 0
}

// PingPongConfig ...
func (t *websocketTransport) PingPongConfig() centrifuge.PingPongConfig {
	return centrifuge.PingPongConfig{
		PingInterval: DefaultWebsocketPingInterval,
		PongTimeout:  DefaultWebsocketPingInterval / 3,
	}
}

// writeData writes data to the WebSocket connection with proper message type and compression.
//
// Parameters:
//   - data: Data to write
//
// Returns:
//   - error: Any error encountered during writing
func (t *websocketTransport) writeData(data []byte) error {
	if t.opts.compressionMinSize > 0 {
		t.conn.EnableWriteCompression(len(data) > t.opts.compressionMinSize)
	}

	var messageType = websocket.TextMessage
	if t.Protocol() == centrifuge.ProtocolTypeProtobuf {
		messageType = websocket.BinaryMessage
	}

	t.writeMu.Lock()
	if t.opts.writeTimeout > 0 {
		_ = t.conn.SetWriteDeadline(time.Now().Add(t.opts.writeTimeout))
	}

	err := t.conn.WriteMessage(messageType, data)
	if err != nil {
		t.writeMu.Unlock()
		return err
	}

	if t.opts.writeTimeout > 0 {
		_ = t.conn.SetWriteDeadline(time.Time{})
	}
	t.writeMu.Unlock()

	return nil
}

// Write sends a single message through the WebSocket connection.
//
// Parameters:
//   - message: Data to send
//
// Returns:
//   - error: Any error encountered during writing
func (t *websocketTransport) Write(message []byte) error {
	return t.WriteMany(message)
}

// WriteMany sends multiple messages through the WebSocket connection.
//
// Parameters:
//   - messages: Multiple messages to send
//
// Returns:
//   - error: Any error encountered during writing
func (t *websocketTransport) WriteMany(messages ...[]byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		for i := 0; i < len(messages); i++ {
			err := t.writeData(messages[i])
			if err != nil {
				return err
			}
		}

		return nil
	}
}

const closeFrameWait = 5 * time.Second

// Close gracefully closes the WebSocket transport.
//
// Parameters:
//   - disconnect: Reason for disconnection
//
// Returns:
//   - error: Any error encountered during closure
func (t *websocketTransport) Close(_ centrifuge.Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}

	t.closed = true
	if t.pingTimer != nil {
		t.pingTimer.Stop()
	}

	close(t.closeCh)
	t.mu.Unlock()

	return t.conn.Close()
}

// originCheck returns a websocket upgrader CheckOrigin func backed by checkOrigin.
//
// Parameters:
//   - allowedOrigins: extra origins accepted besides the same host; "*" accepts any origin
//
// Returns:
//   - func(r *http.Request) bool: Origin checking function
func originCheck(allowedOrigins []string) func(r *http.Request) bool {
	return func(r *http.Request) bool {
		return checkOrigin(r, allowedOrigins) == nil
	}
}

// checkOrigin guards the websocket upgrade against cross-site browser connections.
//
// A request is accepted when:
//   - it carries no Origin header (non-browser clients do not send one),
//   - the Origin hostname equals the request Host hostname, ignoring scheme and port
//     (the dashboard dev server and a TLS reverse proxy both serve the page from a
//     different port than the Asset HTTP socket),
//   - both hostnames are loopback (localhost, 127.0.0.0/8, ::1), which are the same
//     machine however they are spelled, or
//   - the Origin matches an entry in allowedOrigins, or allowedOrigins contains "*".
//
// This is not authentication: a non-browser client can omit or forge Origin. It stops
// a third-party web page from opening the socket from a visitor's browser. It does not
// survive DNS rebinding: a page on a name the attacker controls that resolves to this
// node sends a matching Host and Origin.
//
// Parameters:
//   - r: HTTP request to check
//   - allowedOrigins: extra origins accepted besides the same host, already normalised
//     by parseAllowedOrigins (trimmed, no trailing slash)
//
// Returns:
//   - error: Error if origin check fails
func checkOrigin(r *http.Request, allowedOrigins []string) error {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return nil
	}

	for _, allowed := range allowedOrigins {
		if allowed == "*" || strings.EqualFold(allowed, origin) {
			return nil
		}
	}

	u, err := url.Parse(origin)
	if err != nil || u.Hostname() == "" {
		return errors.NewServiceError("request Origin %q is not a valid origin", origin)
	}

	requestHost := r.Host
	if h, _, splitErr := net.SplitHostPort(requestHost); splitErr == nil {
		requestHost = h
	}

	requestHost = strings.TrimSuffix(strings.TrimPrefix(requestHost, "["), "]")

	if requestHost != "" && (strings.EqualFold(u.Hostname(), requestHost) || isLoopbackHost(u.Hostname()) && isLoopbackHost(requestHost)) {
		return nil
	}

	return errors.NewServiceError("request Origin %q is not authorized for Host %q", origin, r.Host)
}

// isLoopbackHost reports whether host is "localhost" or a loopback IP literal.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}

	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}
