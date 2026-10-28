package probe

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/services/probe/guard"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

const (
	maxWSClients         = 16
	wsQueueSize          = 4
	wsReadLimit          = 1024
	wsCloseUnauthorized  = 4401
	defaultWSAuthTimeout = 5 * time.Second
	defaultWSWriteWait   = 5 * time.Second
	wsPingEvery          = 20 * time.Second
	wsPongWait           = 50 * time.Second
	wsCloseWait          = time.Second
)

type Hub struct {
	updates   <-chan snapshot.Snapshot
	logger    ulogger.Logger
	faults    *faultCounter
	fault     func(point string)
	writeWait time.Duration
	pingEvery time.Duration
	slots     chan struct{}

	mu      sync.Mutex
	clients map[*wsClient]struct{}
	latest  wsMessage
	seq     uint64
	closed  bool
}

type wsMessage struct {
	seq uint64
	raw []byte
}

type wsClient struct {
	conn *websocket.Conn
	send chan wsMessage
	done chan struct{}
	once sync.Once
}

type authMessage struct {
	Auth *string `json:"auth"`
}

func NewHub(updates <-chan snapshot.Snapshot) *Hub {
	h := &Hub{
		updates:   updates,
		logger:    ulogger.New("probe"),
		fault:     func(string) {},
		writeWait: defaultWSWriteWait,
		pingEvery: wsPingEvery,
		slots:     make(chan struct{}, maxWSClients),
		clients:   map[*wsClient]struct{}{},
	}

	h.faults = newFaultCounter(groupHub, h.logger, newStopSignal())

	return h
}

func (h *Hub) Run(ctx context.Context) {
	for !h.runOnce(ctx) {
		if ctx.Err() != nil || h.faults.signal.stopped() {
			h.closeAll()

			return
		}
	}
}

func (h *Hub) runOnce(ctx context.Context) (done bool) {
	seq := h.currentSeq()

	defer guard.Recover(guard.RecorderFunc(func(component string, value any) {
		h.faults.failedAt(seq, component, value)
	}), "websocket hub")

	for {
		select {
		case <-ctx.Done():
			h.closeAll()

			return true
		case snap := <-h.updates:
			seq = h.nextSeq()
			h.fault(faultHub)

			raw, err := json.Marshal(snap)
			if err != nil {
				h.logger.Errorf("[Probe] could not encode snapshot for websocket clients: %s", oneLine(err))

				continue
			}

			h.broadcast(wsMessage{seq: seq, raw: raw})
		}
	}
}

func (h *Hub) nextSeq() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.seq++

	return h.seq
}

func (h *Hub) currentSeq() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.seq
}

func (h *Hub) broadcast(msg wsMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.latest = msg

	for c := range h.clients {
		c.enqueue(msg)
	}
}

func (h *Hub) acquire() bool {
	select {
	case h.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (h *Hub) release() {
	<-h.slots
}

func (h *Hub) register(conn *websocket.Conn) (*wsClient, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil, false
	}

	c := &wsClient{conn: conn, send: make(chan wsMessage, wsQueueSize), done: make(chan struct{})}
	h.clients[c] = struct{}{}

	if h.latest.raw != nil {
		c.enqueue(h.latest)
	}

	var current uint64

	go func() {
		defer guard.Recover(guard.RecorderFunc(func(component string, value any) {
			h.remove(c)

			seq := current
			if seq == 0 {
				seq = h.currentSeq()
			}

			h.faults.failedAt(seq, component, value)
		}), "websocket client writer")

		h.writeLoop(c, &current)
	}()

	return c, true
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()

	c.close()
}

func (h *Hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.clients)
}

func (h *Hub) closeAll() {
	h.mu.Lock()
	h.closed = true
	clients := make([]*wsClient, 0, len(h.clients))

	for c := range h.clients {
		clients = append(clients, c)
	}

	h.clients = map[*wsClient]struct{}{}
	h.mu.Unlock()

	for _, c := range clients {
		_ = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "probe stopping"), time.Now().Add(wsCloseWait))
		c.close()
	}
}

func (h *Hub) writeLoop(c *wsClient, current *uint64) {
	ticker := time.NewTicker(h.pingEvery)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case msg := <-c.send:
			*current = msg.seq
			h.fault(faultClientWrite)

			if err := c.conn.SetWriteDeadline(time.Now().Add(h.writeWait)); err != nil {
				h.remove(c)

				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, msg.raw); err != nil {
				h.remove(c)

				return
			}

			h.faults.succeededAt(msg.seq)
		case <-ticker.C:
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(h.writeWait)); err != nil {
				h.remove(c)

				return
			}
		}
	}
}

func (h *Hub) readLoop(c *wsClient) {
	defer h.remove(c)

	extend := func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	}

	if err := extend(""); err != nil {
		return
	}

	c.conn.SetPongHandler(extend)

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}

		if err := extend(""); err != nil {
			return
		}
	}
}

func (c *wsClient) enqueue(msg wsMessage) {
	select {
	case c.send <- msg:
		return
	default:
	}

	select {
	case <-c.send:
	default:
	}

	select {
	case c.send <- msg:
	default:
	}
}

func (c *wsClient) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

func sameOrigin(r *http.Request) bool {
	origin, present := r.Header["Origin"]
	if !present || len(origin) == 0 {
		return true
	}

	u, err := url.Parse(origin[0])
	if err != nil || u.Host == "" {
		return false
	}

	return strings.EqualFold(u.Host, r.Host)
}

func (s *Server) handleWS(c echo.Context) error {
	run := s.current()
	if run == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	hub := run.hub

	if !hub.acquire() {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	defer hub.release()

	upgrader := websocket.Upgrader{CheckOrigin: sameOrigin}

	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return nil
	}

	conn.SetReadLimit(wsReadLimit)

	if !s.wsAuthenticate(conn) {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(wsCloseUnauthorized, "unauthorized"), time.Now().Add(wsCloseWait))
		_ = conn.Close()

		return nil
	}

	client, ok := hub.register(conn)
	if !ok {
		_ = conn.Close()

		return nil
	}

	hub.readLoop(client)

	return nil
}

func (s *Server) wsAuthenticate(conn *websocket.Conn) bool {
	expected := []byte(s.settings.Probe.AuthToken)
	if len(expected) == 0 {
		return false
	}

	if err := conn.SetReadDeadline(time.Now().Add(s.wsAuthTimeout)); err != nil {
		return false
	}

	kind, raw, err := conn.ReadMessage()
	if err != nil || kind != websocket.TextMessage {
		return false
	}

	var msg authMessage
	if err := json.Unmarshal(raw, &msg); err != nil || msg.Auth == nil {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(*msg.Auth), expected) == 1
}
