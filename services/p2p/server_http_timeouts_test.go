package p2p

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// TestSetupHTTPServerTimeouts guards against the HTTP server regressing to a
// zero-value http.Server with unlimited timeouts (slowloris / fd exhaustion).
func TestSetupHTTPServerTimeouts(t *testing.T) {
	s := &Server{
		gCtx:   t.Context(),
		logger: &ulogger.TestLogger{},
	}

	e := s.setupHTTPServer()

	require.Equal(t, httpReadHeaderTimeout, e.Server.ReadHeaderTimeout, "ReadHeaderTimeout must be set to bound the header phase")
	require.Equal(t, httpReadTimeout, e.Server.ReadTimeout, "ReadTimeout must be set to bound the request read")
	require.Equal(t, httpWriteTimeout, e.Server.WriteTimeout, "WriteTimeout must be set to bound the response write")
	require.Equal(t, httpIdleTimeout, e.Server.IdleTimeout, "IdleTimeout must be set to reap idle keep-alive connections")

	// Sanity relationships: values that individually pass NotZero could still
	// be nonsensical (e.g. header timeout longer than the whole read budget).
	require.LessOrEqual(t, e.Server.ReadHeaderTimeout, e.Server.ReadTimeout, "header phase must fit inside the request read budget")
	require.GreaterOrEqual(t, e.Server.IdleTimeout, e.Server.ReadTimeout, "keep-alive reuse should outlive a single request read")
}

// TestHTTPServerClosesSlowHeaderClient opens a raw connection, sends a partial
// request and never completes the headers, then asserts the server closes the
// connection once ReadHeaderTimeout fires instead of holding it open forever.
// The timeouts are shortened to keep the test fast; that the fields are set at
// all is guarded by TestSetupHTTPServerTimeouts.
func TestHTTPServerClosesSlowHeaderClient(t *testing.T) {
	s := &Server{
		gCtx:   t.Context(),
		logger: &ulogger.TestLogger{},
	}

	e := s.setupHTTPServer()
	e.Server.ReadHeaderTimeout = 200 * time.Millisecond
	e.Server.ReadTimeout = 200 * time.Millisecond

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = e.Server.Serve(listener)
	}()

	defer func() {
		_ = e.Close()
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	defer conn.Close()

	// Partial request: request line plus one header, never the terminating CRLF.
	_, err = conn.Write([]byte("GET /health HTTP/1.1\r\nHost: teranode\r\n"))
	require.NoError(t, err)

	// Well past the shortened ReadHeaderTimeout; only hit if the server
	// never closes the connection.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))

	// Drain until the server closes the connection. io.Copy returns nil on a
	// clean EOF; a reset is also acceptable. A read deadline hit means the
	// server held the slow connection open, which is the bug.
	_, err = io.Copy(io.Discard, conn)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Fatal("server held the slow-header connection open past ReadHeaderTimeout")
		}
	}
}

// TestWebsocketSurvivesHTTPServerTimeouts proves the risky half of the change:
// the http.Server timeouts must not apply to an established /p2p-ws stream
// (net/http clears the connection deadlines on Hijack). The timeouts are
// shortened so an affected stream would die within the test's window.
func TestWebsocketSurvivesHTTPServerTimeouts(t *testing.T) {
	s := &Server{
		gCtx:           t.Context(),
		logger:         &ulogger.TestLogger{},
		settings:       test.CreateBaseTestSettings(t),
		notificationCh: make(chan *notificationMsg, 10),
		startTime:      time.Now(),
	}

	e := s.setupHTTPServer()
	e.Server.ReadHeaderTimeout = 300 * time.Millisecond
	e.Server.ReadTimeout = 300 * time.Millisecond
	e.Server.WriteTimeout = 300 * time.Millisecond
	e.Server.IdleTimeout = 300 * time.Millisecond

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = e.Server.Serve(listener)
	}()

	defer func() {
		_ = e.Close()
	}()

	conn, resp, err := websocket.DefaultDialer.Dial("ws://"+listener.Addr().String()+"/p2p-ws", nil)
	require.NoError(t, err)

	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	defer conn.Close()

	// The initial node_status message is sent on connect.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))

	_, msg, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Contains(t, string(msg), "node_status")

	// Outlive every configured timeout, then confirm the stream still works.
	time.Sleep(1200 * time.Millisecond)

	s.notificationCh <- &notificationMsg{Type: "ping-after-timeouts"}

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))

	_, msg, err = conn.ReadMessage()
	require.NoError(t, err, "established websocket must outlive the http.Server timeouts")
	require.Contains(t, string(msg), "ping-after-timeouts")
}
