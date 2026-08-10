package p2p

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
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

	require.NotZero(t, e.Server.ReadHeaderTimeout, "ReadHeaderTimeout must be set to bound the header phase")
	require.NotZero(t, e.Server.ReadTimeout, "ReadTimeout must be set to bound the request read")
	require.NotZero(t, e.Server.IdleTimeout, "IdleTimeout must be set to reap idle keep-alive connections")
	require.Zero(t, e.Server.WriteTimeout, "WriteTimeout must stay unset or it would cap the lifetime of /p2p-ws streams")
}

// TestHTTPServerClosesSlowHeaderClient opens a raw connection, sends a partial
// request and never completes the headers, then asserts the server closes the
// connection once ReadHeaderTimeout fires instead of holding it open forever.
// The timeouts are shortened to keep the test fast; the production values are
// asserted in TestSetupHTTPServerTimeouts.
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

	e.Listener = listener

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
