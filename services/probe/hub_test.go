package probe

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func dialWS(srv *Server, header http.Header) (*websocket.Conn, *http.Response, error) {
	return websocket.DefaultDialer.Dial("ws://"+srv.Addr()+"/ws", header)
}

func mustDialWS(t *testing.T, srv *Server) *websocket.Conn {
	t.Helper()

	conn, resp, err := dialWS(srv, nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func authedWS(t *testing.T, srv *Server) *websocket.Conn {
	t.Helper()

	conn := mustDialWS(t, srv)
	require.NoError(t, conn.WriteJSON(map[string]string{"auth": testToken}))

	return conn
}

func readSnapshot(t *testing.T, conn *websocket.Conn, within time.Duration) snapshot.Snapshot {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(within)))

	kind, raw, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.TextMessage, kind)

	var snap snapshot.Snapshot
	require.NoError(t, json.Unmarshal(raw, &snap))

	return snap
}

func requireClosedWith(t *testing.T, conn *websocket.Conn, code int, within time.Duration) {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(within)))

	kind, raw, err := conn.ReadMessage()
	require.Error(t, err, "got message kind %d: %s", kind, raw)
	require.True(t, websocket.IsCloseError(err, code), "unexpected error: %v", err)
}

func TestWebSocketRejectsBadFirstMessage(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	cases := map[string]string{
		"wrong token":    `{"auth":"wrong-token"}`,
		"empty token":    `{"auth":""}`,
		"missing auth":   `{}`,
		"not json":       `hello`,
		"token prefixed": `{"auth":"` + testToken + `x"}`,
		"json array":     `["` + testToken + `"]`,
	}

	for name, first := range cases {
		t.Run(name, func(t *testing.T) {
			conn := mustDialWS(t, srv)
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(first)))
			requireClosedWith(t, conn, wsCloseUnauthorized, 5*time.Second)
		})
	}
}

func TestWebSocketAuthTimeout(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	srv.wsAuthTimeout = 200 * time.Millisecond
	runProbe(t, srv)

	conn := mustDialWS(t, srv)
	start := time.Now()

	requireClosedWith(t, conn, wsCloseUnauthorized, 5*time.Second)
	require.Less(t, time.Since(start), 3*time.Second)
}

func TestWebSocketReceivesSnapshots(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	conn := authedWS(t, srv)

	first := readSnapshot(t, conn, 2*time.Second)
	require.Equal(t, srv.nodeName(), first.Node)

	var later snapshot.Snapshot

	require.Eventually(t, func() bool {
		later = readSnapshot(t, conn, 2*time.Second)

		return later.Time.After(first.Time) && later.Cache.HitRate.Status == snapshot.StatusOK
	}, 5*time.Second, time.Millisecond)

	require.InDelta(t, 0.95, later.Cache.HitRate.Value, 1e-9)
	require.NotNil(t, later.Alarms)
}

func TestWebSocketRejectsForeignOrigin(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	header := http.Header{}
	header.Set("Origin", "http://evil.example")

	conn, resp, err := dialWS(srv, header)
	require.Error(t, err)
	require.Nil(t, conn)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	header.Set("Origin", "http://"+srv.Addr())

	conn, resp, err = dialWS(srv, header)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, conn.WriteJSON(map[string]string{"auth": testToken}))
	readSnapshot(t, conn, 2*time.Second)
	require.NoError(t, conn.Close())
}

func TestSeventeenthClientRefused(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	conns := make([]*websocket.Conn, 0, maxWSClients)

	for range maxWSClients {
		conn := authedWS(t, srv)
		readSnapshot(t, conn, 2*time.Second)

		conns = append(conns, conn)
	}

	conn, resp, err := dialWS(srv, nil)
	require.Error(t, err)
	require.Nil(t, conn)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	require.NoError(t, conns[0].Close())

	require.Eventually(t, func() bool {
		c, r, dialErr := dialWS(srv, nil)
		if dialErr != nil {
			if r != nil {
				_ = r.Body.Close()
			}

			return false
		}

		_ = r.Body.Close()
		_ = c.Close()

		return true
	}, 5*time.Second, 20*time.Millisecond)
}

func TestSlowClientDroppedWithoutBlockingOthers(t *testing.T) {
	interval := 100 * time.Millisecond
	waiting := make([]uint32, 50000)

	for i := range waiting {
		waiting[i] = uint32(1_000_000 + i)
	}

	big := newFake("chain", okPatch(func(s *snapshot.Snapshot) {
		s.Chain.BlocksWaiting = append([]uint32(nil), waiting...)
	}))

	srv := newProbe(t, probeOpts{fast: []Collector{big}, sampleInterval: interval})
	runProbe(t, srv)

	authedWS(t, srv)

	fast := authedWS(t, srv)

	require.Eventually(t, func() bool { return srv.hubClients() == 2 }, 5*time.Second, 10*time.Millisecond)

	last := readSnapshot(t, fast, 2*time.Second)

	for range 30 {
		snap := readSnapshot(t, fast, 2*interval+interval/2)
		require.False(t, snap.Time.Before(last.Time))

		last = snap
	}

	require.Eventually(t, func() bool {
		readSnapshot(t, fast, 2*interval+interval/2)

		return srv.hubClients() == 1
	}, 60*time.Second, time.Millisecond)

	for range 10 {
		readSnapshot(t, fast, 2*interval+interval/2)
	}
}
