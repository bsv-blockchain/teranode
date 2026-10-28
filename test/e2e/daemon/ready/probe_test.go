package smoke

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

const (
	probeTestToken     = "probe-e2e-token-7f3c9a1d5e"
	probeTestTimeout   = 30 * time.Second
	probeMaxDashboards = 16
)

var probeStages = []string{"ingress", "validator", "cache", "assembly", "chain", "stores"}

type probeClient struct {
	base  string
	ws    string
	token string
	http  *http.Client
}

func newProbeClient(t *testing.T, td *daemon.TestDaemon) *probeClient {
	addr := td.ProbeAddress(t)

	return &probeClient{
		base:  "http://" + addr,
		ws:    "ws://" + addr + "/ws",
		token: probeTestToken,
		http:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *probeClient) request(ctx context.Context, method, path, token string, body any) (int, []byte, error) {
	var reader io.Reader

	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}

		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return 0, nil, err
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)

	return resp.StatusCode, raw, err
}

func (c *probeClient) do(t *testing.T, method, path, token string, body any) (int, []byte) {
	t.Helper()

	status, raw, err := c.request(t.Context(), method, path, token, body)
	require.NoError(t, err)

	return status, raw
}

func (c *probeClient) fetchSnapshot(ctx context.Context) (snapshot.Snapshot, error) {
	var snap snapshot.Snapshot

	status, raw, err := c.request(ctx, http.MethodGet, "/api/snapshot", c.token, nil)
	if err != nil {
		return snap, err
	}

	if status != http.StatusOK {
		return snap, errors.NewServiceError("snapshot status %d", status)
	}

	err = json.Unmarshal(raw, &snap)

	return snap, err
}

func (c *probeClient) dial(t *testing.T, token string) *websocket.Conn {
	t.Helper()

	conn, resp, err := websocket.DefaultDialer.DialContext(t.Context(), c.ws, nil)
	require.NoError(t, err)

	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	auth, err := json.Marshal(map[string]string{"auth": token})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, auth))

	return conn
}

func nextSnapshot(conn *websocket.Conn, timeout time.Duration) (snapshot.Snapshot, error) {
	var snap snapshot.Snapshot

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return snap, err
	}

	kind, raw, err := conn.ReadMessage()
	if err != nil {
		return snap, err
	}

	if kind != websocket.TextMessage {
		return snap, errors.NewServiceError("unexpected websocket message type %d", kind)
	}

	err = json.Unmarshal(raw, &snap)

	return snap, err
}

func readSnapshot(t *testing.T, conn *websocket.Conn, timeout time.Duration) snapshot.Snapshot {
	t.Helper()

	snap, err := nextSnapshot(conn, timeout)
	require.NoError(t, err)

	return snap
}

func hasData(r snapshot.Reading) bool {
	return r.Status != snapshot.StatusNoData && r.Status != ""
}

func probeSettings() func(*settings.Settings) {
	return test.ComposeSettings(
		test.SystemTestSettings(),
		func(s *settings.Settings) {
			s.Probe.AuthToken = probeTestToken
		},
	)
}

func TestProbeServesLiveNodeState(t *testing.T) {
	t.Run("with p2p", func(t *testing.T) {
		probeServesLiveNodeState(t, true)
	})

	t.Run("without p2p", func(t *testing.T) {
		probeServesLiveNodeState(t, false)
	})
}

func probeServesLiveNodeState(t *testing.T, withP2P bool) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableValidator:      true,
		EnableP2P:            withP2P,
		EnableProbe:          true,
		SettingsOverrideFunc: probeSettings(),
		FSMState:             blockchain.FSMStateRUNNING,
	})

	defer td.Stop(t, true)

	pc := newProbeClient(t, td)

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	_, txHashes, err := td.CreateAndSendTxs(t, coinbaseTx, 20)
	require.NoError(t, err)
	require.Len(t, txHashes, 20)

	td.MineAndWait(t, 2)

	bestHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Greater(t, bestHeight, uint32(2))

	require.Eventually(t, func() bool {
		snap, err := pc.fetchSnapshot(t.Context())
		return err == nil && !snap.Time.IsZero()
	}, probeTestTimeout, 100*time.Millisecond, "probe never published its first sample")

	t.Run("rest rejects a missing or wrong token", func(t *testing.T) {
		for _, path := range []string{"/api/snapshot", "/api/alarms", "/api/history?stage=chain", "/health"} {
			status, body := pc.do(t, http.MethodGet, path, "", nil)
			require.Equal(t, http.StatusUnauthorized, status, path)
			require.Empty(t, body, path)

			status, body = pc.do(t, http.MethodGet, path, "wrong-"+probeTestToken, nil)
			require.Equal(t, http.StatusUnauthorized, status, path)
			require.Empty(t, body, path)
		}

		status, _ := pc.do(t, http.MethodPost, "/api/snooze", "wrong", map[string]any{"id": "cache-hit-rate", "minutes": 5})
		require.Equal(t, http.StatusUnauthorized, status)
	})

	t.Run("websocket rejects a wrong token with close code 4401", func(t *testing.T) {
		conn := pc.dial(t, "wrong-"+probeTestToken)
		defer conn.Close()

		require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))

		_, _, err := conn.ReadMessage()
		require.Error(t, err)

		var closeErr *websocket.CloseError
		require.ErrorAs(t, err, &closeErr)
		require.Equal(t, 4401, closeErr.Code)
	})

	t.Run("first websocket snapshot after auth carries the live chain", func(t *testing.T) {
		conn := pc.dial(t, probeTestToken)
		defer conn.Close()

		first := readSnapshot(t, conn, 10*time.Second)
		require.Equal(t, "RUNNING", first.FSM)
		require.NotEmpty(t, first.Node)
		require.False(t, first.Time.IsZero())

		require.Eventually(t, func() bool {
			snap, err := nextSnapshot(conn, 10*time.Second)
			return err == nil && snap.Tip == bestHeight
		}, probeTestTimeout, 100*time.Millisecond, "probe tip never reached the daemon best height %d", bestHeight)
	})

	t.Run("pipeline stages carry data on the in-process stack", func(t *testing.T) {
		var last snapshot.Snapshot

		require.Eventually(t, func() bool {
			snap, err := pc.fetchSnapshot(t.Context())
			if err != nil {
				return false
			}

			last = snap

			return last.Tip == bestHeight &&
				hasData(last.Ingress.PropagationTxPerSec) &&
				hasData(last.Validator.ValidatedPerSec) &&
				hasData(last.Assembly.TipLagBlocks) &&
				hasData(last.Chain.ForkDepth)
		}, probeTestTimeout, 250*time.Millisecond, "stages without data: %+v", last)

		require.Equal(t, "RUNNING", last.FSM)
		require.InDelta(t, 0, last.Assembly.TipLagBlocks.Value, 0.5)
		require.InDelta(t, 0, last.Chain.ForkDepth.Value, 0.5)
		require.False(t, last.Stores.Aerospike.Configured)
		require.False(t, last.Stores.Postgres.Configured)

		sources := map[string]bool{}
		for _, src := range last.Sources {
			sources[src.Name] = src.OK
		}

		if withP2P {
			require.True(t, sources["p2p"], "p2p source must be healthy: %+v", last.Sources)
			require.Equal(t, snapshot.StatusOK, last.Chain.PeerHeight.Status)
		} else {
			require.Contains(t, sources, "p2p")
			require.False(t, sources["p2p"], "p2p source must be down: %+v", last.Sources)
			require.Equal(t, snapshot.StatusNoData, last.Chain.PeerHeight.Status)
		}

		require.True(t, sources["chain"], "chain source must be healthy: %+v", last.Sources)
		require.True(t, sources["metrics"], "metrics source must be healthy: %+v", last.Sources)
		require.NotContains(t, sources, "kafka", "in-memory Kafka must not be probed")

		raw, err := json.Marshal(last)
		require.NoError(t, err)
		require.NotContains(t, string(raw), probeTestToken)
		t.Logf("probe snapshot: %s", raw)
	})

	t.Run("history saw the transactions arrive at ingress", func(t *testing.T) {
		positive, err := historyHasPositive(t.Context(), pc, "ingress", "ingress.propagationTxPerSec")
		require.NoError(t, err)
		require.True(t, positive)
	})

	t.Run("history saw the transactions validated", func(t *testing.T) {
		require.Eventually(t, func() bool {
			positive, err := historyHasPositive(t.Context(), pc, "validator", "validator.validatedPerSec")
			return err == nil && positive
		}, probeTestTimeout, 500*time.Millisecond)
	})

	t.Run("websocket accepts 16 dashboards and rejects the 17th with 503", func(t *testing.T) {
		conns := make([]*websocket.Conn, 0, probeMaxDashboards)

		defer func() {
			for _, c := range conns {
				_ = c.Close()
			}
		}()

		deadline := time.Now().Add(probeTestTimeout)

		for len(conns) < probeMaxDashboards {
			require.True(t, time.Now().Before(deadline), "only %d dashboards connected", len(conns))

			conn, resp, err := websocket.DefaultDialer.DialContext(t.Context(), pc.ws, nil)
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}

			if err != nil {
				require.NotNil(t, resp, "dial failed without a response: %v", err)
				require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
				time.Sleep(100 * time.Millisecond)

				continue
			}

			auth, err := json.Marshal(map[string]string{"auth": probeTestToken})
			require.NoError(t, err)
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, auth))

			conns = append(conns, conn)
		}

		for _, c := range conns {
			snap := readSnapshot(t, c, 10*time.Second)
			require.Equal(t, "RUNNING", snap.FSM)
		}

		_, resp, err := websocket.DefaultDialer.DialContext(t.Context(), pc.ws, nil)
		require.Error(t, err)
		require.NotNil(t, resp)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	})

	t.Run("history returns series for every stage", func(t *testing.T) {
		for _, stage := range probeStages {
			status, raw := pc.do(t, http.MethodGet, "/api/history?stage="+stage+"&minutes=5", pc.token, nil)
			require.Equal(t, http.StatusOK, status, stage)

			var hist struct {
				Stage  string `json:"stage"`
				Series []struct {
					Key    string            `json:"key"`
					Points []json.RawMessage `json:"points"`
				} `json:"series"`
			}

			require.NoError(t, json.Unmarshal(raw, &hist), stage)
			require.Equal(t, stage, hist.Stage)
			require.NotEmpty(t, hist.Series, stage)
			require.NotEmpty(t, hist.Series[0].Points, stage)
		}

		status, _ := pc.do(t, http.MethodGet, "/api/history?stage=nosuchstage", pc.token, nil)
		require.Equal(t, http.StatusBadRequest, status)
	})

	t.Run("snooze round-trips through the state file", func(t *testing.T) {
		statePath := td.Settings.Probe.StatePath

		status, _ := pc.do(t, http.MethodPost, "/api/snooze", pc.token, map[string]any{"id": "no-such-alarm", "minutes": 5})
		require.Equal(t, http.StatusBadRequest, status)

		status, _ = pc.do(t, http.MethodPost, "/api/snooze", pc.token, map[string]any{"id": "cache-hit-rate", "minutes": 30})
		require.Equal(t, http.StatusNoContent, status)

		require.Eventually(t, func() bool {
			return stateSnoozes(statePath)["cache-hit-rate"]
		}, 10*time.Second, 100*time.Millisecond)

		status, _ = pc.do(t, http.MethodPost, "/api/unsnooze", pc.token, map[string]any{"id": "cache-hit-rate"})
		require.Equal(t, http.StatusNoContent, status)

		require.Eventually(t, func() bool {
			return !stateSnoozes(statePath)["cache-hit-rate"]
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("node keeps processing blocks with the probe on", func(t *testing.T) {
		td.MineAndWait(t, 1)

		require.Eventually(t, func() bool {
			snap, err := pc.fetchSnapshot(t.Context())
			return err == nil && snap.Tip == bestHeight+1
		}, probeTestTimeout, 250*time.Millisecond)
	})
}

func historyHasPositive(ctx context.Context, pc *probeClient, stage, key string) (bool, error) {
	status, raw, err := pc.request(ctx, http.MethodGet, "/api/history?stage="+stage+"&minutes=10", pc.token, nil)
	if err != nil {
		return false, err
	}

	if status != http.StatusOK {
		return false, errors.NewServiceError("history status %d", status)
	}

	var hist struct {
		Series []struct {
			Key    string `json:"key"`
			Points []struct {
				V *float64 `json:"v"`
			} `json:"points"`
		} `json:"series"`
	}

	if err := json.Unmarshal(raw, &hist); err != nil {
		return false, err
	}

	for _, series := range hist.Series {
		if series.Key != key {
			continue
		}

		for _, p := range series.Points {
			if p.V != nil && *p.V > 0 {
				return true, nil
			}
		}
	}

	return false, nil
}

func stateSnoozes(path string) map[string]bool {
	raw, err := os.ReadFile(path)
	if err != nil {
		return map[string]bool{}
	}

	var state struct {
		Snoozes map[string]time.Time `json:"snoozes"`
	}

	if json.Unmarshal(raw, &state) != nil {
		return map[string]bool{}
	}

	out := make(map[string]bool, len(state.Snoozes))

	for id := range state.Snoozes {
		out[id] = true
	}

	return out
}
