package smoke

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

const (
	probeTestToken   = "probe-e2e-token-7f3c9a1d5e"
	probeTestTimeout = 30 * time.Second
)

var probeStages = []string{"ingress", "validator", "cache", "assembly", "chain", "stores"}

type probeClient struct {
	t     *testing.T
	base  string
	ws    string
	token string
	http  *http.Client
}

func newProbeClient(t *testing.T, td *daemon.TestDaemon) *probeClient {
	addr := td.ProbeAddress(t)

	return &probeClient{
		t:     t,
		base:  "http://" + addr,
		ws:    "ws://" + addr + "/ws",
		token: probeTestToken,
		http:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *probeClient) do(method, path, token string, body any) (int, []byte) {
	var reader io.Reader

	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(c.t, err)

		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(c.t.Context(), method, c.base+path, reader)
	require.NoError(c.t, err)

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	require.NoError(c.t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err)

	return resp.StatusCode, raw
}

func (c *probeClient) snapshot() snapshot.Snapshot {
	status, raw := c.do(http.MethodGet, "/api/snapshot", c.token, nil)
	require.Equal(c.t, http.StatusOK, status)

	var snap snapshot.Snapshot
	require.NoError(c.t, json.Unmarshal(raw, &snap))

	return snap
}

func (c *probeClient) dial(token string) *websocket.Conn {
	conn, resp, err := websocket.DefaultDialer.DialContext(c.t.Context(), c.ws, nil)
	require.NoError(c.t, err)

	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	auth, err := json.Marshal(map[string]string{"auth": token})
	require.NoError(c.t, err)
	require.NoError(c.t, conn.WriteMessage(websocket.TextMessage, auth))

	return conn
}

func readSnapshot(t *testing.T, conn *websocket.Conn, timeout time.Duration) snapshot.Snapshot {
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(timeout)))

	kind, raw, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.TextMessage, kind)

	var snap snapshot.Snapshot
	require.NoError(t, json.Unmarshal(raw, &snap))

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
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableValidator:      true,
		EnableP2P:            true,
		EnableProbe:          true,
		UseUnifiedLogger:     true,
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

	t.Run("rest rejects a missing or wrong token", func(t *testing.T) {
		for _, path := range []string{"/api/snapshot", "/api/alarms", "/api/history?stage=chain", "/health"} {
			status, body := pc.do(http.MethodGet, path, "", nil)
			require.Equal(t, http.StatusUnauthorized, status, path)
			require.Empty(t, body, path)

			status, body = pc.do(http.MethodGet, path, "wrong-"+probeTestToken, nil)
			require.Equal(t, http.StatusUnauthorized, status, path)
			require.Empty(t, body, path)
		}

		status, _ := pc.do(http.MethodPost, "/api/snooze", "wrong", map[string]any{"id": "cache-hit-rate", "minutes": 5})
		require.Equal(t, http.StatusUnauthorized, status)
	})

	t.Run("websocket rejects a wrong token with close code 4401", func(t *testing.T) {
		conn := pc.dial("wrong-" + probeTestToken)
		defer conn.Close()

		require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))

		_, _, err := conn.ReadMessage()
		require.Error(t, err)

		var closeErr *websocket.CloseError
		require.ErrorAs(t, err, &closeErr)
		require.Equal(t, 4401, closeErr.Code)
	})

	t.Run("first websocket snapshot after auth carries the live chain", func(t *testing.T) {
		conn := pc.dial(probeTestToken)
		defer conn.Close()

		first := readSnapshot(t, conn, 10*time.Second)
		require.Equal(t, "RUNNING", first.FSM)
		require.NotEmpty(t, first.Node)
		require.False(t, first.Time.IsZero())

		require.Eventually(t, func() bool {
			snap := readSnapshot(t, conn, 10*time.Second)
			return snap.Tip == bestHeight
		}, probeTestTimeout, 100*time.Millisecond, "probe tip never reached the daemon best height %d", bestHeight)
	})

	t.Run("pipeline stages carry data on the in-process stack", func(t *testing.T) {
		var last snapshot.Snapshot

		require.Eventually(t, func() bool {
			last = pc.snapshot()

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

		raw, err := json.Marshal(last)
		require.NoError(t, err)
		require.NotContains(t, string(raw), probeTestToken)
		t.Logf("probe snapshot: %s", raw)
	})

	t.Run("history saw the transactions arrive at ingress", func(t *testing.T) {
		require.True(t, historyHasPositive(t, pc, "ingress", "ingress.propagationTxPerSec"))
	})

	t.Run("history returns series for every stage", func(t *testing.T) {
		for _, stage := range probeStages {
			status, raw := pc.do(http.MethodGet, "/api/history?stage="+stage+"&minutes=5", pc.token, nil)
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

		status, _ := pc.do(http.MethodGet, "/api/history?stage=nosuchstage", pc.token, nil)
		require.Equal(t, http.StatusBadRequest, status)
	})

	t.Run("snooze round-trips through the state file", func(t *testing.T) {
		statePath := td.Settings.Probe.StatePath

		status, _ := pc.do(http.MethodPost, "/api/snooze", pc.token, map[string]any{"id": "no-such-alarm", "minutes": 5})
		require.Equal(t, http.StatusBadRequest, status)

		status, _ = pc.do(http.MethodPost, "/api/snooze", pc.token, map[string]any{"id": "cache-hit-rate", "minutes": 30})
		require.Equal(t, http.StatusNoContent, status)

		require.Eventually(t, func() bool {
			return stateSnoozes(t, statePath)["cache-hit-rate"]
		}, 10*time.Second, 100*time.Millisecond)

		status, _ = pc.do(http.MethodPost, "/api/unsnooze", pc.token, map[string]any{"id": "cache-hit-rate"})
		require.Equal(t, http.StatusNoContent, status)

		require.Eventually(t, func() bool {
			return !stateSnoozes(t, statePath)["cache-hit-rate"]
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("node keeps processing blocks with the probe on", func(t *testing.T) {
		td.MineAndWait(t, 1)

		require.Eventually(t, func() bool {
			return pc.snapshot().Tip == bestHeight+1
		}, probeTestTimeout, 250*time.Millisecond)
	})
}

func historyHasPositive(t *testing.T, pc *probeClient, stage, key string) bool {
	status, raw := pc.do(http.MethodGet, "/api/history?stage="+stage+"&minutes=10", pc.token, nil)
	require.Equal(t, http.StatusOK, status)

	var hist struct {
		Series []struct {
			Key    string `json:"key"`
			Points []struct {
				V *float64 `json:"v"`
			} `json:"points"`
		} `json:"series"`
	}

	require.NoError(t, json.Unmarshal(raw, &hist))

	for _, series := range hist.Series {
		if series.Key != key {
			continue
		}

		for _, p := range series.Points {
			if p.V != nil && *p.V > 0 {
				return true
			}
		}
	}

	return false
}

func stateSnoozes(t *testing.T, path string) map[string]bool {
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
