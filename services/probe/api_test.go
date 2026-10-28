package probe

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/probe/alarm"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

type route struct {
	method string
	path   string
	body   any
}

var restRoutes = []route{
	{http.MethodGet, "/health", nil},
	{http.MethodGet, "/api/snapshot", nil},
	{http.MethodGet, "/api/alarms", nil},
	{http.MethodGet, "/api/history?stage=cache", nil},
	{http.MethodPost, "/api/snooze", map[string]any{"id": "cache-hit-rate", "minutes": 5}},
	{http.MethodPost, "/api/unsnooze", map[string]any{"id": "cache-hit-rate"}},
}

func doRaw(t *testing.T, method, url, authorization string, body any) int {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequest(method, url, strings.NewReader(string(raw)))
	require.NoError(t, err)

	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp.StatusCode
}

func TestAPIRequiresBearer(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	for _, r := range restRoutes {
		u := apiURL(srv, r.path)

		for _, header := range []string{"", "Bearer ", "Bearer wrong", "Bearer " + testToken + "x", "Basic " + testToken, testToken} {
			require.Equal(t, http.StatusUnauthorized, doRaw(t, r.method, u, header, r.body), "%s %s with %q", r.method, r.path, header)
		}

		status, _ := doRequest(t, r.method, u, testToken, r.body)
		require.Less(t, status, 300, "%s %s with the token", r.method, r.path)
	}

	status, _ := doRequest(t, http.MethodGet, apiURL(srv, "/api/no-such-route"), testToken, nil)
	require.NotEqual(t, http.StatusOK, status)
}

func TestSnoozeRejectsUnknownIdAndBadMinutes(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	snooze := apiURL(srv, "/api/snooze")
	unsnooze := apiURL(srv, "/api/unsnooze")

	bad := []any{
		map[string]any{"id": "no-such-alarm", "minutes": 5},
		map[string]any{"id": "", "minutes": 5},
		map[string]any{"minutes": 5},
		map[string]any{"id": strings.Repeat("a", 129), "minutes": 5},
		map[string]any{"id": "cache-hit-rate", "minutes": -1},
		map[string]any{"id": "cache-hit-rate", "minutes": 1441},
		map[string]any{"id": "cache-hit-rate", "minutes": 1.5},
		map[string]any{"id": "cache-hit-rate", "minutes": "5"},
		"not json",
		`{"id":"cache-hit-rate","minutes":5}{"id":"x"}`,
	}

	for _, body := range bad {
		status, _ := doRequest(t, http.MethodPost, snooze, testToken, body)
		require.Equal(t, http.StatusBadRequest, status, "body %v", body)
	}

	for _, body := range []any{
		map[string]any{"id": "no-such-alarm"},
		map[string]any{"id": ""},
		map[string]any{"id": strings.Repeat("a", 129)},
		"not json",
	} {
		status, _ := doRequest(t, http.MethodPost, unsnooze, testToken, body)
		require.Equal(t, http.StatusBadRequest, status, "body %v", body)
	}

	for _, body := range []any{
		map[string]any{"id": "cache-hit-rate", "minutes": 0},
		map[string]any{"id": "cache-hit-rate"},
		map[string]any{"id": "cache-hit-rate", "minutes": 1},
		map[string]any{"id": "cache-hit-rate", "minutes": 1440},
		map[string]any{"id": alarm.CatchupInProgressID, "minutes": 10},
		map[string]any{"id": alarm.SourceUnreachableID + "stores", "minutes": 10},
	} {
		status, _ := doRequest(t, http.MethodPost, snooze, testToken, body)
		require.Equal(t, http.StatusNoContent, status, "body %v", body)
	}

	status, _ := doRequest(t, http.MethodPost, unsnooze, testToken, map[string]any{"id": "cache-hit-rate"})
	require.Equal(t, http.StatusNoContent, status)
}

func TestSnoozeDefaultDuration(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	status, _ := doRequest(t, http.MethodPost, apiURL(srv, "/api/snooze"), testToken, map[string]any{"id": "cache-hit-rate", "minutes": 0})
	require.Equal(t, http.StatusNoContent, status)

	until, ok := srv.pipe().store.Snoozes(time.Now())["cache-hit-rate"]
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(srv.settings.Probe.SnoozeDefault), until, 5*time.Second)
}

func TestSnoozeEndpointPersists(t *testing.T) {
	path := t.TempDir() + "/probe/state.json"

	first := newProbe(t, probeOpts{statePath: path})
	require.NoError(t, first.Init(context.Background()))

	cancel, errCh := startServer(t, first)

	status, _ := doRequest(t, http.MethodPost, apiURL(first, "/api/snooze"), testToken, map[string]any{"id": "cache-hit-rate", "minutes": 30})
	require.Equal(t, http.StatusNoContent, status)

	cancel()
	require.NoError(t, first.Stop(context.Background()))
	require.NoError(t, <-errCh)

	second := newProbe(t, probeOpts{statePath: path})
	runProbe(t, second)

	until, ok := second.pipe().store.Snoozes(time.Now())["cache-hit-rate"]
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(30*time.Minute), until, time.Minute)

	status, _ = doRequest(t, http.MethodPost, apiURL(second, "/api/unsnooze"), testToken, map[string]any{"id": "cache-hit-rate"})
	require.Equal(t, http.StatusNoContent, status)

	_, ok = second.pipe().store.Snoozes(time.Now())["cache-hit-rate"]
	require.False(t, ok)
}

func TestAlarmsEndpointReturnsLog(t *testing.T) {
	path := t.TempDir() + "/state.json"
	now := time.Now().UTC()

	st, err := alarm.OpenStore(path)
	require.NoError(t, err)

	for _, a := range []snapshot.Alarm{
		{ID: "cache-hit-rate", Stage: "cache", Severity: "warn", RaisedAt: now.Add(-2 * time.Hour)},
		{ID: "aerospike-read-p99", Stage: "stores", Severity: "crit", RaisedAt: now.Add(-30 * time.Hour)},
		{ID: "persister-behind", Stage: "chain", Severity: "warn", RaisedAt: now.Add(-60 * time.Hour)},
	} {
		require.NoError(t, st.AppendAlarm(a))
	}

	require.NoError(t, st.Close())

	srv := newProbe(t, probeOpts{statePath: path, retention: 48 * time.Hour})
	runProbe(t, srv)

	ids := func(query string) []string {
		status, raw := doRequest(t, http.MethodGet, apiURL(srv, "/api/alarms"+query), testToken, nil)
		require.Equal(t, http.StatusOK, status, "query %s", query)

		var body struct {
			Alarms []snapshot.Alarm `json:"alarms"`
		}

		require.NoError(t, json.Unmarshal(raw, &body))
		require.NotNil(t, body.Alarms)

		out := make([]string, 0, len(body.Alarms))

		for _, a := range body.Alarms {
			out = append(out, a.ID)
		}

		return out
	}

	require.Equal(t, []string{"cache-hit-rate"}, ids(""))
	require.Equal(t, []string{"cache-hit-rate"}, ids("?since=180"))
	require.Empty(t, ids("?since=60"))
	require.Equal(t, []string{"aerospike-read-p99", "cache-hit-rate"}, ids("?since="+url.QueryEscape(now.Add(-40*time.Hour).Format(time.RFC3339))))
	require.Equal(t, []string{"aerospike-read-p99", "cache-hit-rate"}, ids("?since="+url.QueryEscape(now.Add(-1000*time.Hour).Format(time.RFC3339))))
	require.Equal(t, []string{"aerospike-read-p99", "cache-hit-rate"}, ids("?since=100000"))

	for _, bad := range []string{"abc", "-5", "0", "1.5", "2026-13-01"} {
		status, _ := doRequest(t, http.MethodGet, apiURL(srv, "/api/alarms?since="+url.QueryEscape(bad)), testToken, nil)
		require.Equal(t, http.StatusBadRequest, status, "since %s", bad)
	}
}

type historyBody struct {
	Stage  string `json:"stage"`
	Series []struct {
		Key    string `json:"key"`
		Points []struct {
			T  time.Time       `json:"t"`
			V  *float64        `json:"v"`
			St snapshot.Status `json:"st"`
		} `json:"points"`
	} `json:"series"`
}

func TestHistoryEndpointProjectsStage(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	var body historyBody

	require.Eventually(t, func() bool {
		status, raw := doRequest(t, http.MethodGet, apiURL(srv, "/api/history?stage=cache&minutes=1"), testToken, nil)
		require.Equal(t, http.StatusOK, status)

		body = historyBody{}
		require.NoError(t, json.Unmarshal(raw, &body))

		return len(body.Series) > 0 && len(body.Series[0].Points) >= 5
	}, 5*time.Second, 20*time.Millisecond)

	require.Equal(t, "cache", body.Stage)

	keys := map[string]int{}

	for i, series := range body.Series {
		require.True(t, strings.HasPrefix(series.Key, "cache."), series.Key)
		keys[series.Key] = i

		for j := 1; j < len(series.Points); j++ {
			require.False(t, series.Points[j].T.Before(series.Points[j-1].T))
		}
	}

	hit := body.Series[keys["cache.hitRate"]]
	require.Equal(t, "cache.hitRate", hit.Key)

	last := hit.Points[len(hit.Points)-1]
	require.Equal(t, snapshot.StatusOK, last.St)
	require.NotNil(t, last.V)
	require.InDelta(t, 0.95, *last.V, 1e-9)

	fill, ok := keys["cache.fill"]
	require.True(t, ok)

	for _, p := range body.Series[fill].Points {
		require.Nil(t, p.V)
		require.Equal(t, snapshot.StatusNoData, p.St)
	}

	_, raw := doRequest(t, http.MethodGet, apiURL(srv, "/api/history?stage=cache&minutes=1"), testToken, nil)
	require.Contains(t, string(raw), `"v":null,"st":"nodata"`)

	status, _ := doRequest(t, http.MethodGet, apiURL(srv, "/api/history?stage=stores"), testToken, nil)
	require.Equal(t, http.StatusOK, status)

	for _, q := range []string{"stage=flows", "stage=", "", "stage=cache&minutes=0", "stage=cache&minutes=11", "stage=cache&minutes=abc", "stage=cache&minutes=-1"} {
		status, _ := doRequest(t, http.MethodGet, apiURL(srv, "/api/history?"+q), testToken, nil)
		require.Equal(t, http.StatusBadRequest, status, "query %s", q)
	}
}

func TestSnapshotEndpointServesEvaluatedSnapshot(t *testing.T) {
	srv := newProbe(t, probeOpts{})
	runProbe(t, srv)

	require.Eventually(t, func() bool {
		snap := servedSnapshot(t, srv)

		return snap.Cache.HitRate.Status != snapshot.StatusNoData && snap.Cache.HitRate.Note != ""
	}, 5*time.Second, 20*time.Millisecond)
}

func TestTokenNeverLogged(t *testing.T) {
	const secret = "s3cr3t-probe-token-value-7f1c"

	logger := &captureLogger{}

	failing := newFake("kafka", func(context.Context, int) (snapshot.Patch, error) {
		return nil, context.DeadlineExceeded
	})

	srv := newProbe(t, probeOpts{token: secret, logger: logger, fast: []Collector{newFake("metrics", okPatch(healthyPatch))}, slow: []Collector{failing}})
	srv.wsAuthTimeout = 200 * time.Millisecond
	runProbe(t, srv)

	for _, r := range restRoutes {
		u := apiURL(srv, r.path)
		doRaw(t, r.method, u, "Bearer "+secret+"-wrong", r.body)
		doRaw(t, r.method, u, "Bearer wrong", r.body)
		doRaw(t, r.method, u, secret, r.body)
	}

	doRequest(t, http.MethodPost, apiURL(srv, "/api/snooze"), secret, map[string]any{"id": secret, "minutes": 5})
	doRequest(t, http.MethodGet, apiURL(srv, "/api/history?stage="+secret), secret, nil)

	bad := mustDialWS(t, srv)
	require.NoError(t, bad.WriteJSON(map[string]string{"auth": secret + "-wrong"}))
	requireClosedWith(t, bad, wsCloseUnauthorized, 5*time.Second)

	garbage := mustDialWS(t, srv)
	require.NoError(t, garbage.WriteMessage(websocket.TextMessage, []byte(secret)))
	requireClosedWith(t, garbage, wsCloseUnauthorized, 5*time.Second)

	idle := mustDialWS(t, srv)
	requireClosedWith(t, idle, wsCloseUnauthorized, 5*time.Second)

	good := mustDialWS(t, srv)
	require.NoError(t, good.WriteJSON(map[string]string{"auth": secret}))
	readSnapshot(t, good, 2*time.Second)
	readSnapshot(t, good, 2*time.Second)
	require.NoError(t, good.Close())

	require.NoError(t, srv.Stop(context.Background()))

	lines := logger.Lines()
	require.NotEmpty(t, lines)

	for _, l := range lines {
		require.NotContains(t, l, secret)
	}
}
