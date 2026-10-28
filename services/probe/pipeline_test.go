package probe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/guard"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

const testSampleInterval = 20 * time.Millisecond

type probeOpts struct {
	token          string
	fast           []Collector
	slow           []Collector
	floors         []string
	statePath      string
	logger         ulogger.Logger
	clock          func() time.Time
	sampleInterval time.Duration
	retention      time.Duration
}

type captureLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	lines []string
}

func (l *captureLogger) add(level, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, level+" "+fmt.Sprintf(format, args...))
}

func (l *captureLogger) Debugf(format string, args ...interface{}) { l.add("DEBUG", format, args...) }
func (l *captureLogger) Infof(format string, args ...interface{})  { l.add("INFO", format, args...) }
func (l *captureLogger) Warnf(format string, args ...interface{})  { l.add("WARN", format, args...) }
func (l *captureLogger) Errorf(format string, args ...interface{}) { l.add("ERROR", format, args...) }
func (l *captureLogger) Fatalf(format string, args ...interface{}) { l.add("FATAL", format, args...) }

func (l *captureLogger) New(string, ...ulogger.Option) ulogger.Logger { return l }

func (l *captureLogger) Duplicate(...ulogger.Option) ulogger.Logger { return l }

func (l *captureLogger) WithTraceContext(context.Context) ulogger.Logger { return l }

func (l *captureLogger) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.lines...)
}

func newProbe(t *testing.T, o probeOpts) *Server {
	t.Helper()

	s := settings.NewSettings()
	s.Probe.HTTPListenAddress = "localhost:0"
	s.Probe.AuthToken = o.token
	s.Probe.SampleInterval = o.sampleInterval
	s.Probe.SlowSampleInterval = 50 * time.Millisecond
	s.Probe.StatePath = o.statePath
	s.Probe.Floors = o.floors

	if o.token == "" {
		s.Probe.AuthToken = testToken
	}

	if o.sampleInterval == 0 {
		s.Probe.SampleInterval = testSampleInterval
	}

	if o.statePath == "" {
		s.Probe.StatePath = filepath.Join(t.TempDir(), "state", "probe_state.json")
	}

	if o.retention > 0 {
		s.Probe.AlarmLogRetention = o.retention
	}

	logger := o.logger
	if logger == nil {
		logger = ulogger.TestLogger{}
	}

	srv := New(logger, s, nil, nil)

	fast, slow := o.fast, o.slow
	if fast == nil && slow == nil {
		fast = []Collector{newFake("metrics", okPatch(healthyPatch))}
	}

	srv.collectors = func(context.Context, guard.Recorder) collectorSet {
		return collectorSet{fast: fast, slow: slow}
	}

	if o.clock != nil {
		srv.now = o.clock
	}

	return srv
}

func healthyPatch(s *snapshot.Snapshot) {
	s.Cache.HitRate = snapshot.Reading{Value: 0.95, Status: snapshot.StatusOK}
	s.Ingress.SubtreeQueue = snapshot.Reading{Value: 3, Status: snapshot.StatusOK}
}

func runProbe(t *testing.T, srv *Server) chan error {
	t.Helper()

	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	done := make(chan error, 1)

	go func() { done <- <-errCh }()

	t.Cleanup(func() {
		cancel()

		go func() { _ = srv.Stop(context.Background()) }()

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("probe did not stop")
		}
	})

	return done
}

func apiURL(srv *Server, path string) string {
	return "http://" + srv.Addr() + path
}

func doRequest(t *testing.T, method, url, token string, body any) (int, []byte) {
	t.Helper()

	var reader io.Reader

	switch b := body.(type) {
	case nil:
	case string:
		reader = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		require.NoError(t, err)

		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, url, reader)
	require.NoError(t, err)

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	if reader != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() { require.NoError(t, resp.Body.Close()) }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, raw
}

func servedSnapshot(t *testing.T, srv *Server) snapshot.Snapshot {
	t.Helper()

	status, raw := doRequest(t, http.MethodGet, apiURL(srv, "/api/snapshot"), testToken, nil)
	require.Equal(t, http.StatusOK, status)

	var snap snapshot.Snapshot
	require.NoError(t, json.Unmarshal(raw, &snap))

	return snap
}

func hasAlarm(snap snapshot.Snapshot, id string) bool {
	for _, a := range snap.Alarms {
		if a.ID == id {
			return true
		}
	}

	return false
}

func TestPipelineSetsDrainAndAlarms(t *testing.T) {
	clock := newFakeClock()

	metrics := newFake("metrics", func(_ context.Context, call int) (snapshot.Patch, error) {
		clock.Advance(5 * time.Second)

		queue := float64(max(4000-10*call, 0))

		return func(s *snapshot.Snapshot) {
			s.Ingress.SubtreeQueue = snapshot.Reading{Value: queue, Status: snapshot.StatusOK}
			s.Cache.HitRate = snapshot.Reading{Value: 0.1, Status: snapshot.StatusOK}
			s.Cache.Fill = snapshot.Reading{Value: 0.5, Status: snapshot.StatusOK}
		}, nil
	})

	chain := newFake("chain", okPatch(func(s *snapshot.Snapshot) {
		s.FSM = "RUNNING"
		s.Chain.BlocksWaiting = []uint32{100, 101, 102}
	}))

	srv := newProbe(t, probeOpts{
		fast:   []Collector{metrics, chain},
		floors: []string{"cache.hitRate=0.9"},
		clock:  clock.Now,
	})
	runProbe(t, srv)

	var snap snapshot.Snapshot

	require.Eventually(t, func() bool {
		snap = servedSnapshot(t, srv)

		return snap.DrainSeconds != nil && hasAlarm(snap, "cache-hit-rate")
	}, 20*time.Second, 20*time.Millisecond)

	require.False(t, snap.DrainGrowing)

	backlog := snap.Ingress.SubtreeQueue.Value + float64(len(snap.Chain.BlocksWaiting))
	require.InDelta(t, backlog/2, *snap.DrainSeconds, 0.01)

	raw := srv.sampler().Latest()
	require.Nil(t, raw.DrainSeconds)
	require.Empty(t, raw.Alarms)
}

func TestPipelineReportsGrowingBacklog(t *testing.T) {
	clock := newFakeClock()

	metrics := newFake("metrics", func(_ context.Context, call int) (snapshot.Patch, error) {
		clock.Advance(5 * time.Second)

		return func(s *snapshot.Snapshot) {
			s.Ingress.SubtreeQueue = snapshot.Reading{Value: float64(10 * call), Status: snapshot.StatusOK}
		}, nil
	})

	srv := newProbe(t, probeOpts{fast: []Collector{metrics}, clock: clock.Now})
	runProbe(t, srv)

	require.Eventually(t, func() bool {
		snap := servedSnapshot(t, srv)

		return snap.DrainGrowing && snap.DrainSeconds == nil
	}, 20*time.Second, 20*time.Millisecond)
}

func TestBadFloorIsConfigurationError(t *testing.T) {
	srv := newProbe(t, probeOpts{floors: []string{"cache.hitRate=abc"}})

	err := srv.Init(context.Background())
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrConfiguration))

	srv = newProbe(t, probeOpts{floors: []string{"cache.hitRate"}})
	require.Error(t, srv.Init(context.Background()))

	srv = newProbe(t, probeOpts{floors: []string{"cache.hitRate=0.9"}})
	require.NoError(t, srv.Init(context.Background()))
}

func panicLines(lines []string) int {
	n := 0

	for _, l := range lines {
		if strings.Contains(l, probeStoppedMessage) {
			n++
		}
	}

	return n
}

func TestRepeatedPanicsStopProbeNotDaemon(t *testing.T) {
	logger := &captureLogger{}

	bad := newFake("metrics", func(context.Context, int) (snapshot.Patch, error) {
		panic("boom")
	})

	srv := newProbe(t, probeOpts{fast: []Collector{bad}, logger: logger})
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	addr := srv.Addr()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "probe did not stop itself")
	}

	for _, liveness := range []bool{true, false} {
		status, msg, err := srv.Health(context.Background(), liveness)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, probeStoppedHealth, msg)
	}

	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, ln.Close())

	require.NoError(t, srv.Stop(context.Background()))
	require.NoError(t, srv.Stop(context.Background()))

	require.Equal(t, 1, panicLines(logger.Lines()))

	for _, l := range logger.Lines() {
		require.NotContains(t, l, "\n")
	}
}

func TestPipelinePanicsStopProbe(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	srv.processHook = func(*snapshot.Snapshot) { panic("pipeline fault") }
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "probe did not stop itself")
	}

	status, msg, err := srv.Health(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, probeStoppedHealth, msg)
	require.Equal(t, 1, panicLines(logger.Lines()))
	require.NoError(t, srv.Stop(context.Background()))
}

func TestPipelineRecoversFromIsolatedPanics(t *testing.T) {
	var (
		mu    sync.Mutex
		calls int
	)

	srv := newProbe(t, probeOpts{})
	srv.processHook = func(*snapshot.Snapshot) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()

		if n%2 == 0 {
			panic("intermittent")
		}
	}

	errCh := runProbe(t, srv)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return calls > 20
	}, 10*time.Second, 10*time.Millisecond)

	select {
	case err := <-errCh:
		require.FailNow(t, "probe stopped on isolated panics", "%v", err)
	default:
	}

	status, msg, err := srv.Health(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.NotEqual(t, probeStoppedHealth, msg)
}

func TestDefaultStatePath(t *testing.T) {
	s := settings.NewSettings()
	require.Equal(t, defaultStateFile, statePath(s))

	s.Probe.StatePath = "/var/lib/probe/state.json"
	require.Equal(t, "/var/lib/probe/state.json", statePath(s))
}

func TestKafkaHostsSplit(t *testing.T) {
	require.Empty(t, kafkaHosts(""))
	require.Empty(t, kafkaHosts(" , "))
	require.Equal(t, []string{"a:9092", "b:9092"}, kafkaHosts("a:9092, b:9092,"))
}
