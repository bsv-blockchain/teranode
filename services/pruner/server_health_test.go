package pruner

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func behindMonitor(t *testing.T, failReadiness bool) *BacklogMonitor {
	t.Helper()
	cfg := defaultTestConfig()
	cfg.FailReadiness = failReadiness
	m, clk := newTestMonitor(cfg)
	m.OnPruneRequested(100)
	m.OnRunStarted(100)
	for h := uint32(101); h <= 104; h++ {
		clk.advance(time.Minute)
		m.OnPruneRequested(h)
	}
	require.Equal(t, BacklogBehind, m.Snapshot().State)
	return m
}

func serverWithBacklog(m *BacklogMonitor) *Server {
	return &Server{logger: ulogger.TestLogger{}, settings: &settings.Settings{}, backlog: m}
}

func TestHealth_ReadinessFailsWhileBehind(t *testing.T) {
	s := serverWithBacklog(behindMonitor(t, true))
	status, body, err := s.Health(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Contains(t, body, "PrunerBacklog")
	require.Contains(t, body, "pruner BEHIND")
}

func TestHealth_ReadinessPassesWhenFailReadinessDisabled(t *testing.T) {
	s := serverWithBacklog(behindMonitor(t, false))
	status, body, err := s.Health(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Contains(t, body, "pruner BEHIND", "state is still reported in the message")
}

func TestHealth_LivenessNeverFailsOnBacklog(t *testing.T) {
	s := serverWithBacklog(behindMonitor(t, true))
	status, _, err := s.Health(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
}

func TestHealth_ReadinessOKWhenNoMonitor(t *testing.T) {
	s := serverWithBacklog(nil)
	status, _, err := s.Health(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
}
