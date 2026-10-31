package settings

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProbeSettingsDefaults(t *testing.T) {
	s := NewSettings()

	require.Equal(t, "localhost:8100", s.Probe.HTTPListenAddress)
	require.Equal(t, "", s.Probe.AuthToken)
	require.Equal(t, 2*time.Second, s.Probe.SampleInterval)
	require.Equal(t, 15*time.Second, s.Probe.SlowSampleInterval)
	require.Equal(t, 30*time.Minute, s.Probe.BaselineWarmup)
	require.Equal(t, 15*time.Minute, s.Probe.SnoozeDefault)
	require.Equal(t, 168*time.Hour, s.Probe.AlarmLogRetention)
	require.Empty(t, s.Probe.MetricsTargets)
	require.Equal(t, "", s.Probe.StatePath)
	require.Nil(t, s.Probe.AerospikeURL)
	require.Nil(t, s.Probe.PostgresURL)
	require.Equal(t, 0.0, s.Probe.PostgresDiskCapacityGB)
	require.Empty(t, s.Probe.Floors)
}

func TestProbeSettingsReadOverrides(t *testing.T) {
	t.Setenv("probe_authToken", "secret")
	t.Setenv("probe_sampleInterval", "5s")
	t.Setenv("probe_metricsTargets", "a:1|b:2")
	t.Setenv("probe_floors", "tps=100|hit=0.9")
	t.Setenv("probe_postgresURL", "postgres://u:p@h:5432/db")
	t.Setenv("probe_postgresDiskCapacityGB", "250.5")

	s := NewSettings()

	require.Equal(t, "secret", s.Probe.AuthToken)
	require.Equal(t, 5*time.Second, s.Probe.SampleInterval)
	require.Equal(t, []string{"a:1", "b:2"}, s.Probe.MetricsTargets)
	require.Equal(t, []string{"tps=100", "hit=0.9"}, s.Probe.Floors)
	require.NotNil(t, s.Probe.PostgresURL)
	require.Equal(t, "h:5432", s.Probe.PostgresURL.Host)
	require.Equal(t, 250.5, s.Probe.PostgresDiskCapacityGB)
}
