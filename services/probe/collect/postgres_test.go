package collect

import (
	"context"
	"net"
	"net/url"
	"runtime"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

func TestSqliteStoreIsNotConfigured(t *testing.T) {
	s := settings.NewSettings()
	s.Probe.PostgresURL = nil
	s.BlockChain.StoreURL = &url.URL{Scheme: "sqlite", Path: "/blockchain"}

	require.Nil(t, PostgresURL(s))

	p, err := NewPostgres(PostgresURL(s), time.Second, 0)
	require.NoError(t, err)

	defer p.Close()

	require.Equal(t, "postgres", p.Name())

	patch, err := p.Collect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, patch)

	snap := snapshot.Snapshot{}
	snap.Stores.Postgres.Configured = true
	patch(&snap)

	st := snap.Stores.Postgres
	require.False(t, st.Configured)

	for _, r := range []snapshot.Reading{st.DiskFill, st.SizeGB, st.ReadP99Sec, st.OpsPerSec, st.CommitsPerSec} {
		require.Equal(t, snapshot.StatusNoData, r.Status)
	}
}

func TestPostgresURL(t *testing.T) {
	pg := &url.URL{Scheme: "postgres", Host: "a:5432", Path: "/x"}
	other := &url.URL{Scheme: "postgres", Host: "b:5432", Path: "/y"}

	tests := []struct {
		name  string
		probe *url.URL
		store *url.URL
		want  *url.URL
	}{
		{"probe setting wins", pg, other, pg},
		{"store url with postgres scheme", nil, other, other},
		{"sqlite store", nil, &url.URL{Scheme: "sqlite", Path: "/b"}, nil},
		{"memory store", nil, &url.URL{Scheme: "sqlitememory"}, nil},
		{"no store", nil, nil, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := settings.NewSettings()
			s.Probe.PostgresURL = tc.probe
			s.BlockChain.StoreURL = tc.store

			require.Equal(t, tc.want, PostgresURL(s))
		})
	}

	require.Nil(t, PostgresURL(nil))
}

func TestNewPostgresRejectsBadInput(t *testing.T) {
	u := &url.URL{Scheme: "postgres", Host: "127.0.0.1:5432", Path: "/db"}

	_, err := NewPostgres(u, 0, 0)
	require.Error(t, err)

	_, err = NewPostgres(u, time.Second, -1)
	require.Error(t, err)

	_, err = NewPostgres(&url.URL{Scheme: "postgres", Host: "127.0.0.1:5432"}, time.Second, 0)
	require.Error(t, err)
}

func TestDiskFillUsesCapacitySetting(t *testing.T) {
	r := diskFillReading(3, 12)
	require.Equal(t, snapshot.StatusOK, r.Status)
	require.InDelta(t, 0.25, r.Value, 1e-9)

	r = diskFillReading(3, 0)
	require.Equal(t, snapshot.StatusNoData, r.Status)
	require.Equal(t, "disk capacity not set", r.Note)

	r = diskFillReading(30, 12)
	require.Equal(t, snapshot.StatusOK, r.Status)
	require.Equal(t, 1.0, r.Value)

	r = diskFillReading(0, 12)
	require.Equal(t, snapshot.StatusOK, r.Status)
	require.Equal(t, 0.0, r.Value)
}

func TestCounterRate(t *testing.T) {
	v, ok := counterRate(100, 160, 10)
	require.True(t, ok)
	require.InDelta(t, 6.0, v, 1e-9)

	v, ok = counterRate(100, 100, 10)
	require.True(t, ok)
	require.Equal(t, 0.0, v)

	_, ok = counterRate(160, 100, 10)
	require.False(t, ok)

	_, ok = counterRate(100, 160, 0)
	require.False(t, ok)
}

func TestMeanExecSeconds(t *testing.T) {
	v, ok := meanExecSeconds(1000, 1500, 10, 110)
	require.True(t, ok)
	require.InDelta(t, 0.005, v, 1e-12)

	_, ok = meanExecSeconds(1000, 1500, 10, 10)
	require.False(t, ok)

	_, ok = meanExecSeconds(1000, 900, 10, 20)
	require.False(t, ok)

	_, ok = meanExecSeconds(1000, 1500, 20, 10)
	require.False(t, ok)
}

func TestPostgresConnConfigIsReadOnlyAndQuotesCredentials(t *testing.T) {
	u := &url.URL{Scheme: "postgres", User: url.UserPassword("us er", `p a'ss\w=rd`), Host: "db.example:6543", Path: "/my_database"}

	p, err := NewPostgres(u, 3*time.Second, 0)
	require.NoError(t, err)

	cfg := p.connConfig()
	require.Equal(t, "on", cfg.RuntimeParams["default_transaction_read_only"])
	require.Equal(t, "us er", cfg.User)
	require.Equal(t, `p a'ss\w=rd`, cfg.Password)
	require.Equal(t, "db.example", cfg.Host)
	require.Equal(t, uint16(6543), cfg.Port)
	require.Equal(t, "my_database", cfg.Database)
	require.Equal(t, 3*time.Second, cfg.ConnectTimeout)
}

func acceptAndHold(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			defer conn.Close()
		}
	}()

	return ln.Addr().String()
}

func TestHungServerTimesOut(t *testing.T) {
	addr := acceptAndHold(t)

	p, err := NewPostgres(&url.URL{Scheme: "postgres", User: url.UserPassword("u", "topsecret"), Host: addr, Path: "/db"}, 300*time.Millisecond, 0)
	require.NoError(t, err)

	defer p.Close()

	start := time.Now()
	patch, err := p.Collect(context.Background())

	require.Error(t, err)
	require.Nil(t, patch)
	require.Less(t, time.Since(start), 3*time.Second)
	require.NotContains(t, err.Error(), "topsecret")
}

func TestRepeatedFailedPostgresCollectsDoNotLeakGoroutines(t *testing.T) {
	hungAddr := acceptAndHold(t)

	closed, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	closedAddr := closed.Addr().String()
	require.NoError(t, closed.Close())

	hung, err := NewPostgres(&url.URL{Scheme: "postgres", User: url.UserPassword("u", "p"), Host: hungAddr, Path: "/db"}, 200*time.Millisecond, 0)
	require.NoError(t, err)

	down, err := NewPostgres(&url.URL{Scheme: "postgres", User: url.UserPassword("u", "p"), Host: closedAddr, Path: "/db"}, 200*time.Millisecond, 0)
	require.NoError(t, err)

	baseline := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		patch, herr := hung.Collect(context.Background())
		require.Error(t, herr)
		require.Nil(t, patch)

		patch, derr := down.Collect(context.Background())
		require.Error(t, derr)
		require.Nil(t, patch)
	}

	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseline+5
	}, 10*time.Second, 100*time.Millisecond, "goroutines: baseline %d", baseline)

	hung.Close()
	down.Close()
	hung.Close()
}

func TestCollectAfterCloseFails(t *testing.T) {
	p, err := NewPostgres(&url.URL{Scheme: "postgres", Host: "127.0.0.1:1", Path: "/db"}, 200*time.Millisecond, 0)
	require.NoError(t, err)

	p.Close()
	p.Close()

	patch, err := p.Collect(context.Background())
	require.Error(t, err)
	require.Nil(t, patch)
}
