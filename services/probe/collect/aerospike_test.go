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

func TestNotConfigured(t *testing.T) {
	a, err := NewAerospike(nil, time.Second)
	require.NoError(t, err)
	require.Equal(t, "aerospike", a.Name())

	patch, err := a.Collect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, patch)

	s := snapshot.NewSnapshot("n")
	s.Stores.Aerospike.Configured = true
	patch(&s)

	st := s.Stores.Aerospike
	require.False(t, st.Configured)
	require.Equal(t, snapshot.StatusNoData, st.SizeGB.Status)
	require.Equal(t, snapshot.StatusNoData, st.DiskFill.Status)
	require.Equal(t, snapshot.StatusNoData, st.ReadP99Sec.Status)

	a.Close()
	a.Close()
}

func TestHungNodeTimesOut(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer ln.Close()

	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			defer conn.Close()
		}
	}()

	u := &url.URL{Scheme: "aerospike", Host: ln.Addr().String(), Path: "/test"}

	timeout := 500 * time.Millisecond

	a, err := NewAerospike(u, timeout)
	require.NoError(t, err)

	defer a.Close()

	start := time.Now()
	patch, err := a.Collect(context.Background())
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Nil(t, patch)
	require.Less(t, elapsed, timeout+300*time.Millisecond)
}

func TestUnreachableNodeAtStartDoesNotFailNew(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	u := &url.URL{Scheme: "aerospike", Host: addr, Path: "/test"}

	a, err := NewAerospike(u, time.Second)
	require.NoError(t, err)

	patch, err := a.Collect(context.Background())
	require.Error(t, err)
	require.Nil(t, patch)

	a.Close()
	a.Close()

	patch, err = a.Collect(context.Background())
	require.Error(t, err)
	require.Nil(t, patch)
}

func TestNewAerospikeRejectsBadInput(t *testing.T) {
	_, err := NewAerospike(&url.URL{Scheme: "aerospike", Host: "127.0.0.1:3000"}, time.Second)
	require.Error(t, err)

	_, err = NewAerospike(&url.URL{Scheme: "aerospike", Host: "127.0.0.1:3000", Path: "/test"}, 0)
	require.Error(t, err)

	_, err = NewAerospike(&url.URL{Scheme: "aerospike", Host: "127.0.0.1:notaport", Path: "/test"}, time.Second)
	require.Error(t, err)
}

func TestAerospikeURL(t *testing.T) {
	probeURL, err := url.Parse("aerospike://probe:pw@probehost:3100/probens")
	require.NoError(t, err)

	utxoURL, err := url.Parse("aerospike://utxohost:3000/utxo?set=utxo")
	require.NoError(t, err)

	pgURL, err := url.Parse("postgres://u:p@pg:5432/db")
	require.NoError(t, err)

	tests := []struct {
		name  string
		probe *url.URL
		utxo  *url.URL
		want  *url.URL
	}{
		{"probe url wins", probeURL, utxoURL, probeURL},
		{"falls back to utxo store", nil, utxoURL, utxoURL},
		{"ignores non aerospike utxo store", nil, pgURL, nil},
		{"neither set", nil, nil, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &settings.Settings{}
			s.Probe.AerospikeURL = tc.probe
			s.UtxoStore.UtxoStore = tc.utxo

			require.Equal(t, tc.want, AerospikeURL(s))
		})
	}

	require.Nil(t, AerospikeURL(nil))
}

func TestParseReadLatencyP99(t *testing.T) {
	tests := []struct {
		name   string
		info   string
		ns     string
		want   float64
		wantOK bool
	}{
		{
			name:   "first bucket above one percent picks the next threshold",
			info:   "{test}-read:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,100.0,5.20,0.70,0.00;",
			ns:     "test",
			want:   0.008,
			wantOK: true,
		},
		{
			name:   "all buckets under one percent picks the smallest threshold",
			info:   "{test}-read:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,100.0,0.90,0.10,0.00;",
			ns:     "test",
			want:   0.001,
			wantOK: true,
		},
		{
			name:   "all buckets above one percent picks the largest threshold",
			info:   "{test}-read:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,100.0,50.00,20.00,3.00;",
			ns:     "test",
			want:   0.064,
			wantOK: true,
		},
		{
			name:   "exactly one percent is not under one percent",
			info:   "{test}-read:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,100.0,1.00,1.00,0.99;",
			ns:     "test",
			want:   0.064,
			wantOK: true,
		},
		{
			name:   "picks the read histogram among several",
			info:   "batch-index:;{test}-write:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,50.0,90.00,90.00,90.00;{test}-read:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,100.0,5.20,0.70,0.00;{other}-read:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,100.0,80.00,80.00,80.00;",
			ns:     "test",
			want:   0.008,
			wantOK: true,
		},
		{
			name:   "microsecond and second thresholds",
			info:   "{test}-read:10:00:00-GMT,ops/sec,>250us,>1ms,>1s;10:00:10,10.0,30.00,0.50,0.00;",
			ns:     "test",
			want:   0.001,
			wantOK: true,
		},
		{
			name:   "captured from the server with back window all zero picks the smallest threshold",
			info:   "{test}-read:msec,504.3,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00",
			ns:     "test",
			want:   0.001,
			wantOK: true,
		},
		{
			name:   "msec format thresholds double per column",
			info:   "{test}-read:msec,100.0,40.00,12.50,3.10,0.80,0.10,0.00",
			ns:     "test",
			want:   0.008,
			wantOK: true,
		},
		{
			name:   "msec format all above one percent picks the largest threshold",
			info:   "{test}-read:msec,100.0,90.00,80.00,70.00",
			ns:     "test",
			want:   0.004,
			wantOK: true,
		},
		{
			name:   "usec format scales to seconds",
			info:   "{test}-read:usec,100.0,60.00,0.50",
			ns:     "test",
			want:   0.000002,
			wantOK: true,
		},
		{
			name:   "msec format with no ops is no data",
			info:   "{test}-read:msec,0.0,0.00,0.00,0.00",
			ns:     "test",
			wantOK: false,
		},
		{
			name:   "empty histogram name only is no data",
			info:   "{test}-read:",
			ns:     "test",
			wantOK: false,
		},
		{
			name:   "error text is no data",
			info:   "{test}-read:error-no-data-yet-or-back-too-small;",
			ns:     "test",
			wantOK: false,
		},
		{
			name:   "no ops in the window is no data",
			info:   "{test}-read:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,0.0,0.00,0.00,0.00;",
			ns:     "test",
			wantOK: false,
		},
		{
			name:   "other namespace only is no data",
			info:   "{other}-read:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,100.0,5.20,0.70,0.00;",
			ns:     "test",
			wantOK: false,
		},
		{
			name:   "empty is no data",
			info:   "",
			ns:     "test",
			wantOK: false,
		},
		{
			name:   "server error is no data",
			info:   "ERROR:4:unknown histogram",
			ns:     "test",
			wantOK: false,
		},
		{
			name:   "garbage percentages are no data",
			info:   "{test}-read:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,100.0,x,y,z;",
			ns:     "test",
			wantOK: false,
		},
		{
			name:   "column count mismatch is no data",
			info:   "{test}-read:10:00:00-GMT,ops/sec,>1ms,>8ms,>64ms;10:00:10,100.0,5.20;",
			ns:     "test",
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseReadLatencyP99(tc.info, tc.ns)
			require.Equal(t, tc.wantOK, ok)

			if tc.wantOK {
				require.InDelta(t, tc.want, got, 1e-9)
			}
		})
	}
}

func TestNamespaceUsage(t *testing.T) {
	tests := []struct {
		name     string
		info     string
		wantUsed float64
		wantFill float64
		wantOK   bool
	}{
		{
			name:     "aerospike 7 data bytes",
			info:     "name=test;data_used_bytes=250;data_total_bytes=1000;memory_used_bytes=5",
			wantUsed: 250,
			wantFill: 0.25,
			wantOK:   true,
		},
		{
			name:     "aerospike 6 device bytes",
			info:     "name=test;device_used_bytes=300;device_total_bytes=1200;memory_used_bytes=5",
			wantUsed: 300,
			wantFill: 0.25,
			wantOK:   true,
		},
		{
			name:     "memory only namespace",
			info:     "name=test;memory_used_bytes=100;memory-size=400",
			wantUsed: 100,
			wantFill: 0.25,
			wantOK:   true,
		},
		{
			name:   "zero total is no data",
			info:   "name=test;data_used_bytes=0;data_total_bytes=0",
			wantOK: false,
		},
		{
			name:   "nothing known is no data",
			info:   "name=test;objects=3",
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			used, fill, ok := namespaceUsage(tc.info)
			require.Equal(t, tc.wantOK, ok)

			if tc.wantOK {
				require.InDelta(t, tc.wantUsed, used, 1e-9)
				require.InDelta(t, tc.wantFill, fill, 1e-9)
			}
		})
	}
}

func TestAggregateNodes(t *testing.T) {
	tests := []struct {
		name      string
		nodes     []nodeStats
		wantSize  float64
		wantFill  float64
		wantP99   float64
		wantP99OK bool
		wantOK    bool
	}{
		{
			name:      "single node",
			nodes:     []nodeStats{{name: "a", answered: true, usage: true, sizeGB: 2, fill: 0.25, p99: 0.002, p99ok: true}},
			wantSize:  2,
			wantFill:  0.25,
			wantP99:   0.002,
			wantP99OK: true,
			wantOK:    true,
		},
		{
			name: "three nodes sum sizes and take the fullest and slowest",
			nodes: []nodeStats{
				{name: "a", answered: true, usage: true, sizeGB: 10, fill: 0.20, p99: 0.001, p99ok: true},
				{name: "b", answered: true, usage: true, sizeGB: 20, fill: 0.80, p99: 0.008, p99ok: true},
				{name: "c", answered: true, usage: true, sizeGB: 5, fill: 0.50, p99: 0.002, p99ok: true},
			},
			wantSize:  35,
			wantFill:  0.80,
			wantP99:   0.008,
			wantP99OK: true,
			wantOK:    true,
		},
		{
			name: "one node without latency does not blank the others",
			nodes: []nodeStats{
				{name: "a", answered: true, usage: true, sizeGB: 10, fill: 0.30, p99: 0.004, p99ok: true},
				{name: "b", answered: true, usage: true, sizeGB: 10, fill: 0.10},
			},
			wantSize:  20,
			wantFill:  0.30,
			wantP99:   0.004,
			wantP99OK: true,
			wantOK:    true,
		},
		{
			name: "no node with latency",
			nodes: []nodeStats{
				{name: "a", answered: true, usage: true, sizeGB: 1, fill: 0.1},
				{name: "b", answered: true, usage: true, sizeGB: 2, fill: 0.2},
			},
			wantSize: 3,
			wantFill: 0.2,
			wantOK:   true,
		},
		{
			name: "a failed node is ignored even when it carries values",
			nodes: []nodeStats{
				{name: "a", answered: true, usage: true, sizeGB: 4, fill: 0.40, p99: 0.001, p99ok: true},
				{name: "b", sizeGB: 100, fill: 0.99, p99: 0.064, p99ok: true},
			},
			wantSize:  4,
			wantFill:  0.40,
			wantP99:   0.001,
			wantP99OK: true,
			wantOK:    true,
		},
		{
			name:   "answered node without usage figures",
			nodes:  []nodeStats{{name: "a", answered: true}},
			wantOK: false,
		},
		{
			name:   "all nodes failed",
			nodes:  []nodeStats{{name: "a"}, {name: "b"}},
			wantOK: false,
		},
		{
			name:   "no nodes",
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			size, fill, p99, p99ok, ok := aggregateNodes(tc.nodes)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.wantP99OK, p99ok)
			require.InDelta(t, tc.wantSize, size, 1e-9)
			require.InDelta(t, tc.wantFill, fill, 1e-9)
			require.InDelta(t, tc.wantP99, p99, 1e-9)
		})
	}
}

func TestRepeatedFailedCollectsDoNotLeakGoroutines(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer ln.Close()

	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			defer conn.Close()
		}
	}()

	closed, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	closedAddr := closed.Addr().String()
	require.NoError(t, closed.Close())

	hung, err := NewAerospike(&url.URL{Scheme: "aerospike", Host: ln.Addr().String(), Path: "/test"}, 200*time.Millisecond)
	require.NoError(t, err)

	down, err := NewAerospike(&url.URL{Scheme: "aerospike", Host: closedAddr, Path: "/test"}, 200*time.Millisecond)
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
}
