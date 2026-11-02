//go:build aerospike

package collect

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	aero "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	aerospikeutils "github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/stretchr/testify/require"
)

func writeRecords(t *testing.T, client *aero.Client, from, count int) {
	t.Helper()

	payload := make([]byte, 100*1024)
	for i := range payload {
		payload[i] = byte(i * 7)
	}

	policy := aero.NewWritePolicy(0, 0)

	for i := from; i < from+count; i++ {
		key, err := aero.NewKey("test", "probe", "rec-"+strconv.Itoa(i))
		require.NoError(t, err)
		require.NoError(t, client.Put(policy, key, aero.BinMap{"b": payload}))
	}
}

func TestCollectSizeAndFill(t *testing.T) {
	rawURL, cleanup, err := aerospikeutils.InitAerospikeContainer()
	require.NoError(t, err)

	defer func() { require.NoError(t, cleanup()) }()

	u, err := url.Parse(rawURL)
	require.NoError(t, err)

	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)

	client, aerr := aero.NewClient(u.Hostname(), port)
	require.NoError(t, aerr)

	defer client.Close()

	a, err := NewAerospike(u, 5*time.Second)
	require.NoError(t, err)

	defer a.Close()

	collect := func() snapshot.StoreStats {
		patch, cerr := a.Collect(context.Background())
		require.NoError(t, cerr)
		require.NotNil(t, patch)

		s := snapshot.NewSnapshot("n")
		patch(&s)

		return s.Stores.Aerospike
	}

	writeRecords(t, client, 0, 200)

	var first snapshot.StoreStats

	require.Eventually(t, func() bool {
		first = collect()
		return first.SizeGB.Value > 0
	}, 30*time.Second, 500*time.Millisecond)

	require.True(t, first.Configured)
	require.Equal(t, snapshot.StatusOK, first.SizeGB.Status)
	require.Equal(t, snapshot.StatusOK, first.DiskFill.Status)
	require.Greater(t, first.SizeGB.Value, 0.0)
	require.Greater(t, first.DiskFill.Value, 0.0)
	require.Less(t, first.DiskFill.Value, 1.0)

	t.Logf("first: size=%v fill=%v p99=%+v", first.SizeGB.Value, first.DiskFill.Value, first.ReadP99Sec)

	writeRecords(t, client, 200, 400)

	var second snapshot.StoreStats

	require.Eventually(t, func() bool {
		second = collect()
		return second.SizeGB.Value > first.SizeGB.Value && second.DiskFill.Value > first.DiskFill.Value
	}, 30*time.Second, 500*time.Millisecond)

	require.Less(t, second.DiskFill.Value, 1.0)

	t.Logf("second: size=%v fill=%v p99=%+v", second.SizeGB.Value, second.DiskFill.Value, second.ReadP99Sec)
}

func startReadLatencyContainer(t *testing.T) (*url.URL, *aero.Client) {
	t.Helper()

	rawURL, cleanup, err := aerospikeutils.InitAerospikeContainer()
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, cleanup()) })

	u, err := url.Parse(rawURL)
	require.NoError(t, err)

	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)

	client, aerr := aero.NewClient(u.Hostname(), port)
	require.NoError(t, aerr)

	t.Cleanup(client.Close)

	return u, client
}

func TestCollectReadLatencyNotExposedWhenBenchmarksOff(t *testing.T) {
	u, client := startReadLatencyContainer(t)

	writeRecords(t, client, 0, 5)

	key, kerr := aero.NewKey("test", "probe", "rec-0")
	require.NoError(t, kerr)

	for i := 0; i < 200; i++ {
		_, gerr := client.Get(nil, key)
		require.NoError(t, gerr)
	}

	raw, ierr := client.GetNodes()[0].RequestInfo(aero.NewInfoPolicy(), readLatencyCommand("test"))
	require.NoError(t, ierr)
	require.Equal(t, "{test}-read:", raw[readLatencyCommand("test")])

	a, err := NewAerospike(u, 5*time.Second)
	require.NoError(t, err)

	defer a.Close()

	patch, err := a.Collect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, patch)

	s := snapshot.NewSnapshot("n")
	patch(&s)

	require.Equal(t, snapshot.StatusNoData, s.Stores.Aerospike.ReadP99Sec.Status)
	require.Equal(t, "latency not exposed", s.Stores.Aerospike.ReadP99Sec.Note)
}

func TestCollectReadLatencyWhenBenchmarksOn(t *testing.T) {
	u, client := startReadLatencyContainer(t)

	node := client.GetNodes()[0]

	_, ierr := node.RequestInfo(aero.NewInfoPolicy(), "set-config:context=namespace;id=test;enable-benchmarks-read=true")
	require.NoError(t, ierr)

	writeRecords(t, client, 0, 5)

	key, kerr := aero.NewKey("test", "probe", "rec-0")
	require.NoError(t, kerr)

	a, err := NewAerospike(u, 5*time.Second)
	require.NoError(t, err)

	defer a.Close()

	var got snapshot.Reading

	require.Eventually(t, func() bool {
		for i := 0; i < 500; i++ {
			if _, gerr := client.Get(nil, key); gerr != nil {
				return false
			}
		}

		patch, cerr := a.Collect(context.Background())
		if cerr != nil || patch == nil {
			return false
		}

		s := snapshot.NewSnapshot("n")
		patch(&s)
		got = s.Stores.Aerospike.ReadP99Sec

		return got.Status == snapshot.StatusOK
	}, 60*time.Second, time.Second)

	raw, rerr := node.RequestInfo(aero.NewInfoPolicy(), readLatencyCommand("test"))
	require.NoError(t, rerr)
	t.Logf("read latency info => %q ; p99=%v", raw[readLatencyCommand("test")], got.Value)

	require.Greater(t, got.Value, 0.0)
	require.LessOrEqual(t, got.Value, 0.064)
}
