package collect

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHTTPSourceWorkerPanicReachesCaller(t *testing.T) {
	src := NewHTTPSource([]string{"a:1", "b:2"}, time.Second).(*httpSource)
	src.scrapeFn = func(_ context.Context, target string) (MetricSet, error) {
		if target == "http://b:2/metrics" {
			panic("scrape fault")
		}

		return MetricSet{}, nil
	}

	require.PanicsWithValue(t, "metrics scrape panicked: scrape fault", func() {
		_, _ = src.Gather(context.Background())
	})
}

func TestAerospikeReadPanicReachesCaller(t *testing.T) {
	a, err := NewAerospike(&url.URL{Scheme: "aerospike", Host: "127.0.0.1:1", Path: "/test"}, time.Second)
	require.NoError(t, err)

	defer a.Close()

	a.readFn = func() (aerospikeReadings, error) { panic("read fault") }

	require.PanicsWithValue(t, "aerospike read panicked: read fault", func() {
		_, _ = a.Collect(context.Background())
	})
}

func TestAerospikeLatePanicSurfacesOnNextCollect(t *testing.T) {
	a, err := NewAerospike(&url.URL{Scheme: "aerospike", Host: "127.0.0.1:1", Path: "/test"}, 50*time.Millisecond)
	require.NoError(t, err)

	defer a.Close()

	release := make(chan struct{})
	done := make(chan struct{})

	a.readFn = func() (aerospikeReadings, error) {
		<-release
		defer close(done)

		panic("late fault")
	}

	_, err = a.Collect(context.Background())
	require.Error(t, err)

	close(release)
	<-done

	require.Eventually(t, a.hasPendingPanic, time.Second, time.Millisecond)

	a.readFn = func() (aerospikeReadings, error) { return aerospikeReadings{}, nil }

	require.PanicsWithValue(t, "aerospike read panicked: late fault", func() {
		_, _ = a.Collect(context.Background())
	})

	require.NotPanics(t, func() {
		_, err = a.Collect(context.Background())
	})
	require.NoError(t, err)
}
