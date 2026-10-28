package collect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func serveText(body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
}

func TestHTTPSourceMergesTargets(t *testing.T) {
	a := serveText("# TYPE alpha_total counter\nalpha_total 3\n# TYPE shared gauge\nshared{n=\"a\"} 1\n")
	defer a.Close()

	b := serveText("# TYPE beta gauge\nbeta 7\n# TYPE shared gauge\nshared{n=\"b\"} 2\n")
	defer b.Close()

	set, err := NewHTTPSource([]string{a.URL, b.URL}, time.Second).Gather(context.Background())
	require.NoError(t, err)

	v, ok := set.Sum("alpha_total")
	require.True(t, ok)
	require.Equal(t, 3.0, v)

	v, ok = set.Sum("beta")
	require.True(t, ok)
	require.Equal(t, 7.0, v)

	require.Len(t, set["shared"].Metric, 2)

	v, ok = set.Sum("shared")
	require.True(t, ok)
	require.Equal(t, 3.0, v)
}

func TestHTTPSourceTimeout(t *testing.T) {
	good := serveText("# TYPE beta gauge\nbeta 7\n")
	defer good.Close()

	release := make(chan struct{})
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))

	defer hung.Close()
	defer close(release)

	timeout := 200 * time.Millisecond
	start := time.Now()
	set, err := NewHTTPSource([]string{hung.URL, good.URL}, timeout).Gather(context.Background())
	elapsed := time.Since(start)

	require.Less(t, elapsed, timeout+100*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), hung.URL)

	v, ok := set.Sum("beta")
	require.True(t, ok)
	require.Equal(t, 7.0, v)
}

func TestHTTPSourceBadStatusAndBadBody(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer bad.Close()

	garbage := serveText("this is {{ not metrics\n")
	defer garbage.Close()

	good := serveText("# TYPE beta gauge\nbeta 1\n")
	defer good.Close()

	set, err := NewHTTPSource([]string{bad.URL, garbage.URL, good.URL}, time.Second).Gather(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), bad.URL)
	require.Contains(t, err.Error(), garbage.URL)

	_, ok := set.Sum("beta")
	require.True(t, ok)
}

func TestHTTPSourceRefusesOversizedBody(t *testing.T) {
	big := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := []byte(strings.Repeat("# padding padding padding padding padding padding\n", 1000))

		for written := 0; written <= maxBodyBytes; written += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer big.Close()

	set, err := NewHTTPSource([]string{big.URL}, 10*time.Second).Gather(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
	require.Empty(t, set)
}

func TestLocalSourceSeesRegisteredGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "probe_test_gauge", Help: "test"})
	reg.MustRegister(g)
	g.Set(42)

	set, err := newLocalSource(reg).Gather(context.Background())
	require.NoError(t, err)

	v, ok := set.Sum("probe_test_gauge")
	require.True(t, ok)
	require.Equal(t, 42.0, v)
}

func TestNewLocalSourceUsesDefaultGatherer(t *testing.T) {
	_, err := NewLocalSource().Gather(context.Background())
	require.NoError(t, err)
}

func TestSumSkipsMissing(t *testing.T) {
	set := MetricSet{}

	v, ok := set.Sum("nope")
	require.False(t, ok)
	require.Zero(t, v)
}

func TestSumHistogramUsesSampleSum(t *testing.T) {
	srv := serveText("# TYPE h histogram\nh_bucket{le=\"1\"} 1\nh_bucket{le=\"+Inf\"} 2\nh_sum 5.5\nh_count 2\n# TYPE u untyped\nu{a=\"1\"} 2\nu{a=\"2\"} 3\n")
	defer srv.Close()

	set, err := NewHTTPSource([]string{srv.URL}, time.Second).Gather(context.Background())
	require.NoError(t, err)

	v, ok := set.Sum("h")
	require.True(t, ok)
	require.Equal(t, 5.5, v)

	v, ok = set.Sum("u")
	require.True(t, ok)
	require.Equal(t, 5.0, v)
}
