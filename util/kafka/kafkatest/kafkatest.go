// Package kafkatest provides a performance testing framework for Kafka producers
// and consumers backed by a real Redpanda container (via testcontainers).
//
// Usage from a *testing.T or *testing.B:
//
//	env := kafkatest.MustStartEnv(t, ctx)
//	defer env.Close()
//	// env.BrokerAddr is ready — create producers/consumers against it.
//
// Tests that need no broker of their own share one per test binary instead, and
// isolate themselves by topic and consumer group (UniqueName):
//
//	env := kafkatest.SharedEnv(t)
//	topic := kafkatest.UniqueName("my-test")
//
// with TerminateSharedEnv called from the package's TestMain.
//
// The framework also provides helpers to run throughput/latency measurements and
// format results in a human-readable table.
package kafkatest

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	redpandaImage   = "redpandadata/redpanda"
	redpandaVersion = "v24.3.1"
)

// Env holds the Redpanda container and connection details for a test run.
type Env struct {
	Container  testcontainers.Container
	BrokerAddr string
	mu         sync.Mutex
}

// MustStartEnv starts a single-node Redpanda container. It skips the test if
// the container runtime itself is unavailable, calls t.Fatal on any other
// startup error, and registers a cleanup function so callers don't need to
// defer Close.
func MustStartEnv(t testing.TB, ctx context.Context) *Env {
	t.Helper()

	env, err := startEnv(ctx)
	test.SkipIfContainerUnavailable(t, err)

	t.Cleanup(func() { env.Close() })

	return env
}

var (
	sharedOnce sync.Once
	sharedEnv  *Env
	sharedErr  error

	uniqueNameCounter atomic.Uint64
)

// SharedEnv returns the one Redpanda container shared by every caller in this
// test binary, starting it on first use. It skips the test if the container
// runtime itself is unavailable and fails it on any other startup error.
//
// Callers share the broker, so each must use topics and consumer groups of its
// own (UniqueName). The container outlives the test: the package's TestMain
// must call TerminateSharedEnv after m.Run, because CI turns off the
// testcontainers reaper that would otherwise remove it when the process exits.
func SharedEnv(t testing.TB) *Env {
	t.Helper()

	sharedOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		sharedEnv, sharedErr = startEnv(ctx)
	})

	test.SkipIfContainerUnavailable(t, sharedErr)

	return sharedEnv
}

// TerminateSharedEnv stops the container SharedEnv started, if any. Call it
// from TestMain after m.Run. No test may call SharedEnv afterwards.
func TerminateSharedEnv() {
	if sharedEnv != nil {
		sharedEnv.Close()
	}
}

// UniqueName returns prefix followed by a suffix no other call in this process
// returns, for a topic or consumer group on a shared broker. A rerun of the
// same test under -count gets a new name too.
func UniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), uniqueNameCounter.Add(1))
}

// startEnv starts a single-node Redpanda container on a free host port.
func startEnv(ctx context.Context) (*Env, error) {
	port, err := getFreePort()
	if err != nil {
		return nil, err
	}

	portStr := fmt.Sprintf("%d/tcp", port)
	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("%s:%s", redpandaImage, redpandaVersion),
		ExposedPorts: []string{portStr},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{
				network.MustParsePort(portStr): []network.PortBinding{
					{HostIP: netip.IPv4Unspecified(), HostPort: fmt.Sprintf("%d", port)},
				},
			}
		},
		Cmd: []string{
			"redpanda", "start",
			"--overprovisioned",
			"--smp=1",
			"--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", port),
			"--advertise-kafka-addr", fmt.Sprintf("PLAINTEXT://localhost:%d", port),
		},
		WaitingFor: wait.ForLog("Successfully started Redpanda!").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return &Env{
		Container:  container,
		BrokerAddr: fmt.Sprintf("localhost:%d", port),
	}, nil
}

// Close terminates the Redpanda container.
func (e *Env) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Container != nil {
		_ = e.Container.Terminate(context.Background())
		e.Container = nil
	}
}

// TopicURL returns a kafka:// URL string suitable for passing to
// NewKafkaAsyncProducerFromURL / NewKafkaConsumerGroupFromURL.
func (e *Env) TopicURL(topic string, queryParams ...string) string {
	q := "partitions=1&replication=1&retention=60000&flush_frequency=100ms&replay=1"
	if len(queryParams) > 0 {
		q = queryParams[0]
	}
	return fmt.Sprintf("kafka://%s/%s?%s", e.BrokerAddr, topic, q)
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// ---------- Result collection ----------

// Result holds one measurement from a performance test.
type Result struct {
	Name           string
	MessageCount   int
	MessageSize    int
	Elapsed        time.Duration
	ThroughputMBps float64
	ThroughputMsgs float64
	P50Latency     time.Duration
	P90Latency     time.Duration
	P99Latency     time.Duration
}

// ComputeThroughput fills ThroughputMBps and ThroughputMsgs from the other fields.
func (r *Result) ComputeThroughput() {
	if r.Elapsed <= 0 {
		return
	}
	secs := r.Elapsed.Seconds()
	r.ThroughputMsgs = float64(r.MessageCount) / secs
	totalBytes := float64(r.MessageCount) * float64(r.MessageSize)
	r.ThroughputMBps = totalBytes / secs / (1024 * 1024)
}

// ComputeLatencyPercentiles calculates P50/P90/P99 from a slice of latencies.
func (r *Result) ComputeLatencyPercentiles(latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	r.P50Latency = percentile(latencies, 50)
	r.P90Latency = percentile(latencies, 90)
	r.P99Latency = percentile(latencies, 99)
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// FormatResults renders a slice of results as a human-readable table.
func FormatResults(results []Result) string {
	var sb strings.Builder
	sb.WriteString("\n┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐\n")
	sb.WriteString(fmt.Sprintf("│ %-40s │ %8s │ %8s │ %10s │ %10s │ %8s │ %8s │ %8s │\n",
		"Test", "Msgs", "MsgSize", "MB/s", "msgs/s", "P50", "P90", "P99"))
	sb.WriteString("├──────────────────────────────────────────┼──────────┼──────────┼────────────┼────────────┼──────────┼──────────┼──────────┤\n")
	for _, r := range results {
		sb.WriteString(fmt.Sprintf("│ %-40s │ %8d │ %7dB │ %9.2f │ %10.0f │ %8s │ %8s │ %8s │\n",
			truncate(r.Name, 40),
			r.MessageCount,
			r.MessageSize,
			r.ThroughputMBps,
			r.ThroughputMsgs,
			fmtDur(r.P50Latency),
			fmtDur(r.P90Latency),
			fmtDur(r.P99Latency),
		))
	}
	sb.WriteString("└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘\n")
	return sb.String()
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-1] + "…"
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "—"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d.Microseconds()))
	}
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}
