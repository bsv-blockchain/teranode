package propagation

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// recordingProducer is a KafkaAsyncProducerI test double that records whether
// Stop() was invoked. stopCalled is closed on the first Stop() call so a test can
// wait for it deterministically; Stop is safe to call more than once.
type recordingProducer struct {
	once       sync.Once
	stopCalled chan struct{}
}

func newRecordingProducer() *recordingProducer {
	return &recordingProducer{stopCalled: make(chan struct{})}
}

func (r *recordingProducer) Start(_ context.Context, _ chan *kafka.Message) {}
func (r *recordingProducer) Stop() error {
	r.once.Do(func() { close(r.stopCalled) })
	return nil
}
func (r *recordingProducer) BrokersURL() []string             { return nil }
func (r *recordingProducer) Publish(_ *kafka.Message)         {}
func (r *recordingProducer) TryPublish(_ *kafka.Message) bool { return false }

// TestStop_UDPWorkerWedged_BoundedByCtx proves Stop() does not hang when an
// in-flight UDP/tx worker is wedged (e.g. a publish blocked on a dead broker).
// Before the fix, Stop() blocked unconditionally on ps.udpWg.Wait(), which would
// stall the entire reverse-order shutdown loop past StopTimeout. The fix races
// the wait against ctx.Done(), so Stop() must return bounded by the ctx and
// surface the deadline error. (A panic would crash the test process, so simply
// returning is also the "no panic" assertion.)
func TestStop_UDPWorkerWedged_BoundedByCtx(t *testing.T) {
	ps := &PropagationServer{
		logger: ulogger.TestLogger{},
		// httpServer and validatorKafkaProducerClient left nil — those cleanup
		// steps are skipped; this isolates the udpWg wait.
	}

	// A wedged worker: holds a udpWg slot and never returns until the test
	// releases it.
	block := make(chan struct{})
	ps.udpWg.Add(1)
	go func() {
		defer ps.udpWg.Done()
		<-block
	}()

	const timeout = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()
	err := ps.Stop(ctx)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded, "Stop must surface the stop-budget deadline")
	require.Less(t, elapsed, 2*time.Second, "Stop must be bounded by ctx, not block on the wedged worker")

	// Release the wedged worker and join it so no goroutine outlives the test.
	close(block)
	ps.udpWg.Wait()
}

// TestStop_CleanPath_ReturnsNil verifies the normal path: when the UDP/tx workers
// drain before the budget expires, Stop() runs the full cleanup and returns nil.
func TestStop_CleanPath_ReturnsNil(t *testing.T) {
	ps := &PropagationServer{
		logger: ulogger.TestLogger{},
	}

	// A worker that finishes immediately, so udpWg drains before Stop is called.
	release := make(chan struct{})
	ps.udpWg.Add(1)
	go func() {
		defer ps.udpWg.Done()
		<-release
	}()
	close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := ps.Stop(ctx)
	elapsed := time.Since(start)

	require.NoError(t, err, "clean stop must return nil")
	require.Less(t, elapsed, 2*time.Second, "clean stop must return promptly")
}

// TestStop_ContinuesCleanupAfterUDPTimeout is the regression test for the
// no-early-return behaviour: when the UDP wait times out, Stop() must STILL run
// the remaining bounded cleanup (HTTP shutdown + validator producer Stop) rather
// than bailing. With a wedged udpWg plus a real HTTP listener and a recording
// producer, the test fails if either cleanup step is skipped — which is exactly
// what an early return at the timeout would do.
func TestStop_ContinuesCleanupAfterUDPTimeout(t *testing.T) {
	prod := newRecordingProducer()

	// A real loopback HTTP server so we can prove Shutdown is attempted (the
	// listener stops accepting after Stop()).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := ln.Addr().String()

	e := echo.New()
	e.HideBanner = true
	e.GET("/health", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })
	e.Listener = ln

	go func() { _ = e.Server.Serve(ln) }()

	ps := &PropagationServer{
		logger:                       ulogger.TestLogger{},
		httpServer:                   e,
		validatorKafkaProducerClient: prod,
	}

	// Wedged worker: stays in-flight (holds a udpWg slot) for the whole test, so
	// the udpWg.Wait() can only end via the ctx timeout, never by draining.
	block := make(chan struct{})
	ps.udpWg.Add(1)
	go func() {
		defer ps.udpWg.Done()
		<-block
	}()

	const timeout = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()
	stopErr := ps.Stop(ctx)
	elapsed := time.Since(start)

	require.ErrorIs(t, stopErr, context.DeadlineExceeded, "Stop must surface the stop-budget deadline")
	require.Less(t, elapsed, 2*time.Second, "Stop must be bounded by ctx, not block on the wedged worker")

	// Proof #1: cleanup continued past the udp timeout — the validator producer
	// Stop() was invoked even though the udp worker is still wedged. Without the
	// "continue cleanup" behaviour, this channel would never close.
	select {
	case <-prod.stopCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("validator producer Stop() was not invoked — Stop() skipped cleanup after the udp timeout")
	}

	// Proof #2: HTTP shutdown was attempted — the listener no longer accepts.
	require.Eventually(t, func() bool {
		conn, derr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if derr != nil {
			return true
		}

		_ = conn.Close()

		return false
	}, 2*time.Second, 50*time.Millisecond, "http server listener should be closed after Stop")

	// Release the wedged worker and join it so no goroutine outlives the test.
	close(block)
	ps.udpWg.Wait()
}
