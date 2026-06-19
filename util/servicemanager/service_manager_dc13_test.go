package servicemanager

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// stubService is a minimal in-test implementation of the Service interface used
// to exercise ServiceManager.Wait()'s shutdown semantics (DC13). It is a real
// component implementing the actual interface — no mocking framework.
//
// Sentinels use the PROJECT errors package (the repo's depguard lint forbids the
// stdlib errors package outside it), and identity is asserted with the project
// errors.Is, which matches by error code along the wrapped chain. The sentinels
// below use distinct codes (Processing / Configuration) so the assertions are
// unambiguous and the Stop wrapper (NewServiceError) does not alias them.
type stubService struct {
	// startErr, if non-nil, is returned immediately from Start (simulating a
	// service that fails). Otherwise Start blocks until its context is cancelled
	// and returns ctx.Err().
	startErr error
	// stopErr is returned from Stop (unless blockStop is set).
	stopErr error
	// blockStop makes Stop block on ctx.Done(), so the per-service StopTimeout
	// is what unblocks it; Stop then returns ctx.Err().
	blockStop bool

	started         atomic.Bool
	stopCalled      atomic.Bool
	stopHadDeadline atomic.Bool
}

func (s *stubService) Init(_ context.Context) error { return nil }

func (s *stubService) Start(ctx context.Context, ready chan<- struct{}) error {
	s.started.Store(true)

	if ready != nil {
		select {
		case ready <- struct{}{}:
		default:
		}
	}

	if s.startErr != nil {
		return s.startErr
	}

	<-ctx.Done()

	return ctx.Err()
}

func (s *stubService) Stop(ctx context.Context) error {
	s.stopCalled.Store(true)

	if _, ok := ctx.Deadline(); ok {
		s.stopHadDeadline.Store(true)
	}

	if s.blockStop {
		<-ctx.Done()
		return ctx.Err()
	}

	return s.stopErr
}

func (s *stubService) Health(_ context.Context, _ bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func newTestServiceManager(t *testing.T) *ServiceManager {
	t.Helper()

	logger := ulogger.New("dc13-test", ulogger.WithWriter(io.Discard))

	return NewServiceManager(context.Background(), logger)
}

// TestWait_StopErrorIsReturned: a Stop() failure is surfaced from Wait().
func TestWait_StopErrorIsReturned(t *testing.T) {
	sm := newTestServiceManager(t)

	stopSentinel := errors.NewConfigurationError("stop-boom")
	svc := &stubService{stopErr: stopSentinel}

	require.NoError(t, sm.AddService("svc", svc))

	// Clean shutdown signal; Start returns context.Canceled (normalized to nil),
	// so the only error left is the Stop() failure.
	sm.ForceShutdown()

	err := sm.Wait()
	require.Error(t, err)
	require.True(t, errors.Is(err, stopSentinel), "Wait must surface the Stop() error")
	require.True(t, svc.stopCalled.Load())
}

// TestWait_AllServicesStoppedAfterEarlierFailure: an earlier Stop() failure must
// not prevent the remaining services from being stopped.
func TestWait_AllServicesStoppedAfterEarlierFailure(t *testing.T) {
	sm := newTestServiceManager(t)

	// Stop runs in reverse registration order: svc3, svc2, svc1.
	svc1 := &stubService{}
	svc2 := &stubService{stopErr: errors.NewProcessingError("svc2-stop-boom")}
	svc3 := &stubService{stopErr: errors.NewProcessingError("svc3-stop-boom")}

	require.NoError(t, sm.AddService("svc1", svc1))
	require.NoError(t, sm.AddService("svc2", svc2))
	require.NoError(t, sm.AddService("svc3", svc3))

	sm.ForceShutdown()

	err := sm.Wait()
	require.Error(t, err)

	// Every service's Stop() ran despite svc3/svc2 failing first.
	require.True(t, svc1.stopCalled.Load(), "svc1.Stop must run after earlier failures")
	require.True(t, svc2.stopCalled.Load())
	require.True(t, svc3.stopCalled.Load())
}

// TestWait_CanceledShutdownReturnsNil: a plain context.Canceled shutdown (no
// Stop failures) returns nil.
func TestWait_CanceledShutdownReturnsNil(t *testing.T) {
	sm := newTestServiceManager(t)

	svc := &stubService{}
	require.NoError(t, sm.AddService("svc", svc))

	sm.ForceShutdown()

	require.NoError(t, sm.Wait())
	require.True(t, svc.stopCalled.Load())
}

// TestWait_StopTimeoutHonored: a configured StopTimeout bounds a blocking Stop(),
// and the per-service Stop context carries that deadline.
func TestWait_StopTimeoutHonored(t *testing.T) {
	sm := newTestServiceManager(t)
	sm.StopTimeout = 200 * time.Millisecond

	svc := &stubService{blockStop: true}
	require.NoError(t, sm.AddService("svc", svc))

	sm.ForceShutdown()

	start := time.Now()
	err := sm.Wait()
	elapsed := time.Since(start)

	// Blocking Stop is released by the timeout, not the 30s default and not instantly.
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond)
	require.Less(t, elapsed, 5*time.Second)
	require.True(t, svc.stopHadDeadline.Load(), "Stop ctx must carry the StopTimeout deadline")
	require.Error(t, err)
	// context.DeadlineExceeded is not a project error; project errors.Is matches
	// it via substring on the rendered (wrapped) message.
	require.True(t, errors.Is(err, context.DeadlineExceeded))
}

// TestWait_StopTimeoutDefaultsWhenUnset: a ServiceManager built by the
// constructor uses DefaultStopTimeout.
func TestWait_StopTimeoutDefaultsWhenUnset(t *testing.T) {
	sm := newTestServiceManager(t)
	require.Equal(t, DefaultStopTimeout, sm.StopTimeout)
}

// TestWait_OriginalErrorMatchableWhenJoinedWithStopError: when both an original
// Wait (Start) error and a Stop error are present, the combined error preserves
// errors.Is identity for the original error (the point of passing it first to
// the project errors.Join). Distinct codes (Processing vs Configuration) make
// both assertions unambiguous.
func TestWait_OriginalErrorMatchableWhenJoinedWithStopError(t *testing.T) {
	sm := newTestServiceManager(t)

	startSentinel := errors.NewProcessingError("start-boom")
	stopSentinel := errors.NewConfigurationError("stop-boom")

	// svc1 fails its Start (becomes the original g.Wait() error and cancels the
	// group); svc2 blocks in Start until cancelled, then fails its Stop.
	svc1 := &stubService{startErr: startSentinel}
	svc2 := &stubService{stopErr: stopSentinel}

	require.NoError(t, sm.AddService("svc1", svc1))
	require.NoError(t, sm.AddService("svc2", svc2))

	err := sm.Wait()
	require.Error(t, err)
	require.True(t, errors.Is(err, startSentinel), "original Wait error identity must be preserved")
	require.True(t, errors.Is(err, stopSentinel), "Stop error must also be surfaced")
}
