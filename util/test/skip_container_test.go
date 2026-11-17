package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// fakeSkipT is a minimal skipT double that records whether Skipf or Fatalf
// was called, without terminating the goroutine the way the real *testing.T
// methods do - letting the table test assert the outcome instead of actually
// failing/skipping itself.
type fakeSkipT struct {
	skippedMsg string
	failedMsg  string
}

func (f *fakeSkipT) Helper() {}

func (f *fakeSkipT) Skipf(format string, args ...interface{}) {
	f.skippedMsg = fmt.Sprintf(format, args...)
}

func (f *fakeSkipT) Fatalf(format string, args ...interface{}) {
	f.failedMsg = fmt.Sprintf(format, args...)
}

// withContainerRuntimeHealthProbe swaps the package's Docker/OrbStack health
// probe for the duration of a test, restoring the previous probe on return.
func withContainerRuntimeHealthProbe(t *testing.T, probe func(context.Context) error) {
	t.Helper()

	original := containerRuntimeHealthProbe
	containerRuntimeHealthProbe = probe

	t.Cleanup(func() {
		containerRuntimeHealthProbe = original
	})
}

// TestSkipIfContainerUnavailable_NilErr verifies the helper is a no-op when
// the container-start call it guards succeeded.
func TestSkipIfContainerUnavailable_NilErr(t *testing.T) {
	withContainerRuntimeHealthProbe(t, func(context.Context) error {
		t.Fatal("health probe must not run when err is nil")
		return nil
	})

	ft := &fakeSkipT{}
	SkipIfContainerUnavailable(ft, nil)

	require.Empty(t, ft.skippedMsg, "should not skip on a nil error")
	require.Empty(t, ft.failedMsg, "should not fail on a nil error")
}

// TestSkipIfContainerUnavailable_RuntimeDown asserts that when the container
// runtime itself is genuinely unreachable, the helper skips the test -
// regardless of what the underlying start error says.
func TestSkipIfContainerUnavailable_RuntimeDown(t *testing.T) {
	startErrs := map[string]error{
		"docker daemon unreachable": errors.NewError(
			"Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?"),
		"generic start failure": errors.NewError("failed to start postgres container"),
	}

	for name, startErr := range startErrs {
		t.Run(name, func(t *testing.T) {
			withContainerRuntimeHealthProbe(t, func(context.Context) error {
				return errors.NewError("Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?")
			})

			ft := &fakeSkipT{}
			SkipIfContainerUnavailable(ft, startErr)

			require.NotEmpty(t, ft.skippedMsg, "expected the test to be skipped when the container runtime is down")
			require.Empty(t, ft.failedMsg, "should not fail when the container runtime is down")
		})
	}
}

// TestSkipIfContainerUnavailable_NonRuntimeErrors asserts that the concrete,
// confirmed-non-Docker failure modes fail the test loudly instead of being
// silently skipped, as long as the container runtime itself is healthy.
func TestSkipIfContainerUnavailable_NonRuntimeErrors(t *testing.T) {
	startErrs := map[string]error{
		"project root not found":              errors.NewError("could not resolve absolute path for init script: project root (go.mod) not found"),
		"container not running after startup": errors.NewError("postgres container is not running after startup"),
		"db connection verify failed":         errors.NewError("failed to verify database connection: dial tcp 127.0.0.1:5432: connect: connection refused"),
		"readiness timeout":                   errors.NewError("timeout waiting for database to be ready"),
		"image pull rate limit":               errors.NewError("failed to pull image postgres:latest: toomanyrequests: You have reached your pull rate limit"),
	}

	for name, startErr := range startErrs {
		t.Run(name, func(t *testing.T) {
			withContainerRuntimeHealthProbe(t, func(context.Context) error {
				return nil // container runtime is healthy
			})

			ft := &fakeSkipT{}
			SkipIfContainerUnavailable(ft, startErr)

			require.NotEmpty(t, ft.failedMsg, "expected the test to fail (not skip) when the runtime is healthy but start failed")
			require.Empty(t, ft.skippedMsg, "should not skip when the runtime is healthy")
		})
	}
}
