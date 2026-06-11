package util

import (
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSafeSetLimit(t *testing.T) {
	// runAll launches n trivial goroutines on g and asserts they all run to
	// completion. If SafeSetLimit had left a zero-capacity limit, Go would block
	// forever and the test would hang (caught by the go test timeout).
	runAll := func(t *testing.T, g *errgroup.Group, n int) {
		t.Helper()

		var counter atomic.Int32

		for i := 0; i < n; i++ {
			g.Go(func() error {
				counter.Add(1)
				return nil
			})
		}

		require.NoError(t, g.Wait())
		require.Equal(t, int32(n), counter.Load())
	}

	logger := ulogger.TestLogger{}

	t.Run("valid positive limit", func(t *testing.T) {
		g := &errgroup.Group{}

		SafeSetLimit(logger, g, 2)
		runAll(t, g, 5)
	})

	t.Run("zero limit falls back to a usable default", func(t *testing.T) {
		g := &errgroup.Group{}

		// Must not panic and must not deadlock — a raw SetLimit(0) would make
		// every Go call block forever.
		SafeSetLimit(logger, g, 0)
		runAll(t, g, 5)
	})

	t.Run("negative limit falls back to a usable default", func(t *testing.T) {
		g := &errgroup.Group{}

		SafeSetLimit(logger, g, -1)
		runAll(t, g, 5)
	})
}
