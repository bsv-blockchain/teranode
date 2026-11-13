package utxo

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// Store-backed wedge and retry coverage is in tests.ConflictRecoveryDescendants
// and tests.ConflictRecoveryFailures; they cannot retain erased spend links.
func TestRollback_CapsJoinedErrors(t *testing.T) {
	var failures []error
	for i := 0; i < 30; i++ {
		failures = append(failures, errors.NewProcessingError("failure %d", i))
	}
	err := capRollbackFailures(failures)
	require.Error(t, err)
	require.Equal(t, 20, strings.Count(err.Error(), "; "))
	require.Contains(t, err.Error(), "10 more rollback failures omitted")
}

func TestRollback_CapsMessageLength(t *testing.T) {
	huge := errors.NewProcessingError("%s", strings.Repeat("x", 100_000))
	err := capRollbackFailures([]error{huge, errors.NewProcessingError("flag clear failed")})
	require.Error(t, err)
	require.Less(t, len(err.Error()), 2*maxRollbackMessageBytes+200)
	require.Contains(t, err.Error(), "flag clear failed", "a later failure survives a huge earlier one")
}

// Some stores ignore ctx in their reads. After one read fails, forEachConflictRead must not start
// the reads that remain, so a failed check on a wide cascade returns without reading it all.
func TestForEachConflictRead_StopsAfterFailure(t *testing.T) {
	const n = 10_000

	var started atomic.Int32

	err := forEachConflictRead(t.Context(), n, func(ctx context.Context, i int) error {
		started.Add(1)

		if i == 0 {
			return errors.NewProcessingError("read %d failed", i)
		}

		// Hold every slot until the failure cancels the group.
		<-ctx.Done()

		return nil
	})
	require.Error(t, err)
	require.LessOrEqual(t, started.Load(), int32(conflictingWalkFanOut), "no read may start after the failure")
}

// A cancelled ctx can stop forEachConflictRead before every read ran. It must report that, or a
// caller takes unread outpoints as verified.
func TestForEachConflictRead_CancelledReportsError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var started atomic.Int32

	err := forEachConflictRead(ctx, 5, func(context.Context, int) error {
		started.Add(1)
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, started.Load(), int32(5))
}
