package utxo

import (
	"strings"
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
