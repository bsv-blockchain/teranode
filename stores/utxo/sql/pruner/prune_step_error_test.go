package pruner

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestIsPruneRetryableSeesTheDriverErrorThroughAStepError pins why the pruning
// transaction reports statement failures as pruneStepError and not as
// errors.NewStorageError: the latter replaces a foreign wrapped error with a
// bare *errors.Error carrying only its message, so the retry predicate could
// never see the driver's serialization code and the retry never fired.
func TestIsPruneRetryableSeesTheDriverErrorThroughAStepError(t *testing.T) {
	serialization := &pgconn.PgError{Code: "40001"}

	require.True(t, isPruneRetryable(serialization), "raw driver error")
	require.True(t, isPruneRetryable(&pruneStepError{step: "failed to delete transactions", err: serialization}),
		"the step error must keep the driver error reachable")
	require.False(t, isPruneRetryable(errors.NewStorageError("failed to delete transactions", serialization)),
		"control: NewStorageError loses the driver type, which is why it is not used inside the attempt")
	require.False(t, isPruneRetryable(&pruneStepError{step: "x", err: &pgconn.PgError{Code: "23505"}}),
		"a non-conflict code is not retried")
}
