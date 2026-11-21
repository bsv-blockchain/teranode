package errors

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// TestUtxoSpendingTxPrunedIsClassified pins ERR_UTXO_SPENDING_TX_PRUNED on the
// two client-facing classifiers in this package. It is this node's record, not a
// consensus verdict. Usually the spender was mined, fully spent and pruned here,
// and the answer stays while the parent output still names it. The same code also
// answers a spend an attempt left behind without creating its transaction, and an
// Aerospike deletedChildren marker, so it does not by itself say the transaction
// confirmed.
// Unclassified it fell to codes.Internal with no verdict in the body, reporting a
// settled answer to a submitter as a server fault.
func TestUtxoSpendingTxPrunedIsClassified(t *testing.T) {
	require.Equal(t, codes.FailedPrecondition, ErrorCodeToGRPCCode(ERR_UTXO_SPENDING_TX_PRUNED),
		"answered like ERR_UTXO_SPENT beside it")

	require.True(t, isPublicCause(ERR_UTXO_SPENDING_TX_PRUNED),
		"the submitter needs the verdict, and the message carries only their own txid and outpoint")

	err := NewUtxoSpendingTxPrunedError("[Spend] invalid spend for %s:%d: spending transaction was pruned", "abc", 0)
	cause := DeepestPublicCause(NewProcessingError("wrapped", err))
	require.NotNil(t, cause, "the verdict must survive wrapping so the response body carries it")
	require.Contains(t, cause.Error(), "spending transaction was pruned")
	require.Contains(t, cause.Error(), "invalid spend for abc:0", "the producer's %s and %d are formatted, not left in the message")
	require.NotContains(t, cause.Error(), "%", "no unformatted verb")
}
