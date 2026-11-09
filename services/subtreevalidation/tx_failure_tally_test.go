package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func tallyHash(seed byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = seed

	return h
}

func TestTxFailureTally_EmptyHasNoFailures(t *testing.T) {
	var tally txFailureTally

	require.Zero(t, tally.count())
	require.Equal(t, "no failures", tally.String())
}

// TestTxFailureTally_CountsByRootCauseWithOneExample pins the shape of the
// summary line: the innermost code names the cause, the largest cause comes
// first, and each cause carries the first failed txid as its example.
func TestTxFailureTally_CountsByRootCauseWithOneExample(t *testing.T) {
	var tally txFailureTally

	spent := func(seed byte) error {
		return errors.NewTxInvalidError("tx invalid",
			errors.NewProcessingError("spend failed",
				errors.NewUtxoSpentError(tallyHash(seed), 0, tallyHash(0xff), nil)))
	}

	tally.add(tallyHash(1), spent(1))
	tally.add(tallyHash(2), spent(2))
	tally.add(tallyHash(3), spent(3))
	tally.add(tallyHash(4), errors.NewTxPolicyError("fee too low"))
	tally.add(tallyHash(5), errors.NewProcessingError("decorate failed", errors.NewStorageError("circuit breaker open")))
	tally.add(tallyHash(6), errors.NewProcessingError("decorate failed", errors.NewStorageError("circuit breaker open")))

	require.Equal(t, uint64(6), tally.count())

	require.Equal(t,
		"UTXO_SPENT=3 (e.g. "+tallyHash(1).String()+"), "+
			"STORAGE_ERROR=2 (e.g. "+tallyHash(5).String()+"), "+
			"TX_POLICY=1 (e.g. "+tallyHash(4).String()+")",
		tally.String())
}

func TestTxFailureTally_TiesAreOrderedByCode(t *testing.T) {
	var tally txFailureTally

	tally.add(tallyHash(1), errors.NewTxMissingParentError("no parent"))
	tally.add(tallyHash(2), errors.NewStorageError("down"))

	require.Equal(t,
		"STORAGE_ERROR=1 (e.g. "+tallyHash(2).String()+"), "+
			"TX_MISSING_PARENT=1 (e.g. "+tallyHash(1).String()+")",
		tally.String())
}

func TestRootErrorCode(t *testing.T) {
	require.Equal(t, "UNKNOWN", rootErrorCode(nil))
	require.Equal(t, "UNKNOWN", rootErrorCode(context.Canceled), "no coded error in the chain")
	require.Equal(t, "UNKNOWN", rootErrorCode(errors.New(errors.ERR_UNKNOWN, "plain")))
	require.Equal(t, "STORAGE_ERROR", rootErrorCode(errors.NewStorageError("down")))
	require.Equal(t, "STORAGE_ERROR", rootErrorCode(errors.NewProcessingError("outer", errors.NewStorageError("down"))),
		"the innermost coded error names the cause")
	require.Equal(t, "TX_INVALID", rootErrorCode(errors.NewTxInvalidError("bad", context.Canceled)),
		"a non-coded cause below the innermost coded error does not change the name")
}
