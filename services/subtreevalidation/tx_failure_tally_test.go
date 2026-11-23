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
	tally.add(tallyHash(5), errors.NewProcessingError("spend failed", errors.NewServiceUnavailableError("[SPEND] circuit breaker open, rejecting request")))
	tally.add(tallyHash(6), errors.NewProcessingError("spend failed", errors.NewServiceUnavailableError("[SPEND] circuit breaker open, rejecting request")))

	require.Equal(t, uint64(6), tally.count())

	require.Equal(t,
		"UTXO_SPENT=3 (e.g. "+tallyHash(1).String()+"), "+
			"SERVICE_UNAVAILABLE=2 (e.g. "+tallyHash(5).String()+"), "+
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

// foreignWrapper is a non-teranode error that wraps one error, like fmt.Errorf with %w.
type foreignWrapper struct{ err error }

func (w foreignWrapper) Error() string { return "foreign: " + w.err.Error() }
func (w foreignWrapper) Unwrap() error { return w.err }

func TestRootErrorCode(t *testing.T) {
	require.Equal(t, "UNKNOWN", rootErrorCode(nil))
	require.Equal(t, "UNKNOWN", rootErrorCode(context.Canceled), "no coded error in the chain")
	require.Equal(t, "UNKNOWN", rootErrorCode(errors.New(errors.ERR_UNKNOWN, "plain")))
	require.Equal(t, "STORAGE_ERROR", rootErrorCode(errors.NewStorageError("down")))
	require.Equal(t, "STORAGE_ERROR", rootErrorCode(errors.NewProcessingError("outer", errors.NewStorageError("down"))),
		"the innermost coded error names the cause")
	require.Equal(t, "TX_INVALID", rootErrorCode(errors.NewTxInvalidError("bad", context.Canceled)),
		"a non-coded cause below the innermost coded error does not change the name")
	require.Equal(t, "UTXO_ERROR", rootErrorCode(foreignWrapper{errors.NewProcessingError("spend", errors.NewUtxoError("invalid spend"))}),
		"a foreign wrapper above the chain is dug through")
	require.Equal(t, "PROCESSING", rootErrorCode(errors.NewProcessingError("spend", foreignWrapper{errors.NewUtxoError("invalid spend")})),
		"the errors package flattens a foreign error inside a chain to an ERR_UNKNOWN link, so the cause below it is not reachable")
	require.Equal(t, "UTXO_ERROR", rootErrorCode(errors.UnwrapGRPC(errors.WrapGRPC(errors.NewProcessingError("spend", errors.NewUtxoError("invalid spend"))))),
		"a chain that crossed gRPC keeps its cause")
}

// TestRootErrorCode_CappedJoinIsCountedByItsCause pins the many-input case: the
// store and the validator join a transaction's failed spends with JoinCapped(10),
// which ends a chain of more than 10 with an ERR_ERROR "... and N more errors"
// marker. That marker must not be the cause; an open circuit breaker on an
// 11-input transaction reads SERVICE_UNAVAILABLE, not ERROR.
func TestRootErrorCode_CappedJoinIsCountedByItsCause(t *testing.T) {
	joined := func(n int, mk func(i int) error) error {
		errs := make([]error, n)
		for i := range errs {
			errs[i] = mk(i)
		}

		return errors.NewTxInvalidError("tx invalid",
			errors.NewProcessingError("failed to spend utxos", errors.JoinCapped(10, errs...)))
	}

	breaker := func(int) error {
		return errors.NewServiceUnavailableError("[SPEND] circuit breaker open, rejecting request")
	}

	for _, n := range []int{1, 10, 11, 100} {
		require.Equal(t, "SERVICE_UNAVAILABLE", rootErrorCode(joined(n, breaker)), "%d failed inputs", n)
	}

	spent := func(i int) error {
		return errors.NewUtxoSpentError(tallyHash(byte(i)), uint32(i), tallyHash(0xff), nil)
	}

	for _, n := range []int{1, 10, 11, 100} {
		require.Equal(t, "UTXO_SPENT", rootErrorCode(joined(n, spent)), "%d failed inputs", n)
	}

	require.Equal(t, "UNKNOWN", rootErrorCode(errors.NewError("... and 3 more errors")),
		"a chain of only the marker names nothing")
}
