package errors

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// parkCommitChain builds the chain a block-validation verdict has by the time
// legacy netsync's parkCommitFailure classifies it (services/legacy/netsync/
// block_park_policy.go): the quick-validation pipeline's wrap, quickValidateBlock's
// ProcessingError, the gRPC hop (WrapGRPC in the block-validation server,
// UnwrapGRPC in its client), and netsync ProcessBlock's own ProcessingError on
// top. inner is what the pipeline raised.
func parkCommitChain(inner error) error {
	server := NewProcessingError("[quickValidateBlock][hash] failed to process block subtrees",
		NewProcessingError("[processBlockSubtrees][hash] subtree 0 failed", inner))

	return NewProcessingError("failed to process block", UnwrapGRPC(WrapGRPC(server)))
}

// TestParkCommitErrorCodesSurviveTheGRPCRoundTrip pins that the four codes the
// park's commit-failure table keys its local-fault rows on, and the two codes it
// routes before them, survive the production chain with their identity intact:
// codes 120 (block corrupt), 3 (not found), 91 (blob not found), 30 (tx not
// found) and 10 (block not found) are each still matched by Is after WrapGRPC
// and UnwrapGRPC under three ProcessingError wraps, none of them reads as a
// transient local fault, and a Service or Storage wrap around any of them does.
// The last case is the judgement row's shape for the full route's
// invalid-transaction verdict on a legacy block: codes 11 (block invalid) and 31
// (tx invalid) survive and code 120 does not appear, which is what keeps that
// verdict off the corrupt row the table tests first.
// WrapGRPC walks the whole chain into status details and UnwrapGRPC rebuilds it
// in order, so a change to either that dropped or reordered a link would make
// a table row dead in production while a bare-error test stayed green.
func TestParkCommitErrorCodesSurviveTheGRPCRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		inner   error
		matches []error
		misses  []error
	}{
		{
			name:    "corrupt verdict",
			inner:   NewBlockCorruptError("[bindSubtreeBodyToHeader][hash] subtree does not hash to its key"),
			matches: []error{ErrBlockCorrupt},
			misses:  []error{ErrBlockInvalid, ErrNotFound, ErrTxNotFound},
		},
		{
			name:    "the blob store's bare not-found under readSubtree's wrap",
			inner:   NewNotFoundError("[readSubtree/hash] failed to get subtree data", ErrNotFound),
			matches: []error{ErrNotFound},
			misses:  []error{ErrTxNotFound, ErrBlobNotFound, ErrBlockNotFound, ErrBlockCorrupt},
		},
		{
			name:    "a blob-not-found under readSubtreeStructure's not-found wrap",
			inner:   NewNotFoundError("[readSubtreeStructure/hash] failed to get subtree", NewBlobNotFoundError("no such blob")),
			matches: []error{ErrNotFound, ErrBlobNotFound},
			misses:  []error{ErrTxNotFound, ErrBlockCorrupt},
		},
		{
			name:    "the SQL store's batched spend: a utxo error whose Join chains each spend's tx-not-found",
			inner:   NewUtxoError("error in sql spend (batched mode) - errors", Join(NewTxNotFoundError("output 0 of %s not found", "aa"), NewTxNotFoundError("output 1 of %s not found", "bb"))),
			matches: []error{ErrTxNotFound},
			misses:  []error{ErrNotFound, ErrBlobNotFound, ErrBlockCorrupt},
		},
		{
			name:    "a missing parent block carries code 3 inside code 10",
			inner:   NewBlockNotFoundError("block not found", ErrNotFound),
			matches: []error{ErrBlockNotFound, ErrNotFound},
			misses:  []error{ErrTxNotFound, ErrBlockCorrupt},
		},
		{
			name:    "the full route's legacy verdict for an invalid transaction: invalid around processing around tx-invalid, no corrupt code",
			inner:   NewBlockInvalidError("[ValidateBlock][hash] block contains invalid transactions", NewProcessingError("[CheckBlockSubtrees] failed to process transactions", NewTxInvalidError("transaction in subtree is invalid"))),
			matches: []error{ErrBlockInvalid, ErrTxInvalid},
			misses:  []error{ErrBlockCorrupt, ErrNotFound, ErrTxNotFound},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := parkCommitChain(tc.inner)

			for _, target := range tc.matches {
				require.True(t, Is(err, target), "code %d must survive the round trip", target.(*Error).Code())
			}

			for _, target := range tc.misses {
				require.False(t, Is(err, target), "code %d must not appear from nowhere", target.(*Error).Code())
			}

			require.False(t, IsTransientLocalError(err), "none of these is a transient local fault")
			require.False(t, IsContextError(err))
		})
	}

	t.Run("a service wrap, the full route's shape, reads transient however deep", func(t *testing.T) {
		err := NewProcessingError("failed to process block", UnwrapGRPC(WrapGRPC(
			NewServiceError("failed block validation BlockFound", NewNotFoundError("subtree not found", ErrNotFound)))))

		require.True(t, IsTransientLocalError(err))
		require.True(t, Is(err, ErrNotFound), "the inner code is still there; the classifier's order is what keeps the blob")
	})

	t.Run("a storage error around a foreign error reads transient", func(t *testing.T) {
		err := parkCommitChain(NewNotFoundError("[readSubtreeStructure/hash] failed to get subtree", NewStorageError("open", os.ErrPermission)))

		require.True(t, IsTransientLocalError(err))
		require.True(t, Is(err, ErrNotFound))
	})
}
