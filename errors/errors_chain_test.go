// nolint:forbidigo,depguard // This test file needs the standard errors package for testing the custom errors package
package errors

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// buildChain returns the head of a chain of n+1 *Error links: a head with
// headCode followed by n links with linkCode, the tail wrapping tailErr (may
// be nil). Links are connected directly so construction is O(n) regardless of
// SetWrappedErr's behaviour.
func buildChain(headCode, linkCode ERR, n int, tailErr error) *Error {
	head := &Error{code: headCode, message: "head"}
	cur := head

	for i := 0; i < n; i++ {
		next := &Error{code: linkCode, message: fmt.Sprintf("link %d", i)}
		cur.wrappedErr = next
		cur = next
	}

	cur.wrappedErr = tailErr

	return head
}

// TestErrorIsDeepChain pins that errors.Is over a very deep chain both
// terminates quickly and returns the right answer. The previous recursive
// (*Error).Is was O(N²) in reflection-based errors.As calls: a ~50k-link
// chain (one link per failed spend of a consolidation tx) turned a single
// errors.Is into CPU-hours and stalled mainnet IBD on block 820116.
func TestErrorIsDeepChain(t *testing.T) {
	const chainLen = 100_000

	t.Run("match at tail", func(t *testing.T) {
		head := buildChain(ERR_UTXO_ERROR, ERR_STORAGE_ERROR, chainLen, NewTxConflictingError("tail"))

		start := time.Now()
		require.True(t, Is(head, ErrTxConflicting))
		require.Less(t, time.Since(start), 2*time.Second, "deep-chain errors.Is must be linear, not quadratic")
	})

	t.Run("no match", func(t *testing.T) {
		head := buildChain(ERR_UTXO_ERROR, ERR_STORAGE_ERROR, chainLen, nil)

		start := time.Now()
		require.False(t, Is(head, ErrTxConflicting))
		require.Less(t, time.Since(start), 2*time.Second)
	})

	t.Run("match at head", func(t *testing.T) {
		head := buildChain(ERR_TX_CONFLICTING, ERR_STORAGE_ERROR, chainLen, nil)
		require.True(t, Is(head, ErrTxConflicting))
	})
}

// TestErrorIsThroughForeignWrapper pins that a non-*Error link in the middle
// of the chain does not stop the walk: errors.As digs through foreign
// wrappers to the next *Error, matching the previous unwrap behaviour.
func TestErrorIsThroughForeignWrapper(t *testing.T) {
	inner := NewTxConflictingError("inner")
	foreign := fmt.Errorf("foreign wrapper: %w", inner)
	head := &Error{code: ERR_UTXO_ERROR, message: "head", wrappedErr: foreign}

	require.True(t, Is(head, ErrTxConflicting))
	require.False(t, Is(head, ErrTxNotFound))
}

// TestErrorIsNonErrorTarget pins the substring fallback for targets that are
// not *Error. Error() truncates the chain at a fixed depth, so this stays
// bounded even for deep chains.
func TestErrorIsNonErrorTarget(t *testing.T) {
	head := NewProcessingError("something failed", stderrors.New("disk full"))

	require.True(t, head.Is(stderrors.New("disk full")))
	require.False(t, head.Is(stderrors.New("not present")))
}

// fakeDriverError stands in for a concrete driver error (e.g. *pgconn.PgError,
// *sqlite.Error) whose type identity errors.As must recover from a New-wrapped
// chain.
type fakeDriverError struct{ code string }

func (f fakeDriverError) Error() string { return "fake driver error " + f.code }

// TestNewPreservesWrappedForeignError pins the #1913 fix: a trailing non-*Error
// argument to the New* constructors must survive in the chain with its original
// type. The historical behaviour flattened it into a synthetic
// &Error{message}, so errors.As could never classify driver errors wrapped
// through New — isLockError-style typed checks silently fell back to string
// matching.
func TestNewPreservesWrappedForeignError(t *testing.T) {
	inner := fakeDriverError{code: "40001"}

	head := NewStorageError("failed to insert", inner)

	var recovered fakeDriverError
	require.True(t, stderrors.As(head, &recovered), "errors.As must recover the driver error type")
	require.Equal(t, "40001", recovered.code)

	// The chain renders the foreign message directly instead of a synthetic
	// ERR_UNKNOWN wrapper.
	require.Contains(t, head.Error(), "fake driver error 40001")
	require.NotContains(t, head.Error(), "ERR_UNKNOWN")

	// *Error trailing arguments keep the previous behaviour untouched.
	typed := NewStorageError("failed", NewTxConflictingError("conflict"))
	require.True(t, Is(typed, ErrTxConflicting))
	innerTx, ok := typed.WrappedErr().(*Error)
	require.True(t, ok)
	require.Equal(t, ERR_TX_CONFLICTING, innerTx.Code())
}

// TestNewConvertsGRPCStatusCause pins that wrapping a raw gRPC status through
// New keeps the outer Teranode code. A raw status left in the chain makes
// status.FromError (which walks Unwrap) classify the outer *Error as a gRPC
// wrapper, so Is/UserMessage/WrapGRPC all use the inner status and discard it.
func TestNewConvertsGRPCStatusCause(t *testing.T) {
	raw := status.Error(codes.Unavailable, "dial 10.0.0.5:9905 refused")
	err := NewServiceError("p2p call failed", raw)

	require.True(t, Is(err, ErrServiceError), "the outer code must survive a raw gRPC status cause")
	require.Equal(t, "SERVICE_ERROR (59): p2p call failed", UserMessage(err))
	require.NotContains(t, UserMessage(err), "10.0.0.5:9905", "UserMessage must not leak the inner chain")

	wrapped := WrapGRPC(err)
	st, ok := status.FromError(wrapped)
	require.True(t, ok)
	require.Equal(t, codes.Internal, st.Code(), "the outer SERVICE_ERROR code decides the status")
	require.NotEmpty(t, st.Details(), "the chain must still be serialized as details")

	roundTripped := UnwrapGRPC(wrapped)
	require.True(t, Is(roundTripped, ErrServiceError))
}

// TestSetWrappedErrForeignTail pins that appending to a chain whose tail is a
// foreign (non-*Error) error keeps every link. The historical code rebuilt the
// cursor from a stale or nil lastWrappedErr, so the appended error landed on a
// detached node and never reached the chain.
func TestSetWrappedErrForeignTail(t *testing.T) {
	head := NewStorageError("outer", fakeDriverError{code: "40001"})
	head.SetWrappedErr(NewTxConflictingError("appended"))

	require.True(t, Is(head, ErrStorageError))
	require.True(t, Is(head, ErrTxConflicting))

	var foreign fakeDriverError
	require.True(t, stderrors.As(head, &foreign))
	require.Equal(t, "40001", foreign.code)

	require.Contains(t, head.Error(), "appended")
	require.Contains(t, head.Error(), "fake driver error 40001")
}

// TestSetWrappedErrForeignOnForeignTail pins the fallback for appending a
// foreign error under an existing foreign leaf: both foreign errors survive
// instead of the new one replacing the old.
func TestSetWrappedErrForeignOnForeignTail(t *testing.T) {
	head := NewStorageError("outer", io.ErrUnexpectedEOF)
	head.SetWrappedErr(context.Canceled)

	require.True(t, stderrors.Is(head, io.ErrUnexpectedEOF))
	require.True(t, stderrors.Is(head, context.Canceled))
}

// TestSetWrappedErrDeepChainAppend pins that appending to an already-deep
// chain terminates quickly. The walk to the tail used errors.As per link
// (reflection); building a chain by repeated appends was O(N²).
func TestSetWrappedErrDeepChainAppend(t *testing.T) {
	const chainLen = 100_000

	head := buildChain(ERR_UTXO_ERROR, ERR_STORAGE_ERROR, chainLen, nil)

	start := time.Now()
	head.SetWrappedErr(NewTxConflictingError("appended tail"))
	require.Less(t, time.Since(start), 2*time.Second)

	require.True(t, Is(head, ErrTxConflicting))
}

// TestJoinPreservesBothChains pins that Join appends the second error without
// mutating it or dropping its own wrapped chain when the first chain ends in a
// foreign (non-*Error) cause. That shape is common now that New keeps driver
// errors verbatim: a batched spend error wraps an *aerospike.AerospikeError,
// and Join (or JoinCapped, via the validator) combines many of them.
func TestJoinPreservesBothChains(t *testing.T) {
	t.Run("first foreign tail and second nested cause", func(t *testing.T) {
		a := NewStorageError("a", io.EOF)
		b := NewProcessingError("b", NewTxNotFoundError("c", sql.ErrNoRows))

		joined := Join(a, b)

		require.True(t, Is(joined, ErrStorageError))
		require.True(t, Is(joined, ErrProcessing))
		require.True(t, Is(joined, ErrTxNotFound))
		require.True(t, stderrors.Is(joined, sql.ErrNoRows), "the second error's nested cause must survive")
		require.True(t, stderrors.Is(joined, io.EOF), "the first error's foreign tail must survive")

		// Join must not write into its arguments: b keeps its own cause and
		// must not gain a's foreign tail.
		require.True(t, Is(b, ErrTxNotFound))
		require.True(t, stderrors.Is(b, sql.ErrNoRows))
		require.False(t, stderrors.Is(b, io.EOF))
		require.NotContains(t, b.Error(), "EOF")
	})

	t.Run("shared singleton keeps its tail", func(t *testing.T) {
		joined := Join(NewProcessingError("x", context.Canceled), ErrNotFound)

		require.True(t, Is(joined, ErrNotFound))
		require.True(t, stderrors.Is(joined, context.Canceled))
		require.Nil(t, ErrNotFound.WrappedErr(), "Join must not attach a tail to the caller's singleton")
	})

	t.Run("capped batch keeps every spend cause", func(t *testing.T) {
		drivers := []error{
			stderrors.New("driver 0"),
			stderrors.New("driver 1"),
			stderrors.New("driver 2"),
		}
		spends := []error{
			NewStorageError("spend 0", drivers[0]),
			NewStorageError("spend 1", NewTxNotFoundError("spend 1 detail", drivers[1])),
			NewStorageError("spend 2", drivers[2]),
		}

		joined := JoinCapped(10, spends...)
		require.NotNil(t, joined)

		for _, driver := range drivers {
			require.True(t, stderrors.Is(joined, driver), "every spend's driver cause must survive")
		}

		// The first error is the join's head, so it necessarily grows. The
		// later errors are inputs that must be left untouched.
		for i := 1; i < len(spends); i++ {
			require.True(t, Is(spends[i], ErrStorageError), "spend %d must keep its own code", i)
			for j, driver := range drivers {
				if i != j {
					require.False(t, stderrors.Is(spends[i], driver), "Join must not write spend %d's cause into spend %d", j, i)
				}
			}
		}
	})
}

func TestJoinCapped(t *testing.T) {
	t.Run("nil and empty", func(t *testing.T) {
		require.Nil(t, JoinCapped(10))
		require.Nil(t, JoinCapped(10, nil, nil))
	})

	t.Run("under cap keeps everything", func(t *testing.T) {
		err := JoinCapped(10, NewStorageError("a"), NewStorageError("b"))
		require.NotNil(t, err)
		require.NotContains(t, err.Error(), "more errors")
		require.True(t, Is(err, ErrStorageError))
	})

	t.Run("over cap truncates with count", func(t *testing.T) {
		errs := make([]error, 100)
		for i := range errs {
			errs[i] = NewStorageError("spend %d failed", i)
		}

		err := JoinCapped(10, errs...)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "and 90 more errors")

		// Chain length: 10 kept + 1 summary link.
		depth := 0
		for cur, ok := err.(*Error); ok && cur != nil; cur, ok = cur.wrappedErr.(*Error) {
			depth++
		}
		require.Equal(t, 11, depth)

		require.True(t, Is(err, ErrStorageError))
	})

	t.Run("nils do not count toward cap", func(t *testing.T) {
		err := JoinCapped(2, nil, NewStorageError("a"), nil, NewStorageError("b"), nil)
		require.NotNil(t, err)
		require.NotContains(t, err.Error(), "more errors")
	})

	t.Run("non-positive cap keeps one", func(t *testing.T) {
		err := JoinCapped(0, NewStorageError("a"), NewStorageError("b"))
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "and 1 more errors")
	})

	t.Run("non-Error values survive", func(t *testing.T) {
		err := JoinCapped(10, stderrors.New("plain"), NewStorageError("typed"))
		require.NotNil(t, err)
		require.True(t, strings.Contains(err.Error(), "plain"))
	})
}

func BenchmarkErrorIsDeepChain(b *testing.B) {
	for _, n := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			head := buildChain(ERR_UTXO_ERROR, ERR_STORAGE_ERROR, n, nil)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = Is(head, ErrTxConflicting)
			}
		})
	}
}

func BenchmarkSetWrappedErrAppend(b *testing.B) {
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				head := buildChain(ERR_UTXO_ERROR, ERR_STORAGE_ERROR, n, nil)
				b.StartTimer()

				head.SetWrappedErr(NewStorageError("tail"))
			}
		})
	}
}
