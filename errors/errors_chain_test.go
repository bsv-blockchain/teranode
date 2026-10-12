// nolint:forbidigo,depguard // This test file needs the standard errors package for testing the custom errors package
package errors

import (
	stderrors "errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
