package blockvalidation

import (
	"fmt"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestPeerResourceURL(t *testing.T) {
	server := &Server{}
	hash := chainhash.HashH([]byte("subtree-1368"))
	base := "http://peer:8000/api/v1"

	plain := server.peerResourceURL(base, "subtree_data", &hash, false)
	require.Equal(t, fmt.Sprintf("%s/subtree_data/%s", base, hash.String()), plain)

	// Each bypass gets a distinct query string so a proxy_cache keyed on
	// $request_uri cannot serve the same (possibly poisoned) entry twice.
	first := server.peerResourceURL(base, "subtree_data", &hash, true)
	second := server.peerResourceURL(base, "subtree", &hash, true)
	require.Equal(t, fmt.Sprintf("%s/subtree_data/%s?cachebust=1", base, hash.String()), first)
	require.Equal(t, fmt.Sprintf("%s/subtree/%s?cachebust=2", base, hash.String()), second)
}

func TestPoisonedSubtreeDataError(t *testing.T) {
	hash := chainhash.HashH([]byte("subtree-1368"))

	empty := newPoisonedSubtreeDataError("peer-a", "http://peer-a:8000", &hash, 1023, 1024, 0)
	require.True(t, errors.Is(empty, errors.ErrExternal), "must be a peer-side error so alternatives are still tried")
	require.False(t, errors.IsLocalError(empty), "a local error would skip the alternative-peer loop")
	require.Contains(t, empty.Error(), "peer-a")
	require.Contains(t, empty.Error(), "http://peer-a:8000")
	require.Contains(t, empty.Error(), "0 bytes")
	require.True(t, isCacheBypassRetryable(empty))

	short := newPoisonedSubtreeDataError("peer-a", "http://peer-a:8000", &hash, 7, 1024, 4096)
	require.Contains(t, short.Error(), "7 of 1024")
	require.Contains(t, short.Error(), "4096 bytes")
	require.True(t, isCacheBypassRetryable(short))
}

func TestIsCacheBypassRetryableWalksWrappedChain(t *testing.T) {
	hash := chainhash.HashH([]byte("subtree-1368"))
	poisoned := newPoisonedSubtreeDataError("peer-a", "http://peer-a:8000", &hash, 1, 2, 0)

	wrapped := errors.NewServiceError("outer layer", errors.NewProcessingError("middle layer", poisoned))
	require.True(t, isCacheBypassRetryable(wrapped))

	require.False(t, isCacheBypassRetryable(errors.NewNetworkError("connection refused")))
	require.False(t, isCacheBypassRetryable(nil))
}

func TestMarkCacheBypassRetryable(t *testing.T) {
	marked := markCacheBypassRetryable(errors.NewNotFoundError("empty subtree received"))
	require.True(t, isCacheBypassRetryable(marked))
	require.True(t, errors.Is(marked, errors.ErrNotFound), "marking must not change the error code")
	require.Nil(t, markCacheBypassRetryable(nil))
}

// TestMarkCacheBypassRetryableForeignError pins the guarantee that the marker
// survives even when err has no native *errors.Error anywhere in its chain
// (e.g. a raw error returned by an HTTP client). markCacheBypassRetryable must
// wrap such an error rather than silently drop the marker. io.EOF stands in
// for that foreign error: a plain stdlib sentinel reached through the io
// package rather than importing "errors" directly (forbidden by depguard) or
// using fmt.Errorf (forbidden by forbidigo in this package).
func TestMarkCacheBypassRetryableForeignError(t *testing.T) {
	foreign := io.EOF

	marked := markCacheBypassRetryable(foreign)
	require.True(t, isCacheBypassRetryable(marked), "the marker must survive a foreign error type")
	require.True(t, errors.Is(marked, foreign), "the original error must still be reachable")
	require.Contains(t, marked.Error(), "EOF", "the original error's message must still be reachable")
}
