package subtreevalidation

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
)

// failedPeerSubtreeCacheMaxSize bounds the negative cache. Each announcement that fails adds an
// entry, so a peer announcing distinct hashes that fail fast would otherwise grow it for up to
// two TTLs; when full, the oldest entry is dropped.
const failedPeerSubtreeCacheMaxSize = 4096

// failedPeerSubtreeKey identifies one failed peer subtree: its hash and the DataHub URL it was
// fetched from. The subtree and its missing transactions both come from that URL, so a failure
// may say nothing about the subtree itself; another announcer of the same hash, with its own
// URL, is still validated.
type failedPeerSubtreeKey struct {
	hash    chainhash.Hash
	baseURL string
}

// newFailedPeerSubtreeCache returns the negative cache of peer subtrees that failed validation,
// or nil when ttl is not positive and the cache is off.
func newFailedPeerSubtreeCache(ttl time.Duration) *expiringmap.ExpiringMap[failedPeerSubtreeKey, string] {
	if ttl <= 0 {
		return nil
	}

	return expiringmap.New[failedPeerSubtreeKey, string](ttl).WithMaxSize(failedPeerSubtreeCacheMaxSize)
}

// recentlyFailedPeerSubtree reports whether hash, fetched from baseURL, failed validation within
// the cache TTL, and the code of that failure.
func (u *Server) recentlyFailedPeerSubtree(hash *chainhash.Hash, baseURL string) (string, bool) {
	if u.failedPeerSubtrees == nil {
		return "", false
	}

	return u.failedPeerSubtrees.Get(failedPeerSubtreeKey{hash: *hash, baseURL: baseURL})
}

// recordFailedPeerSubtree keeps hash, fetched from baseURL, in the negative cache after a
// validation failure err. A cancellation is not a verdict on the subtree and is not kept: the
// same subtree is validated again on its next announcement.
func (u *Server) recordFailedPeerSubtree(ctx context.Context, hash *chainhash.Hash, baseURL string, err error) {
	if u.failedPeerSubtrees == nil || ctx.Err() != nil || errors.Is(err, errors.ErrContextCanceled) {
		return
	}

	code := errors.ERR_UNKNOWN.String()

	var e *errors.Error
	if errors.As(err, &e) {
		code = e.Code().String()
	}

	u.failedPeerSubtrees.Set(failedPeerSubtreeKey{hash: *hash, baseURL: baseURL}, code)
}
