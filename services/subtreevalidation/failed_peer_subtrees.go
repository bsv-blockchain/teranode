package subtreevalidation

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
)

// newFailedPeerSubtreeCache returns the negative cache of peer subtrees that failed validation,
// or nil when ttl is not positive and the cache is off.
func newFailedPeerSubtreeCache(ttl time.Duration) *expiringmap.ExpiringMap[chainhash.Hash, string] {
	if ttl <= 0 {
		return nil
	}

	return expiringmap.New[chainhash.Hash, string](ttl)
}

// recentlyFailedPeerSubtree reports whether hash failed validation within the cache TTL, and
// the code of that failure.
func (u *Server) recentlyFailedPeerSubtree(hash *chainhash.Hash) (string, bool) {
	if u.failedPeerSubtrees == nil {
		return "", false
	}

	return u.failedPeerSubtrees.Get(*hash)
}

// recordFailedPeerSubtree keeps hash in the negative cache after a validation failure err. A
// cancellation is not a verdict on the subtree and is not kept: the same subtree is validated
// again on its next announcement.
func (u *Server) recordFailedPeerSubtree(ctx context.Context, hash *chainhash.Hash, err error) {
	if u.failedPeerSubtrees == nil || ctx.Err() != nil || errors.Is(err, errors.ErrContextCanceled) {
		return
	}

	code := errors.ERR_UNKNOWN.String()

	var e *errors.Error
	if errors.As(err, &e) {
		code = e.Code().String()
	}

	u.failedPeerSubtrees.Set(*hash, code)
}
