package blockvalidation

import (
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/jellydator/ttlcache/v3"
)

// catchupQueueOwner distinguishes a retry from an older worker for the same hash.
type catchupQueueOwner struct{ hash chainhash.Hash }

// enqueueCatchup retains one owner per queued/active target. Ownership is bounded
// by channel capacity plus the single consumer; repeated announcements only add
// distinct peer alternatives. A full queue never blocks a producer or retains
// ownership for its rejected target.
func (u *Server) enqueueCatchup(item processBlockCatchup) bool {
	u.catchupQueueMu.Lock()
	defer u.catchupQueueMu.Unlock()
	if u.catchupQueueStopped {
		return false
	}
	hash := *item.block.Hash()
	if u.catchupQueued == nil {
		u.catchupQueued = make(map[chainhash.Hash]processBlockCatchup)
	}
	if primary, ok := u.catchupQueued[hash]; ok {
		if primary.peerID == item.peerID && primary.baseURL == item.baseURL {
			return true
		}
		var alternatives []processBlockCatchup
		if entry := u.catchupAlternatives.Get(hash); entry != nil {
			alternatives = entry.Value()
		}
		for _, alternative := range alternatives {
			if alternative.peerID == item.peerID && alternative.baseURL == item.baseURL {
				return true
			}
		}
		if len(alternatives) >= maxCatchupAlternatives {
			return true
		}
		// Copy because the active consumer may be traversing the previous snapshot.
		alternatives = append(append([]processBlockCatchup(nil), alternatives...), item)
		u.catchupAlternatives.Set(hash, alternatives, ttlcache.NoTTL)
		return true
	}
	item.owner = &catchupQueueOwner{hash: hash}
	u.catchupQueued[hash] = item
	priorMarker := u.processBlockNotify.Get(hash)
	var priorMarkerTTL time.Duration
	if priorMarker != nil {
		priorMarkerTTL = remainingCacheTTL(priorMarker.ExpiresAt())
	}
	u.processBlockNotify.Set(hash, true, ttlcache.NoTTL)
	alternatives := u.catchupAlternatives.Get(hash)
	var priorAlternativesTTL time.Duration
	if alternatives != nil {
		priorAlternativesTTL = remainingCacheTTL(alternatives.ExpiresAt())
		u.catchupAlternatives.Set(hash, alternatives.Value(), ttlcache.NoTTL)
	}
	select {
	case u.catchupCh <- item:
		return true
	default:
		delete(u.catchupQueued, hash)
		if priorMarker == nil {
			u.processBlockNotify.Delete(hash)
		} else {
			u.processBlockNotify.Set(hash, priorMarker.Value(), priorMarkerTTL)
		}
		if alternatives != nil {
			u.catchupAlternatives.Set(hash, alternatives.Value(), priorAlternativesTTL)
		}
		return false
	}
}

func remainingCacheTTL(expiry time.Time) time.Duration {
	if expiry.IsZero() {
		return ttlcache.NoTTL
	}
	return time.Until(expiry)
}

// releaseCatchupOwnership runs on every consumer exit, including cancellation
// and exceptional catchup results. Explicit success/error cleanup may already
// have removed advisory entries. Any leftovers regain their normal TTL, so a
// canceled service or a rejected attempt cannot leave an immortal marker.
func (u *Server) releaseCatchupOwnership(item processBlockCatchup) {
	if item.owner == nil {
		return
	}
	u.catchupQueueMu.Lock()
	defer u.catchupQueueMu.Unlock()
	hash := item.owner.hash
	if current, ok := u.catchupQueued[hash]; !ok || current.owner != item.owner {
		return
	}
	delete(u.catchupQueued, hash)
	if marker := u.processBlockNotify.Get(hash); marker != nil {
		u.processBlockNotify.Set(hash, marker.Value(), ttlcache.DefaultTTL)
	}
	if alternatives := u.catchupAlternatives.Get(hash); alternatives != nil {
		u.catchupAlternatives.Set(hash, alternatives.Value(), ttlcache.DefaultTTL)
	}
}

// Shutdown discards process-local pending work; restart reconstructs catchup from
// stored chain progress. Stop new producers before releasing NoTTL ownership.
func (u *Server) stopCatchupQueue() {
	u.catchupQueueMu.Lock()
	defer u.catchupQueueMu.Unlock()
	u.catchupQueueStopped = true
	for hash := range u.catchupQueued {
		u.processBlockNotify.Delete(hash)
		u.catchupAlternatives.Delete(hash)
	}
	clear(u.catchupQueued)
	for {
		select {
		case _, ok := <-u.catchupCh:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

// finishCatchupTarget ends ownership atomically with terminal cache cleanup,
// before publishing a failure that may synchronously cause a new announcement.
// An older worker's deferred release must never expire that newer retry.
func (u *Server) finishCatchupTarget(item processBlockCatchup) {
	u.finishCatchupTargetWithAlternatives(item, false)
}

// Policy declines belong to a serving peer, so another announcement may use
// alternatives retained from the rejected generation.
func (u *Server) finishPolicyDeclinedTarget(item processBlockCatchup) {
	u.finishCatchupTargetWithAlternatives(item, true)
}

func (u *Server) finishCatchupTargetWithAlternatives(item processBlockCatchup, preserveAlternatives bool) {
	u.catchupQueueMu.Lock()
	defer u.catchupQueueMu.Unlock()
	hash := *item.block.Hash()
	if current, ok := u.catchupQueued[hash]; ok && current.owner != item.owner {
		return
	}
	delete(u.catchupQueued, hash)
	u.processBlockNotify.Delete(hash)
	if preserveAlternatives {
		if alternatives := u.catchupAlternatives.Get(hash); alternatives != nil {
			u.catchupAlternatives.Set(hash, alternatives.Value(), ttlcache.DefaultTTL)
		}
	} else {
		u.catchupAlternatives.Delete(hash)
	}
}
