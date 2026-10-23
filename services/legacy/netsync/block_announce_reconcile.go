package netsync

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-wire"
)

// blockAnnounceReconcileInterval is how often the reconciler reads the chain tip. A tip is
// announced by the reconciler only when it is still unannounced one full interval after the
// reconciler first saw it, which leaves blocks-final that long to deliver it first.
const blockAnnounceReconcileInterval = 30 * time.Second

// recentAnnouncementsSize bounds the blocks remembered as announced. blocks-final announces
// every valid block, side-chain blocks too, so the last announcement need not be the tip; a
// few recent ones are enough to tell whether the tip was among them.
const recentAnnouncementsSize = 16

// recentAnnouncements remembers the last recentAnnouncementsSize block hashes announced to
// legacy peers. The zero value is ready to use.
type recentAnnouncements struct {
	mu     sync.Mutex
	hashes [recentAnnouncementsSize]chainhash.Hash
	next   int
	count  int
}

func (r *recentAnnouncements) add(hash *chainhash.Hash) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.hashes[r.next] = *hash
	r.next = (r.next + 1) % recentAnnouncementsSize

	if r.count < recentAnnouncementsSize {
		r.count++
	}
}

func (r *recentAnnouncements) contains(hash *chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.count; i++ {
		if r.hashes[i].IsEqual(hash) {
			return true
		}
	}

	return false
}

// announceBlock relays a block to legacy peers, signals the rebroadcast queue, and raises the
// known height of every peer that announced it. Both the blocks-final consumer and the
// reconciler announce through here, so each records what it announced.
func (sm *SyncManager) announceBlock(hash *chainhash.Hash, header *wire.BlockHeader, height uint32) {
	sm.peerNotifier.RelayInventory(wire.NewInvVect(wire.InvTypeBlock, hash), header)
	sm.peerNotifier.BlockConnected()

	// Raise the known height of every peer that announced this block, as btcd's handleBlockMsg
	// does. Without it a peer's height moved only when it announced a block whose header this
	// node already had, so at the tip startSync could find no peer ahead and nothing re-asked a
	// block whose park record was dropped (2026-10-10, 13 minutes at 970,502).
	if h, err := safeconversion.Uint32ToInt32(height); err == nil && h > 0 {
		sm.peerNotifier.UpdatePeerHeights(hash, h, nil)
	}

	sm.announced.add(hash)
}

// runBlockAnnounceReconciler announces the chain tip to legacy peers when blocks-final did not.
//
// The Blockchain service publishes blocks-final after it commits a block, and the publish is
// asynchronous: a crash between the commit and the publish, or a publish dropped while Kafka is
// down, leaves a committed block that Legacy never announces. The Block Persister has its own
// recovery for this (it polls for blocks not yet persisted); this is Legacy's.
func (sm *SyncManager) runBlockAnnounceReconciler(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var pending *chainhash.Hash

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pending = sm.reconcileAnnouncedTip(ctx, pending)
		}
	}
}

// reconcileAnnouncedTip runs one reconciler tick. pending is the unannounced tip the previous
// tick saw, or nil. It announces the tip when the tip is unannounced and equal to pending, and
// returns the tip to hold as pending for the next tick: nil once the tip is announced, the new
// tip when it moved, and pending unchanged when the tip cannot be read.
func (sm *SyncManager) reconcileAnnouncedTip(ctx context.Context, pending *chainhash.Hash) *chainhash.Hash {
	header, meta, err := sm.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		sm.logger.Warnf("[blockAnnounceReconciler] failed to read the best block header: %v", err)
		return pending
	}

	hash := header.Hash()

	if sm.announced.contains(hash) {
		return nil
	}

	if pending == nil || !pending.IsEqual(hash) {
		return hash
	}

	sm.logger.Warnf("[blockAnnounceReconciler][%s] best block at height %d was not announced from blocks-final, announcing it now", hash, meta.Height)
	sm.announceBlock(hash, header.ToWireBlockHeader(), meta.Height)

	return nil
}
