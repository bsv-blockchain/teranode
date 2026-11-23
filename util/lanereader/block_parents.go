package lanereader

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/lane"
)

const (
	// prepareTimeout bounds how long loading a block's ancestry for its parent check may take,
	// waiting for another load included. The load runs beside the block's validation, never in its
	// way.
	prepareTimeout = 10 * time.Second

	// maxPendingParents bounds the parents a block's check holds while its ancestry loads; past it
	// they are counted not_ready.
	maxPendingParents = 100_000
)

// BlockParents is the lanes shadow of one block's parent check (model.LaneParentObserver): told of
// each parent record the check reads, it answers the same question from the index, on the chain
// ending at the block's parent, and counts the comparison under ReaderBlockParent. The chain's
// rows load in the background; parents told of before they are in are held, up to
// maxPendingParents, and answered once they are.
type BlockParents struct {
	r *Reader

	mu      sync.Mutex
	done    bool
	view    *lane.ChainView
	pending []pendingParent
}

type pendingParent struct {
	hash     chainhash.Hash
	pos      uint64
	blockIDs []uint32
}

// BlockParents starts the shadow of the parent check of a block whose parent is parent. On a nil
// Reader it returns nil, which model.ContextWithLaneParentObserver ignores.
func (r *Reader) BlockParents(ctx context.Context, parent chainhash.Hash) *BlockParents {
	if r == nil {
		return nil
	}

	b := &BlockParents{r: r}

	go func() {
		prepareCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), prepareTimeout)
		defer cancel()

		view, _ := r.Prepare(prepareCtx, parent)

		b.mu.Lock()
		b.done, b.view = true, view
		pending := b.pending
		b.pending = nil
		b.mu.Unlock()

		for _, p := range pending {
			r.CompareBlocks(ReaderBlockParent, view, p.hash, p.pos, p.blockIDs)
		}
	}()

	return b
}

// ObserveParent answers one parent, or holds it until the chain's rows are in.
func (b *BlockParents) ObserveParent(parentHash chainhash.Hash, lanePosition uint64, blockIDs []uint32) {
	b.mu.Lock()

	if b.done {
		view := b.view
		b.mu.Unlock()

		b.r.CompareBlocks(ReaderBlockParent, view, parentHash, lanePosition, blockIDs)

		return
	}

	if len(b.pending) >= maxPendingParents {
		b.mu.Unlock()
		count(ReaderBlockParent, KindNotReady, lane.ViaNone)

		return
	}

	b.pending = append(b.pending, pendingParent{hash: parentHash, pos: lanePosition, blockIDs: append([]uint32(nil), blockIDs...)})
	b.mu.Unlock()
}
