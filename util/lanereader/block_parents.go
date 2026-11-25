package lanereader

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// prepareTimeout bounds how long loading a block's ancestry for its parent check may take,
// waiting for another load included. The load runs beside the block's validation, never in its
// way. A variable so a test can shorten it.
var prepareTimeout = 10 * time.Second

// maxPendingParents bounds the parents a block's check holds while its ancestry loads; past it
// they are counted not_ready.
const maxPendingParents = 100_000

// BlockParents is the lanes shadow of one block's parent check (model.LaneParentObserver): told of
// each parent record the check reads, it answers the same question from the index, on the chain
// ending at the block's parent, and counts the comparison under ReaderBlockParent. The chain's
// rows load in the background; parents told of before they are in are held, up to
// maxPendingParents, and answered once they are.
type BlockParents struct {
	r *Reader

	// loaded is set once the chain's load has ended; after that a parent is answered without the
	// mutex, which guards only pending.
	loaded  atomic.Pointer[loadedChain]
	mu      sync.Mutex
	pending []pendingParent
}

// loadedChain is how a block's chain load ended: its view, or nil and the kind to count instead.
type loadedChain struct {
	view   *lane.ChainView
	noView string
}

type pendingParent struct {
	hash     chainhash.Hash
	pos      uint64
	coinbase bool
	blockIDs []uint32
}

// BlockParents starts the shadow of the parent check of a block whose parent is parent. On a nil
// Reader it returns nil, whose ObserveParent does nothing.
func (r *Reader) BlockParents(ctx context.Context, parent chainhash.Hash) *BlockParents {
	if r == nil {
		return nil
	}

	b := &BlockParents{r: r}

	go func() {
		prepareCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), prepareTimeout)
		defer cancel()

		view, noView := r.prepare(prepareCtx, parent)

		b.mu.Lock()
		b.loaded.Store(&loadedChain{view: view, noView: noView})
		pending := b.pending
		b.pending = nil
		b.mu.Unlock()

		for _, p := range pending {
			r.compareBlocks(ReaderBlockParent, view, noView, p.hash, p.pos, p.coinbase, p.blockIDs)
		}
	}()

	return b
}

// ObserveParent answers one parent, or holds it until the chain's rows are in. On a nil
// BlockParents it does nothing.
func (b *BlockParents) ObserveParent(parentHash chainhash.Hash, lanePosition uint64, coinbase bool, blockIDs []uint32) {
	if b == nil {
		return
	}

	if l := b.loaded.Load(); l != nil {
		b.r.compareBlocks(ReaderBlockParent, l.view, l.noView, parentHash, lanePosition, coinbase, blockIDs)

		return
	}

	b.mu.Lock()

	if l := b.loaded.Load(); l != nil {
		b.mu.Unlock()
		b.r.compareBlocks(ReaderBlockParent, l.view, l.noView, parentHash, lanePosition, coinbase, blockIDs)

		return
	}

	if len(b.pending) >= maxPendingParents {
		b.mu.Unlock()
		count(ReaderBlockParent, KindNotReady, lane.ViaNone)

		return
	}

	b.pending = append(b.pending, pendingParent{hash: parentHash, pos: lanePosition, coinbase: coinbase, blockIDs: append([]uint32(nil), blockIDs...)})
	b.mu.Unlock()
}
