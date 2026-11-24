package subtreeprocessor

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"golang.org/x/sync/errgroup"
)

// What a reorg onto a prepared competitor F1 (and its child F2) needs from the lost block B is read
// while B is still the tip, so the reorg does not read B again (issue 2269). B is immutable then, and
// F1's set is built, so B is probed against it in the background:
//
//   - C = B minus F1, the nodes of B that F1 lacks, with their positions in B (the candidates for D1);
//   - D2 against F1 = F1 minus B, and the hits.
//
// The reorg's union is F1+F2, and with S1, S2 the hash sets of the two blocks and U their union:
//
//   - D1 = C minus S2, C probed against U: a node of B that F1 lacks is D1 unless F2 has it;
//   - D2 = U minus B = (S1 minus B) + (S2 minus S1 minus C): a hash of S2 that F1 lacks is in B only if
//     it is in C. S1 minus B is what the probe found.
//
// The second term needs to know which hashes of U are F1's, and F1's set is let go before U is built to
// save its memory. forkPrepared.firstMask keeps that as one bit per position of U, built while both
// sets are alive. A reorg then starts from the mask, clears the bits of S1 minus B, sets the bit of every
// C node F2 has, and reads D2 off the unset bits as computeReorgDelta does. Duplicates and the
// coinbase placeholder fall out of the same bitmap as in the reference: Find answers one position for a
// duplicated hash, and the placeholder is left out when D2 is read.

// forkLostDelta is the probe of B against the competitor's set, shared by the competitor's entry in the
// fork slot and its child's. It is ready when the probe finished against the tip it started on.
type forkLostDelta struct {
	cancel context.CancelFunc
	ctx    context.Context
	// refs is the entries holding it: it is cancelled when the last lets go
	refs atomic.Int32
	// done is closed when the fields below are set
	done chan struct{}

	lost         chainhash.Hash
	lostSubtrees []chainhash.Hash
	first        chainhash.Hash
	delta        *reorgDelta
	err          error
}

func newForkLostDelta() *forkLostDelta {
	l := &forkLostDelta{done: make(chan struct{})}
	l.ctx, l.cancel = context.WithCancel(context.Background())
	l.refs.Store(1)

	return l
}

// ready reports whether the probe finished and succeeded. It does not wait.
func (l *forkLostDelta) ready() bool {
	if l == nil {
		return false
	}

	select {
	case <-l.done:
		return l.err == nil && l.delta != nil
	default:
		return false
	}
}

// holdBy returns cancel for an entry that holds l: the probe stops when every entry that holds it has
// been cancelled. The competitor's entry is cancelled when its child's set starts to build, which must
// not stop the probe its child inherits.
func (l *forkLostDelta) holdBy(cancel context.CancelFunc) context.CancelFunc {
	var once sync.Once

	return func() {
		cancel()
		once.Do(func() {
			if l.refs.Add(-1) == 0 {
				l.cancel()
			}
		})
	}
}

// runForkLostDelta probes the tip against e's set, the competitor's, and records the result in e.lost.
// It runs in the background: a reorg that takes e before it finishes does not wait for it.
func (stp *SubtreeProcessor) runForkLostDelta(e *forkPrepared) {
	l := e.lost
	defer close(l.done)

	if stp.blockchainClient == nil {
		l.err = errors.NewProcessingError("no blockchain client to read the tip with")
		return
	}

	lost, err := stp.blockchainClient.GetBlock(l.ctx, &e.tip)
	if err != nil {
		l.err = errors.NewProcessingError("cannot read the tip", err)
		return
	}

	l.err = stp.probeLostAgainstFirst(l.ctx, e, lost)
	if l.err != nil {
		stp.logger.Debugf("[PrepareBlock][%s] not precomputing the lost block's delta: %v", e.blocks[0].String(), l.err)
	}
}

// probeLostAgainstFirst computes the lost block's delta against e's set once the memory gate passes, and
// keeps it only if the tip is still the lost block when it finishes.
func (stp *SubtreeProcessor) probeLostAgainstFirst(ctx context.Context, e *forkPrepared, lost *model.Block) error {
	l := e.lost

	// worst case: every node of the lost block is D1 (a node with its position), and every hash of the
	// competitor is D2
	const nodeBytes = 32 + 3*8

	if !stp.forkSetFits(lost.TransactionCount*nodeBytes + e.txCount*32) {
		return errors.NewProcessingError("not enough memory to keep the lost block's delta")
	}

	delta, err := stp.computeReorgDelta(ctx, []*model.Block{lost}, e.txMap, forkPrepareWorkers())
	if err != nil {
		return err
	}

	if stp.forkStale(e) {
		return errors.NewProcessingError("tip moved while the lost block was probed")
	}

	l.lost, l.first, l.delta = *lost.Hash(), e.blocks[0], delta
	l.lostSubtrees = make([]chainhash.Hash, len(lost.Subtrees))

	for i, st := range lost.Subtrees {
		l.lostSubtrees[i] = *st
	}

	return nil
}

// markFirstSet is one bit for every position of union, set for the hashes of first.
func markFirstSet(first, union *SortedHashSet, workers int) []atomic.Uint64 {
	mask := make([]atomic.Uint64, (union.Positions()+63)/64)

	first.IterPositions(workers, func(_ int, _ uint64, h *chainhash.Hash) {
		if pos, ok := union.Find(*h); ok {
			word, bit := &mask[pos/64], uint64(1)<<(pos%64)
			if word.Load()&bit == 0 {
				word.Or(bit)
			}
		}
	})

	return mask
}

// forkDeltaChunk is how many candidates one task of the probe against the union covers.
const forkDeltaChunk = 64 << 10

// deriveForkDelta is the delta of the lost block against union, the first winner and the one after it,
// from pre, the lost block's delta against the first winner's set, and mask, the first winner's hashes
// in union. It equals computeReorgDelta over the lost block and union.
func deriveForkDelta(pre *reorgDelta, mask []atomic.Uint64, union *SortedHashSet, workers int) *reorgDelta {
	bitmap := make([]atomic.Uint64, len(mask))
	for i := range mask {
		bitmap[i].Store(mask[i].Load())
	}

	// the first winner's hashes the lost block lacks are D2 whatever the next winner holds
	for _, h := range pre.D2 {
		if pos, ok := union.Find(h); ok {
			bitmap[pos/64].Store(bitmap[pos/64].Load() &^ (1 << (pos % 64)))
		}
	}

	chunks := make([][]reorgDeltaNode, (len(pre.D1)+forkDeltaChunk-1)/forkDeltaChunk)

	var g errgroup.Group

	g.SetLimit(max(1, workers))

	for c := range chunks {
		g.Go(func() error {
			for _, node := range pre.D1[c*forkDeltaChunk : min((c+1)*forkDeltaChunk, len(pre.D1))] {
				pos, ok := union.Find(node.Hash)
				if !ok {
					chunks[c] = append(chunks[c], node)
					continue
				}

				word, bit := &bitmap[pos/64], uint64(1)<<(pos%64)
				if word.Load()&bit == 0 {
					word.Or(bit)
				}
			}

			return nil
		})
	}

	_ = g.Wait()

	delta := &reorgDelta{}

	for _, chunk := range chunks {
		delta.D1 = append(delta.D1, chunk...)
	}

	delta.Hits = pre.Hits + uint64(len(pre.D1)-len(delta.D1))
	delta.D2 = unsetPositions(union, bitmap, workers)

	return delta
}

// lostDelta is the delta of the lost block against winners, the set of the blocks e was prepared for,
// from the probe made while the lost block was the tip. It returns nil when it cannot be sure it equals
// what computeReorgDelta finds: no entry, no finished probe, another lost block or another first winner,
// or a second winner whose mask was not kept. The caller then reads the lost block.
func (e *forkPrepared) lostDelta(lost *model.Block, winners *SortedHashSet, workers int) *reorgDelta {
	if e == nil || !e.lost.ready() || e.lost.first != e.blocks[0] || e.lost.lost != *lost.Hash() ||
		!sameSubtrees(e.lost.lostSubtrees, lost.Subtrees) {
		return nil
	}

	switch len(e.blocks) {
	case 1:
		return e.lost.delta
	case 2:
		if e.firstMask != nil {
			return deriveForkDelta(e.lost.delta, e.firstMask, winners, workers)
		}
	}

	return nil
}
