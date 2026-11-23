package subtreeprocessor

import (
	"context"
	"runtime"
	"slices"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// forkPrepared is the set of the winners of a one-back fork reorg (issue 2269), built while the
// winners are validated: the set of a competitor of the tip, or of a competitor and its child, so a
// reorg that moves onto them does not read their subtrees again. The set only depends on the blocks'
// subtree lists, so it is used for the same blocks with the same lists, and only while the tip is
// the one it was prepared against. The set of a child holds the competitor's hashes too, because the
// fast path needs one set over every winner and a merge of two at reorg time costs as much as
// reading them; the competitor's own set is let go when the build of its child's starts. The slot
// holds the competitor, then its first child alone.
type forkPrepared struct {
	tip      chainhash.Hash
	blocks   []chainhash.Hash
	subtrees [][]chainhash.Hash
	cancel   context.CancelFunc
	// done is closed when txCount, txMap, conflicting and err are set
	done        chan struct{}
	txCount     uint64
	txMap       *SortedHashSet
	conflicting []chainhash.Hash
	err         error
}

// forkPrepareWorkers is how many workers a prepare for a fork builds with: a quarter of the cores,
// so intake keeps the rest while a set of up to a billion hashes is built. Go has no priorities, so
// this bounds the width and nothing else.
func forkPrepareWorkers() int {
	return max(1, runtime.GOMAXPROCS(0)/4)
}

// forkSetFits is the memory gate before a set is built: the heap the last GC found live, less the
// bytes that are let go before the build, plus the set's bytes and the pending set's 48 bytes a
// transaction, must stay within three quarters of the memory limit. An unset limit passes, as in the
// fast path. The pending count is the processor's atomic one: the tx map itself is swapped by the
// processor goroutine.
func (stp *SubtreeProcessor) forkSetFits(setBytes, releasing uint64) bool {
	live, limit := reorgMemoryStats()
	if limit <= 0 || limit >= 1<<62 {
		return true
	}

	live -= min(live, releasing)

	return live+setBytes+stp.TxCount()*fastPathBytesPerPending <= uint64(limit)/4*3 //nolint:gosec // limit is positive
}

// prepareForkBlock starts building the set of a block that is not a child of the tip: a competitor
// of the tip, which has the tip's parent, or a child of a competitor that is prepared. Anything
// else is ignored. One fork is prepared at a time: a competitor other than the prepared one
// replaces it. A block already in the slot is not started again; one that fails is taken out of the
// slot, so the next hint for it starts over.
func (stp *SubtreeProcessor) prepareForkBlock(block *model.Block) {
	tip := stp.currentBlockHeader.Load()
	if tip == nil {
		return
	}

	hash, tipHash := *block.Hash(), *tip.Hash()
	if hash == tipHash {
		return
	}

	subtrees := make([]chainhash.Hash, len(block.Subtrees))
	for i, subtree := range block.Subtrees {
		subtrees[i] = *subtree
	}

	stp.preparedMu.Lock()
	stp.dropStaleForkLocked(tipHash)

	var parent *forkPrepared

	if !block.Header.HashPrevBlock.IsEqual(tip.HashPrevBlock) {
		for _, e := range stp.preparedFork {
			if len(e.blocks) == 1 && e.blocks[0].IsEqual(block.Header.HashPrevBlock) {
				parent = e
			}
		}

		if parent == nil {
			stp.preparedMu.Unlock()
			return
		}
	}

	e := &forkPrepared{tip: tipHash, done: make(chan struct{})}

	if parent != nil {
		e.blocks = append(append(e.blocks, parent.blocks...), hash)
		e.subtrees = append(append(e.subtrees, parent.subtrees...), subtrees)
	} else {
		e.blocks = []chainhash.Hash{hash}
		e.subtrees = [][]chainhash.Hash{subtrees}
	}

	for _, existing := range stp.preparedFork {
		if slices.Equal(existing.blocks, e.blocks) {
			stp.preparedMu.Unlock()
			return
		}
	}

	for _, existing := range stp.preparedFork {
		// the competitor again, or a second child of it: the one prepared is kept, and the slot never
		// holds more than the competitor and one child
		if parent == nil && existing.blocks[0] == hash || parent != nil && len(existing.blocks) == 2 {
			stp.preparedMu.Unlock()
			return
		}
	}

	if parent == nil {
		// another competitor: its fork replaces the one prepared
		for _, existing := range stp.preparedFork {
			existing.cancel()
		}

		stp.preparedFork = nil
	}

	var ctx context.Context

	ctx, e.cancel = context.WithCancel(context.Background())
	stp.preparedFork = append(stp.preparedFork, e)
	stp.preparedMu.Unlock()

	go stp.runForkPrepare(ctx, e, parent, block)
}

// runForkPrepare binds the block, checks the memory gate and builds the set of e, over the blocks of
// the competitor too when parent is set. A failure takes e out of the slot.
func (stp *SubtreeProcessor) runForkPrepare(ctx context.Context, e *forkPrepared, parent *forkPrepared, block *model.Block) {
	defer close(e.done)

	e.err = stp.buildForkSet(ctx, e, parent, block)
	if e.err != nil {
		stp.logger.Warnf("[PrepareBlock][%s] not preparing the block for a fork reorg: %v", block.String(), e.err)
		stp.removeFork(e)
	}
}

func (stp *SubtreeProcessor) buildForkSet(ctx context.Context, e *forkPrepared, parent *forkPrepared, block *model.Block) error {
	if parent != nil {
		select {
		case <-parent.done:
		case <-ctx.Done():
			return ctx.Err()
		}

		if parent.err != nil {
			return errors.NewProcessingError("the competitor of the block is not prepared", parent.err)
		}

		e.txCount = parent.txCount
	}

	count, err := stp.bindPreparedBlock(ctx, block)
	if err != nil {
		return err
	}

	e.txCount += count

	if stp.forkStale(e) {
		return errors.NewProcessingError("tip moved since the block was prepared")
	}

	// The set over both blocks is built from the store, not from the competitor's set, which is let
	// go before the build: the gate counts it as freed. The reorg of the competitor alone is then
	// not prepared, which a reorg onto its child makes unlikely to matter.
	var releasing uint64

	setBytes := 2 * e.txCount * fastPathSetBytesPerHash // this set and the room for its child's

	if parent != nil {
		releasing, setBytes = parent.txCount*fastPathSetBytesPerHash, e.txCount*fastPathSetBytesPerHash
	}

	if !stp.forkSetFits(setBytes, releasing) {
		return errors.NewProcessingError("not enough memory to prepare the block")
	}

	if parent != nil {
		stp.removeFork(parent)
	}

	return stp.buildForkTxMap(ctx, e)
}

// buildForkTxMap builds the set of every subtree of e's blocks, bounded to a quarter of the cores,
// with the subtrees that are still to be checked verified.
func (stp *SubtreeProcessor) buildForkTxMap(ctx context.Context, e *forkPrepared) (err error) {
	subtrees := make(map[chainhash.Hash]int)

	for _, list := range e.subtrees {
		for _, st := range list {
			// a subtree two blocks share holds the same hashes, and the set is a set
			if _, ok := subtrees[st]; !ok {
				subtrees[st] = len(subtrees)
			}
		}
	}

	stp.logger.Infof("[PrepareBlock][%s] building the transaction map of %d subtrees for a fork reorg", e.blocks[len(e.blocks)-1].String(), len(subtrees))

	e.txMap, e.conflicting, err = stp.createTransactionMap(ctx, subtrees, len(subtrees), e.txCount,
		txMapBuild{workers: forkPrepareWorkers(), verifyToCheck: true})
	if err != nil {
		return err
	}

	if stp.forkStale(e) {
		e.txMap, e.conflicting = nil, nil

		return errors.NewProcessingError("tip moved while the block was prepared")
	}

	return nil
}

// forkStale reports whether the tip is not the one e was prepared against.
func (stp *SubtreeProcessor) forkStale(e *forkPrepared) bool {
	tip := stp.currentBlockHeader.Load()

	return tip == nil || !tip.Hash().IsEqual(&e.tip)
}

// dropStaleForkLocked lets go of every set prepared against another tip. preparedMu is held.
func (stp *SubtreeProcessor) dropStaleForkLocked(tip chainhash.Hash) {
	kept := stp.preparedFork[:0]

	for _, e := range stp.preparedFork {
		if e.tip == tip {
			kept = append(kept, e)
			continue
		}

		e.cancel()
	}

	clear(stp.preparedFork[len(kept):])
	stp.preparedFork = kept
}

func (stp *SubtreeProcessor) removeFork(e *forkPrepared) {
	stp.preparedMu.Lock()
	defer stp.preparedMu.Unlock()

	for i, existing := range stp.preparedFork {
		if existing == e {
			stp.preparedFork = append(stp.preparedFork[:i:i], stp.preparedFork[i+1:]...)
			break
		}
	}

	e.cancel()
}

// releaseForkPrepared lets go of every set prepared for a fork, built or not.
func (stp *SubtreeProcessor) releaseForkPrepared() {
	stp.preparedMu.Lock()
	entries := stp.preparedFork
	stp.preparedFork = nil
	stp.preparedMu.Unlock()

	for _, e := range entries {
		e.cancel()
	}
}

// takePreparedForkSet returns the set prepared for exactly these blocks, in this order, with these
// subtree lists, if it is built and the tip is still the one it was prepared against. A set that
// is still being built is not waited for: it runs on a quarter of the cores and the reorg's own
// build runs on all of them. Every prepared set is let go, taken or not.
func (stp *SubtreeProcessor) takePreparedForkSet(blocks []*model.Block) (*SortedHashSet, []chainhash.Hash, bool) {
	stp.preparedMu.Lock()
	entries := stp.preparedFork
	stp.preparedFork = nil
	stp.preparedMu.Unlock()

	var taken *forkPrepared

	for _, e := range entries {
		if taken == nil && e.matches(blocks) {
			taken = e
			continue
		}

		e.cancel()
	}

	if taken == nil {
		return nil, nil, false
	}

	defer taken.cancel()

	select {
	case <-taken.done:
	default:
		return nil, nil, false
	}

	if taken.err != nil || stp.forkStale(taken) {
		return nil, nil, false
	}

	return taken.txMap, taken.conflicting, true
}

// matches reports whether e was prepared for exactly these blocks and subtree lists.
func (e *forkPrepared) matches(blocks []*model.Block) bool {
	if len(blocks) != len(e.blocks) {
		return false
	}

	for i, block := range blocks {
		if !e.blocks[i].IsEqual(block.Hash()) || !sameSubtrees(e.subtrees[i], block.Subtrees) {
			return false
		}
	}

	return true
}
