package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// preparedTxMap is the transaction map of a block built while the block is being validated.
// The map only depends on the block's subtree list, so it can be built before the block is
// added, and moveForwardBlock does not have to read the block's subtrees afterwards.
type preparedTxMap struct {
	hash     chainhash.Hash
	subtrees int
	cancel   context.CancelFunc
	// done is closed when txMap, conflicting and err are set
	done        chan struct{}
	txMap       *SortedHashSet
	conflicting []chainhash.Hash
	err         error
}

// PrepareBlock starts building the transaction map of a block on the current tip that is
// still being validated. Nothing about the block is trusted: moveForwardBlock only takes the
// map for the same block hash, once the block has been validated and added. A block that is
// not on the current tip, or that has no subtrees, is ignored. One block is prepared at a
// time; a new one replaces it.
func (stp *SubtreeProcessor) PrepareBlock(block *model.Block) {
	if block == nil || len(block.Subtrees) == 0 {
		return
	}

	currentBlockHeader := stp.currentBlockHeader.Load()
	if currentBlockHeader == nil || !block.Header.HashPrevBlock.IsEqual(currentBlockHeader.Hash()) {
		return
	}

	hash := *block.Hash()

	stp.preparedMu.Lock()

	if stp.prepared != nil {
		if stp.prepared.hash.IsEqual(&hash) {
			stp.preparedMu.Unlock()
			return
		}

		stp.prepared.cancel()
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &preparedTxMap{
		hash:     hash,
		subtrees: len(block.Subtrees),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	stp.prepared = p

	stp.preparedMu.Unlock()

	blockSubtreesMap := make(map[chainhash.Hash]int, len(block.Subtrees))
	for idx, subtree := range block.Subtrees {
		blockSubtreesMap[*subtree] = idx
	}

	stp.logger.Infof("[PrepareBlock][%s] building transaction map of %d subtrees before the block is added", block.String(), len(block.Subtrees))

	go func() {
		defer close(p.done)

		p.txMap, p.conflicting, p.err = stp.CreateTransactionMap(ctx, blockSubtreesMap, len(block.Subtrees), block.TransactionCount)
		if p.err != nil {
			stp.logger.Warnf("[PrepareBlock][%s] building transaction map failed, it is built when the block is added: %v", block.String(), p.err)
		} else {
			stp.logger.Infof("[PrepareBlock][%s] building transaction map DONE", block.String())
		}
	}()
}

// takePreparedTxMap returns the map prepared for block, waiting for it if it is still being
// built. It only returns one built over exactly the subtrees moveForwardBlock maps
// (numSubtreesToMap of them, none of them this node's own). Any prepared map is dropped.
func (stp *SubtreeProcessor) takePreparedTxMap(ctx context.Context, block *model.Block, numSubtreesToMap int) (*SortedHashSet, []chainhash.Hash, bool) {
	stp.preparedMu.Lock()
	p := stp.prepared
	stp.prepared = nil
	stp.preparedMu.Unlock()

	if p == nil {
		return nil, nil, false
	}

	if !p.hash.IsEqual(block.Hash()) || p.subtrees != len(block.Subtrees) || numSubtreesToMap != len(block.Subtrees) {
		p.cancel()
		return nil, nil, false
	}

	select {
	case <-p.done:
	case <-ctx.Done():
		p.cancel()
		return nil, nil, false
	}

	p.cancel()

	if p.err != nil {
		return nil, nil, false
	}

	return p.txMap, p.conflicting, true
}
