package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
)

// preparedTxMap is the transaction map of a block built while the block is being validated.
// The map only depends on the block's subtree list, so it can be built before the block is
// added, and moveForwardBlock does not have to read the block's subtrees afterwards.
type preparedTxMap struct {
	hash     chainhash.Hash
	subtrees []chainhash.Hash
	cancel   context.CancelFunc
	// done is closed when txCount, txMap, conflicting and err are set
	done        chan struct{}
	txCount     uint64
	txMap       *SortedHashSet
	conflicting []chainhash.Hash
	err         error
}

// PrepareBlock starts building the transaction map of a block on the current tip that is
// still being validated. The block comes from a peer and is not trusted: its subtree list is
// first bound to the header's merkle root, and the map is sized from the bound body, never
// from the claimed transaction count. moveForwardBlock only takes the map for a block with
// the same hash and subtree list, once that block has been validated and added. A block that
// is not on the current tip, or that has no subtrees, is ignored. One block is prepared at a
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

	subtrees := make([]chainhash.Hash, len(block.Subtrees))
	for i, subtree := range block.Subtrees {
		subtrees[i] = *subtree
	}

	stp.preparedMu.Lock()

	if stp.prepared != nil {
		if stp.prepared.hash.IsEqual(&hash) && sameSubtrees(stp.prepared.subtrees, block.Subtrees) {
			stp.preparedMu.Unlock()
			return
		}

		stp.prepared.cancel()
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &preparedTxMap{
		hash:     hash,
		subtrees: subtrees,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	stp.prepared = p

	stp.preparedMu.Unlock()

	go func() {
		defer close(p.done)

		p.txCount, p.err = stp.bindPreparedBlock(ctx, block)
		if p.err != nil {
			stp.logger.Warnf("[PrepareBlock][%s] not preparing the block: %v", block.String(), p.err)
			return
		}

		blockSubtreesMap := make(map[chainhash.Hash]int, len(subtrees))
		for idx, subtree := range subtrees {
			blockSubtreesMap[subtree] = idx
		}

		stp.logger.Infof("[PrepareBlock][%s] building transaction map of %d subtrees before the block is added", block.String(), len(subtrees))

		p.txMap, p.conflicting, p.err = stp.CreateTransactionMap(ctx, blockSubtreesMap, len(subtrees), p.txCount)
		if p.err != nil {
			stp.logger.Warnf("[PrepareBlock][%s] building transaction map failed, it is built when the block is added: %v", block.String(), p.err)
		} else {
			stp.logger.Infof("[PrepareBlock][%s] building transaction map DONE", block.String())
		}
	}()
}

// bindPreparedBlock binds the block's subtree list to its header, as block validation does
// before it loads the rest of a block: the first and last subtrees are read from the subtree
// store and bound to their keys, and the merkle root over the list, with the coinbase in the
// first subtree, must be the header's. It returns the tx count of the bound body: every
// subtree but the last is as long as the first, which block validation checks before the
// block can be added.
func (stp *SubtreeProcessor) bindPreparedBlock(ctx context.Context, block *model.Block) (uint64, error) {
	if block.CoinbaseTx == nil {
		return 0, errors.NewProcessingError("block has no coinbase tx")
	}

	last := len(block.Subtrees) - 1

	first, err := stp.readBoundSubtree(ctx, block.Subtrees[0])
	if err != nil {
		return 0, err
	}

	lastSubtree := first
	if last > 0 {
		if lastSubtree, err = stp.readBoundSubtree(ctx, block.Subtrees[last]); err != nil {
			return 0, err
		}
	}

	if err = block.CheckBodyBoundToHeader(first, lastSubtree); err != nil {
		return 0, err
	}

	return uint64(last)*uint64(first.Length()) + uint64(lastSubtree.Length()), nil // nolint: gosec
}

// readBoundSubtree reads a subtree from the subtree store and checks that it is the one its
// key names.
func (stp *SubtreeProcessor) readBoundSubtree(ctx context.Context, hash *chainhash.Hash) (*subtreepkg.Subtree, error) {
	reader, err := stp.subtreeStore.GetIoReader(ctx, hash[:], fileformat.FileTypeSubtree)
	if err != nil {
		if reader, err = stp.subtreeStore.GetIoReader(ctx, hash[:], fileformat.FileTypeSubtreeToCheck); err != nil {
			return nil, errors.NewServiceError("error getting subtree %s", hash.String(), err)
		}
	}

	defer func() {
		_ = reader.Close()
	}()

	subtree := &subtreepkg.Subtree{}
	if err = subtree.DeserializeFromReader(reader); err != nil {
		return nil, errors.NewProcessingError("error deserializing subtree %s", hash.String(), err)
	}

	if root := subtree.RootHash(); root == nil || !root.IsEqual(hash) {
		return nil, errors.NewProcessingError("subtree %s does not hash to its key", hash.String())
	}

	return subtree, nil
}

// takePreparedTxMap returns the map prepared for block, waiting for it if it is still being
// built. It only returns one built for the same block hash and subtree list, over exactly the
// subtrees moveForwardBlock maps (numSubtreesToMap of them, none of them this node's own).
// Any prepared map is dropped.
func (stp *SubtreeProcessor) takePreparedTxMap(ctx context.Context, block *model.Block, numSubtreesToMap int) (*SortedHashSet, []chainhash.Hash, bool) {
	stp.preparedMu.Lock()
	p := stp.prepared
	stp.prepared = nil
	stp.preparedMu.Unlock()

	if p == nil {
		return nil, nil, false
	}

	if !p.hash.IsEqual(block.Hash()) || !sameSubtrees(p.subtrees, block.Subtrees) || numSubtreesToMap != len(block.Subtrees) {
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

func sameSubtrees(prepared []chainhash.Hash, subtrees []*chainhash.Hash) bool {
	if len(prepared) != len(subtrees) {
		return false
	}

	for i := range prepared {
		if subtrees[i] == nil || !prepared[i].IsEqual(subtrees[i]) {
			return false
		}
	}

	return true
}
