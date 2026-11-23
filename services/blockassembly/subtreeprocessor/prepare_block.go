package subtreeprocessor

import (
	"context"
	"slices"

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

// failed reports whether the prepare has ended with an error.
func (p *preparedTxMap) failed() bool {
	select {
	case <-p.done:
		return p.err != nil
	default:
		return false
	}
}

// PrepareBlock starts building the transaction map of a block on the current tip that is
// still being validated. The block comes from a peer and is not trusted: its subtree list is
// first bound to the header's merkle root, and the map is sized from the bound body, never
// from the claimed transaction count. moveForwardBlock only takes the map for a block with
// the same hash and subtree list, once that block has been validated and added. A block that
// is not on the current tip, or that has no subtrees, is ignored. One block is prepared at a
// time; a new one replaces it. A prepare that failed is replaced by the next hint for the
// block, so block validation's retry gets another attempt.
//
// The same block hash with another subtree list does not replace a prepare until its list is
// bound to the header: a peer can relay the honest header with a list of its own valid
// subtrees, and that must not cancel the honest prepare.
//
// A map that is not taken is dropped as soon as the tip is seen to have moved past its block,
// when it is built, or by the next PrepareBlock or moveForwardBlock. A block that fails
// validation, so never moves the tip, holds its map until then, which is at most one block
// interval.
func (stp *SubtreeProcessor) PrepareBlock(block *model.Block) {
	if block == nil || len(block.Subtrees) == 0 {
		return
	}

	if stp.tipMovedPast(block) {
		return
	}

	hash := *block.Hash()

	subtrees := make([]chainhash.Hash, len(block.Subtrees))
	for i, subtree := range block.Subtrees {
		subtrees[i] = *subtree
	}

	// install is false when the prepare must bind its list before it replaces the current one
	install := true

	stp.preparedMu.Lock()

	if existing := stp.prepared; existing != nil {
		switch {
		case !existing.hash.IsEqual(&hash):
			existing.cancel()
		case existing.failed():
			existing.cancel()
		case sameSubtrees(existing.subtrees, block.Subtrees):
			stp.preparedMu.Unlock()
			return
		default:
			install = false
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &preparedTxMap{
		hash:     hash,
		subtrees: subtrees,
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	if install {
		stp.prepared = p
	}

	stp.preparedMu.Unlock()

	go stp.runPrepare(ctx, p, block, install)
}

// runPrepare binds the block, builds its transaction map, and sets the result on p.
func (stp *SubtreeProcessor) runPrepare(ctx context.Context, p *preparedTxMap, block *model.Block, installed bool) {
	defer close(p.done)

	p.txCount, p.err = stp.bindPreparedBlock(ctx, block)
	if p.err != nil {
		stp.logger.Warnf("[PrepareBlock][%s] not preparing the block: %v", block.String(), p.err)
		return
	}

	if !installed && !stp.installPrepared(p) {
		p.cancel()
		p.err = errors.NewProcessingError("prepared block is not the one being prepared")

		return
	}

	if stp.dropIfTipMovedPast(p, block) {
		return
	}

	blockSubtreesMap := make(map[chainhash.Hash]int, len(p.subtrees))
	for idx, subtree := range p.subtrees {
		blockSubtreesMap[subtree] = idx
	}

	stp.logger.Infof("[PrepareBlock][%s] building transaction map of %d subtrees before the block is added", block.String(), len(p.subtrees))

	p.txMap, p.conflicting, p.err = stp.CreateTransactionMap(ctx, blockSubtreesMap, len(p.subtrees), p.txCount)
	if p.err != nil {
		stp.logger.Warnf("[PrepareBlock][%s] building transaction map failed, it is built when the block is added: %v", block.String(), p.err)
		return
	}

	if stp.dropIfTipMovedPast(p, block) {
		return
	}

	stp.logger.Infof("[PrepareBlock][%s] building transaction map DONE", block.String())
}

// installPrepared makes p, whose list is bound to its header, the prepared block, replacing
// a prepare for the same block hash. It does not replace one for another block, or one with
// the same list that is already installed.
func (stp *SubtreeProcessor) installPrepared(p *preparedTxMap) bool {
	stp.preparedMu.Lock()
	defer stp.preparedMu.Unlock()

	if existing := stp.prepared; existing != nil {
		if !existing.hash.IsEqual(&p.hash) || slices.Equal(existing.subtrees, p.subtrees) {
			return false
		}

		existing.cancel()
	}

	stp.prepared = p

	return true
}

// tipMovedPast reports whether block is no longer on top of the current tip.
func (stp *SubtreeProcessor) tipMovedPast(block *model.Block) bool {
	currentBlockHeader := stp.currentBlockHeader.Load()

	return currentBlockHeader == nil || !block.Header.HashPrevBlock.IsEqual(currentBlockHeader.Hash())
}

// dropIfTipMovedPast drops p and its map when the tip has moved past its block: a hint that
// arrived after the block was added, or a map finished after another block took its place.
// Nothing takes such a map, and it can be several GB.
func (stp *SubtreeProcessor) dropIfTipMovedPast(p *preparedTxMap, block *model.Block) bool {
	if !stp.tipMovedPast(block) {
		return false
	}

	stp.preparedMu.Lock()
	if stp.prepared == p {
		stp.prepared = nil
	}
	stp.preparedMu.Unlock()

	p.cancel()

	p.txMap = nil
	p.conflicting = nil
	p.err = errors.NewProcessingError("tip moved past the prepared block")

	return true
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
