package netsync

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/util"
)

// cachedHeader is what the header cache keeps about one header it holds above
// the committed tip, so the difficulty calculator can read it as it reads a
// stored block: the header, its height and its cumulative chain work, 32 bytes
// big-endian, the form the store keeps chain_work in.
type cachedHeader struct {
	header    *model.BlockHeader
	height    int32
	chainWork [32]byte
}

// branchSource is the blockchain.HeaderSource the header rules judge a cached
// header with: lookup answers for headers held above the committed tip, and
// trunk, the store or the blockchain client, answers for everything committed.
// A walk that starts in the cache crosses into the trunk at the first parent the
// cache does not hold, which is where every cached run is rooted.
type branchSource struct {
	trunk  blockchain.HeaderSource
	lookup func(hash chainhash.Hash) (*cachedHeader, bool)
}

var _ blockchain.HeaderSource = (*branchSource)(nil)

func (s *branchSource) meta(c *cachedHeader) *model.BlockHeaderMeta {
	return &model.BlockHeaderMeta{Height: uint32(c.height), ChainWork: append([]byte(nil), c.chainWork[:]...)} //nolint:gosec // a cached height is above the tip, never negative
}

// GetBlockHeader answers from the cache, then the trunk.
func (s *branchSource) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if c, ok := s.lookup(*hash); ok {
		return c.header, s.meta(c), nil
	}

	return s.trunk.GetBlockHeader(ctx, hash)
}

// GetBlockHeaders returns hash and then its ancestors, newest first, taking
// what the cache holds and asking the trunk for the rest in one call.
func (s *branchSource) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, count uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	headers := make([]*model.BlockHeader, 0, count)
	metas := make([]*model.BlockHeaderMeta, 0, count)

	cur := *hash

	for uint64(len(headers)) < count {
		c, ok := s.lookup(cur)
		if !ok {
			break
		}

		headers = append(headers, c.header)
		metas = append(metas, s.meta(c))
		cur = *c.header.HashPrevBlock
	}

	if remaining := count - uint64(len(headers)); remaining > 0 {
		more, moreMetas, err := s.trunk.GetBlockHeaders(ctx, &cur, remaining)
		if err != nil {
			return nil, nil, err
		}

		headers = append(headers, more...)
		metas = append(metas, moreMetas...)
	}

	return headers, metas, nil
}

// GetHashOfAncestorBlock walks depth parents back through the cache and hands
// whatever depth is left to the trunk, which returns errors.ErrNotFound when the
// chain is shorter, exactly as it would for a stored block.
func (s *branchSource) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	cur := *hash

	for depth > 0 {
		c, ok := s.lookup(cur)
		if !ok {
			return s.trunk.GetHashOfAncestorBlock(ctx, &cur, depth)
		}

		cur = *c.header.HashPrevBlock
		depth--
	}

	return &cur, nil
}

// GetSuitableBlock is the store's GetSuitableBlock (stores/blockchain/sql/
// GetSuitableBlock.go) over the cache: hash and its two parents, in the store's
// order (oldest first, its ORDER BY depth DESC), sorted by time with the same
// sorting network, and the middle one returned. A hash the cache does not hold
// is the trunk's to answer whole.
func (s *branchSource) GetSuitableBlock(ctx context.Context, hash *chainhash.Hash) (*model.SuitableBlock, error) {
	if _, ok := s.lookup(*hash); !ok {
		return s.trunk.GetSuitableBlock(ctx, hash)
	}

	headers, metas, err := s.GetBlockHeaders(ctx, hash, 3)
	if err != nil {
		return nil, err
	}

	if len(headers) != 3 || len(metas) != 3 {
		return nil, errors.NewProcessingError("not enough candidates for suitable block. have %d, need 3", len(headers))
	}

	candidates := make([]*model.SuitableBlock, 0, 3)

	for i := 2; i >= 0; i-- {
		candidates = append(candidates, &model.SuitableBlock{
			Hash:      headers[i].Hash().CloneBytes(),
			Height:    metas[i].Height,
			NBits:     append([]byte(nil), headers[i].Bits.CloneBytes()...),
			Time:      headers[i].Timestamp,
			ChainWork: metas[i].ChainWork,
		})
	}

	util.SortForDifficultyAdjustment(candidates)

	return candidates[1], nil
}
