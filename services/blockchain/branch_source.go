package blockchain

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
)

// BranchHeader is what a branch holds about one header above the trunk, so the
// difficulty calculator can read it as it reads a stored block: the header, its
// height and its cumulative chain work, 32 bytes big-endian, the form the store
// keeps chain_work in.
type BranchHeader struct {
	Header    *model.BlockHeader
	Height    uint32
	ChainWork []byte
}

// branchSource is a HeaderSource over headers that are not stored: lookup
// answers for the headers the branch holds, and trunk, the store or the
// blockchain client, answers for everything committed. A walk that starts in
// the branch crosses into the trunk at the first parent the branch does not
// hold, which is where every branch is rooted.
type branchSource struct {
	trunk  HeaderSource
	lookup func(hash chainhash.Hash) (*BranchHeader, bool)
}

var _ HeaderSource = (*branchSource)(nil)

// NewBranchSource returns a HeaderSource that answers from lookup for the
// headers it holds and from trunk for the rest. It lets CalcNextWorkRequiredFrom
// judge a header whose ancestry is partly not stored yet, such as one in the
// legacy header cache or one fetched from a peer during catchup.
func NewBranchSource(trunk HeaderSource, lookup func(hash chainhash.Hash) (*BranchHeader, bool)) HeaderSource {
	return &branchSource{trunk: trunk, lookup: lookup}
}

func (s *branchSource) meta(c *BranchHeader) *model.BlockHeaderMeta {
	return &model.BlockHeaderMeta{Height: c.Height, ChainWork: append([]byte(nil), c.ChainWork...)}
}

// GetBlockHeader answers from the branch, then the trunk.
func (s *branchSource) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if c, ok := s.lookup(*hash); ok {
		return c.Header, s.meta(c), nil
	}

	return s.trunk.GetBlockHeader(ctx, hash)
}

// GetBlockHeaders returns hash and then its ancestors, newest first, taking
// what the branch holds and asking the trunk for the rest in one call.
func (s *branchSource) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, count uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	headers := make([]*model.BlockHeader, 0, count)
	metas := make([]*model.BlockHeaderMeta, 0, count)

	cur := *hash

	for uint64(len(headers)) < count {
		c, ok := s.lookup(cur)
		if !ok {
			break
		}

		headers = append(headers, c.Header)
		metas = append(metas, s.meta(c))
		cur = *c.Header.HashPrevBlock
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

// GetHashOfAncestorBlock walks depth parents back through the branch and hands
// whatever depth is left to the trunk, which returns errors.ErrNotFound when the
// chain is shorter, exactly as it would for a stored block.
func (s *branchSource) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	cur := *hash

	for depth > 0 {
		c, ok := s.lookup(cur)
		if !ok {
			return s.trunk.GetHashOfAncestorBlock(ctx, &cur, depth)
		}

		cur = *c.Header.HashPrevBlock
		depth--
	}

	return &cur, nil
}

// GetSuitableBlock is the store's GetSuitableBlock (stores/blockchain/sql/
// GetSuitableBlock.go) over the branch: hash and its two parents, in the store's
// order (oldest first, its ORDER BY depth DESC), sorted by time with the same
// sorting network, and the middle one returned. A hash the branch does not hold
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
