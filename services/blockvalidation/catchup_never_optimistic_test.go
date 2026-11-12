package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidateBlocksOnChannel_CatchupIsNeverOptimistic pins that the catch-up path builds its
// ValidateBlockOptions with DisableOptimisticMining unconditionally true — the value no longer
// depends on the operator's two optimistic-mining flags (bitcoin-sv/teranode#4692). It has to, because
// this caller is the one that supplies CachedHeaders, whose run cannot carry the median-time-past
// window the optimistic branch checks synchronously; the peer opt-in governs the peer-served
// new-block path only.
//
// The observable is the ordering that separates the two branches: the optimistic branch AddBlocks
// the received body BEFORE block.Valid runs, the non-optimistic branch validates first and only
// stores what passed. So a block whose subtrees validate but whose body then fails block.Valid must,
// on every flag combination, come back as an error with nothing added.
//
// Mutation proof: restoring optimisticMiningDisabledForPeerPath at the catch-up options literal
// turns the double-opt-in row red — the block is added before validation and the failure no longer
// reaches the caller.
func TestValidateBlocksOnChannel_CatchupIsNeverOptimistic(t *testing.T) {
	for _, tc := range []struct {
		name   string
		global bool
		peer   bool
	}{
		{"both off (shipped default)", false, false},
		{"global on, peer off", true, false},
		{"global off, peer on", false, true},
		{"double opt-in", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runCatchupIsNeverOptimistic(t, tc.global, tc.peer)
		})
	}
}

func runCatchupIsNeverOptimistic(t *testing.T, optimisticMining, optimisticMiningPeerBlocks bool) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	suite.Server.settings.BlockValidation.OptimisticMining = optimisticMining
	suite.Server.settings.BlockValidation.OptimisticMiningPeerBlocks = optimisticMiningPeerBlocks
	suite.Server.blockValidation.settings.BlockValidation.OptimisticMining = optimisticMining
	suite.Server.blockValidation.settings.BlockValidation.OptimisticMiningPeerBlocks = optimisticMiningPeerBlocks

	suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
	suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.MockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 99, MinedSet: true}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	suite.MockBlockchain.On("InvalidateBlock", mock.Anything, mock.Anything).Return([]chainhash.Hash{}, nil).Maybe()
	easyNBits, _ := model.NewNBitFromString("207fffff")
	suite.MockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(easyNBits, nil).Maybe()

	// Subtree validation SUCCEEDS, so the run reaches the point where the two branches diverge.
	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	suite.Server.blockValidation.subtreeValidationClient = subtreeVal

	block := buildOneSubtreeBlock(t, suite, 100)
	// Break the body so block.Valid's own merkle check fails: the header commits to a root the
	// named subtrees do not produce. The header is then re-mined to the easy target, since the full
	// path checks proof of work before it gets anywhere near block.Valid.
	block.Header.HashMerkleRoot = &chainhash.Hash{}

	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			break
		}

		block.Header.Nonce++
	}

	catchupCtx := &CatchupContext{
		blockUpTo:          block,
		baseURL:            "http://peer",
		peerID:             "peer-under-test",
		startTime:          time.Now(),
		useQuickValidation: false, // force the normal (full) validation path
	}

	validateBlocksChan := make(chan blockForValidation, 1)
	validateBlocksChan <- blockForValidation{block: block}
	close(validateBlocksChan)

	var size atomic.Int64
	size.Store(1)

	err := suite.Server.validateBlocksOnChannel(validateBlocksChan, context.Background(), catchupCtx, &size, nil)
	require.Error(t, err, "the failure must reach the caller, which only happens when the body is validated BEFORE it is added")
	require.False(t, errors.Is(err, errors.ErrBlockHeaderContext),
		"the optimistic branch's synchronous header-context check must not run on the catch-up path")

	suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
