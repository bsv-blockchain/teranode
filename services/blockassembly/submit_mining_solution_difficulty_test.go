package blockassembly

import (
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/require"
)

// TestSubmitMiningSolution_RejectsNTimeThatNeedsOtherDifficulty pins that a solution whose nTime
// differs from the candidate's is checked against the difficulty required at that time. On a chain
// with the minimum-difficulty rule, the candidate held by blockassembly_emergencyDifficultyDelay
// sits on parent time plus 20 minutes with the usual nBits. One second later the rule requires the
// minimum nBits, so a block rolled forward by one second is rejected by every peer. block.Valid
// has no chain and cannot see that, so without this check the node stores and announces the block.
func TestSubmitMiningSolution_RejectsNTimeThatNeedsOtherDifficulty(t *testing.T) {
	common := testutil.NewCommonTestSetup(t)

	tSettings := *common.Settings
	tSettings.ChainCfgParams = emergencyTestParams()

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, &tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, &tSettings, store, nil, nil)
	require.NoError(t, err)

	tipTime := uint32(time.Now().Add(-21 * time.Minute).Unix()) //nolint:gosec
	tip := buildSpacedChain(t, t.Context(), client, tipTime)

	_, tipMeta, err := client.GetBlockHeader(t.Context(), tip.Hash())
	require.NoError(t, err)

	heldTime := tipTime + 1200

	usual, err := client.GetNextWorkRequired(t.Context(), tip.Hash(), int64(heldTime))
	require.NoError(t, err)

	minimum, err := client.GetNextWorkRequired(t.Context(), tip.Hash(), int64(heldTime)+1)
	require.NoError(t, err)
	require.NotEqual(t, *usual, *minimum, "the chain must need different difficulty on each side of the boundary or the test proves nothing")

	s := New(common.Logger, &tSettings, nil, nil, nil, client)
	s.blockAssembler = &BlockAssembler{settings: &tSettings, logger: common.Logger, resetCh: make(chan resetRequest, 4)}

	t.Cleanup(func() { _ = s.Stop(t.Context()) })

	s.blockAssembler.bestBlock.Store(&BestBlockInfo{
		Header: &model.BlockHeader{HashPrevBlock: &chainhash.Hash{7}},
		Height: tipMeta.Height,
	})

	height := tipMeta.Height + 1

	candidate := &model.MiningCandidate{
		Id:            chainhash.HashB(tip.Hash()[:]),
		PreviousHash:  tip.Hash().CloneBytes(),
		CoinbaseValue: util.GetBlockSubsidyForHeight(height, tSettings.ChainCfgParams),
		Version:       miningCandidateVersion,
		NBits:         usual.CloneBytes(),
		Time:          heldTime,
		Height:        height,
		MerkleProof:   [][]byte{},
	}

	id, err := chainhash.NewHash(candidate.Id)
	require.NoError(t, err)

	s.storeJobWithCoinbase(id, candidate, nil)

	submit := func(nTime *uint32) error {
		_, submitErr := s.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
			SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
				Id:    candidate.Id,
				Nonce: 0,
				Time:  nTime,
			},
		})

		return submitErr
	}

	const rejected = "required at submitted nTime"

	forward := heldTime + 1
	require.ErrorContains(t, submit(&forward), rejected, "one second past the boundary needs the minimum difficulty")

	farForward := heldTime + 600
	require.ErrorContains(t, submit(&farForward), rejected)

	back := heldTime - 5
	require.NotContains(t, errorText(submit(&back)), rejected, "a roll back inside 20 minutes keeps the usual difficulty and is not refused here")

	same := heldTime
	require.NotContains(t, errorText(submit(&same)), rejected, "the candidate's own time needs no check")
	require.NotContains(t, errorText(submit(nil)), rejected, "no time in the solution means the candidate's time")

	require.NotNil(t, s.jobStore.Get(*id), "a refused solution must keep the job, as the median-time-past refusal does")
}

func errorText(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}
