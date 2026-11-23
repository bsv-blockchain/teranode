package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/stretchr/testify/require"
)

// newMedianTimePastHarness is the coinbase-finality harness on a chain that enforces the
// median-time-past rule. Regtest sets GenerateSupported, which makes the rule advisory, so it is
// cleared here. The memo is left cold: no floor is known for the parent, the state in which the
// memo-only nTime guard has nothing to enforce.
func newMedianTimePastHarness(t *testing.T) *coinbaseFinalityHarness {
	t.Helper()

	h := newCoinbaseFinalityHarness(t)
	h.server.blockAssembler.settings.ChainCfgParams.GenerateSupported = false

	return h
}

// submitAt submits the job's own coinbase with the job's candidate time moved to timestamp, as a
// candidate served on the wall-clock path carries whatever time the clock gave it. parentMTP is
// derived from the candidate time, so read it before calling this.
func (h *coinbaseFinalityHarness) submitAt(t *testing.T, timestamp uint32) (*blockassembly_api.OKResponse, error) {
	t.Helper()

	h.candidate.Time = timestamp

	return h.submit(t, coinbaseEncoding(t, postGenesisCandidateHeight, 5_000_000))
}

// TestSubmitMiningSolution_MedianTimePast pins the median-time-past rule on the submit path.
// block.Valid is called there without the parent chain, so it skips the rule, and the memo-only
// nTime guard enforces nothing when no floor is memoized for the parent. Without this check a
// block stamped at or below the parent's median time past is added locally and rejected by every
// peer.
func TestSubmitMiningSolution_MedianTimePast(t *testing.T) {
	t.Run("a timestamp at the parent's median time past is refused", func(t *testing.T) {
		h := newMedianTimePastHarness(t)
		medianTimePast := h.parentMTP()

		resp, err := h.submitAt(t, medianTimePast)
		require.Error(t, err)
		require.Nil(t, resp)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a sub-median timestamp is a verdict on the miner's data: %v", err)
		require.Contains(t, err.Error(), "is not after the parent's median time past")
		require.NotZero(t, h.chain.lookups.Load(), "a cold memo must be read from the blockchain service")
		h.requireMined(t, false)

		// Nothing local is at fault, so the job stays for a corrected submission and assembly is
		// not reset.
		require.NotNil(t, h.server.jobStore.Get(h.id), "the job must survive a refused timestamp")
		h.requireNoReset(t)
	})

	t.Run("a timestamp one second after the parent's median time past is mined", func(t *testing.T) {
		h := newMedianTimePastHarness(t)
		medianTimePast := h.parentMTP()

		resp, err := h.submitAt(t, medianTimePast+1)
		require.NoError(t, err)
		require.True(t, resp.Ok)
		require.Equal(t, medianTimePast+1, h.requireMined(t, true).Header.Timestamp)
	})

	t.Run("a warm memo needs no lookup", func(t *testing.T) {
		h := newMedianTimePastHarness(t)
		h.warmMTP(t)

		resp, err := h.submitAt(t, h.parentMTP()+1)
		require.NoError(t, err)
		require.True(t, resp.Ok)
		h.requireMined(t, true)
		require.Zero(t, h.chain.lookups.Load(), "the memoized floor already gives the median time past")
	})

	t.Run("an unknown median time past refuses the solution without a reset", func(t *testing.T) {
		// A parent the store does not hold, so the lookup fails as it would against an unreachable
		// or out-of-step blockchain service. The rule cannot be checked, so the block is not added.
		h := newMedianTimePastHarness(t)
		h.candidate.PreviousHash = (&chainhash.Hash{9}).CloneBytes()

		resp, err := h.submitAt(t, h.parentMTP()+1)
		require.Error(t, err)
		require.Nil(t, resp)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a failed lookup is this node's problem, not a verdict on the block: %v", err)
		require.Contains(t, err.Error(), "cannot check the block timestamp against the parent's median time past")
		h.requireMined(t, false)
		require.NotNil(t, h.server.jobStore.Get(h.id), "the job must survive a failed lookup")
		h.requireNoReset(t)
	})

	t.Run("invalid proof of work does not fetch the median time past", func(t *testing.T) {
		h := newMedianTimePastHarness(t)
		h.candidate.Time = h.parentMTP()

		coinbase := coinbaseEncoding(t, postGenesisCandidateHeight, 5_000_000)
		header := &model.BlockHeader{
			Version: h.candidate.Version, HashPrevBlock: h.tip.Hash(),
			HashMerkleRoot: coinbase.TxIDChainHash(), Timestamp: h.candidate.Time, Bits: h.nBits,
		}

		for {
			met, _, _ := header.HasMetTargetDifficulty()
			if !met {
				break
			}

			header.Nonce++
		}

		resp, err := h.server.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
			SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
				Id: h.idBytes, CoinbaseTx: coinbase.Bytes(), Nonce: header.Nonce,
			},
		})
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "does not meet the target")
		require.Zero(t, h.chain.lookups.Load(), "an invalid nonce must not trigger blockchain reads")
		h.requireMined(t, false)
	})

	t.Run("a chain with GenerateSupported treats the rule as advisory", func(t *testing.T) {
		// Regtest, as newCoinbaseFinalityHarness builds it. block.Valid only warns there, and
		// candidateTime serves unfloored wall-clock candidates, so the submit path must not refuse.
		h := newCoinbaseFinalityHarness(t)

		resp, err := h.submitAt(t, h.parentMTP())
		require.NoError(t, err)
		require.True(t, resp.Ok)
		h.requireMined(t, true)
		require.Zero(t, h.chain.lookups.Load(), "an advisory rule needs no lookup")
	})
}
