package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// makeChain builds a slice of headers ordered newest-first (matching
// blockchainClient.GetBlockHeaders' "ORDER BY height DESC" return order)
// where each header's HashPrevBlock points at the next header's Hash().
func makeChain(t *testing.T, n int, timestamps []uint32) []*model.BlockHeader {
	t.Helper()
	require.Equal(t, n, len(timestamps))

	headers := make([]*model.BlockHeader, n)
	for i := n - 1; i >= 0; i-- {
		h := &model.BlockHeader{
			Version:        1,
			Timestamp:      timestamps[i],
			Bits:           model.NBit{},
			Nonce:          uint32(i),
			HashMerkleRoot: &chainhash.Hash{},
		}

		if i == n-1 {
			h.HashPrevBlock = &chainhash.Hash{}
		} else {
			h.HashPrevBlock = headers[i+1].Hash()
		}

		headers[i] = h
	}

	return headers
}

// TestCandidateParentMedianTimeFromHeaders_ErrorsOnEmpty pins that the
// empty-input case surfaces as a hard error rather than degrading to 0.
// See the equivalent test in services/legacy/netsync for the full
// rationale — the helper is duplicated by design (small, internal, avoids
// a new shared util package) and both copies must hold the same contract.
func TestCandidateParentMedianTimeFromHeaders_ErrorsOnEmpty(t *testing.T) {
	parent := &chainhash.Hash{1}

	_, err := candidateParentMedianTimeFromHeaders(parent, nil)
	require.Error(t, err)

	_, err = candidateParentMedianTimeFromHeaders(parent, []*model.BlockHeader{})
	require.Error(t, err)
}

// TestCandidateParentMedianTimeFromHeaders_ErrorsOnWrongHead pins the
// concurrency-safety check: if the returned headers' newest entry does not
// match the requested parent hash (e.g. a reorg between
// blockchainClient.GetBlockHeaders' probe and the returning SELECT), the
// helper must reject the result.
func TestCandidateParentMedianTimeFromHeaders_ErrorsOnWrongHead(t *testing.T) {
	headers := makeChain(t, 11, []uint32{50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150})

	wrongParent := &chainhash.Hash{0xff}
	_, err := candidateParentMedianTimeFromHeaders(wrongParent, headers)
	require.Error(t, err)
}

// TestCandidateParentMedianTimeFromHeaders_ErrorsOnBrokenLink pins the
// per-link verification: any broken HashPrevBlock → Hash() link rejects.
func TestCandidateParentMedianTimeFromHeaders_ErrorsOnBrokenLink(t *testing.T) {
	headers := makeChain(t, 11, []uint32{50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150})
	headers[4].HashPrevBlock = &chainhash.Hash{0xab}

	parent := headers[0].Hash()
	_, err := candidateParentMedianTimeFromHeaders(parent, headers)
	require.Error(t, err)
}

// TestCandidateParentMedianTimeFromHeaders_SortsThenTakesMiddle pins the
// median definition: header order on input does not affect the result.
func TestCandidateParentMedianTimeFromHeaders_SortsThenTakesMiddle(t *testing.T) {
	headers := makeChain(t, 11, []uint32{100, 90, 110, 80, 120, 70, 130, 60, 140, 50, 150})
	// Sorted: 50,60,70,80,90,100,110,120,130,140,150 → index 5 = 100.

	parent := headers[0].Hash()
	got, err := candidateParentMedianTimeFromHeaders(parent, headers)
	require.NoError(t, err)
	require.Equal(t, uint32(100), got)
}

// TestCandidateParentMedianTimeFromHeaders_NilElementErrors pins that a nil
// element anywhere in the headers slice is rejected rather than panicking
// on the Hash() / HashPrevBlock dereference.
func TestCandidateParentMedianTimeFromHeaders_NilElementErrors(t *testing.T) {
	headers := makeChain(t, 11, []uint32{50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150})
	parent := headers[0].Hash()

	hAtHead := append([]*model.BlockHeader{nil}, headers[1:]...)
	_, err := candidateParentMedianTimeFromHeaders(parent, hAtHead)
	require.Error(t, err)

	hAtMid := make([]*model.BlockHeader, len(headers))
	copy(hAtMid, headers)
	hAtMid[5] = nil
	_, err = candidateParentMedianTimeFromHeaders(parent, hAtMid)
	require.Error(t, err)
}

// TestFetchCandidateParentMedianTime_WalkFallbackRecoversFromBadBatchedFetch
// is the focused integration test for the reorg-race recovery path on the
// subtreevalidation side. Mirrors the legacy/netsync equivalent — the
// GetBlockHeaders cache returns a wrong-head set; the helper must fall
// through to walkParentChain via hash-keyed GetBlockHeader and return the
// correct MTP.
func TestFetchCandidateParentMedianTime_WalkFallbackRecoversFromBadBatchedFetch(t *testing.T) {
	correctTimestamps := []uint32{150, 140, 130, 120, 110, 100, 90, 80, 70, 60, 50}
	correctChain := makeChain(t, 11, correctTimestamps)
	parentHash := correctChain[0].Hash()

	wrongTimestamps := []uint32{999, 998, 997, 996, 995, 994, 993, 992, 991, 990, 989}
	wrongChain := makeChain(t, 11, wrongTimestamps)
	require.False(t, wrongChain[0].Hash().IsEqual(parentHash))

	bcMock := &blockchain.Mock{}
	bcMock.Mock.On("GetBlockHeaders", mock.Anything, parentHash, uint64(blockchain.MedianTimeBlocks)).
		Return(wrongChain, []*model.BlockHeaderMeta{}, nil)
	for _, h := range correctChain {
		hh := h
		bcMock.Mock.On("GetBlockHeader", mock.Anything, hh.Hash()).
			Return(hh, &model.BlockHeaderMeta{}, nil)
	}

	srv := &Server{
		logger:           ulogger.TestLogger{},
		blockchainClient: bcMock,
	}

	got, err := srv.fetchCandidateParentMedianTime(context.Background(), parentHash)
	require.NoError(t, err, "fallback walk must recover when batched path returns wrong-head headers")
	require.Equal(t, uint32(100), got)
}

// TestCandidateParentMedianTimeFromHeaders_ErrorsOnShortRunNotAtGenesis pins the
// run-length half of the window contract. Truncating the run at its OLDEST end
// leaves it anchored at the parent and correctly linked, so neither of those
// guards can see it — but the median is then taken over a narrower window and
// stops being the consensus one. svnode cannot express this state at all: it
// walks pprev pointers, so a window is shorter than 11 only when the chain
// itself ends at genesis. Before the length check the truncated run yielded 140
// instead of 100.
func TestCandidateParentMedianTimeFromHeaders_ErrorsOnShortRunNotAtGenesis(t *testing.T) {
	full := makeChain(t, 11, []uint32{150, 140, 130, 120, 110, 100, 90, 80, 70, 60, 50})
	parent := full[0].Hash()

	// Sanity: the full run is the consensus window and yields 100.
	got, err := candidateParentMedianTimeFromHeaders(parent, full)
	require.NoError(t, err)
	require.Equal(t, uint32(100), got)

	// Truncated at the oldest end: still anchored, still linked, still short.
	short := full[:4]
	require.NotNil(t, short[len(short)-1].HashPrevBlock)
	require.False(t, short[len(short)-1].HashPrevBlock.IsEqual(&chainhash.Hash{}), "fixture's oldest header must not look like genesis")

	_, err = candidateParentMedianTimeFromHeaders(parent, short)
	require.Error(t, err, "a short run that does not reach genesis must be refused, not measured")
}

// TestCandidateParentMedianTimeFromHeaders_AcceptsShortRunAtGenesis is the other
// side of the rule: a chain that genuinely ends at genesis is shorter than 11
// legitimately, exactly as it is in svnode, and must still produce a median.
func TestCandidateParentMedianTimeFromHeaders_AcceptsShortRunAtGenesis(t *testing.T) {
	headers := makeChain(t, 3, []uint32{130, 120, 110})
	parent := headers[0].Hash()

	got, err := candidateParentMedianTimeFromHeaders(parent, headers)
	require.NoError(t, err)
	require.Equal(t, uint32(120), got)
}

// TestCandidateParentMedianTimeFromHeaders_ErrorsOnOverLongRun mirrors the netsync copy: see the
// comment there. A run longer than the window is as wrong as a short one and is invisible to the
// anchor and linkage checks, and block assembly's verifyParentChainRun already refuses it.
func TestCandidateParentMedianTimeFromHeaders_ErrorsOnOverLongRun(t *testing.T) {
	n := int(blockchain.MedianTimeBlocks) + 1

	timestamps := make([]uint32, n)
	for i := range timestamps {
		timestamps[i] = uint32(1_700_000_000 + i)
	}

	headers := makeChain(t, n, timestamps)

	_, err := candidateParentMedianTimeFromHeaders(headers[0].Hash(), headers)
	require.Error(t, err, "a run longer than the median window must be refused")
	require.Contains(t, err.Error(), "more than")
}
