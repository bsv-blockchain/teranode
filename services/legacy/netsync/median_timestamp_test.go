package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// makeChain builds a slice of headers ordered newest-first (matching
// blockchainClient.GetBlockHeaders' "ORDER BY height DESC" return order)
// where each header's HashPrevBlock points at the next header's Hash().
// The newest header carries a synthetic version 1 so we have a deterministic
// Hash() — exact hash values do not matter for the tests; what matters is
// that consecutive headers are linked.
func makeChain(t *testing.T, n int, timestamps []uint32) []*model.BlockHeader {
	t.Helper()
	require.Equal(t, n, len(timestamps), "timestamps length must match chain depth")

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
// blockchainClient.GetBlockHeaders returns an empty slice (no error) when the
// start hash is unknown to the store; if the helper silently returned 0 the
// caller would pass Options.CandidateParentMedianTime=0 down to the
// validator, which now rejects post-CSV consensus requests with a
// ProcessingError when that field is missing. Erroring at the source gives a
// precise diagnostic instead of waiting for the validator's downstream
// rejection.
func TestCandidateParentMedianTimeFromHeaders_ErrorsOnEmpty(t *testing.T) {
	parent := &chainhash.Hash{1}

	_, err := candidateParentMedianTimeFromHeaders(parent, nil)
	require.Error(t, err, "nil headers must error")

	_, err = candidateParentMedianTimeFromHeaders(parent, []*model.BlockHeader{})
	require.Error(t, err, "empty headers slice must error")
}

// TestCandidateParentMedianTimeFromHeaders_ErrorsOnWrongHead pins the
// concurrency-safety check: blockchainClient.GetBlockHeaders' main-chain
// fast path probes on_main_chain in a separate SQL statement from the
// SELECT that returns rows, so a reorg between the two can return main-
// chain headers at the requested height range that DON'T match the
// requested parent. The helper must reject that mismatch.
func TestCandidateParentMedianTimeFromHeaders_ErrorsOnWrongHead(t *testing.T) {
	headers := makeChain(t, 11, []uint32{50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150})

	wrongParent := &chainhash.Hash{0xff}
	_, err := candidateParentMedianTimeFromHeaders(wrongParent, headers)
	require.Error(t, err, "first header not matching parent hash must error (reorg-race signal)")
}

// TestCandidateParentMedianTimeFromHeaders_ErrorsOnBrokenLink pins the
// per-link verification: if any header in the middle of the chain does
// not point at the next via HashPrevBlock, the chain is broken and the
// helper must reject it. This catches the case where a fast-path
// height-range query returned a mix of canonical and stale rows.
func TestCandidateParentMedianTimeFromHeaders_ErrorsOnBrokenLink(t *testing.T) {
	headers := makeChain(t, 11, []uint32{50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150})

	// Sabotage the link at depth 5: header[4].HashPrevBlock now points to
	// nothing useful, so the chain is broken between depths 4 and 5.
	headers[4].HashPrevBlock = &chainhash.Hash{0xab}

	parent := headers[0].Hash()
	_, err := candidateParentMedianTimeFromHeaders(parent, headers)
	require.Error(t, err, "broken parent-chain link must error (reorg-race signal)")
}

// TestCandidateParentMedianTimeFromHeaders_SortsThenTakesMiddle pins the
// median definition: bitcoin-sv's CBlockIndex::GetMedianTimePast sorts the
// gathered timestamps and returns pbegin[(pend - pbegin) / 2]. For 11
// elements that is index 5 (zero-indexed) — the 6th sorted timestamp.
// Header order on input must not affect the result.
func TestCandidateParentMedianTimeFromHeaders_SortsThenTakesMiddle(t *testing.T) {
	headers := makeChain(t, 11, []uint32{100, 90, 110, 80, 120, 70, 130, 60, 140, 50, 150})
	// Sorted: 50,60,70,80,90,100,110,120,130,140,150 → index 5 = 100.

	parent := headers[0].Hash()
	got, err := candidateParentMedianTimeFromHeaders(parent, headers)
	require.NoError(t, err)
	require.Equal(t, uint32(100), got)
}

// TestCandidateParentMedianTimeFromHeaders_SingleHeader pins the edge case
// of a single header: parent-chain trivially verified, median of one
// element is that element itself.
func TestCandidateParentMedianTimeFromHeaders_SingleHeader(t *testing.T) {
	headers := makeChain(t, 1, []uint32{1700000000})
	parent := headers[0].Hash()

	got, err := candidateParentMedianTimeFromHeaders(parent, headers)
	require.NoError(t, err)
	require.Equal(t, uint32(1700000000), got)
}

// TestCandidateParentMedianTimeFromHeaders_NilParentErrors pins that a nil
// parent hash is rejected rather than passing the per-link verification
// vacuously.
func TestCandidateParentMedianTimeFromHeaders_NilParentErrors(t *testing.T) {
	headers := makeChain(t, 11, []uint32{50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150})

	_, err := candidateParentMedianTimeFromHeaders(nil, headers)
	require.Error(t, err, "nil parent hash must error")
}

// TestCandidateParentMedianTimeFromHeaders_NilElementErrors pins that a nil
// element anywhere in the headers slice is rejected rather than panicking on
// Hash() / HashPrevBlock dereference. Production paths do not emit nil
// elements, but the helper is meant to hard-fail on bad header data rather
// than crash the validator goroutine that fed it.
func TestCandidateParentMedianTimeFromHeaders_NilElementErrors(t *testing.T) {
	headers := makeChain(t, 11, []uint32{50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150})
	parent := headers[0].Hash()

	// nil at head
	hAtHead := append([]*model.BlockHeader{nil}, headers[1:]...)
	_, err := candidateParentMedianTimeFromHeaders(parent, hAtHead)
	require.Error(t, err, "nil header at depth 0 must error")

	// nil at depth 5 (middle)
	hAtMid := make([]*model.BlockHeader, len(headers))
	copy(hAtMid, headers)
	hAtMid[5] = nil
	_, err = candidateParentMedianTimeFromHeaders(parent, hAtMid)
	require.Error(t, err, "nil header in middle must error")
}

// TestCandidateFinalityTimesForBlock_EraSelection pins the era-routing branch
// inside candidateFinalityTimesForBlock — the dispatch the validator relies
// on to decide whether the post-CSV CandidateParentMedianTime or the pre-CSV
// CandidateBlockTime is on the outgoing request. Without this test the
// dispatch was untested (PreValidateTransactions unit tests pass literal
// 0, 0 for both fields and never exercise the selection itself).
func TestCandidateFinalityTimesForBlock_EraSelection(t *testing.T) {
	const csvHeight uint32 = 1000

	const headerTime int64 = 1700000000

	// Helper: build a block at the given height with the given header
	// timestamp. PrevBlock is left at the zero hash for the pre-CSV path
	// (which doesn't read it). The post-CSV path needs a real parentHash so
	// the blockchain mock can answer GetBlockHeaders against it.
	makeBlock := func(height int32, timestamp int64, prevHash chainhash.Hash) *bsvutil.Block {
		t.Helper()
		msg := &wire.MsgBlock{
			Header: wire.BlockHeader{
				Version:   1,
				PrevBlock: prevHash,
				Timestamp: time.Unix(timestamp, 0),
			},
		}
		b := bsvutil.NewBlock(msg)
		b.SetHeight(height)
		return b
	}

	t.Run("pre-CSV returns block header timestamp, zero parent MTP", func(t *testing.T) {
		bcMock := &blockchain.Mock{}
		sm := &SyncManager{
			logger:           ulogger.TestLogger{},
			blockchainClient: bcMock,
			chainParams:      &chaincfg.Params{CSVHeight: csvHeight},
		}

		block := makeBlock(500, headerTime, chainhash.Hash{})

		got1, got2, err := sm.candidateFinalityTimesForBlock(context.Background(), block, 500)
		require.NoError(t, err)
		require.Equal(t, uint32(headerTime), got1, "pre-CSV must return the block header timestamp as candidateBlockTime")
		require.Equal(t, uint32(0), got2, "pre-CSV must leave candidateParentMedianTime at zero")

		bcMock.AssertNotCalled(t, "GetBlockHeaders")
		bcMock.AssertNotCalled(t, "GetBlockHeader")
	})

	t.Run("post-CSV returns zero block-time, populated parent MTP", func(t *testing.T) {
		// Build the parent chain: 11 headers walking back from parentHash.
		parentChain := makeChain(t, 11, []uint32{150, 140, 130, 120, 110, 100, 90, 80, 70, 60, 50})
		parentHash := parentChain[0].Hash()
		// Median of [50..150 step 10] is 100.

		bcMock := &blockchain.Mock{}
		bcMock.Mock.On("GetBlockHeaders", mock.Anything, parentHash, uint64(blockchain.MedianTimeBlocks)).
			Return(parentChain, []*model.BlockHeaderMeta{}, nil)

		sm := &SyncManager{
			logger:           ulogger.TestLogger{},
			blockchainClient: bcMock,
			chainParams:      &chaincfg.Params{CSVHeight: csvHeight},
		}

		block := makeBlock(1500, headerTime, *parentHash)

		got1, got2, err := sm.candidateFinalityTimesForBlock(context.Background(), block, 1500)
		require.NoError(t, err)
		require.Equal(t, uint32(0), got1, "post-CSV must leave candidateBlockTime at zero so it stays absent on the wire")
		require.Equal(t, uint32(100), got2, "post-CSV must return the candidate-parent MTP")
	})

	t.Run("boundary blockHeight == CSVHeight takes the post-CSV branch", func(t *testing.T) {
		parentChain := makeChain(t, 11, []uint32{150, 140, 130, 120, 110, 100, 90, 80, 70, 60, 50})
		parentHash := parentChain[0].Hash()

		bcMock := &blockchain.Mock{}
		bcMock.Mock.On("GetBlockHeaders", mock.Anything, parentHash, uint64(blockchain.MedianTimeBlocks)).
			Return(parentChain, []*model.BlockHeaderMeta{}, nil)

		sm := &SyncManager{
			logger:           ulogger.TestLogger{},
			blockchainClient: bcMock,
			chainParams:      &chaincfg.Params{CSVHeight: csvHeight},
		}

		block := makeBlock(int32(csvHeight), headerTime, *parentHash)

		got1, got2, err := sm.candidateFinalityTimesForBlock(context.Background(), block, csvHeight)
		require.NoError(t, err)
		require.Equal(t, uint32(0), got1, "at-CSV boundary follows the post-CSV branch (>= CSVHeight)")
		require.Equal(t, uint32(100), got2)
	})
}

// TestCandidateParentMedianTimeForBlock_WalkFallbackRecoversFromBadBatchedFetch
// is the focused integration test for the reorg-race recovery path.
//
// Setup: GetBlockHeaders(parentHash, 11) returns a wrong-head set (as it
// could under the SQL fast-path probe/SELECT race + (parentHash, 11) cache
// poisoning), but GetBlockHeader(hash) — keyed by hash, race-immune —
// returns the correct chain. The helper must detect the wrong head via the
// re-anchor check and fall through to walkParentChain, which produces the
// correct MTP from the per-hash walk.
//
// This pins the core safety property of the two-step fallback: bad data
// from the batched path must NOT silently flow into the validator's
// finality check; recovery through the hash-keyed walk must succeed.
func TestCandidateParentMedianTimeForBlock_WalkFallbackRecoversFromBadBatchedFetch(t *testing.T) {
	// The correct chain we want recovered: timestamps 50..150 in ascending
	// order (newest-first when ordered by depth, matching the helper's
	// expected input order). Median of these 11 timestamps is 100.
	correctTimestamps := []uint32{150, 140, 130, 120, 110, 100, 90, 80, 70, 60, 50}
	correctChain := makeChain(t, 11, correctTimestamps)
	parentHash := correctChain[0].Hash()

	// The wrong chain that the racy batched call returns. Same length but
	// the head hash points at something else, so re-anchor will reject.
	wrongTimestamps := []uint32{999, 998, 997, 996, 995, 994, 993, 992, 991, 990, 989}
	wrongChain := makeChain(t, 11, wrongTimestamps)
	require.False(t, wrongChain[0].Hash().IsEqual(parentHash), "test setup: wrong chain must have a different head from the correct parent")

	bcMock := &blockchain.Mock{}
	bcMock.Mock.On("GetBlockHeaders", mock.Anything, parentHash, uint64(blockchain.MedianTimeBlocks)).
		Return(wrongChain, []*model.BlockHeaderMeta{}, nil)

	// Per-hop lookups must return the correct chain. The walk starts at
	// parentHash, then follows HashPrevBlock through each header.
	for _, h := range correctChain {
		hh := h
		bcMock.Mock.On("GetBlockHeader", mock.Anything, hh.Hash()).
			Return(hh, &model.BlockHeaderMeta{}, nil)
	}

	sm := &SyncManager{
		logger:           ulogger.TestLogger{},
		blockchainClient: bcMock,
	}

	got, err := sm.candidateParentMedianTimeForBlock(context.Background(), parentHash)
	require.NoError(t, err, "fallback walk must recover when the batched path returns wrong-head headers")
	require.Equal(t, uint32(100), got, "MTP must come from the correct chain, not from the racy batched fetch")
}
