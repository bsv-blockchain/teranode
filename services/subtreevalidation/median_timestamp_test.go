package subtreevalidation

import (
	"context"
	"math"
	"net/url"
	"slices"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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

// forkChainStore is a sqlitememory blockchain store holding a main chain of mainLen blocks on
// regtest genesis and a side branch of branchLen blocks leaving it after forkHeight, whose block
// times run two hours ahead of the main chain's, behind a LocalClient.
type forkChainStore struct {
	client     blockchain.ClientI
	main       []*model.Block // main[i] is at height i+1
	branch     []*model.Block // branch[i] is at height forkHeight+1+i
	forkHeight uint32
}

func newForkChainStore(t *testing.T, mainLen, forkHeight, branchLen int) *forkChainStore {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchainsql.New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	main := forkTestBlocks(t, tSettings.ChainCfgParams.GenesisHash, 1, mainLen, 0)
	branch := forkTestBlocks(t, main[forkHeight-1].Hash(), uint32(forkHeight+1), branchLen, 7_200) //nolint:gosec // test sizes are small

	for _, block := range append(append([]*model.Block{}, main...), branch...) {
		_, _, err = store.StoreBlock(t.Context(), block, "")
		require.NoError(t, err)
	}

	best, _, err := store.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.Equal(t, main[mainLen-1].Hash().String(), best.Hash().String(), "the main chain must stay the best chain")

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	return &forkChainStore{client: client, main: main, branch: branch, forkHeight: uint32(forkHeight)} //nolint:gosec // test sizes are small
}

// forkTestBlocks builds n blocks on top of parentHash, the first at height firstHeight, with
// block times 600 seconds apart plus skew.
func forkTestBlocks(t *testing.T, parentHash *chainhash.Hash, firstHeight uint32, n int, skew uint32) []*model.Block {
	t.Helper()

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*coinbase.TxIDChainHash(), 0, 0))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	blocks := make([]*model.Block, 0, n)
	prev := parentHash

	for i := 0; i < n; i++ {
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1_600_000_000 + 600*(firstHeight+uint32(i)) + skew, //nolint:gosec // test sizes are small
				Nonce:          2573394689,
				HashPrevBlock:  prev,
				HashMerkleRoot: subtree.RootHash(),
				Bits:           *nBits,
			},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		}

		blocks = append(blocks, block)
		prev = block.Hash()
	}

	return blocks
}

// branchCoinTimes is the oracle: for each of the first upTo branch blocks, the median of the
// timestamps of the 11 blocks below it along the branch.
func (s *forkChainStore) branchCoinTimes(upTo int) []uint32 {
	timeAt := func(height uint32) uint32 {
		if height > s.forkHeight {
			return s.branch[height-s.forkHeight-1].Header.Timestamp
		}

		return s.main[height-1].Header.Timestamp
	}

	mtps := make([]uint32, 0, upTo)

	for k := 0; k < upTo; k++ {
		h := s.forkHeight + 1 + uint32(k) //nolint:gosec // test sizes are small
		window := make([]uint32, 0, 11)

		for x := h - 11; x < h; x++ {
			window = append(window, timeAt(x))
		}

		slices.Sort(window)
		mtps = append(mtps, window[len(window)/2])
	}

	return mtps
}

// forkWalkSpy counts the walk's blockchain calls and the largest header page it asked for, and
// can corrupt the metas of the first page it serves.
type forkWalkSpy struct {
	blockchain.ClientI

	headerCalls, pageCalls, chainChecks int
	largestPage                         uint64
	corrupt                             func(metas []*model.BlockHeaderMeta)
}

func (s *forkWalkSpy) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	s.headerCalls++

	return s.ClientI.GetBlockHeader(ctx, hash)
}

func (s *forkWalkSpy) GetBlockHeadersByParentLinks(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	s.pageCalls++
	s.largestPage = max(s.largestPage, n)

	headers, metas, err := s.ClientI.GetBlockHeadersByParentLinks(ctx, hash, n)
	if err == nil && s.corrupt != nil && s.pageCalls == 1 {
		copied := make([]*model.BlockHeaderMeta, len(metas))
		for i, m := range metas {
			mc := *m
			copied[i] = &mc
		}

		s.corrupt(copied)
		metas = copied
	}

	return headers, metas, err
}

func (s *forkWalkSpy) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	s.chainChecks++

	return s.ClientI.CheckBlockIsInCurrentChain(ctx, blockIDs)
}

// forkTestServerOn returns a Server over client enforcing BIP68 from csv up to genesis, with
// blockchain_maxBlockHeadersPerRequest set to limit.
func forkTestServerOn(t *testing.T, client blockchain.ClientI, csv, genesis uint32, limit int) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.CSVHeight = csv
	params.GenesisActivationHeight = genesis
	tSettings.ChainCfgParams = &params
	tSettings.BlockChain.MaxBlockHeadersPerRequest = limit

	return &Server{logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: client}
}

// TestFetchCandidateForkMTPs runs the walk over a main chain of 40 blocks and a five-block side
// branch leaving it after height 20. Branch coin times must match an independent median of the
// branch's own timestamps.
func TestFetchCandidateForkMTPs(t *testing.T) {
	s := newForkChainStore(t, 40, 20, 5)

	tests := []struct {
		name        string
		parent      *chainhash.Hash
		height      uint32
		csv         uint32
		genesis     uint32
		limit       int
		corrupt     func(metas []*model.BlockHeaderMeta)
		wantStart   uint32
		wantMTPs    []uint32
		wantErr     string
		wantNoCalls bool
	}{
		{name: "parent on the main chain", parent: s.main[39].Hash(), height: 41},
		{name: "branch tip", parent: s.branch[4].Hash(), height: 26, wantStart: 21, wantMTPs: s.branchCoinTimes(5)},
		{name: "branch tip, three headers per request", parent: s.branch[4].Hash(), height: 26, limit: 3, wantStart: 21, wantMTPs: s.branchCoinTimes(5)},
		{name: "height at genesis activation", parent: s.branch[4].Hash(), height: 26, genesis: 26, wantNoCalls: true},
		{name: "height below CSV activation", parent: s.branch[4].Hash(), height: 26, csv: 27, wantNoCalls: true},
		{name: "height not above the parent", parent: s.branch[4].Hash(), height: 27, wantErr: "is not at height 26"},
		{name: "run at the wrong heights", parent: s.branch[4].Hash(), height: 26, wantErr: "wrong height",
			corrupt: func(metas []*model.BlockHeaderMeta) { metas[4].Height++ }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.genesis == 0 {
				tc.genesis = math.MaxUint32
			}

			spy := &forkWalkSpy{ClientI: s.client, corrupt: tc.corrupt}

			start, mtps, err := forkTestServerOn(t, spy, tc.csv, tc.genesis, tc.limit).fetchCandidateForkMTPs(t.Context(), tc.parent, tc.height)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantStart, start)
			require.Equal(t, tc.wantMTPs, mtps)

			if tc.wantNoCalls {
				require.Zero(t, spy.headerCalls+spy.pageCalls+spy.chainChecks)
			}

			if tc.limit > 0 {
				require.LessOrEqual(t, spy.largestPage, uint64(tc.limit), "no page may exceed blockchain_maxBlockHeadersPerRequest")
				require.Greater(t, spy.pageCalls, 1)
			}
		})
	}
}

// TestFetchCandidateForkMTPs_DeepBranch checks a 70-block branch, longer than one page, so the
// walk pages past it; finding the fork point takes a few chain checks per page, not one per block.
func TestFetchCandidateForkMTPs_DeepBranch(t *testing.T) {
	s := newForkChainStore(t, 100, 20, 70)
	spy := &forkWalkSpy{ClientI: s.client}

	start, mtps, err := forkTestServerOn(t, spy, 0, math.MaxUint32, 0).fetchCandidateForkMTPs(t.Context(), s.branch[69].Hash(), 91)
	require.NoError(t, err)
	require.Equal(t, uint32(21), start)
	require.Equal(t, s.branchCoinTimes(70), mtps)
	require.Equal(t, 2, spy.pageCalls)
	require.LessOrEqual(t, spy.chainChecks, 1+2+7)
}
