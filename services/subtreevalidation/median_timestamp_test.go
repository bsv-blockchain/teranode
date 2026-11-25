package subtreevalidation

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"slices"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
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

// forkTestHeight is a candidate height inside mainnet's BIP68 range (CSV 419,328 up to Genesis
// 620,538).
const forkTestHeight = uint32(500_000)

// forkTestServer returns a Server on mainnet params over bcMock.
func forkTestServer(t *testing.T, bcMock *blockchain.Mock) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams, _ = chaincfg.GetChainParams("mainnet")

	return &Server{logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: bcMock}
}

// forkTestChain returns n headers ending at the candidate's parent at forkTestHeight-1, newest
// first, with timestamps rising by 1000 per block, and their metas, whose IDs equal their index.
func forkTestChain(t *testing.T, n int) ([]*model.BlockHeader, []*model.BlockHeaderMeta) {
	t.Helper()

	timestamps := make([]uint32, n)
	metas := make([]*model.BlockHeaderMeta, n)

	for i := range timestamps {
		timestamps[i] = uint32(1000 * (n - i))
		metas[i] = &model.BlockHeaderMeta{ID: uint32(i), Height: forkTestHeight - 1 - uint32(i)}
	}

	return makeChain(t, n, timestamps), metas
}

// onCurrentChainFrom makes CheckBlockIsInCurrentChain report the meta IDs from forkIdx on as on
// the current chain, and those below it as off it.
func onCurrentChainFrom(bcMock *blockchain.Mock, n, forkIdx int) {
	for i := 0; i < n; i++ {
		bcMock.Mock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{uint32(i)}).Return(i >= forkIdx, nil).Maybe()
	}
}

// forkChainStore is a sqlitememory blockchain store holding a main chain of mainLen blocks on
// top of regtest genesis and a side branch of branchLen blocks leaving it after the block at
// forkHeight, behind a LocalClient. timestamp gives each stored block's time: the branch's are
// two hours ahead of the main chain's at the same height, so the two give different medians.
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

// branchCoinTimes is the oracle: for each branch block, the median of the timestamps of the 11
// blocks below it along the branch (main chain up to the fork point).
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

// forkTestServerOn returns a Server whose chain params enforce BIP68 at every height (CSV 0,
// Genesis at the top), over client, with blockchain_maxBlockHeadersPerRequest set to limit.
func forkTestServerOn(t *testing.T, client blockchain.ClientI, limit int) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.CSVHeight = 0
	params.GenesisActivationHeight = math.MaxUint32
	tSettings.ChainCfgParams = &params
	tSettings.BlockChain.MaxBlockHeadersPerRequest = limit

	return &Server{logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: client}
}

// headerRequestCounter records how many GetBlockHeaders calls were made and the largest count
// any of them asked for.
type headerRequestCounter struct {
	blockchain.ClientI

	calls   int
	largest uint64
}

func (c *headerRequestCounter) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	c.calls++
	c.largest = max(c.largest, n)

	return c.ClientI.GetBlockHeaders(ctx, hash, n)
}

// TestFetchCandidateForkMTPs_ParentOnCurrentChain checks that a candidate extending the main
// chain gets no branch coin times.
func TestFetchCandidateForkMTPs_ParentOnCurrentChain(t *testing.T) {
	s := newForkChainStore(t, 40, 20, 5)

	start, mtps, err := forkTestServerOn(t, s.client, 0).fetchCandidateForkMTPs(t.Context(), s.main[39].Hash(), 41)
	require.NoError(t, err)
	require.Zero(t, start)
	require.Empty(t, mtps)
}

// TestFetchCandidateForkMTPs_BranchCoinTimes checks the coin times of a five-block side branch
// leaving the main chain after height 20, for a candidate on the branch tip. Each value must be
// the median along the branch, which differs from the main chain's median at the same height
// for every branch block above the first. With a header limit of 3 the walk reads the run in
// pages and must give the same answer.
func TestFetchCandidateForkMTPs_BranchCoinTimes(t *testing.T) {
	s := newForkChainStore(t, 40, 20, 5)
	want := s.branchCoinTimes(5)

	for _, limit := range []int{0, 3} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			client := &headerRequestCounter{ClientI: s.client}

			start, mtps, err := forkTestServerOn(t, client, limit).fetchCandidateForkMTPs(t.Context(), s.branch[4].Hash(), 26)
			require.NoError(t, err)
			require.Equal(t, uint32(21), start)
			require.Equal(t, want, mtps)

			if limit > 0 {
				require.LessOrEqual(t, client.largest, uint64(limit), "no header request may exceed blockchain_maxBlockHeadersPerRequest")
				require.Greater(t, client.calls, 1, "the run must be read in more than one page")
			}
		})
	}
}

// TestFetchCandidateForkMTPs_DeepBranch checks a 70-block branch, deeper than one page of
// candidateForkWalkDepth headers, so the walk must page past it to find the fork point.
func TestFetchCandidateForkMTPs_DeepBranch(t *testing.T) {
	s := newForkChainStore(t, 100, 20, 70)

	start, mtps, err := forkTestServerOn(t, s.client, 0).fetchCandidateForkMTPs(t.Context(), s.branch[69].Hash(), 91)
	require.NoError(t, err)
	require.Equal(t, uint32(21), start)
	require.Equal(t, s.branchCoinTimes(70), mtps)
}

// TestFetchCandidateForkMTPs_RejectsAParentAtTheWrongHeight checks that a candidate height that
// does not sit directly above its parent fails rather than misplacing the coin times.
func TestFetchCandidateForkMTPs_RejectsAParentAtTheWrongHeight(t *testing.T) {
	s := newForkChainStore(t, 40, 20, 5)

	_, _, err := forkTestServerOn(t, s.client, 0).fetchCandidateForkMTPs(t.Context(), s.branch[4].Hash(), 27)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not at height 26")
}

// TestFetchCandidateForkMTPs_OutsideBIP68DoesNothing checks that no lookups run at a height where
// BIP68 is not enforced.
func TestFetchCandidateForkMTPs_OutsideBIP68DoesNothing(t *testing.T) {
	bcMock := &blockchain.Mock{}

	for _, h := range []uint32{419_327, 620_538} {
		start, mtps, err := forkTestServer(t, bcMock).fetchCandidateForkMTPs(context.Background(), &chainhash.Hash{1}, h)
		require.NoError(t, err)
		require.Zero(t, start)
		require.Empty(t, mtps)
	}

	bcMock.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
}

// TestFetchCandidateForkMTPs_RejectsAMislinkedRun checks that a run whose metas do not sit at the
// heights below the parent fails rather than yielding coin times for the wrong blocks.
func TestFetchCandidateForkMTPs_RejectsAMislinkedRun(t *testing.T) {
	headers, metas := forkTestChain(t, 20)
	metas[4] = &model.BlockHeaderMeta{ID: 4, Height: metas[4].Height + 1}

	bcMock := &blockchain.Mock{}
	bcMock.Mock.On("GetBlockHeader", mock.Anything, headers[0].Hash()).Return(headers[0], metas[0], nil)
	bcMock.Mock.On("GetBlockHeaders", mock.Anything, headers[0].Hash(), uint64(candidateForkWalkDepth)).Return(headers, metas, nil)
	onCurrentChainFrom(bcMock, 20, 3)

	_, _, err := forkTestServer(t, bcMock).fetchCandidateForkMTPs(context.Background(), headers[0].Hash(), forkTestHeight)
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong height")
}
