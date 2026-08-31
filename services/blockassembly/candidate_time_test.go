package blockassembly

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// buildFutureChain stores medianTimeBlocks (11) linked blocks on top of the
// current best block, stamped with strictly increasing timestamps starting at
// baseTime, and returns the new tip header. With timestamps baseTime..baseTime+10
// the median-time-past seen by a candidate on the tip is baseTime+5.
func buildFutureChain(t *testing.T, ctx context.Context, blockchainClient blockchain.ClientI, baseTime uint32) *model.BlockHeader {
	t.Helper()

	prevHeader, _, err := blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	coinbaseTx, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	nbits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	for i := uint32(0); i < 11; i++ {
		header := &model.BlockHeader{
			Version:        0x20000000,
			HashPrevBlock:  prevHeader.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      baseTime + i,
			Bits:           *nbits,
			Nonce:          i,
		}

		require.NoError(t, blockchainClient.AddBlock(ctx, &model.Block{
			Header:           header,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{},
		}, ""))

		prevHeader = header
	}

	return prevHeader
}

// linkedHeaders builds count in-memory headers linked via HashPrevBlock with
// timestamps baseTime, baseTime+1, ... and returns them newest-first — the
// order GetBlockHeaders returns them in.
func linkedHeaders(t *testing.T, count int, baseTime uint32) []*model.BlockHeader {
	t.Helper()

	// A non-zero seed so the oldest header does not look like the chain start.
	return linkedHeadersFrom(t, count, baseTime, &chainhash.Hash{1})
}

// linkedHeadersFromGenesis is linkedHeaders but the oldest header's parent is
// the all-zero hash, marking the start of the chain.
func linkedHeadersFromGenesis(t *testing.T, count int, baseTime uint32) []*model.BlockHeader {
	t.Helper()

	return linkedHeadersFrom(t, count, baseTime, &chainhash.Hash{})
}

// linkedHeadersFrom builds the run, seeding the oldest header's HashPrevBlock
// with prev.
func linkedHeadersFrom(t *testing.T, count int, baseTime uint32, prev *chainhash.Hash) []*model.BlockHeader {
	t.Helper()

	nbits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	newestFirst := make([]*model.BlockHeader, count)

	for i := 0; i < count; i++ {
		header := &model.BlockHeader{
			Version:        0x20000000,
			HashPrevBlock:  prev,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      baseTime + uint32(i),
			Bits:           *nbits,
			Nonce:          uint32(i),
		}
		newestFirst[count-1-i] = header
		prev = header.Hash()
	}

	return newestFirst
}

func TestCandidateTime(t *testing.T) {
	t.Run("tracks the wall clock when the median-time-past is in the past", func(t *testing.T) {
		server, _ := setupServer(t)
		require.NoError(t, server.blockAssembler.Start(t.Context()))

		bestHeader, _, err := server.blockchainClient.GetBestBlockHeader(t.Context())
		require.NoError(t, err)

		before := time.Now().Unix()
		got, err := server.blockAssembler.candidateTime(t.Context(), bestHeader)
		after := time.Now().Unix()

		require.NoError(t, err)
		require.GreaterOrEqual(t, got, before)
		require.LessOrEqual(t, got, after)
	})

	t.Run("floors at median-time-past+1 when the median is at or above the wall clock", func(t *testing.T) {
		server, _ := setupServer(t)
		require.NoError(t, server.blockAssembler.Start(t.Context()))

		// Stamp the last 11 blocks ~100 minutes into the future (within the
		// 2-hour future window peers accept), dragging the median above the
		// local clock — the scenario where the unfloored candidate violated
		// the median-time rule.
		baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
		tipHeader := buildFutureChain(t, t.Context(), server.blockchainClient, baseTime)

		got, err := server.blockAssembler.candidateTime(t.Context(), tipHeader)
		require.NoError(t, err)

		medianTimePast := int64(baseTime + 5)
		require.Equal(t, medianTimePast+1, got)
	})

	t.Run("the real store satisfies the batched-run contract without the walk", func(t *testing.T) {
		// Every other batched-success subtest supplies the run shape itself via
		// blockchain.Mock, and the real-store tests assert only the resulting
		// floor — which walkParentChain reproduces identically. So the three
		// assumptions verifyParentChainRun encodes about GetBlockHeaders(parent, 11)
		// — newest-first, anchor-inclusive, and at most 11 — are a live
		// cross-package contract with nothing pinning them.
		//
		// Flip ORDER BY b.height DESC to ASC in stores/blockchain/sql, or make the
		// fast path exclusive of the anchor, and every miner poll would silently
		// take MedianTimeBlocks-1 extra sequential GetBlockHeader round-trips plus
		// a warning per tip, with the rest of the suite still green. This asserts
		// the batched run is accepted as-is against the sqlitememory store: exactly
		// one warning, the floor line, and no "was unusable" fallback line.
		server, _ := setupServer(t)

		// The logger is swapped before anything starts: assigning it to a running
		// assembler races the channel-listener goroutines that read b.logger.
		// candidateTime needs only the blockchain client, so nothing is started
		// here — which also keeps unrelated background logging out of the counts.
		logger := &warnCountingLogger{}
		server.blockAssembler.logger = logger

		baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
		tipHeader := buildFutureChain(t, t.Context(), server.blockchainClient, baseTime)

		got, err := server.blockAssembler.candidateTime(t.Context(), tipHeader)
		require.NoError(t, err)
		require.Equal(t, int64(baseTime+5)+1, got)

		// Count the specific line rather than all of them: the fallback line only
		// appears when verifyParentChainRun rejected the store's run.
		require.Zero(t, logger.countMatching("was unusable"), "the real store's batched run must be accepted as-is, not routed through walkParentChain")
		require.Equal(t, 1, logger.countMatching("flooring candidate time"), "the floor must still engage, so this test cannot pass by never reaching the median at all")
	})
}

// TestCandidateTimeFallback covers the defensive branches: when the batched
// GetBlockHeaders call errors or returns a run that is not anchored at the
// requested parent or not correctly linked (a reorg race in the store's
// batched lookup), candidateTime must recover via the hash-keyed parent-chain
// walk — and only error when both attempts fail.
func TestCandidateTimeFallback(t *testing.T) {
	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	headers := linkedHeaders(t, 11, baseTime)
	tip := headers[0]
	wantFloor := int64(baseTime+5) + 1

	stubWalk := func(mockClient *blockchain.Mock) {
		for _, h := range headers {
			mockClient.On("GetBlockHeader", mock.Anything, h.Hash()).Return(h, &model.BlockHeaderMeta{}, nil)
		}
		// the walk stops at depth 11 before dereferencing the oldest header's parent
	}

	newAssembler := func(mockClient *blockchain.Mock) *BlockAssembler {
		return &BlockAssembler{logger: ulogger.TestLogger{}, blockchainClient: mockClient}
	}

	t.Run("anchored batched run needs no fallback", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
		mockClient.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
	})

	t.Run("transport failure degrades without attempting the walk", func(t *testing.T) {
		// A failed call means the blockchain service is unhealthy. Walking it once
		// per header would add MedianTimeBlocks more sequential reads without
		// adding information, against a service that is already failing — so this
		// path degrades immediately rather than amplifying the load.
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, context.DeadlineExceeded)
		stubWalk(mockClient)

		before := time.Now().Unix()
		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.GreaterOrEqual(t, got, before)
		require.Less(t, got, wantFloor, "a transport failure must not yield a floored time")
		mockClient.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
	})

	t.Run("mis-anchored batched run falls back to the hash-keyed walk", func(t *testing.T) {
		// The run is a correctly-linked chain anchored at the WRONG block with
		// different timestamps — what a reorg between the batched lookup's
		// height probe and its range scan produces (the winning fork's blocks
		// at the same heights). Its median differs from the true chain's, so
		// this test fails if the anchor guard is ever weakened: the floor
		// would come out of the wrong fork's timestamps.
		wrongFork := linkedHeaders(t, 11, baseTime+1000)

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(wrongFork, []*model.BlockHeaderMeta{}, nil)
		stubWalk(mockClient)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
		mockClient.AssertNumberOfCalls(t, "GetBlockHeader", 10)
	})

	t.Run("broken link in the batched run falls back to the hash-keyed walk", func(t *testing.T) {
		// Replace one mid-run header with a differently-stamped stray so the
		// run's median differs from the true chain's — swapping two elements
		// would leave the (sorted) median identical and the link guard
		// unpinned under mutation.
		broken := make([]*model.BlockHeader, len(headers))
		copy(broken, headers)
		broken[6] = linkedHeaders(t, 1, baseTime+1000)[0]

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(broken, []*model.BlockHeaderMeta{}, nil)
		stubWalk(mockClient)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
		mockClient.AssertNumberOfCalls(t, "GetBlockHeader", 10)
	})

	t.Run("nil header in the batched run falls back to the hash-keyed walk", func(t *testing.T) {
		withNil := make([]*model.BlockHeader, len(headers))
		copy(withNil, headers)
		withNil[7] = nil

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(withNil, []*model.BlockHeaderMeta{}, nil)
		stubWalk(mockClient)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
	})

	t.Run("degrades to the wall clock when the batched fetch and the walk both fail", func(t *testing.T) {
		// A blockchain hiccup must not stop mining. The floor only changes the
		// outcome when the wall clock is at or below median-time-past, so an
		// unfloored candidate is no worse than the pre-floor behaviour, whereas
		// failing the poll takes mining down outright — including on
		// generateEmptyBlockCandidate, the path whose job is to keep serving
		// work while a block is processed.
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(linkedHeaders(t, 11, baseTime+1000), []*model.BlockHeaderMeta{}, nil)
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, context.DeadlineExceeded)

		before := time.Now().Unix()
		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.GreaterOrEqual(t, got, before)
		require.LessOrEqual(t, got, time.Now().Unix())
	})

	t.Run("nil header from the walk degrades to the wall clock", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(linkedHeaders(t, 11, baseTime+1000), []*model.BlockHeaderMeta{}, nil)
		mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return((*model.BlockHeader)(nil), &model.BlockHeaderMeta{}, nil)

		before := time.Now().Unix()
		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.GreaterOrEqual(t, got, before)
		require.Less(t, got, wantFloor, "a nil header must not yield a floored time")
	})

	t.Run("nil parent header is rejected", func(t *testing.T) {
		_, err := newAssembler(&blockchain.Mock{}).candidateTime(t.Context(), nil)
		require.Error(t, err)
	})

	t.Run("empty batched run falls back to the hash-keyed walk", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)
		stubWalk(mockClient)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
	})

	t.Run("nil head in the batched run falls back to the hash-keyed walk", func(t *testing.T) {
		// The anchor guard, not the loop guard: a nil at index 0 is the shape
		// that would otherwise reach the median calculation.
		withNilHead := make([]*model.BlockHeader, len(headers))
		copy(withNilHead, headers)
		withNilHead[0] = nil

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(withNilHead, []*model.BlockHeaderMeta{}, nil)
		stubWalk(mockClient)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
	})

	t.Run("short batched run that does not reach the chain start falls back to the walk", func(t *testing.T) {
		// Truncated at its OLDEST end, so it is still anchored at the parent and
		// still correctly linked — invisible to both of those guards. Its median
		// is taken over a narrower window and comes out higher than the true
		// median, so this test fails if the run-length guard is removed.
		truncated := headers[:4]
		require.NotEqual(t, int64(truncated[len(truncated)/2].Timestamp)+1, wantFloor, "fixture must make the short-run median differ from the true one")

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(truncated, []*model.BlockHeaderMeta{}, nil)
		stubWalk(mockClient)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.Equal(t, wantFloor, got)
		mockClient.AssertNumberOfCalls(t, "GetBlockHeader", 10)
	})

	t.Run("over-long batched run falls back to the walk", func(t *testing.T) {
		tooMany := linkedHeaders(t, 12, baseTime)

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(tooMany, []*model.BlockHeaderMeta{}, nil)
		for _, h := range tooMany {
			mockClient.On("GetBlockHeader", mock.Anything, h.Hash()).Return(h, &model.BlockHeaderMeta{}, nil)
		}

		got, err := newAssembler(mockClient).candidateTime(t.Context(), tooMany[0])
		require.NoError(t, err)
		require.Equal(t, int64(baseTime+6)+1, got, "the walk's 11 headers end at the over-long run's tip")
	})

	t.Run("the median is fetched once per tip, not once per poll", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

		assembler := newAssembler(mockClient)
		for i := 0; i < 5; i++ {
			got, err := assembler.candidateTime(t.Context(), tip)
			require.NoError(t, err)
			require.Equal(t, wantFloor, got)
		}

		mockClient.AssertNumberOfCalls(t, "GetBlockHeaders", 1)
	})

	t.Run("concurrent misses on one cold tip fetch, warn and occupy a slot once", func(t *testing.T) {
		// The ordinary state on a cold tip: neither GetMiningCandidate entry point
		// takes a lock and every miner polls several times a second, so the
		// check-then-compute-then-store sequence is entered concurrently. Without
		// single-flighting, each racer fetches, each warns, and the round-robin
		// slot has no per-parent affinity so the same parent lands in both slots —
		// evicting the other candidate path's entry and making MinCandidateTime
		// report no floor for a live tip.
		const racers = 8

		logger := &warnCountingLogger{}
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

		assembler := &BlockAssembler{logger: logger, blockchainClient: mockClient}

		start := make(chan struct{})

		var wg sync.WaitGroup

		for i := 0; i < racers; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()
				<-start

				got, err := assembler.candidateTime(t.Context(), tip)
				require.NoError(t, err)
				require.Equal(t, wantFloor, got)
			}()
		}

		close(start)
		wg.Wait()

		mockClient.AssertNumberOfCalls(t, "GetBlockHeaders", 1)
		require.Equal(t, int32(1), logger.warns.Load(), "a cold tip must warn once however many polls race")

		occupied := 0

		for i := range assembler.mtpFloorMemo {
			if entry := assembler.mtpFloorMemo[i].Load(); entry != nil && entry.parent.IsEqual(tip.Hash()) {
				occupied++
			}
		}

		require.Equal(t, 1, occupied, "one parent must occupy at most one slot, or it evicts the other candidate path's entry")
	})

	t.Run("a caller joining an in-flight fetch waits on its own context, not the leader's", func(t *testing.T) {
		// Single-flighting makes one caller's fetch serve every other caller on
		// that parent, which is the point — but it must not make them hostage to
		// it. Miners poll several times a second with their own gRPC deadlines,
		// so a follower whose own deadline fires has to give up and degrade to
		// the wall clock for that poll rather than block until the leader's fetch
		// returns. Nothing is memoized on that path, so its next poll re-flights.
		var enterOnce sync.Once

		entered := make(chan struct{})
		release := make(chan struct{})

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).
			Return(headers, []*model.BlockHeaderMeta{}, nil).
			Run(func(mock.Arguments) {
				enterOnce.Do(func() { close(entered) })
				<-release
			})

		assembler := newAssembler(mockClient)

		leaderDone := make(chan int64, 1)

		go func() {
			got, _ := assembler.candidateTime(context.WithoutCancel(t.Context()), tip)
			leaderDone <- got
		}()

		<-entered

		// The leader is inside the fetch, so this caller joins its flight rather
		// than starting one of its own.
		followerCtx, cancelFollower := context.WithCancel(t.Context())
		followerDone := make(chan int64, 1)
		followerErr := make(chan error, 1)

		go func() {
			got, err := assembler.candidateTime(followerCtx, tip)
			followerErr <- err
			followerDone <- got
		}()

		cancelFollower()

		select {
		case got := <-followerDone:
			require.NoError(t, <-followerErr)
			require.Less(t, got, wantFloor, "a caller that gave up must degrade to the wall clock, not report a floor it never fetched")
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("a follower blocked past its own cancelled context waiting on the leader's fetch")
		}

		close(release)

		require.Equal(t, wantFloor, <-leaderDone, "the leader's own fetch must still complete and floor its candidate")
		mockClient.AssertNumberOfCalls(t, "GetBlockHeaders", 1)
	})

	t.Run("both candidate paths' parents stay memoized together", func(t *testing.T) {
		// The busy branch keys on the blockchain service's tip while the main
		// path keys on the subtree processor's precomputed parent, so a
		// single-slot memo would refetch on every alternation.
		otherHeaders := linkedHeaders(t, 11, baseTime+100)
		other := otherHeaders[0]

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)
		mockClient.On("GetBlockHeaders", mock.Anything, other.Hash(), mock.Anything).Return(otherHeaders, []*model.BlockHeaderMeta{}, nil)

		assembler := newAssembler(mockClient)
		for i := 0; i < 4; i++ {
			_, err := assembler.candidateTime(t.Context(), tip)
			require.NoError(t, err)
			_, err = assembler.candidateTime(t.Context(), other)
			require.NoError(t, err)
		}

		mockClient.AssertNumberOfCalls(t, "GetBlockHeaders", 2)
	})

	t.Run("floor warning fires once per tip, not once per poll", func(t *testing.T) {
		logger := &warnCountingLogger{}
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

		assembler := &BlockAssembler{logger: logger, blockchainClient: mockClient}

		for i := 0; i < 3; i++ {
			got, err := assembler.candidateTime(t.Context(), tip)
			require.NoError(t, err)
			require.Equal(t, wantFloor, got)
		}

		require.Equal(t, int32(1), logger.warns.Load(), "repeated polls on the same tip must warn once")

		// A new tip is a new condition: it must warn again.
		nextHeaders := linkedHeaders(t, 11, baseTime+100)
		nextTip := nextHeaders[0]
		mockClient.On("GetBlockHeaders", mock.Anything, nextTip.Hash(), mock.Anything).Return(nextHeaders, []*model.BlockHeaderMeta{}, nil)

		_, err := assembler.candidateTime(t.Context(), nextTip)
		require.NoError(t, err)
		require.Equal(t, int32(2), logger.warns.Load(), "a different tip must warn again")
	})

	t.Run("walk stops cleanly at the chain start", func(t *testing.T) {
		// A 3-block chain whose oldest header points at the all-zero genesis
		// parent: the walk must return the short run, and the median over 3
		// headers is the middle timestamp. The walk is reached via an unusable
		// batched result rather than a transport failure, which no longer walks.
		short := linkedHeadersFromGenesis(t, 3, baseTime)
		shortTip := short[0]

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(linkedHeaders(t, 11, baseTime+1000), []*model.BlockHeaderMeta{}, nil)
		for _, h := range short {
			mockClient.On("GetBlockHeader", mock.Anything, h.Hash()).Return(h, &model.BlockHeaderMeta{}, nil)
		}

		got, err := newAssembler(mockClient).candidateTime(t.Context(), shortTip)
		require.NoError(t, err)
		require.Equal(t, int64(baseTime+1)+1, got)
		// 2, not 3: the walk is seeded with the parent header the caller holds.
		mockClient.AssertNumberOfCalls(t, "GetBlockHeader", 2)
	})

	t.Run("short batched run reaching the chain start is accepted without a walk", func(t *testing.T) {
		short := linkedHeadersFromGenesis(t, 3, baseTime)
		shortTip := short[0]

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, shortTip.Hash(), mock.Anything).Return(short, []*model.BlockHeaderMeta{}, nil)

		got, err := newAssembler(mockClient).candidateTime(t.Context(), shortTip)
		require.NoError(t, err)
		require.Equal(t, int64(baseTime+1)+1, got)
		mockClient.AssertNotCalled(t, "GetBlockHeader", mock.Anything, mock.Anything)
	})
}

// TestCandidateTimeTwoHourCeiling pins the upper bound. The median-time rule is
// a floor and the two-hours-in-the-future rule is a ceiling; when a lagging
// local clock puts median-time-past+1 above now+2h no timestamp satisfies both.
// model.Block CheckHeaderContextual enforces the ceiling with no currentChain,
// so submitMiningSolution's block.Valid call does catch it — and treats the
// failure as a subtree-processor fault, deleting the job and resetting block
// assembly. Flooring regardless would therefore turn every solution found into
// a full assembler reset, so candidateTime must fail the poll instead.
func TestCandidateTimeTwoHourCeiling(t *testing.T) {
	// The fail-closed branch increments a counter, and these subtests build the
	// assembler directly rather than through New(), which is what registers the
	// package's metrics in production.
	initPrometheusMetrics()

	// Stamped five hours ahead: the floor lands ~3h above the ceiling.
	baseTime := uint32(time.Now().Add(5 * time.Hour).Unix())
	headers := linkedHeaders(t, 11, baseTime)
	tip := headers[0]

	newAssembler := func(generateSupported bool) *BlockAssembler {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

		return &BlockAssembler{
			logger:           ulogger.TestLogger{},
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.Params{GenerateSupported: generateSupported},
			},
		}
	}

	t.Run("fails the poll rather than emitting an unmineable candidate", func(t *testing.T) {
		_, err := newAssembler(false).candidateTime(t.Context(), tip)
		require.Error(t, err)
		require.Contains(t, err.Error(), "two-hour future bound")
	})

	t.Run("reports the stopped block production once per tip", func(t *testing.T) {
		// This branch is the only one that takes mining output to zero, and the
		// error it returns is never logged locally — GetMiningCandidate propagates
		// it and the gRPC handler wraps it without a log line. So it has to log
		// here, and latched, because every poll takes this branch for as long as
		// the clock stays out.
		logger := &warnCountingLogger{}
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

		assembler := &BlockAssembler{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.Params{GenerateSupported: false},
			},
		}

		for i := 0; i < 4; i++ {
			_, err := assembler.candidateTime(t.Context(), tip)
			require.Error(t, err)
		}

		require.Equal(t, int32(1), logger.errs.Load(), "the operator must be told once, not on every poll")
	})

	t.Run("names the future-stamped parent chain as well as the local clock", func(t *testing.T) {
		// A floor above now+2h is equally consistent with a parent chain stamped
		// into the future by a misconfigured or hostile upstream miner as with a
		// slow local clock. This is the one line an operator reads when block
		// production has gone to zero, so naming only the clock sends them to
		// chase NTP when the cause may be upstream.
		logger := &warnCountingLogger{}
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

		assembler := &BlockAssembler{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.Params{GenerateSupported: false},
			},
		}

		_, err := assembler.candidateTime(t.Context(), tip)
		require.Error(t, err)
		require.Contains(t, err.Error(), "local clock")
		require.Contains(t, err.Error(), "future-stamped")
		require.Equal(t, 1, logger.countMatching("local clock"))
		require.Equal(t, 1, logger.countMatching("future-stamped"))
	})

	t.Run("serves the wall clock on chains where the median rule is advisory", func(t *testing.T) {
		// GenerateSupported downgrades the median-time violation to a warning in
		// CheckHeaderContextual, so an unfloored candidate is still valid there
		// and rapid generation must keep working.
		before := time.Now().Unix()

		got, err := newAssembler(true).candidateTime(t.Context(), tip)
		require.NoError(t, err)
		require.GreaterOrEqual(t, got, before)
		require.LessOrEqual(t, got, time.Now().Unix())
	})

	t.Run("the advisory-chain warning fires once per tip, not once per poll", func(t *testing.T) {
		// Same reasoning as the floor warning: this branch stays engaged for as
		// long as the clock condition lasts, on a path polled several times a
		// second, so an unlatched line floods the log during the incident an
		// operator is trying to read it through.
		logger := &warnCountingLogger{}
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

		assembler := &BlockAssembler{
			logger:           logger,
			blockchainClient: mockClient,
			settings: &settings.Settings{
				ChainCfgParams: &chaincfg.Params{GenerateSupported: true},
			},
		}

		for i := 0; i < 4; i++ {
			_, err := assembler.candidateTime(t.Context(), tip)
			require.NoError(t, err)
		}

		require.Equal(t, int32(1), logger.warns.Load(), "repeated polls past the ceiling must warn once")
	})

	t.Run("the floor-lookup failure error fires once per tip, not once per poll", func(t *testing.T) {
		logger := &warnCountingLogger{}
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, context.DeadlineExceeded)

		assembler := &BlockAssembler{logger: logger, blockchainClient: mockClient}

		for i := 0; i < 4; i++ {
			_, err := assembler.candidateTime(t.Context(), tip)
			require.NoError(t, err)
		}

		require.Equal(t, int32(1), logger.errs.Load(), "a persistent lookup failure must be reported once per tip")
	})

	t.Run("nil settings does not fault and fails closed", func(t *testing.T) {
		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

		_, err := (&BlockAssembler{logger: ulogger.TestLogger{}, blockchainClient: mockClient}).candidateTime(t.Context(), tip)
		require.Error(t, err)
	})

	t.Run("a floor just inside the ceiling still applies", func(t *testing.T) {
		// baseTime+5 is the median, so the floor is baseTime+6. Placing the
		// median an hour ahead keeps the floor well below now+2h.
		insideBase := uint32(time.Now().Add(time.Hour).Unix())
		inside := linkedHeaders(t, 11, insideBase)

		mockClient := &blockchain.Mock{}
		mockClient.On("GetBlockHeaders", mock.Anything, inside[0].Hash(), mock.Anything).Return(inside, []*model.BlockHeaderMeta{}, nil)

		got, err := (&BlockAssembler{logger: ulogger.TestLogger{}, blockchainClient: mockClient}).candidateTime(t.Context(), inside[0])
		require.NoError(t, err)
		require.Equal(t, int64(insideBase+5)+1, got)
	})
}

// TestPlaceMtpFloor pins the one-slot-per-parent invariant directly on the
// helper. Single-flighting the miss already prevents two concurrent computations
// of the same parent from reaching here, so the reuse branch is defence against
// a future change to that flighting rather than a live path — which is exactly
// why the invariant is worth stating in a test rather than left implicit. If a
// parent could hold both slots it would evict the other candidate path's entry,
// and MinCandidateTime would report no floor for a live tip.
func TestPlaceMtpFloor(t *testing.T) {
	parentA := chainhash.Hash{1}
	parentB := chainhash.Hash{2}

	assembler := &BlockAssembler{logger: ulogger.TestLogger{}}

	occupiedBy := func(parent chainhash.Hash) int {
		count := 0

		for i := range assembler.mtpFloorMemo {
			if entry := assembler.mtpFloorMemo[i].Load(); entry != nil && entry.parent.IsEqual(&parent) {
				count++
			}
		}

		return count
	}

	assembler.placeMtpFloor(&mtpFloorEntry{parent: parentA, minTime: 100})
	assembler.placeMtpFloor(&mtpFloorEntry{parent: parentB, minTime: 200})

	require.Equal(t, 1, occupiedBy(parentA))
	require.Equal(t, 1, occupiedBy(parentB))

	// Re-placing an existing parent must reuse its slot, not consume the other's.
	assembler.placeMtpFloor(&mtpFloorEntry{parent: parentA, minTime: 300})

	require.Equal(t, 1, occupiedBy(parentA), "a parent must never occupy both slots")
	require.Equal(t, 1, occupiedBy(parentB), "re-placing one parent must not evict the other")

	got, ok := assembler.MinCandidateTime(&parentA)
	require.True(t, ok)
	require.Equal(t, int64(300), got, "the slot must carry the newest entry")

	got, ok = assembler.MinCandidateTime(&parentB)
	require.True(t, ok)
	require.Equal(t, int64(200), got)
}

// TestMinCandidateTime covers the memo read the submit path uses to reject a
// miner-supplied nTime below the consensus floor.
func TestMinCandidateTime(t *testing.T) {
	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	headers := linkedHeaders(t, 11, baseTime)
	tip := headers[0]

	mockClient := &blockchain.Mock{}
	mockClient.On("GetBlockHeaders", mock.Anything, tip.Hash(), mock.Anything).Return(headers, []*model.BlockHeaderMeta{}, nil)

	assembler := &BlockAssembler{logger: ulogger.TestLogger{}, blockchainClient: mockClient}

	t.Run("unknown before a candidate is built", func(t *testing.T) {
		_, ok := assembler.MinCandidateTime(tip.Hash())
		require.False(t, ok)
	})

	t.Run("nil hash is unknown", func(t *testing.T) {
		_, ok := assembler.MinCandidateTime(nil)
		require.False(t, ok)
	})

	t.Run("returns the floor the candidate was built from", func(t *testing.T) {
		_, err := assembler.candidateTime(t.Context(), tip)
		require.NoError(t, err)

		minTime, ok := assembler.MinCandidateTime(tip.Hash())
		require.True(t, ok)
		require.Equal(t, int64(baseTime+5)+1, minTime)
	})

	t.Run("unknown for a parent no candidate was built on", func(t *testing.T) {
		_, ok := assembler.MinCandidateTime(linkedHeaders(t, 11, baseTime+500)[0].Hash())
		require.False(t, ok)
	})
}

// warnCountingLogger counts Warnf calls so tests can pin the once-per-tip
// warning latch in candidateTime; everything else is the no-op TestLogger.
type warnCountingLogger struct {
	ulogger.TestLogger
	warns atomic.Int32
	errs  atomic.Int32

	mu      sync.Mutex
	formats []string
}

func (l *warnCountingLogger) Warnf(format string, args ...interface{}) {
	l.warns.Add(1)
	l.record(format)
}

func (l *warnCountingLogger) Errorf(format string, args ...interface{}) {
	l.errs.Add(1)
	l.record(format)
}

func (l *warnCountingLogger) record(format string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.formats = append(l.formats, format)
}

// countMatching returns how many recorded lines contain substr. Counting a
// specific line is more durable than counting all of them when the assembler
// under test may log for unrelated reasons.
func (l *warnCountingLogger) countMatching(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	count := 0

	for _, f := range l.formats {
		if strings.Contains(f, substr) {
			count++
		}
	}

	return count
}

// TestMiningCandidateTimeFlooredAtMedianTimePast drives the public
// GetMiningCandidate entry point with the best block on a future-stamped
// chain: the candidate's Time must come back strictly greater than the
// median-time-past of the previous 11 blocks. Before the floor it came back
// at the (lagging) wall clock, producing a block every peer rejects. This
// exercises whichever internal branch the assembler's state selects (busy or
// empty-block); the subtree-populated main path is covered by
// TestMiningCandidateMainPathTimeFloored below.
func TestMiningCandidateTimeFlooredAtMedianTimePast(t *testing.T) {
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	tipHeader := buildFutureChain(t, t.Context(), server.blockchainClient, baseTime)
	server.blockAssembler.setBestBlockHeader(tipHeader, 11)

	candidate, _, err := server.blockAssembler.GetMiningCandidate(t.Context())
	require.NoError(t, err)
	require.NotNil(t, candidate)

	medianTimePast := baseTime + 5
	require.Greater(t, candidate.Time, medianTimePast, "candidate time must be strictly greater than the median-time-past of the previous 11 blocks")
	require.Equal(t, medianTimePast+1, candidate.Time)
}

// TestMiningCandidateMainPathTimeFloored pins the floor on the main
// (subtree-populated) candidate path: the assembler holds transactions for
// the future-stamped tip, so GetMiningCandidate must return a candidate with
// those transactions AND the floored timestamp.
func TestMiningCandidateMainPathTimeFloored(t *testing.T) {
	initPrometheusMetrics()

	testItems := setupBlockAssemblyTest(t)
	ba := testItems.blockAssembler
	_, _, _ = setupBlockchainClient(t, testItems)

	ctx := t.Context()

	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	tipHeader := buildFutureChain(t, ctx, ba.blockchainClient, baseTime)

	ba.setBestBlockHeader(tipHeader, 11)
	ba.subtreeProcessor.InitCurrentBlockHeader(tipHeader)

	for i := 0; i < 5; i++ {
		txHash := chainhash.HashH([]byte{byte(i), 0xBB})
		ba.AddTxBatch(
			[]subtreepkg.Node{{Hash: txHash, Fee: uint64(100), SizeInBytes: 250}},
			[]*subtreepkg.TxInpoints{{}},
		)
	}

	medianTimePast := baseTime + 5

	var candidate *model.MiningCandidate

	require.Eventually(t, func() bool {
		var err error
		candidate, _, err = ba.GetMiningCandidate(ctx)

		return err == nil && candidate != nil && candidate.NumTxs > 0
	}, 5*time.Second, 50*time.Millisecond, "expected a subtree-populated candidate for the future-stamped tip")

	require.Equal(t, medianTimePast+1, candidate.Time, "main-path candidate time must be floored at median-time-past+1")
}

// TestEmptyBlockCandidateTimeFlooredAtMedianTimePast covers the empty-block
// path directly, which previously had the same unfloored wall-clock gap.
func TestEmptyBlockCandidateTimeFlooredAtMedianTimePast(t *testing.T) {
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	baseTime := uint32(time.Now().Add(100 * time.Minute).Unix())
	tipHeader := buildFutureChain(t, t.Context(), server.blockchainClient, baseTime)

	candidate, _, err := server.blockAssembler.generateEmptyBlockCandidate(t.Context(), tipHeader, 11)
	require.NoError(t, err)
	require.NotNil(t, candidate)

	medianTimePast := baseTime + 5
	require.Equal(t, medianTimePast+1, candidate.Time)
}
