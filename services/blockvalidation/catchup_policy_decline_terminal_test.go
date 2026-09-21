package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

// reportPeerFailureRecorder wraps a REAL blockchain client (sqlitememory-backed) and counts
// ReportPeerFailure calls. Everything else is delegated to the embedded client, so nothing about
// the blockchain is mocked — only the one outbound rotation signal this test asserts the ABSENCE of
// is intercepted, since "it was never called" is not observable any other way.
type reportPeerFailureRecorder struct {
	blockchain.ClientI

	mu    sync.Mutex
	calls int
}

func (r *reportPeerFailureRecorder) ReportPeerFailure(_ context.Context, _ *chainhash.Hash, _ string, _ string, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++

	return nil
}

func (r *reportPeerFailureRecorder) reportCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.calls
}

// TestProcessCatchupChItem_PolicyDeclineIsTerminal pins the B3 fix (bitcoin-sv/teranode#4692): a
// block this node declines under its own local policy (excessiveblocksize) must END the catch-up
// cycle, without charging anyone.
//
// Before the fix the decline carried ERR_BLOCK_ERROR, which isUnvalidatablePeerError does not match,
// so processCatchupChItem fell through to the alternative-source walk — re-downloading the same
// over-limit block from every peer in turn, and again next cycle — while also charging the primary a
// catch-up failure and firing ReportPeerFailure, i.e. sync-peer rotation, for a decision that is
// entirely ours.
//
// Terminal BUT UNPERSISTED: nothing is stored (asserted against the real sqlitememory store), so a
// hash the rest of the network accepts is never poisoned; only the cycle ends.
//
// Terminal PER SERVING IDENTITY, not per hash. Two things follow, both asserted here: the bound is
// spent on the per-(hash, peerID) decline budget and the shared per-hash catch-up budget is left
// untouched; and the cached alternative announcers SURVIVE, because on a peer-scoped verdict their
// copies of the hash are the recovery route.
// TestProcessCatchupChItem_PolicyDeclineDoesNotCoolTheHashForOtherPeers below is the regression test
// for the property those protect.
//
// Mutation proof: delete the ErrBlockPolicyDeclined branch in processCatchupChItem and the
// catchupFunc call count goes above one (the alternative-source walk runs) and ReportPeerFailure
// fires — reddening both. Swap recordPolicyDeclineAttempt back to
// recordCatchupAttemptUnlessProgress and the per-hash assertions redden. Re-add
// catchupAlternatives.Delete to the branch and the survival assertions redden.
func TestProcessCatchupChItem_PolicyDeclineIsTerminal(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CatchupMaxAttemptsPerBlock = 3
	tSettings.BlockValidation.MaxCorruptAttemptsPerBlock = 3

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	realClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	bcRecorder := &reportPeerFailureRecorder{ClientI: realClient}
	// A full P2PClientI implementation, not one with a nil embedded interface: processCatchupChItem
	// calls isPeerBad and isPeerMalicious before catchupFunc, so a partial fake would panic there.
	p2pRecorder := &subtreeAttributionP2PClient{}

	prev := chainhash.HashH([]byte("policy-decline-prev"))
	mr := chainhash.HashH([]byte("policy-decline-merkle"))
	block := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &mr}}

	const declinedPeer = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"

	catchupCalls := 0

	u := &Server{
		settings:            tSettings,
		logger:              ulogger.TestLogger{},
		blockchainClient:    bcRecorder,
		p2pClient:           p2pRecorder,
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
			ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
		),
		blockPolicyDeclineAttempts: ttlcache.New[blockAttemptKey, int](
			ttlcache.WithTTL[blockAttemptKey, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[blockAttemptKey, int](),
		),
	}
	u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
		catchupCalls++

		return errors.NewBlockPolicyDeclinedError("[ValidateBlock][%s] block size 5 exceeds excessiveblocksize 4 (local policy)", block.Hash().String())
	}

	// Both guards are set, and a cached alternative is seeded. It does double duty: if the branch did
	// not pre-empt the walk, that alternative would be tried and catchupFunc would be called a second
	// time; and it is the announcer whose record must SURVIVE the decline (asserted below).
	u.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	u.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{
		{block: block, peerID: "alternative-peer", baseURL: "http://alternative:8000"},
	}, ttlcache.DefaultTTL)

	u.processCatchupChItem(ctx, processBlockCatchup{block: block, peerID: declinedPeer, baseURL: "http://declined:8000"})

	// Terminal: exactly one attempt, no alternative-source walk.
	require.Equal(t, 1, catchupCalls, "a local policy decline must end the cycle, not walk alternative sources")

	// Nobody is charged: not the reputation counters, not the malicious counter, not a ban score,
	// and not the sync-peer rotation signal.
	require.Equal(t, 0, p2pRecorder.charged(declinedPeer), "no catch-up failure may be charged for our own policy")
	require.Equal(t, 0, p2pRecorder.totalCharges(), "no peer at all may be charged a catch-up failure")
	require.Equal(t, 0, p2pRecorder.totalMaliciousReports(), "a policy decline is not peer misbehaviour")
	require.Empty(t, p2pRecorder.struck(), "a policy decline must not earn the peer a ban score")

	require.Equal(t, 0, bcRecorder.reportCount(), "no sync-peer rotation may be signalled for a local policy decision")

	// The processing marker is cleared so the hash can be re-entered.
	require.Nil(t, u.processBlockNotify.Get(*block.Hash()), "the processing marker must be cleared")

	// But the cached alternatives SURVIVE. They are the only record of the other peers that
	// announced this hash — addBlockToPriorityQueue absorbs an announcement for a hash already in
	// processBlockNotify into that list rather than enqueueing it, and those peers do not announce
	// again — and since the verdict rests on THIS peer's declared size, their copies are the
	// recovery route. Deleting them would discard exactly what the per-peer keying exists to
	// preserve (bitcoin-sv/teranode#4692).
	alternatives := u.catchupAlternatives.Get(*block.Hash())
	require.NotNil(t, alternatives, "the cached alternative announcers must survive a peer-scoped decline")
	require.Len(t, alternatives.Value(), 1, "the alternative seeded above must still be there")
	require.Equal(t, "alternative-peer", alternatives.Value()[0].peerID)

	// The bound is spent on the per-(hash, peerID) decline budget, so re-entry by THIS peer is
	// bounded...
	declines := u.blockPolicyDeclineAttempts.Get(blockAttemptKey{hash: *block.Hash(), peerID: declinedPeer})
	require.NotNil(t, declines, "the per-peer decline counter must be advanced")
	require.Equal(t, 1, declines.Value(), "exactly one decline must be counted against this peer")

	// ...and NOT on the per-HASH catch-up budget, which is keyed by hash alone and whose exhaustion
	// would cool the hash down for every peer. The declining size is the peer-declared
	// block.SizeInBytes varint, so charging it to the hash would let one peer's inflated varint hold
	// an honest tip out of reach of the honest peers whose copy carries the true varint
	// (bitcoin-sv/teranode#4692).
	require.Nil(t, u.blockCatchupAttempts.Get(*block.Hash()),
		"a peer-declared size must not spend the shared per-hash catch-up budget")
	require.False(t, u.catchupAttemptsExhausted(block.Hash()), "the hash must not be in catch-up cooldown")

	// Unpersisted: the decline stored no verdict against the hash, so a node with a larger limit
	// still accepts the block and this node re-accepts it if the operator raises the knob.
	stored, existsErr := blockChainStore.GetBlockExists(ctx, block.Hash())
	require.NoError(t, existsErr)
	require.False(t, stored, "a local policy decline must persist nothing")
}

// TestProcessCatchupChItem_PolicyDeclineDoesNotCoolTheHashForOtherPeers is the self-isolation
// regression test for the terminal decline branch (bitcoin-sv/teranode#4692).
//
// THE ATTACK. The size that drives excessiveBlockSizeDeclined is block.SizeInBytes, read straight
// off the wire by model.readBlockFromReader; the authoritative size is only recomputed
// post-subtree-load inside block.Valid, long after the decline. So a peer can announce the honest
// tip and declare an inflated size for the price of a block message — header, counts, 32 bytes per
// subtree hash, no body. If the resulting decline spent the per-HASH catch-up budget
// (blockCatchupAttempts, keyed by chainhash.Hash alone), CatchupMaxAttemptsPerBlock such messages
// would put that hash in cooldown for EVERY peer, and catchupAttemptsExhausted gates both the
// catch-up consumer and the enqueue path. The honest tip would then be unreachable through the
// honest peers whose copy of the same hash carries the true varint — the self-isolation this work
// exists to remove, reached from a new side.
//
// THE PROPERTY. Declines are counted per (hash, peerID). Enough declines from one peer cool that
// PEER down for the hash; they must never cool the HASH down for other peers. Here two different
// peers each decline the same hash its full decline budget — at or past the per-hash cap — and the
// hash must still not be in catch-up cooldown, while each peer's own budget is spent. It then goes
// one step further than the gate predicate and drives a THIRD, never-seen peer all the way to
// catchupFunc, which is what actually proves the hash is still reachable rather than merely
// un-flagged.
//
// Mutation proof: change the branch back to recordCatchupAttemptUnlessProgress and
// catchupAttemptsExhausted goes true, reddening the central assertion — and the third peer is then
// pre-empted by the per-hash gate, reddening the reachability assertion too.
func TestProcessCatchupChItem_PolicyDeclineDoesNotCoolTheHashForOtherPeers(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CatchupMaxAttemptsPerBlock = 3
	tSettings.BlockValidation.MaxCorruptAttemptsPerBlock = 3

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	realClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	bcRecorder := &reportPeerFailureRecorder{ClientI: realClient}
	p2pRecorder := &subtreeAttributionP2PClient{}

	prev := chainhash.HashH([]byte("policy-decline-shared-hash-prev"))
	mr := chainhash.HashH([]byte("policy-decline-shared-hash-merkle"))
	block := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &mr}}

	const (
		lyingPeer  = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"
		secondPeer = "12D3KooWSecondPeerAnnouncingTheSameHonestTipHash00000"
	)

	catchupCalls := 0

	u := &Server{
		settings:            tSettings,
		logger:              ulogger.TestLogger{},
		blockchainClient:    bcRecorder,
		p2pClient:           p2pRecorder,
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
			ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
		),
		blockPolicyDeclineAttempts: ttlcache.New[blockAttemptKey, int](
			ttlcache.WithTTL[blockAttemptKey, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[blockAttemptKey, int](),
		),
	}
	u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
		catchupCalls++

		return errors.NewBlockPolicyDeclinedError("[ValidateBlock][%s] block size 5 exceeds excessiveblocksize 4 (local policy)", block.Hash().String())
	}

	// Each peer declines exactly its own full budget. That is also at least the per-HASH cap, so if
	// declines were charged to the hash it would already be exhausted after the first peer alone —
	// asserted rather than assumed, since the two caps are independent settings.
	declineCap := tSettings.BlockValidation.MaxCorruptAttemptsPerBlock
	require.GreaterOrEqual(t, declineCap, tSettings.BlockValidation.CatchupMaxAttemptsPerBlock,
		"the fixture must drive at least as many cycles as the per-hash cap, or it cannot detect per-hash charging")

	// Two DIFFERENT peers each announce the same hash and get it declined.
	for i := 0; i < declineCap; i++ {
		u.processCatchupChItem(ctx, processBlockCatchup{block: block, peerID: lyingPeer, baseURL: "http://lying:8000"})
	}

	// THE CENTRAL ASSERTION: the hash is still reachable. Checked between the two peers as well as
	// after both, because it is the first peer's declines that would have exhausted a per-hash cap.
	require.False(t, u.catchupAttemptsExhausted(block.Hash()),
		"one peer's declines on a peer-declared size must not put the hash in cooldown for every peer")

	for i := 0; i < declineCap; i++ {
		u.processCatchupChItem(ctx, processBlockCatchup{block: block, peerID: secondPeer, baseURL: "http://second:8000"})
	}

	require.False(t, u.catchupAttemptsExhausted(block.Hash()),
		"two peers' declines on a peer-declared size must not put the hash in cooldown for every peer")
	require.Nil(t, u.blockCatchupAttempts.Get(*block.Hash()),
		"the shared per-hash catch-up budget must be untouched by a local policy decline")

	// Each peer's OWN budget is spent, which is what bounds re-entry: the cap is reached on each
	// peer's FINAL cycle, so every cycle above ran and one more from either peer is now pre-empted at
	// the top of processCatchupChItem before any work (asserted just below).
	require.True(t, u.policyDeclineAttemptsExhausted(block.Hash(), lyingPeer),
		"the lying peer must be in its own cooldown for this hash")
	require.True(t, u.policyDeclineAttemptsExhausted(block.Hash(), secondPeer),
		"the second peer must be in its own cooldown for this hash")
	require.Equal(t, 2*declineCap, catchupCalls,
		"each peer must get its own full decline budget before being pre-empted")

	// And the bound bites: one more cycle from the exhausted peer never reaches catchupFunc for that
	// peer. The pre-empt is peer-scoped, so it offers the hash to the other peers first (the shape
	// the bad/malicious skip below it uses); this fixture's p2p client serves no peers, so that
	// finds none and the cycle ends without work.
	u.processCatchupChItem(ctx, processBlockCatchup{block: block, peerID: lyingPeer, baseURL: "http://lying:8000"})
	require.Equal(t, 2*declineCap, catchupCalls,
		"a peer past its decline cap must be pre-empted before catchupFunc runs for it")

	// A THIRD peer is untouched by either, and the hash is reachable THROUGH IT — driven down the
	// real path, not just interrogated on the gate predicate. This is the whole claim the re-keying
	// rests on: catchupFunc runs for the honest peer, so its copy of the hash (carrying the true
	// varint) would have been validated had it not also been declined by this all-declining stub.
	const thirdPeer = "12D3KooWThirdHonestPeerNeverAskedYet000000000000000"

	require.False(t, u.policyDeclineAttemptsExhausted(block.Hash(), thirdPeer),
		"a peer that has never declined this hash must not inherit another peer's cooldown")

	u.processCatchupChItem(ctx, processBlockCatchup{block: block, peerID: thirdPeer, baseURL: "http://third:8000"})
	require.Equal(t, 2*declineCap+1, catchupCalls,
		"the hash must still be REACHABLE through a peer that has not spent its own decline budget")

	// Still nobody charged and nothing persisted, across every cycle.
	require.Equal(t, 0, p2pRecorder.totalCharges(), "no peer may be charged for our own policy")
	require.Equal(t, 0, bcRecorder.reportCount(), "no sync-peer rotation may be signalled for our own policy")

	stored, existsErr := blockChainStore.GetBlockExists(ctx, block.Hash())
	require.NoError(t, existsErr)
	require.False(t, stored, "a local policy decline must persist nothing")
}
