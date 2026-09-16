package netsync

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHandleBlockMsg_CorruptBody_NotMarkedFailed pins the netsync corrupt branch
// (bitcoin-sv/teranode#4692): when HandleBlockDirect returns a corrupt-body verdict (here a
// merkle-root mismatch on the unified route), handleBlockMsg must (1) record the corrupt failure
// against the SERVING peer's identity (peer.Addr()) toward the per-(hash, peerID) cap, (2) NOT
// mark the block in recentlyFailedBlocks — marking it would suppress its own descendants as a
// NOT_FOUND cascade, poisoning an honest re-download — and (3) actively re-request the same hash
// via requestMissingBlocks rather than only waiting for a spontaneous re-announcement, mirroring
// the orphan-continuation branch. It returns the corrupt error (not nil). Properties (2) and (3)
// must hold simultaneously: the skip prevents poisoning descendants, while the re-request keeps the
// legacy batch flowing. This test runs OUTSIDE headers-first mode, where the getblocks re-request is
// answered with an inv that the getdata loop turns into a real request; the headers-first case,
// where that inv is discarded and only the direct getdata recovers the block, is pinned separately
// by TestHandleBlockMsg_CorruptBody_HeadersFirst_ReRequestsBlock below.
//
// Mutation proof: deleting the `if errors.IsBlockCorrupt(err)` branch makes a corrupt error fall
// through to `recentlyFailedBlocks.Set(...)` (and skip the corrupt-attempt record), reddening both
// the "not marked failed" and the "corrupt attempt recorded / cap reached" assertions.
func TestHandleBlockMsg_CorruptBody_NotMarkedFailed(t *testing.T) {
	initPrometheusMetrics()

	const height = int32(500)

	// Build a well-formed unified-route block, then give it an easy PoW target so the difficulty
	// pre-check passes and execution reaches CheckMerkleRoot. buildExtendedSubtreeBlock commits the
	// body in the header, so zero the merkle root afterwards: the root computed from the built
	// subtrees then cannot match it — a body-derived corrupt verdict. Both edits land BEFORE the
	// nonce is mined, since the header hash covers them.
	block, _, _ := buildExtendedSubtreeBlock(t, height, 5)
	msgBlock := block.MsgBlock()
	msgBlock.Header.Bits = 0x207fffff             // regtest max target
	msgBlock.Header.MerkleRoot = chainhash.Hash{} // body no longer bound to the header
	// The unified route requires a checkpoint-ancestry proof now, and the only source of
	// one is a header-cache run rooted at the committed tip, so the block is anchored on
	// a tip this fixture can also hand to committedTip.
	tipHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	msgBlock.Header.PrevBlock = *tipHeader.Hash()
	// Mine a nonce that meets the (easy) target: the max-target check still rejects ~half of random
	// hashes, so a fixed nonce would be flaky. HandleBlockDirect checks PoW on the model header, so
	// mine against that same predicate.
	for {
		var hdr bytes.Buffer
		require.NoError(t, msgBlock.Header.Serialize(&hdr))
		mh, err := model.NewBlockHeaderFromBytes(hdr.Bytes())
		require.NoError(t, err)
		if ok, _, _ := mh.HasMetTargetDifficulty(); ok {
			break
		}
		msgBlock.Header.Nonce++
	}
	blockHash := msgBlock.Header.BlockHash()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// The parent header lookup: the wire header's PrevBlock is the zero hash; return a parent one
	// height below so the height-consistency check in HandleBlockDirect passes.
	parentMeta := &model.BlockHeaderMeta{Height: uint32(height) - 1}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, parentMeta, nil)
	// requestMissingBlocks' own dependencies, so the corrupt branch's re-request can be observed.
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{bestHeader.Hash()}, nil)

	sm, p := newBackoffTestManager(t, blockchainClient, blockHash)

	// Wire the unified-route dependencies so prepareSubtrees runs cheaply (no UTXO/validator stack)
	// and reaches CheckMerkleRoot.
	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockValidation.MaxCorruptAttemptsPerBlock = 1 // one corrupt record reaches the cap
	sm.settings = tSettings
	sm.chainParams = params
	sm.subtreeStore = memory.New()
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}} // SupportsOutpointOnlySpend()==true
	sm.validationClient = nil                                               // unified route must not touch it
	sm.blockCorruptAttempts = expiringmap.New[legacyCorruptAttemptKey, *corruptAttemptState](10 * time.Minute)
	t.Cleanup(func() { sm.blockCorruptAttempts.Stop() })

	// The proof itself: a checkpoint pinned at this block's own height, and a header
	// cache filled with the one-header run that matches it. Without this the delivery is
	// unproven, the unified route is denied, and the test would exercise the ordinary
	// route instead of the one it is about.
	params.Checkpoints = append(params.Checkpoints, chaincfg.Checkpoint{Height: height, Hash: &blockHash})
	sm.headerCache = newHeaderCache().WithCheckpoints(params.Checkpoints)
	require.True(t, sm.headerCache.Fill(*tipHeader.Hash(), height, []*wire.BlockHeader{&msgBlock.Header}))
	require.True(t, sm.blockOrigin(blockHash).headerProven, "the fixture must actually prove the header")

	require.True(t, sm.legacyUnified(sm.blockOrigin(blockHash), uint32(height)),
		"unified route must be ON for this fixture")

	err := sm.handleBlockMsg(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   blockHash,
		blockHeight: height,
		peer:        p,
	})

	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "the corrupt verdict must propagate out of handleBlockMsg, got: %v", err)

	// (1) recorded against the serving peer's identity and reached the cap (proves the record ran on
	// peer.Addr()).
	require.True(t, sm.corruptBlockAttemptsExhausted(blockHash, p.Addr()),
		"a corrupt delivery must be counted toward the per-(hash, peerID) cap on the serving peer's identity")

	// (2) NOT marked failed — the descendant NOT_FOUND-cascade suppression must not fire for a
	// re-downloadable corrupt body.
	_, failed := sm.recentlyFailedBlocks.Get(blockHash)
	require.False(t, failed, "a corrupt body must NOT be marked recentlyFailed (would poison its descendants)")

	// (3) The getblocks continuation fired: requestMissingBlocks calls GetBestBlockHeader then
	// GetBlockLocator before pushing it. This pins the CALL, not the outcome; the outcome — the hash
	// actually being requested again — is pinned by the headers-first test below.
	blockchainClient.AssertCalled(t, "GetBestBlockHeader", mock.Anything)
	blockchainClient.AssertCalled(t, "GetBlockLocator", mock.Anything, mock.Anything, mock.Anything)
}

// corruptReRequestScenario is the shared fixture for the three headers-first corrupt-drop cases that
// differ ONLY in the corrupt cap (bitcoin-sv/teranode#4692): below the cap the dropped hash is
// re-requested directly, at the cap it is not, and with the cap disabled it is. Extracted rather than
// copied so the three cannot drift apart.
type corruptReRequestScenario struct {
	sm            *SyncManager
	state         *peerSyncState
	blockHash     chainhash.Hash
	pendingHashes [2]chainhash.Hash
	peerAddr      string
	err           error
	sawGetData    func(chainhash.Hash) bool
}

// runCorruptReRequestScenario drives one corrupt delivery through handleBlockMsg in headers-first
// mode with maxCorruptAttempts as the cap, and returns what the wire and the request maps saw.
func runCorruptReRequestScenario(t *testing.T, maxCorruptAttempts int) corruptReRequestScenario {
	t.Helper()

	const height = int32(500)

	// Build a well-formed unified-route block, then give it an easy PoW target so the difficulty
	// pre-check passes and execution reaches CheckMerkleRoot. buildExtendedSubtreeBlock commits the
	// body in the header, so zero the merkle root afterwards: the root computed from the built
	// subtrees then cannot match it — a body-derived corrupt verdict. Both edits land BEFORE the
	// nonce is mined, since the header hash covers them.
	block, _, _ := buildExtendedSubtreeBlock(t, height, 5)
	msgBlock := block.MsgBlock()
	msgBlock.Header.Bits = 0x207fffff             // regtest max target
	msgBlock.Header.MerkleRoot = chainhash.Hash{} // body no longer bound to the header
	// The header cache anchors on the node's committed tip and refuses a run that does
	// not build on it, so the dropped block must name that tip as its parent or the
	// wanted-range pass has nothing to recover it with.
	tipHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	tipHash := *tipHeader.Hash()
	msgBlock.Header.PrevBlock = tipHash
	// Mine a nonce that meets the (easy) target: the max-target check still rejects ~half of random
	// hashes, so a fixed nonce would be flaky. HandleBlockDirect checks PoW on the model header, so
	// mine against that same predicate.
	for {
		var hdr bytes.Buffer
		require.NoError(t, msgBlock.Header.Serialize(&hdr))
		mh, err := model.NewBlockHeaderFromBytes(hdr.Bytes())
		require.NoError(t, err)
		if ok, _, _ := mh.HasMetTargetDifficulty(); ok {
			break
		}
		msgBlock.Header.Nonce++
	}
	blockHash := msgBlock.Header.BlockHash()

	// The descendants the wanted-range pass is expected to reach for once the dropped
	// block is asked for again. They are a real linked run above the dropped block
	// rather than two invented hashes, because the header cache only accepts a run
	// that links, and it is the cache the pass reads.
	pendingHeaders, pending := linkedRun(blockHash, 2)
	pendingHashes := [2]chainhash.Hash{pending[0], pending[1]}

	cacheRun := append([]*wire.BlockHeader{&msgBlock.Header}, pendingHeaders...)

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// The dropped hash and its descendants must all report "not held", which is the
	// haveInventory branch that requests them. Registered FIRST so it wins over the
	// catch-all below, which testify resolves in registration order.
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.MatchedBy(func(h *chainhash.Hash) bool {
		return h != nil && (h.IsEqual(&blockHash) || h.IsEqual(&pendingHashes[0]) || h.IsEqual(&pendingHashes[1]))
	})).Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), errors.NewNotFoundError("not found")).Maybe()
	// The parent header lookup: return a parent one height below so the
	// height-consistency check in HandleBlockDirect passes.
	parentMeta := &model.BlockHeaderMeta{Height: uint32(height) - 1}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, parentMeta, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{{}}, nil)
	// The committed tip sits one below the dropped block, so the wanted range the
	// recovery pass computes starts at exactly that block.
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(tipHeader, &model.BlockHeaderMeta{Height: uint32(height) - 1}, nil)

	var getDataMu sync.Mutex
	getDataHashes := map[chainhash.Hash]struct{}{}
	sawGetData := func(h chainhash.Hash) bool {
		getDataMu.Lock()
		defer getDataMu.Unlock()
		_, ok := getDataHashes[h]

		return ok
	}
	remoteCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetData: func(_ *peer.Peer, msg *wire.MsgGetData) {
				getDataMu.Lock()
				defer getDataMu.Unlock()
				for _, iv := range msg.InvList {
					if iv.Type == wire.InvTypeBlock {
						getDataHashes[iv.Hash] = struct{}{}
					}
				}
			},
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
	}
	localCfg := peer.Config{
		Listeners:        peer.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
	}

	remote, p, err := MakeConnectedPeers(t, remoteCfg, localCfg, 120)
	require.NoError(t, err)
	require.True(t, remote.Connected())

	sm := newBackoffTestManagerForPeer(t, blockchainClient, blockHash, p)

	// Wire the unified-route dependencies so prepareSubtrees runs cheaply (no UTXO/validator stack)
	// and reaches CheckMerkleRoot.
	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockValidation.MaxCorruptAttemptsPerBlock = maxCorruptAttempts
	sm.settings = tSettings
	sm.chainParams = params
	sm.subtreeStore = memory.New()
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}} // SupportsOutpointOnlySpend()==true
	sm.validationClient = nil                                               // unified route must not touch it
	sm.blockCorruptAttempts = expiringmap.New[legacyCorruptAttemptKey, *corruptAttemptState](10 * time.Minute)
	t.Cleanup(func() { sm.blockCorruptAttempts.Stop() })

	// Headers-first is the whole point of these cases. The recovery route is the
	// wanted-range pass, which names heights out of the header cache, so the cache is
	// filled with a run rooted at the committed tip: the dropped block at height 500
	// and two descendants above it. Without that fill the pass names nothing and the
	// re-request leg would prove nothing.
	sm.headersFirstMode.Store(true)
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.storeSyncPeer(p, &syncPeerState{})
	// The cache carries the ancestry proof as well as the heights: a checkpoint pinned at
	// the top of the run proves everything below it by linkage, which is what the unified
	// route this fixture is about now additionally requires.
	params.Checkpoints = append(params.Checkpoints, chaincfg.Checkpoint{Height: height + 2, Hash: &pendingHashes[1]})
	sm.headerCache = newHeaderCache().WithCheckpoints(params.Checkpoints)
	require.True(t, sm.headerCache.Fill(tipHash, height, cacheRun))
	require.True(t, sm.blockOrigin(blockHash).headerProven, "the fixture must actually prove the header")
	require.True(t, sm.legacyUnified(sm.blockOrigin(blockHash), uint32(height)),
		"unified route must be ON for this fixture")

	state, ok := sm.peerStates.Get(p)
	require.True(t, ok)
	// eligibleBlockPeers only considers connected sync candidates, and the assigner is
	// nil without one — which would make every re-request assertion below pass or fail
	// for the wrong reason.
	state.syncCandidate = true
	state.noteBestKnownHeight(height + 10)

	err = sm.handleBlockMsg(&blockQueueMsg{block: msgBlock, blockHash: blockHash, blockHeight: height, peer: p})

	return corruptReRequestScenario{
		sm:            sm,
		state:         state,
		blockHash:     blockHash,
		pendingHashes: pendingHashes,
		peerAddr:      p.Addr(),
		err:           err,
		sawGetData:    sawGetData,
	}
}

// TestHandleBlockMsg_CorruptBody_HeadersFirst_ReRequestsBlock pins the recovery half of the corrupt
// branch in the mode that actually needs it (bitcoin-sv/teranode#4692).
//
// Upstream recovered the dropped hash with a direct getdata to the same peer, because both of its
// other routes were blocked: a getblocks is answered with an inv and processInvMsg discards invs
// while headers-first mode is on, and its header-block walk only ever went forward from a cursor
// the dropped hash had already passed. Neither structure exists here. The recovery is the ordinary
// wanted-range pass, which recomputes what the node wants from the committed tip every time it
// runs, so the dropped hash is the first thing it names; the delivery has already released this
// peer's ownership of it, so nothing holds it back, and the assigner is free to place it with a
// different peer, which a direct getdata could not do.
//
// The assertion is the outcome rather than the call: a connected peer pair, and the remote end's
// OnGetData listener records every block hash that actually arrived on the wire. Both legs are
// pinned — the dropped hash itself, and a descendant of it, which only a pass that got past the
// dropped block can have asked for.
//
// Mutation proof: delete the sm.fetchHeaderBlocks() call on the corrupt branch and no getdata for
// either hash reaches the wire before the deadline.
func TestHandleBlockMsg_CorruptBody_HeadersFirst_ReRequestsBlock(t *testing.T) {
	initPrometheusMetrics()

	// Cap of 2, so this single corrupt delivery stays BELOW it and the re-request is expected.
	// At the cap the re-request is deliberately suppressed — pinned by the sibling test below — so a
	// cap of 1 here would exercise that gate instead of the re-request this test is about.
	sc := runCorruptReRequestScenario(t, 2)

	require.Error(t, sc.err)
	require.True(t, errors.IsBlockCorrupt(sc.err))

	require.True(t, WaitUntil(func() bool { return sc.sawGetData(sc.blockHash) }, 2*time.Second),
		"a corrupt drop in headers-first mode must put a getdata for the same hash back on the wire")

	require.True(t, WaitUntil(func() bool { return sc.sawGetData(sc.pendingHashes[0]) }, 2*time.Second),
		"the pass must carry on past the dropped block, so its descendants are requested in the same breath")

	// The ledger is what admits the answer: a block delivered with no owner on record costs an
	// honest peer its connection, so a re-request that did not record itself would be worse than
	// no re-request at all.
	require.True(t, sc.sm.blockDownloads.RequestedWithin(sc.blockHash, blockRequestRetryInterval),
		"the re-request must be recorded in the download ledger, or the delivery it invites is treated as unrequested")
}

// TestHandleBlockMsg_CorruptBody_AtCap_DoesNotReRequestBlock pins the wasted-re-request fix
// (bitcoin-sv/teranode#4692). On the corrupt attempt that REACHES the per-(hash, peerID) cap, the
// re-request must be skipped: the gate at the top of the delivery path would drop that peer's next
// delivery of this hash anyway, but only after the whole block body had crossed the wire — the
// exact waste that gate's own comment says it avoids by not re-requesting.
//
// The absence is checked against a positive control from the sibling test above, which proves the
// same fixture does put a getdata on the wire when the cap allows it, so this is a real absence
// rather than a race.
//
// Mutation proof: remove the corruptBlockAttemptsExhausted guard around the pass and the dropped
// hash appears on the wire, reddening the negative assertion.
func TestHandleBlockMsg_CorruptBody_AtCap_DoesNotReRequestBlock(t *testing.T) {
	initPrometheusMetrics()

	// Cap of 1: this single corrupt delivery reaches it.
	sc := runCorruptReRequestScenario(t, 1)

	require.Error(t, sc.err)
	require.True(t, errors.IsBlockCorrupt(sc.err), "the corrupt verdict must still propagate, got: %v", sc.err)
	require.True(t, sc.sm.corruptBlockAttemptsExhausted(sc.blockHash, sc.peerAddr),
		"the fixture must actually reach the cap, or this test proves nothing")

	require.False(t, WaitUntil(func() bool { return sc.sawGetData(sc.blockHash) }, 500*time.Millisecond),
		"at the cap the dropped hash must NOT be re-requested: the gate would discard that delivery only after the full body crossed the wire")

	// The corollary: nothing is owed by anybody for this hash until the cooldown window lapses.
	require.False(t, sc.sm.blockDownloads.RequestedWithin(sc.blockHash, blockRequestRetryInterval),
		"the ledger must not record a request the node deliberately did not make")

	// Unchanged by the gate: a corrupt body is still never marked failed, so its descendants are not
	// suppressed as a NOT_FOUND cascade.
	_, failed := sc.sm.recentlyFailedBlocks.Get(sc.blockHash)
	require.False(t, failed, "a corrupt body must NOT be marked recentlyFailed even at the cap")
}

// TestHandleBlockMsg_CorruptBody_CapDisabled_StillReRequestsBlock is the mutation-proof against
// implementing the gate as `attempts < MaxCorruptAttemptsPerBlock` (bitcoin-sv/teranode#4692). A cap
// of <= 0 means DISABLED, so with maxAttempts 0 that arithmetic reads `1 < 0` — false — and would
// suppress the re-request on EVERY corrupt body on a node that deliberately turned the cap off.
// Gating on corruptBlockAttemptsExhausted instead returns false when the cap is disabled, so the
// re-request still fires, which is the pre-existing behaviour.
func TestHandleBlockMsg_CorruptBody_CapDisabled_StillReRequestsBlock(t *testing.T) {
	initPrometheusMetrics()

	sc := runCorruptReRequestScenario(t, 0)

	require.Error(t, sc.err)
	require.True(t, errors.IsBlockCorrupt(sc.err))
	require.False(t, sc.sm.corruptBlockAttemptsExhausted(sc.blockHash, sc.peerAddr),
		"a cap of 0 disables the bound, so no (hash, peer) can ever be exhausted")

	require.True(t, WaitUntil(func() bool { return sc.sawGetData(sc.blockHash) }, 2*time.Second),
		"with the cap disabled the dropped hash must still be re-requested")
}
