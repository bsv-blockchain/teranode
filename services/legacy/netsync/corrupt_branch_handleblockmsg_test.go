package netsync

import (
	"bytes"
	"container/list"
	"sync/atomic"
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
	// pre-check passes and execution reaches CheckMerkleRoot. Its header merkle root is left zeroed,
	// so the computed root from the built subtrees cannot match it — a body-derived corrupt verdict.
	block, _, _ := buildExtendedSubtreeBlock(t, height, 5)
	msgBlock := block.MsgBlock()
	msgBlock.Header.Bits = 0x207fffff // regtest max target
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

	require.True(t, sm.legacyUnified(uint32(height)), "unified route must be ON for this fixture")

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

// TestHandleBlockMsg_CorruptBody_HeadersFirst_ReRequestsBlock pins the recovery half of the corrupt
// branch in the mode that actually needs it (bitcoin-sv/teranode#4692). In headers-first mode the
// getblocks re-request is inert: the peer answers with an inv, and processInvMsg discards invs while
// headersFirstMode is set, so the hash never reaches state.requestQueue and no getdata is ever
// issued. The header-block pipeline cannot recover it either — fetchHeaderBlocks walks forward from
// sm.startHeader, and this block's header node was removed from headerList before validation ran.
//
// So the branch must issue a DIRECT getdata, and must put the hash back into both request maps: the
// branch cleared them before validation, handleBlockMsg disconnects a peer that delivers a block it
// has no record of requesting, and BlockRequested reads the per-peer map.
//
// The assertion is the outcome, not the call: a connected peer pair, and the remote end's OnGetData
// listener records what actually arrived on the wire.
//
// Mutation proof: replacing sm.requestBlockDirect with the bare sm.requestMissingBlocks that
// preceded it leaves no getdata on the wire and both maps empty, reddening all three assertions.
func TestHandleBlockMsg_CorruptBody_HeadersFirst_ReRequestsBlock(t *testing.T) {
	initPrometheusMetrics()

	const height = int32(500)

	// Build a well-formed unified-route block, then give it an easy PoW target so the difficulty
	// pre-check passes and execution reaches CheckMerkleRoot. Its header merkle root is left zeroed,
	// so the computed root from the built subtrees cannot match it — a body-derived corrupt verdict.
	block, _, _ := buildExtendedSubtreeBlock(t, height, 5)
	msgBlock := block.MsgBlock()
	msgBlock.Header.Bits = 0x207fffff // regtest max target
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

	var gotGetData atomic.Bool
	remoteCfg := peer.Config{
		Listeners: peer.MessageListeners{
			OnGetData: func(_ *peer.Peer, msg *wire.MsgGetData) {
				for _, iv := range msg.InvList {
					if iv.Type == wire.InvTypeBlock && iv.Hash.IsEqual(&blockHash) {
						gotGetData.Store(true)
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
	tSettings.BlockValidation.MaxCorruptAttemptsPerBlock = 1 // one corrupt record reaches the cap
	sm.settings = tSettings
	sm.chainParams = params
	sm.subtreeStore = memory.New()
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}} // SupportsOutpointOnlySpend()==true
	sm.validationClient = nil                                               // unified route must not touch it
	sm.blockCorruptAttempts = expiringmap.New[legacyCorruptAttemptKey, *corruptAttemptState](10 * time.Minute)
	t.Cleanup(func() { sm.blockCorruptAttempts.Stop() })

	require.True(t, sm.legacyUnified(uint32(height)), "unified route must be ON for this fixture")

	// Headers-first is the whole point of this test; both of these are dereferenced on that path.
	sm.headersFirstMode.Store(true)
	sm.headerList = list.New()
	sm.blockSizeTracker = newBlockSizeTracker(10)

	state, ok := sm.peerStates.Get(p)
	require.True(t, ok)

	err = sm.handleBlockMsg(&blockQueueMsg{block: msgBlock, blockHash: blockHash, blockHeight: height, peer: p})
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err))

	require.True(t, WaitUntil(func() bool { return gotGetData.Load() }, 2*time.Second),
		"a corrupt drop in headers-first mode must put a getdata for the same hash on the wire")

	_, inGlobal := sm.requestedBlocks.Get(blockHash)
	require.True(t, inGlobal, "sm.requestedBlocks must be re-armed, or the inv route would request the block twice")
	_, inPeer := state.requestedBlocks.Get(blockHash)
	require.True(t, inPeer, "state.requestedBlocks must be re-armed, or handleBlockMsg disconnects the peer that answers")
}
