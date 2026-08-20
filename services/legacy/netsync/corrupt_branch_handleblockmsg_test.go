package netsync

import (
	"bytes"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHandleBlockMsg_CorruptBody_NotMarkedFailed pins the netsync corrupt branch
// (bitcoin-sv/teranode#4692): when HandleBlockDirect returns a corrupt-body verdict (here a
// merkle-root mismatch on the unified route), handleBlockMsg must (1) record the corrupt failure
// against the SERVING peer's identity (peer.Addr()) toward the per-(hash, peerID) cap, and (2) NOT
// mark the block in recentlyFailedBlocks — marking it would suppress its own descendants as a
// NOT_FOUND cascade, poisoning an honest re-download. It returns the corrupt error (not nil).
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
	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	// The parent header lookup: the wire header's PrevBlock is the zero hash; return a parent one
	// height below so the height-consistency check in HandleBlockDirect passes.
	parentMeta := &model.BlockHeaderMeta{Height: uint32(height) - 1}
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, parentMeta, nil)

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
}
