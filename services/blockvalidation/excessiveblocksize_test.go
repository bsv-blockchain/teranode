package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// recordingBanScoreP2PClient records whether AddBanScore (the corrupt-body strike) was called.
type recordingBanScoreP2PClient struct {
	P2PClientI

	called bool
}

func (r *recordingBanScoreP2PClient) AddBanScore(_ context.Context, _ string, _ string) error {
	r.called = true
	return nil
}

// TestValidateBlock_ExcessiveBlockSize_PlainPolicyDecline is the bitcoin-sv/teranode#4692 fix
// (bitcoin-sv/teranode#4692): excessiveblocksize is a local POLICY knob, not evidence of corruption or
// consensus invalidity. A block that exceeds THIS node's limit (a block other miners may have
// legitimately mined and proven with real work) must be declined WITHOUT striking the serving peer,
// WITHOUT a corrupt classification (so it is not re-downloaded / counted toward the corrupt cap), and
// WITHOUT invalid=true (never poison the hash).
func TestValidateBlock_ExcessiveBlockSize_PlainPolicyDecline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, blockchainStore, block := newProcessBlockFoundHarness(ctx, t)

	// Tiny limit so any real block exceeds it; the gate reads this live.
	s.settings.Policy.ExcessiveBlockSize = 1

	fake := &recordingBanScoreP2PClient{}
	s.blockValidation.p2pClient = fake

	err := s.blockValidation.ValidateBlockWithOptions(ctx, block, "legacy", &ValidateBlockOptions{PeerID: "peer-honest"})

	require.Error(t, err, "an oversized block must be declined")
	require.Contains(t, err.Error(), "excessiveblocksize", "the decline must name the policy knob")
	require.False(t, errors.IsBlockCorrupt(err), "a policy decline must NOT be corrupt (no re-download / corrupt-cap accounting)")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a policy decline must NOT poison the block (no invalid=true)")
	require.False(t, fake.called, "a local policy decline must NOT strike the peer that served the honest chain")

	// Nothing was written: the decline leaves no record at all, which is strictly stronger than
	// "not marked invalid" — a block this node merely declines on local policy must stay acceptable
	// to a node with a larger limit, and must be re-acceptable here if the operator raises the knob.
	// The store's StoreBlock is what would record an invalid=true verdict, and it also sets the
	// existence flag, so "does not exist" is exactly "no verdict was recorded".
	exists, existsErr := s.blockValidation.GetBlockExists(ctx, block.Hash())
	require.NoError(t, existsErr)
	require.False(t, exists, "an oversized block must not be persisted by the policy decline")

	storeExists, storeErr := blockchainStore.GetBlockExists(ctx, block.Hash())
	require.NoError(t, storeErr)
	require.False(t, storeExists,
		"the decline must not reach the store at all, so no invalid=true verdict exists against this hash")
}

// TestProcessBlockFound_ExcessiveBlockSize_DeclineIsBounded covers the second half of bitcoin-sv/teranode#4692: the decline itself is correct, but nothing remembered it, so the
// same peer's re-announcements cost a fresh block-message fetch on every delivery. A dedicated
// per-(hash, peerID) tracker now bounds that, sized by the corrupt cap's settings but spending its
// own budget.
func TestProcessBlockFound_ExcessiveBlockSize_DeclineIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, blockchainStore, block := newProcessBlockFoundHarness(ctx, t)

	// Tiny limit so the harness block exceeds it; the gate reads this live.
	s.settings.Policy.ExcessiveBlockSize = 1
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 3

	fake := &recordingBanScoreP2PClient{}
	s.blockValidation.p2pClient = fake

	const peerA, peerB = "peer-A", "peer-B"

	// Every delivery up to the cap is judged and declined on policy. Capture the uncapped decline so
	// the capped suppression below can be asserted to carry the IDENTICAL classification.
	var uncappedDecline error
	for i := 0; i < 3; i++ {
		err := s.processBlockFound(ctx, block.Hash(), peerA, "legacy", block)
		require.Error(t, err, "delivery %d must be declined on policy", i+1)
		require.Contains(t, err.Error(), "excessiveblocksize")
		require.True(t, errors.Is(err, errors.ErrBlockError), "a policy decline is classified ERR_BLOCK_ERROR")
		require.False(t, errors.IsBlockCorrupt(err), "a policy decline must never be corrupt")
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a policy decline must never poison the hash")
		uncappedDecline = err
	}

	require.True(t, s.policyDeclineAttemptsExhausted(block.Hash(), peerA), "the cap is reached for this (hash, peer)")

	// The next delivery from the same peer is suppressed at the gate, before any fetch. The cap is a
	// RATE LIMIT on re-fetching a block we already declined, NOT a different verdict — so the drop is
	// INDISTINGUISHABLE in classification from the uncapped decline it rate-limits (bitcoin-sv/teranode#4692):
	// same ERR_BLOCK_ERROR sentinel, NOT corrupt (so no corrupt-body strike lands for our local policy
	// choice), never ErrBlockInvalid, and never nil (no false accept).
	capErr := s.processBlockFound(ctx, block.Hash(), peerA, "legacy", block)
	require.Error(t, capErr, "a capped delivery must be suppressed with an error, never reported as accepted")
	require.Equal(t, errors.IsBlockCorrupt(uncappedDecline), errors.IsBlockCorrupt(capErr),
		"capped and uncapped declines must share the corrupt-classification (both false)")
	require.True(t, errors.Is(capErr, errors.ErrBlockError), "the cap suppression carries the SAME class as the decline it rate-limits")
	require.False(t, errors.IsBlockCorrupt(capErr), "the cap suppression must NOT be corrupt (no corrupt-body strike for a policy decline)")
	require.False(t, errors.Is(capErr, errors.ErrBlockInvalid), "the cap suppression must never poison the hash")
	require.Contains(t, capErr.Error(), "suppressed", "the message must identify a cap suppression")

	// Same suppression on the REAL route, with no pre-loaded block. This pins the gate ABOVE the
	// fetch: without useBlock, processBlockFound would otherwise call fetchSingleBlock, whose HTTP
	// request against the "legacy" base URL fails and surfaces a ProcessingError. An ERR_BLOCK_ERROR
	// cap-suppression (not a ProcessingError fetch failure) is only possible if the gate ran first.
	capErr = s.processBlockFound(ctx, block.Hash(), peerA, "legacy")
	require.Error(t, capErr, "a capped delivery must be suppressed before fetchSingleBlock is ever reached")
	require.True(t, errors.Is(capErr, errors.ErrBlockError), "the pre-fetch suppression is the policy-decline class, got: %v", capErr)
	require.False(t, errors.Is(capErr, errors.ErrProcessing), "must be the cap suppression, not a fetch ProcessingError")
	require.Contains(t, capErr.Error(), "suppressed", "must be the cap suppression, not a fetch error")

	// The suppressed drop stored nothing and poisoned nothing — same as the decline it rate-limits.
	stored, existsErr := blockchainStore.GetBlockExists(ctx, block.Hash())
	require.NoError(t, existsErr)
	require.False(t, stored, "a policy-cap suppression must NOT store the block")

	// The honest-tip-wedge property: a different serving identity keeps a full, independent budget,
	// so the same hash is judged again rather than suppressed.
	err := s.processBlockFound(ctx, block.Hash(), peerB, "legacy", block)
	require.Error(t, err, "a different peer must not be suppressed by the first peer's declines")
	require.Contains(t, err.Error(), "excessiveblocksize")

	// Budget independence: the policy declines never spent the corrupt budget, and no peer was
	// struck for serving a block this node merely declines on local policy.
	require.False(t, s.corruptAttemptsExhausted(block.Hash(), peerA),
		"local policy declines must not consume the corrupt re-download budget")
	require.False(t, fake.called, "a local policy decline must never strike the serving peer")

	// The converse: corrupt failures for the same pair do not suppress a policy-declined fetch. Drive
	// the corrupt cap directly (the block never reaches validation here) and confirm the policy gate
	// for the untouched peer is still open.
	for i := 0; i < 3; i++ {
		s.recordCorruptAttempt(block.Hash(), peerB)
	}

	require.True(t, s.corruptAttemptsExhausted(block.Hash(), peerB))
	require.False(t, s.policyDeclineAttemptsExhausted(block.Hash(), peerB),
		"corrupt failures must not spend the policy-decline budget")
}
