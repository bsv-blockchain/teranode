package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestValidateSubtrees_SizeMismatchIsCorrupt covers the quick-path subtree-shape check
// (bitcoin-sv/teranode#4692): a non-final subtree whose size differs from the first is a body-derived
// defect, returned as ERR_BLOCK_CORRUPT DIRECTLY (not wrapped in ErrProcessing, which would shadow
// it and route it as a transient error). Asserts the classification, not just failure.
func TestValidateSubtrees_SizeMismatchIsCorrupt(t *testing.T) {
	mk := func(nodes int, seed byte) *subtreepkg.Subtree {
		st, err := subtreepkg.NewTreeByLeafCount(8)
		require.NoError(t, err)

		for i := 0; i < nodes; i++ {
			require.NoError(t, st.AddNode(chainhash.Hash{seed, byte(i)}, 1, 0))
		}

		return st
	}

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
		},
		// slice[0] has 2 nodes, slice[1] has 1 — a non-final size mismatch. slice[2] (the last) is
		// deliberately not checked by the loop.
		SubtreeSlices: []*subtreepkg.Subtree{mk(2, 0x01), mk(1, 0x02), mk(1, 0x03)},
	}

	bv := &BlockValidation{logger: ulogger.TestLogger{}}

	_, err := bv.validateSubtrees(context.Background(), block, 0)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "a non-final subtree size mismatch must be corrupt, got: %v", err)
	require.Contains(t, err.Error(), "size mismatch")
}

// TestValidateSubtrees_MerkleMismatchIsCorruptUnwrapped covers the A1 corrupt-vs-infra split
// (bitcoin-sv/teranode#4692): when CheckMerkleRoot returns a corrupt verdict (the computed merkle does
// not match the header's), validateSubtrees returns it UNWRAPPED so it is not shadowed by an outer
// ErrProcessing and mis-routed as a transient error. Asserts the classification survives.
func TestValidateSubtrees_MerkleMismatchIsCorruptUnwrapped(t *testing.T) {
	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())                     // node 0: coinbase placeholder
	require.NoError(t, st.AddNode(chainhash.Hash{0xAA}, 1, 100)) // node 1: a real tx

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{}, // zeroed: cannot match the computed root -> corrupt
			Timestamp:      1,
		},
		CoinbaseTx:       coinbase,
		SubtreeSlices:    []*subtreepkg.Subtree{st},
		Subtrees:         []*chainhash.Hash{st.RootHash()},
		TransactionCount: 2,
	}

	bv := &BlockValidation{logger: ulogger.TestLogger{}}

	_, err = bv.validateSubtrees(context.Background(), block, 0)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "a merkle-root mismatch must be returned as corrupt (unwrapped), got: %v", err)
}

// TestValidateSubtrees_InfraMerkleErrorIsWrapped covers the other half of the A1 split
// (bitcoin-sv/teranode#4692): when CheckMerkleRoot returns a NON-corrupt (infrastructure) error — here
// a subtree-count mismatch, which CheckMerkleRoot classifies as a storage error — validateSubtrees
// WRAPS it with the [validateSubtrees][hash] site context (not corrupt, so not shadow-routed as a
// transient), keeping the infrastructure classification in the cause chain.
func TestValidateSubtrees_InfraMerkleErrorIsWrapped(t *testing.T) {
	mk := func(seed byte) *subtreepkg.Subtree {
		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, st.AddNode(chainhash.Hash{seed}, 1, 0))

		return st
	}

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1,
		},
		// Two loaded slices (same size, so the size loop passes) but only one Subtrees hash: this
		// count mismatch makes CheckMerkleRoot fail with a NON-corrupt storage error.
		SubtreeSlices:    []*subtreepkg.Subtree{mk(0x01), mk(0x02)},
		Subtrees:         []*chainhash.Hash{{0x01}},
		TransactionCount: 2,
	}

	bv := &BlockValidation{logger: ulogger.TestLogger{}}

	_, err := bv.validateSubtrees(context.Background(), block, 0)
	require.Error(t, err)
	require.False(t, errors.IsBlockCorrupt(err), "an infrastructure merkle error must NOT be classified corrupt")
	require.True(t, errors.Is(err, errors.ErrProcessing), "it must be wrapped as a processing error, got: %v", err)
	require.Contains(t, err.Error(), "merkle root check failed")
}

// TestProcessBlockFound_CorruptCapGate_DropsAndSelfHeals drives the RUNNING per-hash corrupt cap
// end-to-end through processBlockFound — the universal chokepoint that both the worker and the
// direct ProcessBlock route funnel through (bitcoin-sv/teranode#4692). It proves BEHAVIOUR, not mere
// execution:
//   - a below-cap corrupt delivery is validated and surfaces a corrupt error (and is accounted),
//   - once the cap is reached the next corrupt delivery is DROPPED before validation — it returns
//     nil (the corrupt error is suppressed), proving the expensive validation was skipped rather
//     than repeated,
//   - clearing the counter re-admits the hash and the corrupt error surfaces again (self-heal),
//   - the cap holds with a nil p2pClient, so it is ban-score-independent,
//   - an EMPTY peerID is never capped (fail-open): an unidentified delivery is never gated out.
func TestProcessBlockFound_CorruptCapGate_DropsAndSelfHeals(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, blockchainStore, block := newProcessBlockFoundHarness(ctx, t)

	// Low cap so the test is fast; the gate reads this live.
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2

	// A real serving-peer identity — the cap is keyed per (hash, peerID).
	const peerID = "peerA"

	// Corrupt the BODY, not the header: a 1-byte coinbase scriptSig fails the outer coinbase-length
	// check (< 2 bytes) and returns ERR_BLOCK_CORRUPT. The coinbase is not committed by the block
	// hash, so PoW stays valid and block.Hash() (the counter key) is unchanged across deliveries.
	block.CoinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	// Deliveries below the cap are validated and surface a corrupt error, each accounted toward the cap.
	for i := 1; i <= 2; i++ {
		err := s.processBlockFound(ctx, block.Hash(), peerID, "legacy", block)
		require.Error(t, err, "delivery %d must reach validation and fail", i)
		require.True(t, errors.IsBlockCorrupt(err), "delivery %d must surface a corrupt error, got: %v", i, err)
	}

	require.True(t, s.corruptAttemptsExhausted(block.Hash(), peerID), "cap reached after 2 corrupt deliveries")

	// Past the cap the corrupt delivery is SUPPRESSED before validation: it returns a
	// corrupt-classified, NON-POISONING error (never nil-as-accepted, bitcoin-sv/teranode#4692). The
	// error is corrupt but NOT ErrBlockInvalid, and the block was never stored — so nothing is
	// poisoned and no caller can read the drop as acceptance.
	err := s.processBlockFound(ctx, block.Hash(), peerID, "legacy", block)
	require.Error(t, err, "past the cap the delivery must be suppressed with an error, never reported as accepted")
	require.True(t, errors.IsBlockCorrupt(err), "the cap-suppressed drop must be corrupt-classified, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "the cap-suppressed drop must NEVER be ErrBlockInvalid (no poison)")
	require.Contains(t, err.Error(), "cap reached", "the message must identify a cap-suppressed re-download, not a fresh corrupt body")

	stored, existsErr := blockchainStore.GetBlockExists(ctx, block.Hash())
	require.NoError(t, existsErr)
	require.False(t, stored, "a cap-suppressed drop must NOT store the block")

	// Self-heal: clearing the counter (as a genuine success would) re-admits the hash, so the same
	// corrupt body is validated again and surfaces its error — the hash was rate-limited, never condemned.
	s.clearCorruptAttempts(block.Hash(), peerID)
	require.False(t, s.corruptAttemptsExhausted(block.Hash(), peerID), "cleared counter reopens the gate")

	err = s.processBlockFound(ctx, block.Hash(), peerID, "legacy", block)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "after the cap clears the hash is validated again (self-heal), got: %v", err)

	// An EMPTY peerID is uncapped (fail-open): however many corrupt deliveries arrive under no
	// identity, the gate never reports exhausted and never drops one as nil-accept. This guards the
	// blocker directly — an unidentified delivery can never wedge the honest tip.
	for i := 1; i <= 4; i++ {
		err = s.processBlockFound(ctx, block.Hash(), "", "legacy", block)
		require.Error(t, err, "empty-peer delivery %d must reach validation (uncapped)", i)
		require.True(t, errors.IsBlockCorrupt(err), "empty-peer delivery %d must surface a corrupt error, got: %v", i, err)
	}
	require.False(t, s.corruptAttemptsExhausted(block.Hash(), ""), "an empty peerID is never exhausted (fail-open)")
}

// TestProcessBlockFound_CapDisabled_NeverDrops proves the <= 0 escape hatch: with the cap disabled
// the gate never drops, even after many corrupt deliveries (documents the re-opened DoS).
func TestProcessBlockFound_CapDisabled_NeverDrops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 0 // disabled
	block.CoinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	// A non-empty peerID so this genuinely exercises the maxAttempts <= 0 branch rather than the
	// empty-peerID fail-open path (which is uncapped for a different reason).
	const peerID = "peerA"

	// Many corrupt deliveries; with the cap disabled every one still reaches validation and surfaces
	// the corrupt error (never silently dropped by the gate).
	for i := 1; i <= 5; i++ {
		err := s.processBlockFound(ctx, block.Hash(), peerID, "legacy", block)
		require.Error(t, err, "delivery %d must still reach validation when the cap is disabled", i)
		require.True(t, errors.IsBlockCorrupt(err), "delivery %d must surface a corrupt error, got: %v", i, err)
	}

	require.False(t, s.corruptAttemptsExhausted(block.Hash(), peerID), "cap <= 0 never reports exhausted")
}

// TestProcessBlockFound_CorruptCap_HonestPeerNotWedged drives the (hash, peerID) re-key end-to-end
// through processBlockFound (bitcoin-sv/teranode#4692, ordishs' NEW finding): a bad peer that exhausts
// its budget for the tip hash must NOT suppress that same hash when an HONEST peer serves it. Because
// the honest peer keys a different (hash, peerID) bucket, its delivery is not dropped by the gate —
// it reaches validation. Here the same corrupt body is used for both peers, so the honest delivery
// still surfaces a corrupt error rather than nil: the point proven is that it was NOT gated out
// (reached validation), which is exactly the honest-tip-not-wedged property.
func TestProcessBlockFound_CorruptCap_HonestPeerNotWedged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2
	block.CoinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	const badPeer, honestPeer = "peerBad", "peerHonest"

	// The bad peer exhausts its own budget for the tip hash.
	for i := 1; i <= 2; i++ {
		err := s.processBlockFound(ctx, block.Hash(), badPeer, "legacy", block)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "bad-peer delivery %d must reach validation, got: %v", i, err)
	}
	require.True(t, s.corruptAttemptsExhausted(block.Hash(), badPeer), "the bad peer is capped for this hash")
	require.False(t, s.corruptAttemptsExhausted(block.Hash(), honestPeer), "the honest peer keeps a fresh budget")

	// The honest peer serving the SAME hash is NOT gated out — its delivery reaches validation (and,
	// with this deliberately-corrupt body, still surfaces the corrupt error rather than a silent nil
	// drop). The bad peer never wedged the honest tip.
	err := s.processBlockFound(ctx, block.Hash(), honestPeer, "legacy", block)
	require.Error(t, err, "the honest peer must not be gated out by the bad peer's exhausted budget")
	require.True(t, errors.IsBlockCorrupt(err), "the honest delivery reached validation, got: %v", err)
}

// TestProcessBlockFound_CorruptCap_HitNeitherClearsNorPoisons is §4 test 4 (bitcoin-sv/teranode#4692):
// a cap-hit suppression returns a corrupt-classified, non-poisoning error (never nil-as-accepted),
// does NOT clear its own counter (so the cooldown still bounds the peer), sets no invalid=true, and
// leaves GetBlockExists false — the hash is rate-limited, never condemned. When the window lapses
// (simulated by clearing) the honest body is admitted again (self-heal).
func TestProcessBlockFound_CorruptCap_HitNeitherClearsNorPoisons(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, blockchainStore, block := newProcessBlockFoundHarness(ctx, t)
	s.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 2
	block.CoinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	const peerID = "peerCap"

	// Reach the cap.
	for i := 1; i <= 2; i++ {
		err := s.processBlockFound(ctx, block.Hash(), peerID, "legacy", block)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err))
	}
	require.True(t, s.corruptAttemptsExhausted(block.Hash(), peerID), "cap reached")

	// The cap-hit delivery is suppressed with a corrupt-classified, non-poisoning error (never
	// nil-as-accepted) but must NOT clear the counter...
	err := s.processBlockFound(ctx, block.Hash(), peerID, "legacy", block)
	require.Error(t, err, "a cap-hit delivery is suppressed with an error, never reported as accepted")
	require.True(t, errors.IsBlockCorrupt(err), "the cap-hit suppression is corrupt-classified, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "the cap-hit suppression must NEVER be ErrBlockInvalid")
	require.True(t, s.corruptAttemptsExhausted(block.Hash(), peerID),
		"the cap-hit skip must NOT clear its own counter (else the cooldown would not bound the peer)")

	// ...and must NOT poison: the block was never stored invalid, and does not exist.
	exists, err := blockchainStore.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists, "a rate-limited corrupt hash must never be persisted (no poison)")

	// Self-heal: once the window lapses (simulated), the honest body is admitted for validation again.
	s.clearCorruptAttempts(block.Hash(), peerID)
	require.False(t, s.corruptAttemptsExhausted(block.Hash(), peerID), "the window lapsing re-admits the hash")
}
