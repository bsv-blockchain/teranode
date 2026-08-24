package blockvalidation

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	p2pconstants "github.com/bsv-blockchain/teranode/interfaces/p2p"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// corruptBanScoreCall records one AddBanScore invocation for assertion.
type corruptBanScoreCall struct {
	peerID string
	reason string
}

// corruptStrikeP2PClient is a P2PClientI that records AddBanScore calls. It embeds the
// interface so the many unused methods are inherited (never called by these tests).
type corruptStrikeP2PClient struct {
	P2PClientI

	mu    sync.Mutex
	calls []corruptBanScoreCall
}

func (r *corruptStrikeP2PClient) AddBanScore(_ context.Context, peerID string, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, corruptBanScoreCall{peerID: peerID, reason: reason})
	return nil
}

func (r *corruptStrikeP2PClient) recorded() []corruptBanScoreCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]corruptBanScoreCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// newCorruptTestBlock builds a minimal block whose header hashes cleanly, enough for
// penalizeCorruptBlockPeer's block.Hash() calls.
func newCorruptTestBlock(t *testing.T) *model.Block {
	t.Helper()

	nBits, err := model.NewNBitFromString("2000ffff")
	require.NoError(t, err)

	merkleRoot := chainhash.Hash{}

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &merkleRoot,
			Timestamp:      1234567890,
			Bits:           *nBits,
			Nonce:          0,
		},
	}
}

// TestPenalizeCorruptBlockPeer_StrikesServingPeerWithCorruptReason verifies the peer
// strike is attributed to the serving peer with the dedicated corrupt reason
// (bitcoin-sv/teranode#4692) — never invalid_block, and never storing the block invalid.
func TestPenalizeCorruptBlockPeer_StrikesServingPeer(t *testing.T) {
	fake := &corruptStrikeP2PClient{}
	bv := &BlockValidation{logger: ulogger.TestLogger{}, p2pClient: fake}
	block := newCorruptTestBlock(t)

	bv.penalizeCorruptBlockPeer(context.Background(), "peer-A", block, "merkle root does not match")

	calls := fake.recorded()
	require.Len(t, calls, 1)
	require.Equal(t, "peer-A", calls[0].peerID, "penalty must be attributed to the serving peer")
	require.Equal(t, p2pconstants.ReasonCorruptBlockBody.String(), calls[0].reason, "must use the dedicated corrupt reason, not invalid_block")
}

// TestPenalizeCorruptBlockPeer_NoopWhenNoAttribution verifies the strike is a safe
// no-op when there is no p2p client or no serving peer, so single-peer / in-process
// deployments still function.
func TestPenalizeCorruptBlockPeer_NoopWhenNoAttribution(t *testing.T) {
	block := newCorruptTestBlock(t)

	// Nil p2p client: must not panic.
	bvNilClient := &BlockValidation{logger: ulogger.TestLogger{}}
	require.NotPanics(t, func() {
		bvNilClient.penalizeCorruptBlockPeer(context.Background(), "peer-A", block, "x")
	})

	// Empty peerID: nothing recorded.
	fake := &corruptStrikeP2PClient{}
	bv := &BlockValidation{logger: ulogger.TestLogger{}, p2pClient: fake}
	bv.penalizeCorruptBlockPeer(context.Background(), "", block, "x")
	require.Empty(t, fake.recorded(), "no strike without an attributable peer")
}

// TestPenalizeCorruptBlockPeer_LegacyPeerIDNoop pins the legacy-peerID gate (bitcoin-sv/teranode#4692):
// a "legacy:"-namespaced peerID must be gated exactly like an empty one, so AddBanScore is never
// called on the centralized registry — legacy attribution stays exclusively in
// services/legacy/peer_server.go's own strikeIfCorruptBlockBody, avoiding the phantom registry
// entry and the double charge.
func TestPenalizeCorruptBlockPeer_LegacyPeerIDNoop(t *testing.T) {
	block := newCorruptTestBlock(t)

	fake := &corruptStrikeP2PClient{}
	bv := &BlockValidation{logger: ulogger.TestLogger{}, p2pClient: fake}
	bv.penalizeCorruptBlockPeer(context.Background(), "legacy:1.2.3.4:8333", block, "x")
	require.Empty(t, fake.recorded(), "a legacy-namespaced peerID must never reach AddBanScore")
}

// TestIsUnvalidatablePeerError_CorruptIsNotUnvalidatable pins the routing invariant
// (bitcoin-sv/teranode#4692): a corrupt block body must NOT be treated as give-up/malicious, so
// catchup re-downloads from another peer instead of poisoning. Genuine consensus
// failures remain unvalidatable.
func TestIsUnvalidatablePeerError_CorruptIsNotUnvalidatable(t *testing.T) {
	require.False(t, isUnvalidatablePeerError(errors.NewBlockCorruptError("corrupt body")),
		"corrupt must not be unvalidatable — it must be re-downloaded, not given up on")

	// Even when a corrupt cause is wrapped, it must not be classified unvalidatable.
	require.False(t, isUnvalidatablePeerError(errors.NewProcessingError("outer", errors.NewBlockCorruptError("corrupt body"))))

	// Genuine consensus failures stay unvalidatable.
	require.True(t, isUnvalidatablePeerError(errors.NewBlockInvalidError("invalid")))
	require.True(t, isUnvalidatablePeerError(errors.NewTxInvalidError("tx invalid")))
}
