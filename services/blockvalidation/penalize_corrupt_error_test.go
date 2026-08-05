package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// erroringBanScoreP2PClient is a P2PClientI whose AddBanScore always fails, to exercise the
// error-logging branch of penalizeCorruptBlockPeer.
type erroringBanScoreP2PClient struct {
	P2PClientI

	called bool
}

func (e *erroringBanScoreP2PClient) AddBanScore(_ context.Context, _ string, _ string) error {
	e.called = true
	return errors.NewError("ban score service unavailable")
}

// TestPenalizeCorruptBlockPeer_AddBanScoreFailureIsHandled covers the AddBanScore-failure branch of
// penalizeCorruptBlockPeer (bitcoin-sv/teranode#4692): when the ban-score RPC errors, the strike is
// still attempted and the failure is logged and swallowed (best-effort) — it must never propagate or
// panic, so a degraded p2p service cannot break block validation.
func TestPenalizeCorruptBlockPeer_AddBanScoreFailureIsHandled(t *testing.T) {
	fake := &erroringBanScoreP2PClient{}
	bv := &BlockValidation{logger: ulogger.TestLogger{}, p2pClient: fake}

	block := newCorruptTestBlock(t)

	require.NotPanics(t, func() {
		bv.penalizeCorruptBlockPeer(context.Background(), "peer-X", block, "merkle root does not match")
	}, "an AddBanScore RPC failure must be swallowed, not propagated")

	require.True(t, fake.called, "the corrupt-body strike must be attempted against the serving peer")
}
