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

// TestValidateBlock_ExcessiveBlockSize_PlainPolicyDecline is the freemans13 item 2 fix
// (bitcoin-sv/teranode#4692): excessiveblocksize is a local POLICY knob, not evidence of corruption or
// consensus invalidity. A block that exceeds THIS node's limit (a block other miners may have
// legitimately mined and proven with real work) must be declined WITHOUT striking the serving peer,
// WITHOUT a corrupt classification (so it is not re-downloaded / counted toward the corrupt cap), and
// WITHOUT invalid=true (never poison the hash).
func TestValidateBlock_ExcessiveBlockSize_PlainPolicyDecline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

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
}
