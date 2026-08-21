package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// recordingBlockValidation embeds the no-op mock and records the peerID passed to ProcessBlock, so a
// test can assert what identity the legacy route plumbs to block validation.
type recordingBlockValidation struct {
	*blockvalidation.MockBlockValidation
	peerIDs []string
}

func (r *recordingBlockValidation) ProcessBlock(_ context.Context, _ *model.Block, _ uint32, peerID, _ string, _ uint32) error {
	r.peerIDs = append(r.peerIDs, peerID)
	return nil
}

// TestSyncManager_ProcessBlock_PlumbsServingPeerIdentity pins that the legacy sync route forwards the
// serving peer's identity to block validation rather than a shared empty string
// (bitcoin-sv/teranode#4692). The per-(hash, peerID) attempt caps only bucket per peer if a real
// identity reaches them; collapsing every legacy delivery to "" would let one peer's cap gate an
// honest tip served by another (and, before the wider fix, read a cap-exhausted decline as accepted).
//
// Mutation proof: change the peerID argument at the blockValidation.ProcessBlock call site to "" and
// both deliveries record "" instead of their distinct identities, so the distinct-identity assertion
// reddens.
func TestSyncManager_ProcessBlock_PlumbsServingPeerIdentity(t *testing.T) {
	spy := &recordingBlockValidation{MockBlockValidation: &blockvalidation.MockBlockValidation{}}

	sm := &SyncManager{
		settings:        test.CreateBaseTestSettings(t),
		logger:          ulogger.TestLogger{},
		blockValidation: spy,
	}

	// A minimal block with a hashable header (ProcessBlock logs block.Hash()).
	nBits, err := model.NewNBitFromString("2000ffff")
	require.NoError(t, err)
	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
			Bits:           *nBits,
			Nonce:          0,
		},
		Height: 100,
	}

	require.NoError(t, sm.ProcessBlock(context.Background(), block, "peer-A:8333"))
	require.NoError(t, sm.ProcessBlock(context.Background(), block, "peer-B:8333"))

	require.Equal(t, []string{"peer-A:8333", "peer-B:8333"}, spy.peerIDs,
		"the legacy route must plumb each serving peer's identity to block validation, not a shared empty string")
}
