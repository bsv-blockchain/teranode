package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// maliciousReportRecorder is a P2PClientI that records RecordCatchupMalicious calls, so a test can
// assert whether releaseCatchup / the catchup validation loop flagged a peer malicious.
type maliciousReportRecorder struct {
	P2PClientI

	mu    sync.Mutex
	peers []string
}

func (m *maliciousReportRecorder) RecordCatchupMalicious(_ context.Context, peerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers = append(m.peers, peerID)

	return nil
}

func (m *maliciousReportRecorder) recorded() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.peers))
	copy(out, m.peers)

	return out
}

// TestValidateBlocksOnChannel_CorruptBody_NotReportedMalicious proves the catchup corrupt branch
// (bitcoin-sv/teranode#4692): when a block delivered during catchup fails with a corrupt-body error,
// validateBlocksOnChannel must NOT flag the serving peer malicious — the peer was already struck via
// AddBanScore at the corrupt site, the block was not stored invalid, and an honest relay can forward
// a corrupted body, so re-download from another peer is the correct handling. Contrast: a genuine
// consensus-invalid block IS reported malicious, proving the corrupt branch is a distinct decision.
func TestValidateBlocksOnChannel_CorruptBody_NotReportedMalicious(t *testing.T) {
	// A block whose received body fails the outer coinbase-length check (1-byte scriptSig < 2) is
	// classified ERR_BLOCK_CORRUPT by ValidateBlockWithOptions — an unbound-body defect, produced
	// without any store round-trip.
	newCorruptBlock := func(t *testing.T) *model.Block {
		t.Helper()

		coinbaseTx := bt.NewTx()
		require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
		coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00}) // 1 byte < 2 -> corrupt

		nBits, _ := model.NewNBitFromString("2000ffff")
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
			Bits:           *nBits,
		}

		block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
		require.NoError(t, err)

		return block
	}

	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	suite.MockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()

	fake := &maliciousReportRecorder{}
	suite.Server.p2pClient = fake

	block := newCorruptBlock(t)

	catchupCtx := &CatchupContext{
		blockUpTo:          block,
		baseURL:            "http://peer",
		peerID:             "peer-corrupt",
		startTime:          time.Now(),
		useQuickValidation: false, // force the normal (non-quick) validation path
	}

	validateBlocksChan := make(chan blockForValidation, 1)
	validateBlocksChan <- blockForValidation{block: block}
	close(validateBlocksChan)

	var size atomic.Int64
	size.Store(1)

	err := suite.Server.validateBlocksOnChannel(validateBlocksChan, context.Background(), catchupCtx, &size, nil)

	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "the corrupt-body error must propagate out of the catchup loop, got: %v", err)
	require.Empty(t, fake.recorded(), "a corrupt block body must NOT flag the serving peer malicious (bitcoin-sv/teranode#4692)")
}
