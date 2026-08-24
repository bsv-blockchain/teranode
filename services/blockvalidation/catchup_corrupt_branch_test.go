package blockvalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// catchupReportRecorder is a P2PClientI that records the peer-reputation calls releaseCatchupLock
// makes after it classifies a terminal catchup error, so a test can assert the classification took
// the corrupt-body branch (no malicious report, no generic peer-error window) rather than the
// generic peer-error default.
type catchupReportRecorder struct {
	P2PClientI

	mu             sync.Mutex
	malicious      []string
	genericError   []string
	failureKinds   []string
	failureHashes  []string
	failurePeerIDs []string
}

func (m *catchupReportRecorder) RecordCatchupMalicious(_ context.Context, peerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.malicious = append(m.malicious, peerID)

	return nil
}

func (m *catchupReportRecorder) UpdateCatchupError(_ context.Context, peerID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.genericError = append(m.genericError, peerID)

	return nil
}

// RecordCatchupFailureWithKind records the dedicated failure-kind calls releaseCatchupLock makes
// (bitcoin-sv/teranode#4692) — the incomplete-block and corrupt-body reputation signals both route
// through here, distinct from the generic malicious/peer-error reports above.
func (m *catchupReportRecorder) RecordCatchupFailureWithKind(_ context.Context, peerID, failureKind, blockHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failurePeerIDs = append(m.failurePeerIDs, peerID)
	m.failureKinds = append(m.failureKinds, failureKind)
	m.failureHashes = append(m.failureHashes, blockHash)

	return nil
}

func (m *catchupReportRecorder) maliciousReported() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.malicious))
	copy(out, m.malicious)

	return out
}

func (m *catchupReportRecorder) genericErrorReported() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.genericError))
	copy(out, m.genericError)

	return out
}

func (m *catchupReportRecorder) failuresWithKindReported() ([]string, []string, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	peerIDs := make([]string, len(m.failurePeerIDs))
	copy(peerIDs, m.failurePeerIDs)
	kinds := make([]string, len(m.failureKinds))
	copy(kinds, m.failureKinds)
	hashes := make([]string, len(m.failureHashes))
	copy(hashes, m.failureHashes)

	return peerIDs, kinds, hashes
}

// newReleaseCatchupBlock builds a minimal, well-formed block to stand in as the catchup target
// (releaseCatchupLock only reads its Hash()/Height for the dashboard record).
func newReleaseCatchupBlock(t *testing.T) *model.Block {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x01, 0x00, 0x00})

	nBits, _ := model.NewNBitFromString("2000ffff")
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: coinbaseTx.TxIDChainHash(),
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
	}

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}

// TestReleaseCatchupLock_CorruptBodyClassifiedNonPeer proves that releaseCatchupLock's error
// classification distinguishes a corrupt block body from a consensus-invalid one
// (bitcoin-sv/teranode#4692). A corrupt terminal error must classify as "corrupt_block_body",
// which is NOT a generic peer error: the serving peer was already struck at the corrupt site and
// an honest relay can forward a corrupted body, so releaseCatchupLock must neither report it
// malicious nor open a generic peer-error window. It still feeds catch-up peer selection a
// genuine, correctly-labelled signal via the dedicated corrupt_block_body failure kind — the
// generic-peer-error suppression above would otherwise leave selection with zero signal at all.
// The positive control confirms a genuine ErrBlockInvalid does the opposite (classified
// "validation_failure", peer reported malicious), so the corrupt case is a real, distinct
// decision — not a branch that can never fire.
//
// Mutation proof: deleting `case errors.IsBlockCorrupt(*err)` from the classification switch drops a
// corrupt error to the switch defaults (errorType "unknown_error", isPeerError true), which reddens
// both the ErrorType assertion and the "no generic peer-error report" assertion below.
func TestReleaseCatchupLock_CorruptBodyClassifiedNonPeer(t *testing.T) {
	t.Run("corrupt body is non-peer, not malicious, but feeds peer selection", func(t *testing.T) {
		rec := &catchupReportRecorder{}
		u := &Server{logger: ulogger.TestLogger{}, p2pClient: rec}

		block := newReleaseCatchupBlock(t)
		catchupCtx := &CatchupContext{
			blockUpTo:        block,
			baseURL:          "http://peer",
			peerID:           "peer-corrupt",
			startTime:        time.Now(),
			corruptBlockHash: block.Hash().String(),
		}

		corruptErr := error(errors.NewBlockCorruptError("[BLOCK] body is corrupt"))
		u.releaseCatchupLock(catchupCtx, &corruptErr)

		require.NotNil(t, u.previousCatchupAttempt)
		require.Equal(t, "corrupt_block_body", u.previousCatchupAttempt.ErrorType,
			"a corrupt body must classify as corrupt_block_body, not the generic peer-error default")
		require.Empty(t, rec.maliciousReported(),
			"a corrupt body must NOT flag the serving peer malicious (bitcoin-sv/teranode#4692)")
		require.Empty(t, rec.genericErrorReported(),
			"a corrupt body must NOT open a generic peer-error window (isPeerError=false)")

		peerIDs, kinds, hashes := rec.failuresWithKindReported()
		require.Equal(t, []string{"peer-corrupt"}, peerIDs,
			"a corrupt body must feed catch-up peer selection via RecordCatchupFailureWithKind")
		require.Equal(t, []string{catchupFailureKindCorruptBlockBody}, kinds)
		require.Equal(t, []string{block.Hash().String()}, hashes)
	})

	t.Run("consensus-invalid body is malicious (positive control)", func(t *testing.T) {
		rec := &catchupReportRecorder{}
		u := &Server{logger: ulogger.TestLogger{}, p2pClient: rec}

		block := newReleaseCatchupBlock(t)
		catchupCtx := &CatchupContext{
			blockUpTo: block,
			baseURL:   "http://peer",
			peerID:    "peer-invalid",
			startTime: time.Now(),
		}

		invalidErr := error(errors.NewBlockInvalidError("[BLOCK] block violates consensus"))
		u.releaseCatchupLock(catchupCtx, &invalidErr)

		require.NotNil(t, u.previousCatchupAttempt)
		require.Equal(t, "validation_failure", u.previousCatchupAttempt.ErrorType,
			"a consensus-invalid body must classify as validation_failure")
		require.Equal(t, []string{"peer-invalid"}, rec.maliciousReported(),
			"a consensus-invalid body must flag the serving peer malicious")
	})
}
