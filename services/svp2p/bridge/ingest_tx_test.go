package bridge

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// makeIngestTestTx builds a real, well-formed bt.Tx (own inputs/outputs
// invented per-call so hashes differ), mirroring handle_block_test.go's own
// makeTxMap/makeSameBlockParentChainTxMap helpers. What matters for IngestTx
// is only that it decodes and hashes deterministically, not that its inputs
// resolve to anything real: validationClient is the seam under test here,
// not the UTXO store (IngestTx never touches sm.utxoStore or
// sm.blockchainClient directly — see bridge.go's Bridge interface doc).
func makeIngestTestTx(t *testing.T, seed string) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.Version = 1

	require.NoError(t, tx.From(chainhash.HashH([]byte(seed)).String(), 0, "76a914", uint64(1_000_000)))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(900_000)))

	return tx
}

// recordingValidator wraps validator.MockValidator's ValidateFunc with a
// thread-safe log of every tx hash it was called with, so a test can prove
// exactly which transactions reached the validator — the barrier F4 requires
// for the "already rejected, no validator contact" case: a call count of
// zero for the suppressed tx does not by itself prove the short-circuit
// path ran rather than the test never reaching the code at all, so the test
// below pairs it with a fresh tx and asserts the recorded calls are exactly
// what was expected, not merely absent.
type recordingValidator struct {
	*validator.MockValidator

	mu    sync.Mutex
	calls []chainhash.Hash
}

func newRecordingValidator(respond func(hash chainhash.Hash) (*meta.Data, error)) *recordingValidator {
	rv := &recordingValidator{MockValidator: &validator.MockValidator{}}

	rv.MockValidator.ValidateFunc = func(_ context.Context, tx *bt.Tx) (*meta.Data, error) {
		hash := *tx.TxIDChainHash()

		rv.mu.Lock()
		rv.calls = append(rv.calls, hash)
		rv.mu.Unlock()

		return respond(hash)
	}

	return rv
}

func (rv *recordingValidator) callCount() int {
	rv.mu.Lock()
	defer rv.mu.Unlock()

	return len(rv.calls)
}

func (rv *recordingValidator) called(hash chainhash.Hash) bool {
	rv.mu.Lock()
	defer rv.mu.Unlock()

	for _, h := range rv.calls {
		if h == hash {
			return true
		}
	}

	return false
}

// newIngestTxTestBridge builds the minimal svp2pBridge IngestTx actually
// touches: validationClient and the bounded rejectedTxns set it owns. Every
// other field (utxoStore, blockchainClient, subtreeStore, ...) is left at
// its zero value deliberately: IngestTx's contract (bridge.go) never reads
// them, and the "real stores" testing convention (AGENTS.md) governs the
// stores IngestTx actually depends on — here, none.
func newIngestTxTestBridge(v validator.Interface) *svp2pBridge {
	return &svp2pBridge{
		logger:           ulogger.TestLogger{},
		validationClient: v,
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](maxRejectedTxns),
	}
}

func TestIngestTx_Accepted(t *testing.T) {
	tx := makeIngestTestTx(t, "accepted-tx")
	txHash := *tx.TxIDChainHash()

	rv := newRecordingValidator(func(chainhash.Hash) (*meta.Data, error) {
		return &meta.Data{Fee: 1234, SizeInBytes: 250}, nil
	})

	sm := newIngestTxTestBridge(rv.MockValidator)

	result, err := sm.IngestTx(t.Context(), tx.Bytes(), "peer1:8333")
	require.NoError(t, err)
	require.True(t, result.Accepted)
	require.False(t, result.Orphan)
	require.Nil(t, result.Reject)
	require.Equal(t, txHash, result.TxHash)
	require.Equal(t, uint64(1234), result.Fee)
	require.Equal(t, uint64(250), result.Size)

	// An accepted tx must never land in the rejected set.
	_, rejected := sm.rejectedTxns.Get(txHash)
	require.False(t, rejected)
}

func TestIngestTx_MissingParentClassifiesOrphan(t *testing.T) {
	tx := makeIngestTestTx(t, "orphan-tx")
	txHash := *tx.TxIDChainHash()

	rv := newRecordingValidator(func(chainhash.Hash) (*meta.Data, error) {
		return nil, errors.ErrTxMissingParent
	})

	sm := newIngestTxTestBridge(rv.MockValidator)

	result, err := sm.IngestTx(t.Context(), tx.Bytes(), "peer1:8333")
	require.NoError(t, err)
	require.False(t, result.Accepted)
	require.True(t, result.Orphan)
	require.Nil(t, result.Reject)
	require.Equal(t, txHash, result.TxHash)

	// Legacy never rejects an orphan (netsync/manager.go:1256-1273): the
	// parent may still arrive, so the tx must not be short-circuited later.
	_, rejected := sm.rejectedTxns.Get(txHash)
	require.False(t, rejected)
}

func TestIngestTx_InvalidTxReturnsRejectAndIsRecorded(t *testing.T) {
	tx := makeIngestTestTx(t, "invalid-tx")
	txHash := *tx.TxIDChainHash()

	rv := newRecordingValidator(func(chainhash.Hash) (*meta.Data, error) {
		return nil, errors.ErrTxInvalid
	})

	sm := newIngestTxTestBridge(rv.MockValidator)

	result, err := sm.IngestTx(t.Context(), tx.Bytes(), "peer1:8333")
	require.NoError(t, err)
	require.False(t, result.Accepted)
	require.False(t, result.Orphan)
	require.NotNil(t, result.Reject)
	require.Equal(t, wire.CmdTx, result.Reject.Cmd)
	require.Equal(t, wire.RejectInvalid, result.Reject.Code)
	require.Equal(t, txHash, result.Reject.Hash)

	_, rejected := sm.rejectedTxns.Get(txHash)
	require.True(t, rejected)
}

// TestIngestTx_RejectedTxIgnoredWithoutValidatorContact is F4's trap test:
// re-sending an already-rejected tx must short-circuit before the validator
// is ever called. A bare assertion that the call count stayed at zero cannot
// tell "correctly short-circuited" apart from "the test never drove the code
// at all" (F4), so this test pairs the negative with a positive: it re-sends
// the rejected tx AND a fresh, valid one in the same run, and asserts the
// validator saw exactly the fresh one.
func TestIngestTx_RejectedTxIgnoredWithoutValidatorContact(t *testing.T) {
	rejectedTx := makeIngestTestTx(t, "already-rejected-tx")
	rejectedHash := *rejectedTx.TxIDChainHash()

	freshTx := makeIngestTestTx(t, "fresh-tx")
	freshHash := *freshTx.TxIDChainHash()

	rv := newRecordingValidator(func(hash chainhash.Hash) (*meta.Data, error) {
		if hash == freshHash {
			return &meta.Data{Fee: 500, SizeInBytes: 225}, nil
		}

		return nil, errors.ErrTxInvalid
	})

	sm := newIngestTxTestBridge(rv.MockValidator)

	// First arrival: genuinely rejected, and recorded.
	first, err := sm.IngestTx(t.Context(), rejectedTx.Bytes(), "peer1:8333")
	require.NoError(t, err)
	require.NotNil(t, first.Reject)
	require.Equal(t, 1, rv.callCount())

	// Re-arrival of the same, now-rejected tx: must be ignored silently
	// (netsync/manager.go:1217-1223, "do not send a reject message here
	// because ... the transaction was unsolicited") and must not reach the
	// validator a second time.
	second, err := sm.IngestTx(t.Context(), rejectedTx.Bytes(), "peer2:8333")
	require.NoError(t, err)
	require.False(t, second.Accepted)
	require.False(t, second.Orphan)
	require.Nil(t, second.Reject)
	require.Equal(t, rejectedHash, second.TxHash)

	// The barrier: a fresh tx arriving in the same run IS processed, proving
	// the pipeline is alive and the suppression above was not just an
	// unreached code path.
	third, err := sm.IngestTx(t.Context(), freshTx.Bytes(), "peer2:8333")
	require.NoError(t, err)
	require.True(t, third.Accepted)
	require.Equal(t, freshHash, third.TxHash)

	require.Equal(t, 2, rv.callCount(), "validator must be called exactly once for the rejected tx and once for the fresh one")
	require.True(t, rv.called(rejectedHash))
	require.True(t, rv.called(freshHash))
}
