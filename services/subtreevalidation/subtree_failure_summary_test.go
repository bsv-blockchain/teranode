package subtreevalidation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// errorCaptureLogger records every line logged at ERROR.
type errorCaptureLogger struct {
	ulogger.Logger

	mu   sync.Mutex
	errs []string
}

func (l *errorCaptureLogger) Errorf(format string, args ...interface{}) {
	l.mu.Lock()
	l.errs = append(l.errs, fmt.Sprintf(format, args...))
	l.mu.Unlock()
	l.Logger.Errorf(format, args...)
}

func (l *errorCaptureLogger) errorLines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]string, len(l.errs))
	copy(out, l.errs)

	return out
}

// spentInputValidator fails every transaction the way the validator does when an
// input is already spent: a processing error around UTXO_SPENT, not a TX_INVALID.
type spentInputValidator struct {
	failingValidator
}

func newSpentInputFailure(tx *bt.Tx) error {
	return errors.NewProcessingError("[Validate][%s] failed to spend utxos", tx.TxIDChainHash().String(),
		errors.NewUtxoSpentError(*tx.TxIDChainHash(), 0, chainhash.Hash{}, nil))
}

func requireOneSummary(t *testing.T, lines []string, want ...string) {
	t.Helper()

	require.Len(t, lines, 1, "one ERROR line per subtree, not one per transaction: %v", lines)

	for _, w := range want {
		require.Contains(t, lines[0], w)
	}
}

// TestBlessMissingTransactions_FailuresAreSummarisedOncePerSubtree pins issue
// 2108 on the peer-announced subtree path: two transactions that both fail with a
// spent input produce one ERROR line naming the cause, its count and an example
// txid, and the returned error carries the same tally.
func TestBlessMissingTransactions_FailuresAreSummarisedOncePerSubtree(t *testing.T) {
	server, _, txs, _ := newUnlockFailureServer(t, true, 0)
	logger := &errorCaptureLogger{Logger: server.logger}
	server.logger = logger

	server.validatorClient = &spentInputValidator{failingValidator{Interface: server.validatorClient, err: newSpentInputFailure(txs[1])}}

	subtreeHash := chainhash.HashH([]byte("subtree"))
	missing := []missingTx{{tx: txs[1], idx: 0}, {tx: txs[2], idx: 1}}
	txMetaSlice := make([]metaSliceItem, len(missing))

	err := server.blessMissingTransactions(context.Background(), subtreeHash, missing, "", "", txMetaSlice, 2, map[uint32]bool{}, false)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrSpent, "the first failure is still chained as the cause")
	require.NotErrorIs(t, err, errors.ErrTxInvalid)

	example := txs[1].TxIDChainHash().String()
	require.Contains(t, err.Error(), "found 2 errors while processing subtree: UTXO_SPENT=2 (e.g. "+example+")")

	requireOneSummary(t, logger.errorLines(),
		"[validateSubtree]["+subtreeHash.String()+"] 2 of 2 missing txs failed: UTXO_SPENT=2 (e.g. "+example+")")
}

// TestProcessTransactionsInLevels_FailuresAreSummarisedOncePerBlock pins issue
// 2108 on the block path: the per-transaction ERROR line is gone, one summary per
// block names each cause with its count and an example, and the returned error
// carries the tally rather than only the first cause.
func TestProcessTransactionsInLevels_FailuresAreSummarisedOncePerBlock(t *testing.T) {
	server, _, txs, _ := newUnlockFailureServer(t, true, 0)
	logger := &errorCaptureLogger{Logger: server.logger}
	server.logger = logger

	server.validatorClient = &spentInputValidator{failingValidator{Interface: server.validatorClient, err: newSpentInputFailure(txs[1])}}

	blockHash := chainhash.HashH([]byte("block"))

	err := server.processTransactionsInLevels(context.Background(), []*bt.Tx{txs[1], txs[2]}, blockHash, chainhash.Hash{}, 100, 0, 0, map[uint32]bool{}, true)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrSpent)
	require.NotErrorIs(t, err, errors.ErrTxInvalid)

	example := txs[1].TxIDChainHash().String()
	require.Contains(t, err.Error(), "Completed processing with 2 errors (0 missing-parent): UTXO_SPENT=2 (e.g. "+example+")")

	lines := logger.errorLines()
	requireOneSummary(t, lines,
		"[processTransactionsInLevels]["+blockHash.String()+"] 2 of 2 txs failed (0 missing-parent): UTXO_SPENT=2 (e.g. "+example+")")

	for _, line := range lines {
		require.False(t, strings.Contains(line, "Processing error for transaction"), "the per-transaction ERROR line is gone: %s", line)
	}
}
