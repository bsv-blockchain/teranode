package subtreevalidation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
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

// perTxValidator fails the transactions it has an error for and passes the
// rest to the real validator.
type perTxValidator struct {
	validator.Interface
	errs map[chainhash.Hash]error
}

func (s *perTxValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts *validator.Options) (*meta.Data, error) {
	if err, ok := s.errs[*tx.TxIDChainHash()]; ok {
		return nil, err
	}

	return s.Interface.ValidateWithOptions(ctx, tx, blockHeight, opts)
}

// childOf returns a transaction that spends parent, so it sits in the level after it.
func childOf(t *testing.T, parent *bt.Tx) *bt.Tx {
	t.Helper()

	privateKey, _ := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	return transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(parent, 0),
		transactions.WithP2PKHOutputs(1, 500),
	)
}

func breakerOpen(tx *bt.Tx) error {
	return errors.NewProcessingError("[Validate][%s] failed to spend utxos", tx.TxIDChainHash().String(),
		errors.NewServiceUnavailableError("[SPEND] circuit breaker open, rejecting request"))
}

// TestBlessMissingTransactions_FailuresAreSummarisedOncePerSubtree pins issue
// 2108 on the block subtree path (keepAside=false): two transactions that both
// fail with a spent input produce one ERROR line from this function naming the
// cause, its count and an example txid, and the returned error carries the same
// tally. On the peer path the handler logs that returned error as a second line.
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
// call (one load batch of the block) names each cause with its count and an
// example, and the returned error carries the tally rather than only the first
// cause.
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
		"[processTransactionsInLevels]["+blockHash.String()+"] 2 of 2 txs in this batch failed (0 missing-parent): UTXO_SPENT=2 (e.g. "+example+")")

	for _, line := range lines {
		require.False(t, strings.Contains(line, "Processing error for transaction"), "the per-transaction ERROR line is gone: %s", line)
	}
}

// A truly invalid transaction ends the subtree early. The store fault tallied on
// its sibling before that had its own ERROR line before the tally, so the summary
// must still be logged on that exit.
func TestBlessMissingTransactions_SummaryIsLoggedWhenAnInvalidTxEndsTheSubtree(t *testing.T) {
	server, _, txs, _ := newUnlockFailureServer(t, true, 0)
	logger := &errorCaptureLogger{Logger: server.logger}
	server.logger = logger

	server.validatorClient = &perTxValidator{Interface: server.validatorClient, errs: map[chainhash.Hash]error{
		*txs[1].TxIDChainHash(): breakerOpen(txs[1]),
		*txs[2].TxIDChainHash(): errors.NewTxInvalidError("[Validate][%s] script failed", txs[2].TxIDChainHash().String()),
	}}

	subtreeHash := chainhash.HashH([]byte("subtree"))
	missing := []missingTx{{tx: txs[1], idx: 0}, {tx: txs[2], idx: 1}}
	txMetaSlice := make([]metaSliceItem, len(missing))

	err := server.blessMissingTransactions(context.Background(), subtreeHash, missing, "", "", txMetaSlice, 2, map[uint32]bool{}, false)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrTxInvalid, "the invalid transaction still ends the subtree")

	requireOneSummary(t, logger.errorLines(),
		"[validateSubtree]["+subtreeHash.String()+"] 2 of 2 missing txs failed: ",
		"SERVICE_UNAVAILABLE=1 (e.g. "+txs[1].TxIDChainHash().String()+")",
		"TX_INVALID=1 (e.g. "+txs[2].TxIDChainHash().String()+")")
}

// A partial failure: one transaction passes, one hits a store fault and one is
// missing its parent. The summary carries the right denominator and both causes.
func TestBlessMissingTransactions_MixedCausesAreTalliedAgainstAllMissingTxs(t *testing.T) {
	server, _, txs, _ := newUnlockFailureServer(t, true, 0)
	child := childOf(t, txs[2])
	logger := &errorCaptureLogger{Logger: server.logger}
	server.logger = logger

	server.validatorClient = &perTxValidator{Interface: server.validatorClient, errs: map[chainhash.Hash]error{
		*txs[2].TxIDChainHash(): breakerOpen(txs[2]),
		*child.TxIDChainHash():  errors.NewTxMissingParentError("[Validate][%s] parent not found", child.TxIDChainHash().String()),
	}}

	subtreeHash := chainhash.HashH([]byte("subtree"))
	missing := []missingTx{{tx: txs[1], idx: 0}, {tx: txs[2], idx: 1}, {tx: child, idx: 2}}
	txMetaSlice := make([]metaSliceItem, len(missing))

	err := server.blessMissingTransactions(context.Background(), subtreeHash, missing, "", "", txMetaSlice, 2, map[uint32]bool{}, false)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrServiceUnavailable, "the first failure is chained as the cause")
	require.True(t, txMetaSlice[0].isSet, "the passing transaction is blessed")

	requireOneSummary(t, logger.errorLines(),
		"[validateSubtree]["+subtreeHash.String()+"] 2 of 3 missing txs failed: ",
		"SERVICE_UNAVAILABLE=1 (e.g. "+txs[2].TxIDChainHash().String()+")",
		"TX_MISSING_PARENT=1 (e.g. "+child.TxIDChainHash().String()+")")
}

// When every failure is a missing parent the subtree still fails, but that is the
// expected ordering case the block path resolves, so it is not an ERROR.
func TestBlessMissingTransactions_AllMissingParentsIsNotAnError(t *testing.T) {
	server, _, txs, _ := newUnlockFailureServer(t, true, 0)
	logger := &errorCaptureLogger{Logger: server.logger}
	server.logger = logger

	server.validatorClient = &perTxValidator{Interface: server.validatorClient, errs: map[chainhash.Hash]error{
		*txs[1].TxIDChainHash(): errors.NewTxMissingParentError("[Validate][%s] parent not found", txs[1].TxIDChainHash().String()),
		*txs[2].TxIDChainHash(): errors.NewTxMissingParentError("[Validate][%s] parent not found", txs[2].TxIDChainHash().String()),
	}}

	subtreeHash := chainhash.HashH([]byte("subtree"))
	missing := []missingTx{{tx: txs[1], idx: 0}, {tx: txs[2], idx: 1}}
	txMetaSlice := make([]metaSliceItem, len(missing))

	err := server.blessMissingTransactions(context.Background(), subtreeHash, missing, "", "", txMetaSlice, 2, map[uint32]bool{}, false)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrTxMissingParent)
	require.Contains(t, err.Error(), "found 2 errors while processing subtree: TX_MISSING_PARENT=2 (e.g. "+txs[1].TxIDChainHash().String()+")")

	require.Empty(t, logger.errorLines(), "all-missing-parent is logged at Info, not ERROR")
}

// The block path's early exit on an invalid transaction must log the summary too.
func TestProcessTransactionsInLevels_SummaryIsLoggedWhenAnInvalidTxEndsTheLevel(t *testing.T) {
	server, _, txs, _ := newUnlockFailureServer(t, true, 0)
	logger := &errorCaptureLogger{Logger: server.logger}
	server.logger = logger

	server.validatorClient = &perTxValidator{Interface: server.validatorClient, errs: map[chainhash.Hash]error{
		*txs[1].TxIDChainHash(): breakerOpen(txs[1]),
		*txs[2].TxIDChainHash(): errors.NewTxInvalidError("[Validate][%s] script failed", txs[2].TxIDChainHash().String()),
	}}

	blockHash := chainhash.HashH([]byte("block"))

	err := server.processTransactionsInLevels(context.Background(), []*bt.Tx{txs[1], txs[2]}, blockHash, chainhash.Hash{}, 100, 0, 0, map[uint32]bool{}, true)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrTxInvalid)

	requireOneSummary(t, logger.errorLines(),
		"[processTransactionsInLevels]["+blockHash.String()+"] 2 of 2 txs in this batch failed (0 missing-parent): ",
		"SERVICE_UNAVAILABLE=1 (e.g. "+txs[1].TxIDChainHash().String()+")",
		"TX_INVALID=1 (e.g. "+txs[2].TxIDChainHash().String()+")")
}

// A partial failure on the block path: the summary carries the batch denominator,
// the missing-parent count and both causes.
func TestProcessTransactionsInLevels_MixedCausesAreTalliedAgainstTheBatch(t *testing.T) {
	server, _, txs, _ := newUnlockFailureServer(t, true, 0)
	child := childOf(t, txs[2])
	logger := &errorCaptureLogger{Logger: server.logger}
	server.logger = logger

	server.validatorClient = &perTxValidator{Interface: server.validatorClient, errs: map[chainhash.Hash]error{
		*txs[2].TxIDChainHash(): breakerOpen(txs[2]),
		*child.TxIDChainHash():  errors.NewTxMissingParentError("[Validate][%s] parent not found", child.TxIDChainHash().String()),
	}}

	blockHash := chainhash.HashH([]byte("block"))

	err := server.processTransactionsInLevels(context.Background(), []*bt.Tx{txs[1], txs[2], child}, blockHash, chainhash.Hash{}, 100, 0, 0, map[uint32]bool{}, true)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	require.Contains(t, err.Error(), "Completed processing with 2 errors (1 missing-parent): ")

	requireOneSummary(t, logger.errorLines(),
		"[processTransactionsInLevels]["+blockHash.String()+"] 2 of 3 txs in this batch failed (1 missing-parent): ",
		"SERVICE_UNAVAILABLE=1 (e.g. "+txs[2].TxIDChainHash().String()+")",
		"TX_MISSING_PARENT=1 (e.g. "+child.TxIDChainHash().String()+")")
}
