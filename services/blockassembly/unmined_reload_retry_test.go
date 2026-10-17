package blockassembly

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	utxoStore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// flakyUtxoStore wraps the real store and fails some Gets with a storage error, the way a store
// that times out looks to the unmined reload.
type flakyUtxoStore struct {
	utxoStore.Store

	// parentReadFailures is how many parent reads (Gets for fields.Utxos) still fail.
	parentReadFailures atomic.Int64
	// down fails every Get.
	down atomic.Bool
	// iteratorDown fails GetUnminedTxIterator, so a reload fails whether it validates inputs or not.
	iteratorDown atomic.Bool
}

func (s *flakyUtxoStore) GetUnminedTxIterator() (utxoStore.UnminedTxIterator, error) {
	if s.iteratorDown.Load() {
		return nil, errors.NewStorageError("[flakyUtxoStore] the store is down")
	}

	return s.Store.GetUnminedTxIterator()
}

func (s *flakyUtxoStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	if s.down.Load() {
		return nil, errors.NewStorageError("[flakyUtxoStore] the store is down")
	}

	for _, name := range f {
		if name == fields.Utxos && s.parentReadFailures.Add(-1) >= 0 {
			return nil, errors.NewStorageError("[flakyUtxoStore] parent read timed out")
		}
	}

	return s.Store.Get(ctx, hash, f...)
}

// mockGenesisChainForReset lets reset run with both the chain and block assembly at genesis, so
// it moves no blocks and only clears the subtrees and reloads the unmined transactions. The same
// set of calls TestReset_ConflictDetectionViaValidateInputs mocks.
func mockGenesisChainForReset(items *baTestItems) {
	genesisHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Bits:           model.NBit{},
		Nonce:          0,
	}
	genesisMeta := &model.BlockHeaderMeta{Height: 0, ID: 1}
	genesisHashSlice := []*chainhash.Hash{genesisHeader.Hash()}

	mockBC := &blockchain.Mock{}
	mockBC.On("GetBestBlockHeader", mock.Anything).Return(genesisHeader, genesisMeta, nil)
	mockBC.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(false, nil)
	mockBC.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return(genesisHashSlice, nil)
	mockBC.On("GetBlockHeader", mock.Anything, mock.Anything).Return(genesisHeader, genesisMeta, nil)
	mockBC.On("GetBlockHeaders", mock.Anything, mock.Anything, uint64(1)).
		Return([]*model.BlockHeader{genesisHeader}, []*model.BlockHeaderMeta{genesisMeta}, nil)
	mockBC.On("GetBlockHeaders", mock.Anything, mock.Anything, uint64(0)).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)
	mockBC.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{}, nil)
	mockBC.On("SetState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	items.blockAssembler.blockchainClient = mockBC
	items.blockAssembler.setBestBlockHeader(genesisHeader, 0)
	items.blockAssembler.subtreeProcessor.InitCurrentBlockHeader(genesisHeader)
}

// TestReset_OneTransientParentReadKeepsTheMempoolInTheTemplate is icellan's blocking point on PR
// 1663. A validating reset clears the subtrees and then reloads the unmined transactions, so a
// reload that fails leaves the mempool out of every template. One parent read that timed out
// failed the whole reload. The check is now asked again, and the transaction is in the candidate
// after the reset.
func TestReset_OneTransientParentReadKeepsTheMempoolInTheTemplate(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	items.blockAssembler.settings.BlockAssembly.OnRestartValidateParentChain = false

	_, child := seedParentAndChild(ctx, t, items)

	flaky := &flakyUtxoStore{Store: items.utxoStore}
	flaky.parentReadFailures.Store(1)
	items.blockAssembler.utxoStore = flaky

	mockGenesisChainForReset(items)

	require.NoError(t, items.blockAssembler.reset(ctx, true), "one parent read that timed out must not fail the reload")
	require.Less(t, flaky.parentReadFailures.Load(), int64(0), "the injected failure was reached")

	require.True(t, containsHash(items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx), *child.TxIDChainHash()),
		"the unmined transaction is in the candidate after the reset")
	require.False(t, items.blockAssembler.unminedReloadPending.Load())
}

// TestReset_FailedReloadIsRetriedByTheHeartbeat covers the other half: a store that stays down
// fails the reload even after the retries. Every caller of reset only logs that, so the reset
// leaves a retry behind, and the heartbeat tick queues a fresh reset once the interval has
// passed. When that reset runs against a store that answers, the mempool is back in the
// candidate.
func TestReset_FailedReloadIsRetriedByTheHeartbeat(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	items.blockAssembler.settings.BlockAssembly.OnRestartValidateParentChain = false

	_, child := seedParentAndChild(ctx, t, items)

	flaky := &flakyUtxoStore{Store: items.utxoStore}
	flaky.down.Store(true)
	items.blockAssembler.utxoStore = flaky
	items.blockAssembler.unminedReloadRetryInterval = time.Millisecond

	mockGenesisChainForReset(items)

	require.Error(t, items.blockAssembler.reset(ctx, true))
	require.False(t, containsHash(items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx), *child.TxIDChainHash()))
	require.True(t, items.blockAssembler.unminedReloadPending.Load(), "the failed reload left a retry")

	flaky.down.Store(false)
	time.Sleep(5 * time.Millisecond)

	require.True(t, items.blockAssembler.retryFailedUnminedReload(), "the heartbeat queued a reset")
	require.False(t, items.blockAssembler.retryFailedUnminedReload(), "and only one")

	// What the assembler's loop does with the request.
	req := <-items.blockAssembler.resetCh
	require.True(t, req.ValidateInputs, "the retry validates inputs, as the reset that failed did")

	_, err := items.blockAssembler.resetWithResponse(ctx, req.ValidateInputs)
	req.ErrCh <- err
	require.NoError(t, err)

	require.True(t, containsHash(items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx), *child.TxIDChainHash()),
		"the unmined transaction is back in the candidate")
}

// TestReset_ASuccessfulResetClearsTheRetry covers a reset that reloads the mempool before the
// heartbeat retry is due, for example a reorg right after the failure. The retry is then not
// needed, and queuing it would clear the subtrees and reload the mempool again for nothing. A
// reset that did not validate inputs does not stand in for a failed one that did.
func TestReset_ASuccessfulResetClearsTheRetry(t *testing.T) {
	cases := []struct {
		name          string
		failValidate  bool
		laterValidate bool
		wantRetry     bool
	}{
		{name: "a validating reset clears a validating retry", failValidate: true, laterValidate: true, wantRetry: false},
		{name: "any reset clears a retry that did not validate", failValidate: false, laterValidate: false, wantRetry: false},
		{name: "a reset that did not validate keeps a validating retry", failValidate: true, laterValidate: false, wantRetry: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			initPrometheusMetrics()

			ctx := t.Context()
			items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
			items.blockAssembler.settings.BlockAssembly.OnRestartValidateParentChain = false

			_, child := seedParentAndChild(ctx, t, items)

			flaky := &flakyUtxoStore{Store: items.utxoStore}
			flaky.iteratorDown.Store(true)
			items.blockAssembler.utxoStore = flaky
			items.blockAssembler.unminedReloadRetryInterval = time.Millisecond

			mockGenesisChainForReset(items)

			require.Error(t, items.blockAssembler.reset(ctx, tc.failValidate))
			require.True(t, items.blockAssembler.unminedReloadPending.Load(), "the failed reload left a retry")

			flaky.iteratorDown.Store(false)

			require.NoError(t, items.blockAssembler.reset(ctx, tc.laterValidate))
			require.True(t, containsHash(items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx), *child.TxIDChainHash()),
				"the later reset put the mempool back in the candidate")

			time.Sleep(5 * time.Millisecond)

			require.Equal(t, tc.wantRetry, items.blockAssembler.retryFailedUnminedReload())

			if tc.wantRetry {
				req := <-items.blockAssembler.resetCh
				require.True(t, req.ValidateInputs, "the kept retry still validates inputs")
				req.ErrCh <- nil
			}
		})
	}
}

// TestReset_ASecondFailureKeepsTheValidatingRetry covers two failed resets in a row, the first
// validating inputs and the second not. The retry still validates, as the first reset asked.
func TestReset_ASecondFailureKeepsTheValidatingRetry(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	items.blockAssembler.settings.BlockAssembly.OnRestartValidateParentChain = false

	seedParentAndChild(ctx, t, items)

	flaky := &flakyUtxoStore{Store: items.utxoStore}
	flaky.iteratorDown.Store(true)
	items.blockAssembler.utxoStore = flaky
	items.blockAssembler.unminedReloadRetryInterval = time.Millisecond

	mockGenesisChainForReset(items)

	require.Error(t, items.blockAssembler.reset(ctx, true))
	require.Error(t, items.blockAssembler.reset(ctx, false))

	time.Sleep(5 * time.Millisecond)

	require.True(t, items.blockAssembler.retryFailedUnminedReload())

	req := <-items.blockAssembler.resetCh
	require.True(t, req.ValidateInputs, "the retry validates inputs, as the first failed reset did")
	req.ErrCh <- nil
}

// TestLoadUnminedTransactions_UndecidedIsLeftOutOnlyAfterTheBound is icellan's second point. A
// transaction undecided in two loads in a row was left out, however close together the loads
// were, so two validating loads during a store outage dropped it. It is now left out only once
// it has been undecided for longer than the bound.
func TestLoadUnminedTransactions_UndecidedIsLeftOutOnlyAfterTheBound(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	_, child := seedParentAndChild(ctx, t, items)

	items.blockAssembler.utxoStore = &erroringParentUtxoStore{
		Store: items.utxoStore,
		self:  *child.TxIDChainHash(),
	}

	excluded := testutil.ToFloat64(prometheusBlockAssemblerUnminedUndecidedExcluded)

	require.Error(t, items.blockAssembler.loadUnminedTransactions(ctx, true, true))
	require.Error(t, items.blockAssembler.loadUnminedTransactions(ctx, true, true),
		"undecided in a second load moments later: the load still fails, nothing is left out")
	require.Equal(t, excluded, testutil.ToFloat64(prometheusBlockAssemblerUnminedUndecidedExcluded))

	items.blockAssembler.undecidedExcludeAfter = time.Nanosecond

	require.NoError(t, items.blockAssembler.loadUnminedTransactions(ctx, true, true),
		"undecided past the bound, with the store answering for the transaction itself: left out")
	require.Greater(t, testutil.ToFloat64(prometheusBlockAssemblerUnminedUndecidedExcluded), excluded)
}

// TestLoadUnminedTransactions_AnOutageLeavesNothingOut pins the evidence the exclusion needs. A
// load in which the store answered for no transaction at all is an outage, and leaving out every
// transaction undecided past the bound would empty the candidate. Such a load fails instead.
func TestLoadUnminedTransactions_AnOutageLeavesNothingOut(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	seedParentAndChild(ctx, t, items)

	flaky := &flakyUtxoStore{Store: items.utxoStore}
	flaky.down.Store(true)
	items.blockAssembler.utxoStore = flaky
	items.blockAssembler.undecidedExcludeAfter = time.Nanosecond

	excluded := testutil.ToFloat64(prometheusBlockAssemblerUnminedUndecidedExcluded)

	require.Error(t, items.blockAssembler.loadUnminedTransactions(ctx, true, true))
	require.Error(t, items.blockAssembler.loadUnminedTransactions(ctx, true, true),
		"the store answered nothing, so the second load fails rather than leave the mempool out")
	require.Equal(t, excluded, testutil.ToFloat64(prometheusBlockAssemblerUnminedUndecidedExcluded))
}

// TestCheckUnminedTxInputs_FailedBodyReadIsNotPermanent is icellan's third point. A record with
// no stored inpoints sends the check to the body, and a failed body read was classed permanent,
// so one timeout left the transaction out. Only a record with neither inpoints nor a body is
// permanent; a body read that failed fails the load and the next one decides it.
func TestCheckUnminedTxInputs_FailedBodyReadIsNotPermanent(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	_, child := seedParentAndChild(ctx, t, items)

	items.blockAssembler.utxoStore = &bodylessUtxoStore{Store: items.utxoStore, stripInpoints: true, refuseTxField: true}

	_, _, err := items.blockAssembler.checkUnminedTxInputs(ctx, *child.TxIDChainHash(), map[uint32]bool{})
	require.Error(t, err)
	require.False(t, isPermanentInputCheck(err), "a body read the store failed is not a record that can never be checked")

	excluded := testutil.ToFloat64(prometheusBlockAssemblerUnminedUndecidedExcluded)

	require.Error(t, items.blockAssembler.loadUnminedTransactions(ctx, true, true), "the load fails rather than leave the transaction out")
	require.Equal(t, excluded, testutil.ToFloat64(prometheusBlockAssemblerUnminedUndecidedExcluded))
}
