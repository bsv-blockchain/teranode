package blockvalidation

import (
	"context"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// conflictBlockFixture is a block whose one subtree is fetched fresh and carries
// one transaction the UTXO store holds as conflicting: it spends parent:0, whose
// recorded spender is counter.
type conflictBlockFixture struct {
	utxoStore   utxostore.Store
	bv          *BlockValidation
	block       *model.Block
	parent      *bt.Tx
	counter     *bt.Tx
	conflicting *bt.Tx
}

// p2pkhTx builds a tx with one P2PKH output, spending the given outputs. salt
// keeps otherwise identical txs apart.
func p2pkhTx(t *testing.T, salt uint32, spends ...*bt.Tx) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = salt

	for _, parent := range spends {
		require.NoError(t, tx.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		tx.Inputs[len(tx.Inputs)-1].UnlockingScript = bscript.NewFromBytes([]byte{})
	}

	require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	return tx
}

// spendAndStore stores tx as the recorded spender of its inputs.
func spendAndStore(t *testing.T, ctx context.Context, store utxostore.Store, tx *bt.Tx, opts ...utxostore.CreateOption) {
	t.Helper()

	_, _, err := store.SpendAndCreate(ctx, tx, 1, utxostore.WithSpendOnly(), utxostore.WithIgnoreConflicting(true))
	require.NoError(t, err)

	_, _, err = store.SpendAndCreate(ctx, tx, 1, append([]utxostore.CreateOption{utxostore.WithCreateOnly()}, opts...)...)
	require.NoError(t, err)
}

// conflictFixtureOptions varies the fixture.
type conflictFixtureOptions struct {
	// counterOpts are the counter-spender's create options.
	counterOpts []utxostore.CreateOption
	// configure adjusts the settings of the subtree validation server.
	configure []func(*settings.Settings)
	// wrap, when set, is how the subtree validation server reads the UTXO store.
	wrap func(utxostore.Store) utxostore.Store
	// spendVout is the parent output the conflicting tx spends. The parent has
	// one output, so any other index is out of range.
	spendVout uint32
}

// newConflictBlockFixture builds the fixture on a fresh setup. The parent is
// mined in genesis, the candidate block's parent, so a counter-spender stored
// with mined is mined on the candidate chain.
func newConflictBlockFixture(t *testing.T, opts conflictFixtureOptions) *conflictBlockFixture {
	t.Helper()

	initPrometheusMetrics()

	ctx := context.Background()

	// the subtree validation server's chain must hold the candidate's parent, so
	// it can resolve the candidate chain's block ids
	syntheticPoW := func(s *settings.Settings) {
		s.ChainCfgParams = test.MainNetParamsForSyntheticPoW()
		s.ChainCfgParams.DaaForkHeight = 0
	}

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setupWithSubtreeValidationStore(t, opts.wrap, append([]func(*settings.Settings){syntheticPoW}, opts.configure...)...)
	t.Cleanup(deferFunc)

	inGenesis := utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0})

	parent := p2pkhTx(t, 7001)
	_, _, err := utxoStore.SpendAndCreate(ctx, parent, 0, inGenesis, utxostore.WithCreateOnly())
	require.NoError(t, err)

	counter := p2pkhTx(t, 7002, parent)
	spendAndStore(t, ctx, utxoStore, counter, opts.counterOpts...)

	conflicting := p2pkhTx(t, 7003, parent)
	conflicting.Inputs[0].PreviousTxOutIndex = opts.spendVout
	_, _, err = utxoStore.SpendAndCreate(ctx, conflicting, 0, utxostore.WithConflicting(true), utxostore.WithCreateOnly())
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*conflicting.TxIDChainHash(), 100, uint64(conflicting.Size()))) //nolint:gosec

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(conflicting, 1))

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))
	httpmock.RegisterResponder("GET", `=~^/subtree_data/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, subtreeDataBytes))

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000)

	replicatedSubtree := subtree.Duplicate()
	replicatedSubtree.ReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec

	tSettings := test.CreateBaseTestSettings(t)
	syntheticPoW(tSettings)
	tSettings.GlobalBlockHeightRetention = uint32(0)

	nBits, _ := model.NewNBitFromString("207fffff")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: replicatedSubtree.RootHash(),
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	// height 100 is below mainnet's lowest checkpoint, so difficulty is skipped
	block, err := model.NewBlock(blockHeader, coinbase, []*chainhash.Hash{subtree.RootHash()}, uint64(subtree.Length()), 123123, 100, 0) //nolint:gosec
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	return &conflictBlockFixture{
		utxoStore:   utxoStore,
		bv:          NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient),
		block:       block,
		parent:      parent,
		counter:     counter,
		conflicting: conflicting,
	}
}

// A block whose fresh subtree carries a conflicting tx with a counter-spender
// mined on the candidate chain is rejected, end to end. Subtree validation proves
// the conflict and returns TX_INVALID. Its fresh path used to wrap that in
// CONTEXT_CANCELED, as it wraps every failure there, and block validation then
// took the verdict for this node's own cancellation: the Kafka path queued the
// block for another retry (isRecoverableBlockMessageError) and catch-up scored it
// local_context_cancelled, so the block was retried indefinitely. The other
// failures of that path stay this node's own, wrapped or not.
func TestValidateBlock_ProvenConflictIsNotACancellation(t *testing.T) {
	ctx := context.Background()

	t.Run("a mined counter-spender rejects the block", func(t *testing.T) {
		f := newConflictBlockFixture(t, conflictFixtureOptions{counterOpts: []utxostore.CreateOption{utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0})}})

		subtreeErr := f.bv.validateBlockSubtrees(ctx, f.block, "", "http://localhost:8000", false)
		require.ErrorIs(t, subtreeErr, errors.ErrTxInvalid)
		require.NotErrorIs(t, subtreeErr, errors.ErrContextCanceled)
		require.False(t, isRecoverableBlockMessageError(subtreeErr))

		err := f.bv.ValidateBlock(ctx, f.block, "http://localhost:8000")
		require.ErrorIs(t, err, errors.ErrTxInvalid)
		require.True(t, errors.IsBlockCorrupt(err), "an invalid tx in an unbound subtree list is a corrupt-body verdict")
		require.NotErrorIs(t, err, errors.ErrContextCanceled)
		require.False(t, errors.IsContextError(err))

		require.False(t, isRecoverableBlockMessageError(err), "the Kafka path must not queue the block for another retry")
		require.False(t, isLocalCatchupFault(err), "catch-up must not score it as this node's own fault")
		require.True(t, isUnboundTxInvalidVerdict(err), "catch-up scores it as a consensus rejection")

		require.Equal(t, "validation_failure", releasedCatchupErrorType(t, f.block, err), "not local_context_cancelled")
	})

	// Every other failure of the conflict check, and of the decorate batches
	// before it, is this node's own: block validation retries the block and never
	// charges the peer for it. Each class is checked as the Kafka path and
	// catch-up classify it. A missing record and a TX_INVALID under another code
	// keep the CONTEXT_CANCELED wrap, which is what keeps them local on catch-up
	// (see settledError); the rest leave without it.
	for _, tc := range []struct {
		name string
		// build arranges the failure on a fresh fixture
		build func(t *testing.T) *conflictBlockFixture
		code  *errors.Error
		// cancelWrap is whether the failure still carries CONTEXT_CANCELED
		cancelWrap bool
		// corrupt is whether ValidateBlock returns a corrupt-body verdict
		corrupt bool
		// catchupLabel is the error type releaseCatchupLock records
		catchupLabel string
	}{
		{
			name: "a pruned parent",
			build: func(t *testing.T) *conflictBlockFixture {
				f := newConflictBlockFixture(t, conflictFixtureOptions{})
				require.NoError(t, f.utxoStore.Delete(ctx, f.parent.TxIDChainHash()))

				return f
			},
			code:         errors.ErrTxNotFound,
			cancelWrap:   true,
			catchupLabel: "local_context_cancelled",
		},
		{
			name: "a walk over its budget",
			build: func(t *testing.T) *conflictBlockFixture {
				// two roots (the conflicting tx and its counter-spender): a budget
				// of 2, against a counter-spender cone of 3
				f := newConflictBlockFixture(t, conflictFixtureOptions{configure: []func(*settings.Settings){func(s *settings.Settings) {
					s.UtxoStore.ConflictingChildrenMaxNodes = 1
				}}})

				prev := f.counter
				for i := uint32(0); i < 3; i++ {
					child := p2pkhTx(t, 7100+i, prev)
					spendAndStore(t, ctx, f.utxoStore, child)
					prev = child
				}

				return f
			},
			code:         errors.ErrUtxoWalkLimitExceeded,
			catchupLabel: "local_processing_fault",
		},
		{
			name: "a store fault in the conflict check",
			build: func(t *testing.T) *conflictBlockFixture {
				return newFaultyConflictBlockFixture(t, errors.NewStorageError("store down"), false)
			},
			code:         errors.ErrStorageError,
			catchupLabel: "local_storage_fault",
		},
		{
			name: "a store fault in a decorate batch",
			build: func(t *testing.T) *conflictBlockFixture {
				return newFaultyConflictBlockFixture(t, errors.NewStorageError("store down"), true)
			},
			code:         errors.ErrStorageError,
			catchupLabel: "local_storage_fault",
		},
		{
			// ctx is live: a store fault is not a cancellation for naming a deadline
			name: "a store fault naming a deadline in the conflict check",
			build: func(t *testing.T) *conflictBlockFixture {
				return newFaultyConflictBlockFixture(t, errors.NewStorageError("store down", context.DeadlineExceeded), false)
			},
			code:         errors.ErrStorageError,
			catchupLabel: "local_storage_fault",
		},
		{
			name: "a store fault naming a deadline in a decorate batch",
			build: func(t *testing.T) *conflictBlockFixture {
				return newFaultyConflictBlockFixture(t, errors.NewStorageError("store down", context.DeadlineExceeded), true)
			},
			code:         errors.ErrStorageError,
			catchupLabel: "local_storage_fault",
		},
		{
			name: "a frozen output in a counter-spender's cone",
			build: func(t *testing.T) *conflictBlockFixture {
				f := newConflictBlockFixture(t, conflictFixtureOptions{})

				utxoHash, err := util.UTXOHashFromOutput(f.counter.TxIDChainHash(), f.counter.Outputs[0], 0)
				require.NoError(t, err)
				require.NoError(t, f.utxoStore.FreezeUTXOs(ctx, []*utxostore.Spend{{TxID: f.counter.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash}}, test.CreateBaseTestSettings(t)))

				return f
			},
			code:         errors.ErrProcessing,
			catchupLabel: "local_processing_fault",
		},
		{
			name: "an input out of range of its parent's outputs",
			build: func(t *testing.T) *conflictBlockFixture {
				return newConflictBlockFixture(t, conflictFixtureOptions{spendVout: 5})
			},
			code:         errors.ErrProcessing,
			catchupLabel: "local_processing_fault",
		},
		{
			// A record that was read but does not decode, which the aerospike
			// store returns as TX_INVALID (classifyRecordError). ValidateBlock's
			// invalid-tx branch already returns a corrupt verdict for it, as
			// before: its infrastructure check looks for context.Canceled, not
			// the CONTEXT_CANCELED code. The wrap is what keeps the Kafka path
			// retrying it and catch-up from reporting the peer malicious.
			name: "a TX_INVALID under another code",
			build: func(t *testing.T) *conflictBlockFixture {
				return newFaultyConflictBlockFixture(t, errors.NewTxInvalidError("could not process tx inpoints"), false)
			},
			code:         errors.ErrTxInvalid,
			cancelWrap:   true,
			corrupt:      true,
			catchupLabel: "local_context_cancelled",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.build(t)

			subtreeErr := f.bv.validateBlockSubtrees(ctx, f.block, "", "http://localhost:8000", false)
			require.ErrorIs(t, subtreeErr, tc.code)
			require.Equal(t, tc.cancelWrap, errors.Is(subtreeErr, errors.ErrContextCanceled), "CONTEXT_CANCELED: %v", subtreeErr)
			require.True(t, isRecoverableBlockMessageError(subtreeErr))

			err := f.bv.ValidateBlock(ctx, f.block, "http://localhost:8000")
			require.ErrorIs(t, err, tc.code)
			require.Equal(t, tc.corrupt, errors.IsBlockCorrupt(err), "corrupt verdict: %v", err)
			require.NotErrorIs(t, err, errors.ErrBlockInvalid)

			// processBlockFound passes a corrupt or invalid verdict on as it is
			// and wraps anything else in a service error, which the Kafka path
			// retries; this one is retryable before that wrap too
			require.True(t, isRecoverableBlockMessageError(err), "the Kafka path retries the block")
			require.True(t, isLocalCatchupFault(err), "catch-up does not charge or rotate away from the peer")
			require.False(t, shouldReportConsensusMalicious(err))
			require.False(t, isUnboundTxInvalidVerdict(err))
			require.Equal(t, tc.catchupLabel, releasedCatchupErrorType(t, f.block, err))
		})
	}

	// The block path does not fail fast, so the missing-tx threshold cannot be
	// driven through ValidateBlock. This is the shape processTxMetaUsingStore
	// returns for it (Test_processTxMetaUsingStore_passedConflictCheckKeepsEntries
	// pins that), under CheckBlockSubtrees' wraps and the gRPC boundary.
	t.Run("the missing-tx threshold is a retryable failure", func(t *testing.T) {
		thresholdErr := errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to process transactions in batch 1",
			errors.NewProcessingError("[processTransactionsInLevels] Failed to check txMeta store",
				errors.NewThresholdExceededError("threshold exceeded for missing txs: 2 > 1")))

		subtreeErr := errors.UnwrapGRPC(errors.WrapGRPC(thresholdErr))
		require.ErrorIs(t, subtreeErr, errors.ErrThresholdExceeded)
		require.True(t, isRecoverableBlockMessageError(subtreeErr))
		require.True(t, isLocalCatchupFault(subtreeErr))
	})
}

// releasedCatchupErrorType is the error type releaseCatchupLock records for a
// catch-up run toward block that ends on err.
func releasedCatchupErrorType(t *testing.T, block *model.Block, err error) string {
	t.Helper()

	server, _, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	catchupCtx := &CatchupContext{
		blockUpTo: block,
		baseURL:   "http://localhost:8000",
		peerID:    "primary",
		startTime: time.Now(),
	}
	require.NoError(t, server.acquireCatchupLock(catchupCtx))

	server.releaseCatchupLock(catchupCtx, &err)
	require.NotNil(t, server.previousCatchupAttempt)

	return server.previousCatchupAttempt.ErrorType
}

// faultyCheckStore fails the fresh path's reads of one tx with err: its decorate
// when decorate is set, otherwise the conflict check's read of its inpoints.
type faultyCheckStore struct {
	utxostore.Store
	tx       chainhash.Hash
	err      error
	decorate bool
}

func (f *faultyCheckStore) Get(ctx context.Context, hash *chainhash.Hash, fs ...fields.FieldName) (*meta.Data, error) {
	if !f.decorate && hash.Equal(f.tx) && slices.Contains(fs, fields.TxInpoints) {
		return nil, f.err
	}

	return f.Store.Get(ctx, hash, fs...)
}

func (f *faultyCheckStore) BatchDecorate(ctx context.Context, items []*utxostore.UnresolvedMetaData, fs ...fields.FieldName) error {
	if f.decorate && slices.ContainsFunc(items, func(item *utxostore.UnresolvedMetaData) bool { return item.Hash.Equal(f.tx) }) {
		return f.err
	}

	return f.Store.BatchDecorate(ctx, items, fs...)
}

// newFaultyConflictBlockFixture is the fixture with the conflicting tx's reads
// failing as faultyCheckStore fails them. Decorate goes through BatchDecorate,
// once.
func newFaultyConflictBlockFixture(t *testing.T, err error, decorate bool) *conflictBlockFixture {
	t.Helper()

	faulty := &faultyCheckStore{err: err, decorate: decorate}

	f := newConflictBlockFixture(t, conflictFixtureOptions{
		wrap: func(store utxostore.Store) utxostore.Store {
			faulty.Store = store
			return faulty
		},
		configure: []func(*settings.Settings){func(s *settings.Settings) {
			s.SubtreeValidation.BatchMissingTransactions = true
			s.BlockValidation.ProcessTxMetaUsingStoreRetries = 0
		}},
	})

	faulty.tx = *f.conflicting.TxIDChainHash()

	return f
}
