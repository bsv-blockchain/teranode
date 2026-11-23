package validator

// Tests for the ingest extend-step DAH-evicted-parent grace.
//
// A resubmitted tx whose parents were all DAH-evicted (or lost with a store
// partition) fails extension with ErrTxMissingParent. The grace accepts it when
// its own record lists its inputs, is mined on the current chain and is not
// conflicting, locked or creating. It must not fire on block paths, with any
// parent present, for a UTXO-set snapshot record, on a store or chain fault, or
// after the caller's context is done.
//
// Every current-chain verdict comes from a real sqlitememory blockchain store.
// graceChainClient only records the ids it is asked about and, where a test sets
// one, injects a fault or a stall that a real store cannot produce on demand.

import (
	"context"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	bcsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func makeTwoParentGraceTx(t *testing.T) (*bt.Tx, *bt.Tx) {
	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))
	parentA := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner A/"),
		transactions.WithP2PKHOutputs(1, 50e8, publicKey),
	)
	parentB := transactions.Create(t,
		transactions.WithCoinbaseData(101, "/Test miner B/"),
		transactions.WithP2PKHOutputs(1, 50e8, publicKey),
	)
	tx := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(parentA, 0),
		transactions.WithInput(parentB, 0),
		transactions.WithP2PKHOutputs(1, 1000),
		transactions.WithChangeOutput(),
	)

	return tx, parentA
}

func makeExtendGraceTx(t *testing.T) *bt.Tx {
	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))
	coinbaseTx := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 50e8, publicKey),
	)
	tx := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 1000),
		transactions.WithChangeOutput(),
	)

	return tx
}

// minedRecord is the record the validator writes for tx once it is mined in
// blockIDs, in the shape GetMeta returns it (no Tx, inpoints from the inputs).
func minedRecord(t *testing.T, tx *bt.Tx, blockIDs ...uint32) *meta.Data {
	t.Helper()

	inpoints, err := subtree.NewTxInpointsFromTx(tx)
	require.NoError(t, err)

	return &meta.Data{TxInpoints: inpoints, BlockIDs: blockIDs}
}

// graceChain is a real sqlitememory blockchain: two main-chain blocks on
// genesis, and one side block on genesis that has less work.
type graceChain struct {
	store   blockchainstore.Store
	client  blockchain.ClientI
	genesis *chainhash.Hash
	mainIDs []uint32
	sideIDs []uint32
	sideTip *chainhash.Hash
}

func newGraceChain(t *testing.T) *graceChain {
	t.Helper()

	logger := ulogger.TestLogger{}
	settings := test.CreateBaseTestSettings(t)

	store, err := bcsql.New(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })

	client, err := blockchain.NewLocalClient(logger, settings, store, nil, nil)
	require.NoError(t, err)

	c := &graceChain{store: store, client: client, genesis: settings.ChainCfgParams.GenesisHash}
	c.mainIDs = c.extend(t, c.genesis, 2, 0)
	c.sideIDs = c.extend(t, c.genesis, 1, 1_000)

	return c
}

// extend stores count blocks on top of parent and returns their ids. salt keeps
// a side branch's hashes apart from the main chain's.
func (c *graceChain) extend(t *testing.T, parent *chainhash.Hash, count int, salt uint32) []uint32 {
	t.Helper()

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	tree, err := subtree.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, tree.AddNode(*coinbase.TxIDChainHash(), 0, 0))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	ids := make([]uint32, 0, count)

	for i := range count {
		block := &model.Block{
			Header: &model.BlockHeader{
				Version: 1,
				// nolint:gosec // test counts are small.
				Timestamp:      1231469665 + salt + uint32(i),
				Nonce:          2573394689,
				HashPrevBlock:  parent,
				HashMerkleRoot: tree.RootHash(),
				Bits:           *nBits,
			},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{tree.RootHash()},
		}

		id, _, err := c.store.StoreBlock(context.Background(), block, "")
		require.NoError(t, err)

		// nolint:gosec // sqlitememory ids are small.
		ids = append(ids, uint32(id))
		parent = block.Hash()
	}

	if salt != 0 {
		c.sideTip = parent
	}

	return ids
}

// graceChainClient answers from the real chain. It records every id set the
// grace asks about, and returns err or stalls instead when a test sets one.
type graceChainClient struct {
	blockchain.ClientI
	asked [][]uint32
	err   error
	stall bool
}

func (c *graceChainClient) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	c.asked = append(c.asked, slices.Clone(blockIDs))

	if c.stall {
		stallUntilDone(ctx)

		return false, errors.NewServiceError("chain check interrupted", context.DeadlineExceeded)
	}

	if c.err != nil {
		return false, c.err
	}

	return c.ClientI.CheckBlockIsInCurrentChain(ctx, blockIDs)
}

type extendGraceSetup struct {
	childMeta    *meta.Data
	childMetaErr error
	parentResult []utxo.ParentOutput
	parentErr    error
	metaStall    bool
	chainErr     error
	chainStall   bool
}

const extendGraceStallLimit = 5 * time.Second

func stallUntilDone(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(extendGraceStallLimit):
	}
}

func setupExtendGraceValidator(t *testing.T, tx *bt.Tx, chain *graceChain, cfg extendGraceSetup) (*Validator, *utxo.MockUtxostore, *graceChainClient) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	mockStore := &utxo.MockUtxostore{}
	settings := test.CreateBaseTestSettings(t)

	chainClient := &graceChainClient{ClientI: chain.client, err: cfg.chainErr, stall: cfg.chainStall}

	validator, err := New(ctx, logger, settings, mockStore, nil, nil, nil, nil, chainClient)
	require.NoError(t, err)

	v := validator.(*Validator)

	parentResult := cfg.parentResult
	if parentResult == nil && cfg.parentErr == nil {
		parentResult = []utxo.ParentOutput{{Status: utxo.ParentOutputTxNotFound}}
	}

	if parentResult != nil {
		mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(parentResult, nil)
	} else {
		mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return(nil, cfg.parentErr)
	}

	mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})

	// The grace must read the CHILD's record, never a parent's.
	childHash := mock.MatchedBy(func(h *chainhash.Hash) bool { return h.IsEqual(tx.TxIDChainHash()) })

	switch {
	case cfg.metaStall:
		mockStore.On("GetMeta", mock.Anything, childHash, mock.Anything).
			Run(func(args mock.Arguments) { stallUntilDone(args.Get(0).(context.Context)) }).
			Return(errors.NewStorageError("meta read interrupted", context.DeadlineExceeded))
	case cfg.childMetaErr != nil:
		mockStore.On("GetMeta", mock.Anything, childHash, mock.Anything).Return(cfg.childMetaErr)
	default:
		require.NotNil(t, cfg.childMeta, "a setup without a meta fault needs childMeta")

		mockStore.On("GetMeta", mock.Anything, childHash, mock.Anything).
			Run(func(args mock.Arguments) {
				data := args.Get(2).(*meta.Data)
				*data = *cfg.childMeta
			}).
			Return(nil)
	}

	return v, mockStore, chainClient
}

func TestValidate_ExtendStepDAHEvictedParentGrace(t *testing.T) {
	chain := newGraceChain(t)

	t.Run("grace allowed when child mined on current chain and not flagged", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[1])})

		blessedBefore := testutil.ToFloat64(prometheusValidatorDAHGraceBlessed)

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.NoError(t, err)
		require.NotNil(t, txMetaData)
		require.Equal(t, []uint32{chain.mainIDs[1]}, txMetaData.BlockIDs)
		require.Equal(t, blessedBefore+1, testutil.ToFloat64(prometheusValidatorDAHGraceBlessed), "a bless must count once")
		// The verdict must come from the chain, asked about the child's own BlockIDs.
		require.Equal(t, [][]uint32{{chain.mainIDs[1]}}, chainClient.asked)
		// The grace must short-circuit BEFORE any spend is attempted: no Spend
		// expectation is registered, so a Spend call would fail the test.
		mockStore.AssertExpectations(t)
	})

	for _, tc := range []struct {
		name   string
		record func(tx *bt.Tx) *meta.Data
	}{
		{"child not mined", func(tx *bt.Tx) *meta.Data { return minedRecord(t, tx) }},
		{"child conflicting", func(tx *bt.Tx) *meta.Data {
			m := minedRecord(t, tx, chain.mainIDs[0])
			m.Conflicting = true

			return m
		}},
		{"child locked", func(tx *bt.Tx) *meta.Data {
			m := minedRecord(t, tx, chain.mainIDs[0])
			m.Locked = true

			return m
		}},
		{"child still creating", func(tx *bt.Tx) *meta.Data {
			m := minedRecord(t, tx, chain.mainIDs[0])
			m.Creating = true

			return m
		}},
	} {
		t.Run("no grace when "+tc.name, func(t *testing.T) {
			tx := makeExtendGraceTx(t)
			v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: tc.record(tx)})

			_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
			// The flags are checked before the blockchain round-trip.
			require.Empty(t, chainClient.asked)
			mockStore.AssertExpectations(t)
		})
	}

	t.Run("no extend-step grace for a block-sourced tx", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[1])})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{InBlock: true})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
		require.Empty(t, chainClient.asked)
	})

	t.Run("no grace when child absent from store", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMetaErr: errors.NewTxNotFoundError("child not found")})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		require.Empty(t, chainClient.asked)
		mockStore.AssertExpectations(t)
	})

	t.Run("no grace when the mined block is on a side branch", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		// BlockIDs alone are not proof of main-chain inclusion: a block that lost
		// a reorg keeps its stale ID on the tx meta.
		v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.sideIDs[0])})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		require.Equal(t, [][]uint32{{chain.sideIDs[0]}}, chainClient.asked)
		mockStore.AssertExpectations(t)
	})

	t.Run("one main-chain id among stale ones is enough", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, _, _ := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.sideIDs[0], chain.mainIDs[0])})

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.NoError(t, err)
		require.NotNil(t, txMetaData)
	})

	t.Run("finality is enforced before the grace can bless", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		// A height-based nLockTime above the validation height with a non-final
		// sequence: the record was final at the height it was mined at, not at 100.
		tx.LockTime = 500
		for _, input := range tx.Inputs {
			input.SequenceNumber = 0
		}

		v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[1])})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNonFinal), "non-final error must surface, got: %v", err)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
		require.Empty(t, chainClient.asked)
	})

	t.Run("chain-check fault keeps missing parent and adds a node fault", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, mockStore, _ := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[1]), chainErr: errors.NewServiceError("blockchain unavailable")})

		faultsBefore := testutil.ToFloat64(prometheusValidatorDAHGraceFault.WithLabelValues("chain_check"))

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		// Missing parent stays in the chain: the counters, the legacy orphan pool and
		// the HTTP 422 mapping all key on it as "deferrable".
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "a fault must stay deferrable, got: %v", err)
		// SERVICE_UNAVAILABLE is on the RPC layer's node-fault list; SERVICE_ERROR is not,
		// and would be reported to the submitter as a verdict on the transaction.
		require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "the fault must surface as a node-fault class, got: %v", err)
		require.Contains(t, err.Error(), tx.Inputs[0].PreviousTxIDChainHash().String(), "the error must still name the missing parent")
		require.Contains(t, errors.PublicError(err).Error(), tx.Inputs[0].PreviousTxIDChainHash().String(), "the submitter-facing error must name the missing parent")
		require.Equal(t, faultsBefore+1, testutil.ToFloat64(prometheusValidatorDAHGraceFault.WithLabelValues("chain_check")), "a chain-check fault must count once")
		mockStore.AssertExpectations(t)
	})

	t.Run("store fault on meta read keeps missing parent and adds a node fault", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		// Inject a class that is not itself a node fault, so only the grace's own
		// classification can satisfy the STORAGE_ERROR assertion.
		v, mockStore, _ := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMetaErr: errors.NewProcessingError("aerospike timeout")})

		faultsBefore := testutil.ToFloat64(prometheusValidatorDAHGraceFault.WithLabelValues("meta_read"))

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "a fault must stay deferrable, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "the grace must classify the read fault as a node fault, got: %v", err)
		require.Contains(t, err.Error(), tx.Inputs[0].PreviousTxIDChainHash().String(), "the error must still name the missing parent")
		require.Equal(t, faultsBefore+1, testutil.ToFloat64(prometheusValidatorDAHGraceFault.WithLabelValues("meta_read")), "a meta-read fault must count once")
		mockStore.AssertExpectations(t)
	})

	t.Run("no grace without a blockchain client (fail closed, no store read)", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, mockStore, _ := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[1])})
		v.blockchainClient = nil

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxMissingParent), "missing-parent error must surface, got: %v", err)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("a parent read fault that is not a missing parent never reaches the grace", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[1]), parentErr: errors.NewStorageError("bulk parent read failed")})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrTxMissingParent), "a read fault is not a missing parent, got: %v", err)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
		require.Empty(t, chainClient.asked)
	})

	t.Run("no grace for a mined child whose parent is present", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[1]), parentResult: []utxo.ParentOutput{{Status: utxo.ParentOutputNoSuchIndex}}})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrTxMissingParent), "a per-output verdict is not a missing parent, got: %v", err)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
		require.Empty(t, chainClient.asked)
	})
}

// A current-chain verdict must be refreshed for every grace decision: a reorg
// can orphan the block between two validations of the same transaction.
func TestExtendGraceChainCheckRecheckedAfterReorg(t *testing.T) {
	chain := newGraceChain(t)
	tx := makeExtendGraceTx(t)

	v, _, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[1])})

	mined, err := v.validateInternal(context.Background(), tx, 100, &Options{})
	require.NoError(t, err)
	require.NotNil(t, mined)

	// Two more side blocks give the side branch more work than the main chain.
	chain.extend(t, chain.sideTip, 2, 2_000)

	onChain, err := chain.client.CheckBlockIsInCurrentChain(context.Background(), []uint32{chain.mainIDs[1]})
	require.NoError(t, err)
	require.False(t, onChain, "precondition: the reorg moved the child's block off the current chain")

	_, err = v.validateInternal(context.Background(), tx, 100, &Options{})
	require.ErrorIs(t, err, errors.ErrTxMissingParent)
	require.Len(t, chainClient.asked, 2, "each decision must ask the chain again")
}

// TestExtendGraceChainCheckErrorsRetried verifies a failed current-chain
// check is retried: a transient blockchain fault must not pin a verdict.
func TestExtendGraceChainCheckErrorsRetried(t *testing.T) {
	chain := newGraceChain(t)
	tx := makeExtendGraceTx(t)

	v, _, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[0]), chainErr: errors.NewServiceError("blockchain unavailable")})

	for range 3 {
		_, err := v.validateInternal(context.Background(), tx, 100, &Options{})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrServiceUnavailable), "the chain fault must surface on every attempt, got: %v", err)
	}

	require.Len(t, chainClient.asked, 3)
}

// The ingest grace runs before any state is written, so it must honour the
// caller's cancellation and deadline. A done context denies the grace and the
// caller sees the original missing-parent error, never a node fault.
func TestExtendGraceHonoursCallerContext(t *testing.T) {
	chain := newGraceChain(t)

	t.Run("cancelled context skips the grace", func(t *testing.T) {
		tx := makeExtendGraceTx(t)
		v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[1])})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		txMetaData, err := v.validateInternal(ctx, tx, 100, &Options{})

		require.Nil(t, txMetaData)
		require.ErrorIs(t, err, errors.ErrTxMissingParent)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
		require.Empty(t, chainClient.asked)
	})

	for _, tc := range []struct {
		name  string
		setup func(tx *bt.Tx) extendGraceSetup
	}{
		{"stalled chain check", func(tx *bt.Tx) extendGraceSetup {
			return extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[1]), chainStall: true}
		}},
		{"stalled meta read", func(*bt.Tx) extendGraceSetup { return extendGraceSetup{metaStall: true} }},
	} {
		t.Run(tc.name+" stops at the caller deadline", func(t *testing.T) {
			tx := makeExtendGraceTx(t)
			v, _, _ := setupExtendGraceValidator(t, tx, chain, tc.setup(tx))

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			faultsBefore := testutil.ToFloat64(prometheusValidatorDAHGraceFault.WithLabelValues("meta_read")) +
				testutil.ToFloat64(prometheusValidatorDAHGraceFault.WithLabelValues("chain_check"))

			start := time.Now()
			txMetaData, err := v.validateInternal(ctx, tx, 100, &Options{})
			elapsed := time.Since(start)

			require.Less(t, elapsed, extendGraceStallLimit, "the grace must stop when the caller deadline expires")
			require.Nil(t, txMetaData)
			require.ErrorIs(t, err, errors.ErrTxMissingParent)
			require.False(t, errors.Is(err, errors.ErrStorageError), "a caller deadline is not a store fault, got: %v", err)
			require.False(t, errors.Is(err, errors.ErrServiceUnavailable), "a caller deadline is not a blockchain fault, got: %v", err)

			faultsAfter := testutil.ToFloat64(prometheusValidatorDAHGraceFault.WithLabelValues("meta_read")) +
				testutil.ToFloat64(prometheusValidatorDAHGraceFault.WithLabelValues("chain_check"))
			require.Equal(t, faultsBefore, faultsAfter, "a caller deadline must not count as a grace fault")
		})
	}
}

// The grace records no spends, so it may fire only when every parent is gone.
// A present parent still holds the outputs this tx spends.
func TestExtendGraceRequiresEveryParentAbsent(t *testing.T) {
	chain := newGraceChain(t)
	notFound := utxo.ParentOutput{Status: utxo.ParentOutputTxNotFound}
	present := utxo.ParentOutput{Status: utxo.ParentOutputMined, Height: 50, Satoshis: 50e8}

	t.Run("every parent absent is granted", func(t *testing.T) {
		tx, _ := makeTwoParentGraceTx(t)
		v, _, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[0]), parentResult: []utxo.ParentOutput{notFound, notFound}})

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.NoError(t, err)
		require.NotNil(t, txMetaData)
		require.Equal(t, [][]uint32{{chain.mainIDs[0]}}, chainClient.asked)
	})

	for _, tc := range []struct {
		name    string
		answers []utxo.ParentOutput
	}{
		{"first absent, second present", []utxo.ParentOutput{notFound, present}},
		{"first present, second absent", []utxo.ParentOutput{present, notFound}},
	} {
		t.Run(tc.name+" is denied", func(t *testing.T) {
			tx, _ := makeTwoParentGraceTx(t)
			v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[0]), parentResult: tc.answers})

			txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

			require.Nil(t, txMetaData)
			require.ErrorIs(t, err, errors.ErrTxMissingParent)
			mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
			require.Empty(t, chainClient.asked)
		})
	}

	readFault := utxo.ParentOutput{Err: errors.NewStorageError("read failed")}

	for _, tc := range []struct {
		name    string
		answers []utxo.ParentOutput
	}{
		{"first absent, second read fault", []utxo.ParentOutput{notFound, readFault}},
		{"first read fault, second absent", []utxo.ParentOutput{readFault, notFound}},
	} {
		t.Run(tc.name+" surfaces the fault in either input order", func(t *testing.T) {
			tx, _ := makeTwoParentGraceTx(t)
			v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[0]), parentResult: tc.answers})

			txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

			require.Nil(t, txMetaData)
			require.False(t, errors.Is(err, errors.ErrTxMissingParent), "a store fault must not read as a missing parent, got: %v", err)
			require.True(t, errors.Is(err, errors.ErrStorageError), "the store fault must surface, got: %v", err)
			mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
			require.Empty(t, chainClient.asked)
		})
	}

	t.Run("a prefetched parent counts as present", func(t *testing.T) {
		tx, parentA := makeTwoParentGraceTx(t)
		v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: minedRecord(t, tx, chain.mainIDs[0]), parentResult: []utxo.ParentOutput{notFound}})

		opts := &Options{PrefetchedParents: map[chainhash.Hash]*meta.Data{*parentA.TxIDChainHash(): {Tx: parentA, BlockHeights: []uint32{50}}}}

		txMetaData, err := v.validateInternal(context.Background(), tx, 100, opts)

		require.Nil(t, txMetaData)
		require.ErrorIs(t, err, errors.ErrTxMissingParent)
		mockStore.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
		require.Empty(t, chainClient.asked)
	})
}

// UTXO-set snapshot records (cmd/seeder, cmd/seedimport) carry a block id but
// were never created from the tx's inputs. The grace must deny them before it
// asks the chain, whatever id they carry.
func TestExtendGraceDeniesSnapshotRecords(t *testing.T) {
	chain := newGraceChain(t)

	onChain, err := chain.client.CheckBlockIsInCurrentChain(context.Background(), []uint32{0})
	require.NoError(t, err)
	require.True(t, onChain, "precondition: the real store places genesis id 0 on the current chain")

	for _, tc := range []struct {
		name   string
		record func(tx *bt.Tx) *meta.Data
	}{
		// cmd/seeder: block id 0, outputs only.
		{"seeder record", func(*bt.Tx) *meta.Data { return &meta.Data{TxInpoints: subtree.NewTxInpoints(), BlockIDs: []uint32{0}} }},
		// cmd/seedimport: a real main-chain block id, outputs only.
		{"seedimport record", func(*bt.Tx) *meta.Data {
			return &meta.Data{TxInpoints: subtree.NewTxInpoints(), BlockIDs: []uint32{chain.mainIDs[1]}}
		}},
		// Inpoints present, but on block id 0 only.
		{"record on genesis id only", func(tx *bt.Tx) *meta.Data { return minedRecord(t, tx, 0) }},
		// Inpoints for another outpoint than the one this tx spends.
		{"record for other inputs", func(tx *bt.Tx) *meta.Data {
			other := tx.Clone()
			other.Inputs[0].PreviousTxOutIndex++

			return minedRecord(t, other, chain.mainIDs[1])
		}},
	} {
		t.Run(tc.name+" is denied", func(t *testing.T) {
			tx := makeExtendGraceTx(t)
			v, mockStore, chainClient := setupExtendGraceValidator(t, tx, chain, extendGraceSetup{childMeta: tc.record(tx)})

			blessedBefore := testutil.ToFloat64(prometheusValidatorDAHGraceBlessed)

			txMetaData, err := v.validateInternal(context.Background(), tx, 100, &Options{})

			require.Nil(t, txMetaData)
			require.ErrorIs(t, err, errors.ErrTxMissingParent)
			require.Equal(t, blessedBefore, testutil.ToFloat64(prometheusValidatorDAHGraceBlessed), "a snapshot record must not be blessed")
			require.Empty(t, chainClient.asked, "a snapshot record is denied before the chain round-trip")
			mockStore.AssertExpectations(t)
		})
	}
}
