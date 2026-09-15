package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	utxoStore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// bodylessUtxoStore is a UTXO store that holds every record but hands back none
// of their serialized bodies, which is the steady state of a store running with
// utxostore_skipTxBodyBelowCheckpoint on (and of any store whose body window has
// aged out). It answers a missing transaction exactly as the wrapped store does,
// so the two cases stay distinguishable.
type bodylessUtxoStore struct {
	utxoStore.Store

	// refuseTxField makes a Get that names fields.Tx fail outright. An existence
	// check must not ask for the body at all, so the tests that pin that choice
	// set this: reverting canonicalCoinbaseAt to fields.Tx then turns them red
	// instead of passing on a record this double happens to strip anyway.
	refuseTxField bool

	// stripInpoints additionally clears the stored inpoints, which is the shape a
	// utxoset record created below the checkpoint actually has: createMinedPlanSQL
	// writes NULL tx_inpoints, and the skip-body gate writes no body.
	stripInpoints bool
}

func (s *bodylessUtxoStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	if s.refuseTxField {
		for _, name := range f {
			if name == fields.Tx {
				return nil, errors.NewStorageError("[bodylessUtxoStore] asked for fields.Tx: an existence check must decide on the record, not the body")
			}
		}
	}

	data, err := s.Store.Get(ctx, hash, f...)
	if err != nil || data == nil {
		return data, err
	}

	// Copy before clearing: the wrapped store may hand out a record other
	// readers hold.
	stripped := *data
	stripped.Tx = nil

	if s.stripInpoints {
		stripped.TxInpoints = subtree.TxInpoints{}
	}

	return &stripped, nil
}

// TestCanonicalCoinbaseAt_BodylessRecordIsPresent pins the fix for the Hetzner
// mainnet incident. canonicalCoinbaseAt decided presence on txMeta.Tx, so the
// first restart after utxostore_skipTxBodyBelowCheckpoint was enabled read every
// coinbase created below the checkpoint as missing, the walk-back never found a
// good floor, and startup escalated MANUAL INTERVENTION REQUIRED against a UTXO
// set that was entirely intact.
func TestCanonicalCoinbaseAt_BodylessRecordIsPresent(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	// height 1: canonical block carries cb1, and the store holds cb1's record.
	cb1 := coinbaseTxForHeader(t, blockHeader1)
	addCanonicalBlockWithCoinbase(ctx, t, items, blockHeader1, cb1)

	_, _, err := items.utxoStore.SpendAndCreate(ctx, cb1, 1, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	// height 2: canonical block carries cb2, whose record was never created.
	cb2 := coinbaseTxForHeader(t, blockHeader2)
	addCanonicalBlockWithCoinbase(ctx, t, items, blockHeader2, cb2)

	// Proof the double really is body-less, so the assertions below are not
	// passing for the wrong reason.
	probe := &bodylessUtxoStore{Store: items.utxoStore}

	stripped, err := probe.Get(ctx, cb1.TxIDChainHash(), fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, stripped)
	require.Nil(t, stripped.Tx)

	// And the store really does answer the second one with a not-found.
	_, err = probe.Get(ctx, cb2.TxIDChainHash(), fields.Tx)
	require.True(t, errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound))

	// refuseTxField pins the field choice: asking for the body at all is the bug
	// under test, so the double refuses it rather than quietly returning a
	// stripped record that would let the old code pass.
	items.blockAssembler.utxoStore = &bodylessUtxoStore{Store: items.utxoStore, refuseTxField: true}

	// The record exists with no body at all: present.
	present, blk, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 1)
	require.NoError(t, err)
	require.True(t, present, "a coinbase whose body was never written is still present")
	require.NotNil(t, blk)

	// No record at all: absent, and still no error.
	absent, blk2, err := items.blockAssembler.canonicalCoinbaseAt(ctx, 2)
	require.NoError(t, err)
	require.False(t, absent, "a coinbase with no record is missing")
	require.NotNil(t, blk2)
}

// TestStartupCoinbaseDivergenceCheck_BodylessCoinbasesRaiseNoAlarm is the same
// fix seen from the startup scan that fired on mainnet: a chain whose coinbases
// are all present but all body-less must boot silently, with no recovery run and
// no MANUAL INTERVENTION line.
func TestStartupCoinbaseDivergenceCheck_BodylessCoinbasesRaiseNoAlarm(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryConsecutiveGood = 2
	items.blockAssembler.settings.BlockAssembly.CoinbaseRecoveryMaxGapBlocks = 100

	headers := buildCanonicalChain(ctx, t, items, 4)
	for h := uint32(1); h <= 4; h++ {
		seedCoinbase(ctx, t, items, headers, h)
	}

	items.blockAssembler.setBestBlockHeader(headers[3], 4)

	logger := &capturingLogger{}
	items.blockAssembler.logger = logger
	items.blockAssembler.utxoStore = &bodylessUtxoStore{Store: items.utxoStore, refuseTxField: true}

	items.blockAssembler.checkCoinbaseDivergenceOnStart(ctx)

	require.False(t, logger.sawWarn("canonical coinbase missing"), "no coinbase is missing")
	require.False(t, logger.sawError("MANUAL INTERVENTION REQUIRED"))
	require.False(t, logger.sawError("startup recovery failed"))
}

// TestUnlockConflictParents_BodylessWinnerStillUnlocksItsParents covers the
// second reader that decided on the body: healStaleConflictIntent's step-5
// unlock read the winner's parents out of txMeta.Tx.Inputs, so a body-less
// winner contributed no parents and was skipped in silence, leaving them locked
// with nothing left to unlock them. The parents live on the identity record as
// stored inpoints, which is what it reads now.
func TestUnlockConflictParents_BodylessWinnerStillUnlocksItsParents(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	parent, child := seedParentAndChild(ctx, t, items)

	// Lock the parent, the state a forward ProcessConflicting leaves at step 2.
	require.NoError(t, items.utxoStore.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))

	locked, err := items.utxoStore.Get(ctx, parent.TxIDChainHash())
	require.NoError(t, err)
	require.True(t, locked.Locked)

	items.blockAssembler.utxoStore = &bodylessUtxoStore{Store: items.utxoStore}

	require.NoError(t, items.blockAssembler.unlockConflictParents(ctx, []chainhash.Hash{*child.TxIDChainHash()}))

	unlocked, err := items.utxoStore.Get(ctx, parent.TxIDChainHash())
	require.NoError(t, err)
	require.False(t, unlocked.Locked, "the winner's parent must be unlocked even when the winner has no body")
}

// TestUnlockConflictParents_MissingWinnerIsSkipped keeps the other half of the
// contract: a winner whose record is gone entirely contributes no parents and is
// not an error, so the heal still completes.
func TestUnlockConflictParents_MissingWinnerIsSkipped(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	never := coinbaseTxForHeader(t, blockHeader2)

	_, err := items.utxoStore.Get(ctx, never.TxIDChainHash())
	require.True(t, errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound),
		"the premise of this test is that the record is absent, not that the read failed some other way")

	require.NoError(t, items.blockAssembler.unlockConflictParents(ctx, []chainhash.Hash{*never.TxIDChainHash()}))
}

// TestUnlockConflictParents_NeitherInpointsNorBodyIsAnError covers the shape a
// utxoset record created below the checkpoint actually has: createMinedPlanSQL
// writes NULL tx_inpoints for every transaction the mined path creates (quick
// validation routes all of a block's transactions through it, not just the
// coinbase), and utxostore_skipTxBodyBelowCheckpoint writes no body. A record
// with neither cannot yield its parents, and reporting that as "no parents"
// would leave them locked while the heal reported success.
func TestUnlockConflictParents_NeitherInpointsNorBodyIsAnError(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	_, child := seedParentAndChild(ctx, t, items)

	items.blockAssembler.utxoStore = &bodylessUtxoStore{Store: items.utxoStore, stripInpoints: true}

	err := items.blockAssembler.unlockConflictParents(ctx, []chainhash.Hash{*child.TxIDChainHash()})
	require.Error(t, err)
	require.Contains(t, err.Error(), "neither stored inpoints nor a transaction body")
	require.Contains(t, err.Error(), child.TxIDChainHash().String())
}

// TestValidateUnminedTxInputs_BodylessRecordValidatesFromInpoints is the same
// (b) fault in the reload path. The read asked for fields.Inputs and bailed on a
// nil txMeta.Tx, so on the SQL store — which populates data.Tx only for
// fields.Tx — every unmined transaction was silently dropped from the mining
// candidate, and on utxoset every record whose body had aged out was too. An
// unmined record carries its inpoints, which is what it reads now.
func TestValidateUnminedTxInputs_BodylessRecordValidatesFromInpoints(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	_, child := seedParentAndChild(ctx, t, items)

	items.blockAssembler.utxoStore = &bodylessUtxoStore{Store: items.utxoStore}

	valid, err := items.blockAssembler.validateUnminedTxInputs(ctx, *child.TxIDChainHash(), map[uint32]bool{}, true)
	require.NoError(t, err)
	require.True(t, valid, "an unmined transaction with stored inpoints must validate without its body")
}

// TestValidateUnminedTxInputs_NeitherInpointsNorBodyIsAnError pins the other
// half: undecidable is not the same answer as invalid, and must not be reported
// as one.
func TestValidateUnminedTxInputs_NeitherInpointsNorBodyIsAnError(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	_, child := seedParentAndChild(ctx, t, items)

	items.blockAssembler.utxoStore = &bodylessUtxoStore{Store: items.utxoStore, stripInpoints: true}

	valid, err := items.blockAssembler.validateUnminedTxInputs(ctx, *child.TxIDChainHash(), map[uint32]bool{}, true)
	require.Error(t, err)
	require.False(t, valid)
	require.Contains(t, err.Error(), "neither stored inpoints nor a transaction body")
}

// seedParentAndChild creates a coinbase parent and one child spending its first
// output, both as unmined records in the UTXO store, and returns them.
func seedParentAndChild(ctx context.Context, t *testing.T, items *baTestItems) (parent, child *bt.Tx) {
	t.Helper()

	parent = coinbaseTxForHeader(t, blockHeader1)

	_, _, err := items.utxoStore.SpendAndCreate(ctx, parent, 1, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	child = bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	child.AddOutput(&bt.Output{Satoshis: parent.Outputs[0].Satoshis - 1, LockingScript: parent.Outputs[0].LockingScript})

	_, _, err = items.utxoStore.SpendAndCreate(ctx, child, 2, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	return parent, child
}

// erroringParentUtxoStore answers the first Get (the transaction's own record)
// from the wrapped store and fails every later one with a storage error, which is
// how a store that is up but faulting looks to validateUnminedTxInputs: the tx
// resolves, its parents do not.
type erroringParentUtxoStore struct {
	utxoStore.Store

	self chainhash.Hash
}

func (s *erroringParentUtxoStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	if hash != nil && hash.IsEqual(&s.self) {
		return s.Store.Get(ctx, hash, f...)
	}

	return nil, errors.NewStorageError("[erroringParentUtxoStore] the store cannot answer for %s", hash.String())
}

// TestValidateUnminedTxInputs_ParentReadErrorIsReturned pins the last of the
// conflations. A failed parent read used to return (false, nil) -- the store
// could not answer, and that was reported as a decision to drop the transaction,
// two lines below the code that had just stopped making exactly that mistake for
// the inpoint resolution.
func TestValidateUnminedTxInputs_ParentReadErrorIsReturned(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupBlockAssemblyTestWithUtxoStore(t, withCoinbaseMaturity(testCoinbaseMaturity))
	require.NotNil(t, items)

	_, child := seedParentAndChild(ctx, t, items)

	items.blockAssembler.utxoStore = &erroringParentUtxoStore{
		Store: items.utxoStore,
		self:  *child.TxIDChainHash(),
	}

	valid, err := items.blockAssembler.validateUnminedTxInputs(ctx, *child.TxIDChainHash(), map[uint32]bool{}, true)
	require.Error(t, err, "a store that cannot answer for a parent is undecidable, not invalid")
	require.False(t, valid)
	require.Contains(t, err.Error(), "failed to load parent")
}
