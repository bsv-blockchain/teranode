package tests

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dummyUnlockingScript is a minimal unlocking script for test transactions
// that need to be stored in the SQL UTXO store (which has NOT NULL on unlocking_script).
var dummyUnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})

// newTestTx creates a new transaction suitable for storage in the UTXO store.
// It references a dummy input and creates one output. The satoshi amount is used
// to make the txid unique across test cases.
func newTestTx(t *testing.T, satoshis uint64) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      Tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: Tx.Inputs[0].PreviousTxScript,
		Satoshis:      Tx.Inputs[0].PreviousTxSatoshis,
	}))
	tx.Inputs[0].UnlockingScript = dummyUnlockingScript
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis))
	return tx
}

var (
	ParentTx, _ = bt.NewTxFromString("010000000000000000ef0158ef6d539bf88c850103fa127a92775af48dba580c36bbde4dc6d8b9da83256d050000006a47304402200ca69c5672d0e0471cd4ff1f9993f16103fc29b98f71e1a9760c828b22cae61c0220705e14aa6f3149130c3a6aa8387c51e4c80c6ae52297b2dabfd68423d717be4541210286dbe9cd647f83a4a6b29d2a2d3227a897a4904dc31769502cb013cbe5044dddffffffff8c2f6002000000001976a914308254c746057d189221c36418ba93337de33bc988ac03002d3101000000001976a91498cde576de501ceb5bb1962c6e49a4d1af17730788ac80969800000000001976a914eb7772212c334c0bdccee75c0369aa675fc21d2088ac706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac00000000")
	Tx, _       = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0" +
		"cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920" +
		"b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000" +
		"000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000" +
		"000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2" +
		"a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	spendTx = &bt.Tx{
		Version:  1,
		LockTime: 0,
		Inputs: []*bt.Input{
			{
				PreviousTxOutIndex: 0,
				SequenceNumber:     0,
				PreviousTxScript:   Tx.Outputs[0].LockingScript,
				PreviousTxSatoshis: Tx.Outputs[0].Satoshis,
			},
		},
		Outputs: []*bt.Output{
			{
				Satoshis:      Tx.Outputs[0].Satoshis,
				LockingScript: Tx.Outputs[0].LockingScript,
			},
			{
				Satoshis:      Tx.Outputs[1].Satoshis,
				LockingScript: Tx.Outputs[1].LockingScript,
			},
		},
	}
	utxoHash0, _ = util.UTXOHashFromOutput(Tx.TxIDChainHash(), Tx.Outputs[0], 0)
	testSpend0   = &utxostore.Spend{
		TxID:         Tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingData: spend.NewSpendingData(TXHash, 0),
	}
	TXHash  = Tx.TxIDChainHash()
	Hash, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	spends  = []*utxostore.Spend{{
		TxID:         TXHash,
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingData: spend.NewSpendingData(Hash, 0),
	}}
)

func Store(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, _, err := db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)

	resp, err := db.Get(ctx, testSpend0.TxID)
	require.NoError(t, err)
	require.Equal(t, testSpend0.TxID.String(), resp.Tx.TxID())

	_, _, err = db.SpendAndCreate(context.Background(), Tx, 1000, utxostore.WithCreateOnly())
	require.ErrorIs(t, err, errors.ErrTxExists)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	_, _, err = db.SpendAndCreate(context.Background(), spendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err)

	_, _, err = db.SpendAndCreate(context.Background(), Tx, 1000, utxostore.WithCreateOnly())
	require.ErrorIs(t, err, errors.ErrTxExists)
}

func Spend(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, _, err := db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	_, _, err = db.SpendAndCreate(ctx, spendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err)

	resp, err := db.Get(ctx, Tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, Tx.TxIDChainHash().String(), resp.Tx.TxID())

	spendTx2 := spendTx.Clone()
	spendTx2.Outputs = spendTx2.Outputs[1:]

	// try to spend with different txid
	_, spends, err := db.SpendAndCreate(context.Background(), spendTx2, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.ErrorIs(t, err, errors.ErrUtxoError)

	// check the individual spend error
	require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
	require.Equal(t, spendTx.TxIDChainHash().String(), spends[0].ConflictingTxID.String())
}

func Restore(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, _, err := db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	_, _, err = db.SpendAndCreate(ctx, spendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err)

	// Build the unspend record with the SpendingData GetSpends populated inside Spend(spendTx).
	restoreSpends := []*utxostore.Spend{{
		TxID:         TXHash,
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingData: spend.NewSpendingData(spendTx.TxIDChainHash(), 0),
	}}

	// try to reset the utxo
	err = db.Unspend(ctx, restoreSpends, false)
	require.NoError(t, err)

	resp, err := db.Get(ctx, testSpend0.TxID)
	require.NoError(t, err)
	require.Equal(t, testSpend0.TxID.String(), resp.Tx.TxID())
}

// UnspendIdempotent proves Unspend is safe to re-apply to an already-unspent
// output — the load-bearing assumption for WAL replay (#861): a crash between
// the Unspend step and a later step means replay re-runs Unspend on an output
// it already unspent. Mirrors SpendIdempotent for the reverse primitive.
func UnspendIdempotent(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, _, err := db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)
	defer func() { _ = db.Delete(ctx, Tx.TxIDChainHash()) }()

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	_, _, err = db.SpendAndCreate(ctx, spendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err)

	restoreSpends := []*utxostore.Spend{{
		TxID:         TXHash,
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingData: spend.NewSpendingData(spendTx.TxIDChainHash(), 0),
	}}

	// First unspend restores the output.
	require.NoError(t, db.Unspend(ctx, restoreSpends, false))

	// Second unspend of the already-unspent output must be a no-op, not an error.
	require.NoError(t, db.Unspend(ctx, restoreSpends, false), "re-unspending an already-unspent output must be idempotent")

	// The output is unspent and the tx is intact.
	resp, err := db.Get(ctx, TXHash)
	require.NoError(t, err)
	require.Equal(t, TXHash.String(), resp.Tx.TxID())
}

func Freeze(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)

	_, _, err := db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)

	err = db.FreezeUTXOs(ctx, spends, tSettings)
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	_, spends, err := db.SpendAndCreate(ctx, spendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.ErrorIs(t, err, errors.ErrUtxoError)
	require.ErrorIs(t, spends[0].Err, errors.ErrFrozen)

	resp, err := db.GetSpend(ctx, testSpend0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_FROZEN), resp.Status)
	require.NotNil(t, resp.SpendingData)
	require.Equal(t, *resp.SpendingData.TxID, subtree.FrozenBytesTxHash)
	require.Equal(t, 0, resp.SpendingData.Vin)

	err = db.UnFreezeUTXOs(ctx, spends, tSettings)
	require.NoError(t, err)

	resp, err = db.GetSpend(ctx, testSpend0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_OK), resp.Status)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	_, _, err = db.SpendAndCreate(ctx, spendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err)

	resp, err = db.GetSpend(ctx, testSpend0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_SPENT), resp.Status)
	require.NotNil(t, resp.SpendingData)
	require.NotNil(t, resp.SpendingData.TxID)
	require.NotNil(t, resp.SpendingData)
	require.NotNil(t, resp.SpendingData.TxID)
	require.Equal(t, spendTx.TxIDChainHash().String(), resp.SpendingData.TxID.String())
}

func ReAssign(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)

	err := db.SetBlockHeight(101)
	require.NoError(t, err)

	_, _, err = db.SpendAndCreate(ctx, Tx, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())

	privKey, err := bec.NewPrivateKey()
	require.NoError(t, err)

	newLockingScript, err := bscript.NewP2PKHFromPubKeyBytes(privKey.PubKey().Compressed())
	require.NoError(t, err)

	newOutput := &bt.Output{
		Satoshis:      Tx.Outputs[0].Satoshis,
		LockingScript: newLockingScript,
	}

	// create a new transaction with a new transaction ID
	spendTx2 := spendTx.Clone()
	_ = spendTx2.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	spendTx2.Inputs[0].PreviousTxSatoshis = newOutput.Satoshis
	spendTx2.Inputs[0].PreviousTxScript = newOutput.LockingScript

	utxoHash2, _ := util.UTXOHashFromOutput(Tx.TxIDChainHash(), newOutput, 0)
	testSpend1 := &utxostore.Spend{
		TxID:         Tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash2,
		SpendingData: spend.NewSpendingData(spendTx2.TxIDChainHash(), 0),
	}

	// try to reassign, should fail, utxo has not yet been frozen
	err = db.ReAssignUTXO(ctx, testSpend0, testSpend1, tSettings)
	require.Error(t, err)

	err = db.FreezeUTXOs(ctx, []*utxostore.Spend{testSpend0}, tSettings)
	require.NoError(t, err)

	// try to reassign, should succeed, utxo has been frozen
	err = db.ReAssignUTXO(ctx, testSpend0, testSpend1, tSettings)
	require.NoError(t, err)

	// should return an error, does not exist anymore
	_, err = db.GetSpend(ctx, testSpend0)
	require.Error(t, err)

	resp, err := db.GetSpend(ctx, testSpend1)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_IMMATURE), resp.Status)
	require.Nil(t, resp.SpendingData)

	// try to spend the old utxo, should fail
	_, _, err = db.SpendAndCreate(ctx, spendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.Error(t, err)

	// try to spend the new utxo, should fail, block height not reached
	_, _, err = db.SpendAndCreate(ctx, spendTx2, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.Error(t, err)

	err = db.SetBlockHeight(1101)
	require.NoError(t, err)

	// try to spend the new utxo, should succeed
	_, _, err = db.SpendAndCreate(ctx, spendTx2, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err)
}

func SetMined(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	err := db.SetBlockHeight(101)
	require.NoError(t, err)

	_, _, err = db.SpendAndCreate(ctx, Tx, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)

	blockIDsMap, err := db.SetMinedMulti(ctx, []*chainhash.Hash{TXHash}, utxostore.MinedBlockInfo{BlockID: 123, BlockHeight: 101, SubtreeIdx: 2})
	require.NoError(t, err)
	require.Len(t, blockIDsMap, 1)
	require.Equal(t, []uint32{123}, blockIDsMap[*TXHash])

	resp, err := db.Get(ctx, testSpend0.TxID)
	require.NoError(t, err)

	require.Equal(t, []uint32{123}, resp.BlockIDs)
	require.Equal(t, []uint32{101}, resp.BlockHeights)
	require.Equal(t, []int{2}, resp.SubtreeIdxs)

	blockIDsMap, err = db.SetMinedMulti(ctx, []*chainhash.Hash{TXHash}, utxostore.MinedBlockInfo{BlockID: 124, BlockHeight: 102, SubtreeIdx: 1})
	require.NoError(t, err)
	require.Len(t, blockIDsMap, 1)
	require.Equal(t, []uint32{123, 124}, blockIDsMap[*TXHash])

	resp, err = db.Get(ctx, testSpend0.TxID)
	require.NoError(t, err)

	require.Equal(t, []uint32{123, 124}, resp.BlockIDs)
	require.Equal(t, []uint32{101, 102}, resp.BlockHeights)
	require.Equal(t, []int{2, 1}, resp.SubtreeIdxs)

	// unset the mined status for the tx for block 123
	blockIDsMap, err = db.SetMinedMulti(ctx, []*chainhash.Hash{TXHash}, utxostore.MinedBlockInfo{BlockID: 123, BlockHeight: 101, SubtreeIdx: 2, UnsetMined: true})
	require.NoError(t, err)
	require.Len(t, blockIDsMap, 1)

	resp, err = db.Get(ctx, testSpend0.TxID)
	require.NoError(t, err)

	require.Equal(t, []uint32{124}, resp.BlockIDs)
	require.Equal(t, []uint32{102}, resp.BlockHeights)
	require.Equal(t, []int{1}, resp.SubtreeIdxs)
}

func Conflicting(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, _, err := db.SpendAndCreate(ctx, ParentTx, 999, utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithConflicting(true), utxostore.WithCreateOnly())
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())

	_, spends, err := db.SpendAndCreate(ctx, spendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.ErrorIs(t, err, errors.ErrUtxoError)
	require.ErrorIs(t, spends[0].Err, errors.ErrTxConflicting)

	// get the conflicting info for the tx
	txMeta, err := db.Get(ctx, Tx.TxIDChainHash(), fields.Conflicting, fields.ConflictingChildren)
	require.NoError(t, err)

	assert.True(t, txMeta.Conflicting)
	require.Len(t, txMeta.ConflictingChildren, 0)

	// get the conflicting info for the parent tx
	txMeta, err = db.Get(ctx, Tx.Inputs[0].PreviousTxIDChainHash(), fields.Conflicting, fields.ConflictingChildren)
	require.NoError(t, err)

	assert.False(t, txMeta.Conflicting)
	require.Len(t, txMeta.ConflictingChildren, 1)
	require.Equal(t, Tx.TxIDChainHash().String(), txMeta.ConflictingChildren[0].String())
}

// ConflictWAL exercises the conflict-resolution write-ahead log (crash safety
// for ProcessConflicting / ReverseProcessConflicting — see #861): a begun
// intent is durable and surfaces in PendingConflictIntents; Begin is idempotent
// on the deterministic intent id; Complete removes it; completing an absent
// intent is a no-op.
func ConflictWAL(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	h1 := chainhash.HashH([]byte("wal-tx-1"))
	h2 := chainhash.HashH([]byte("wal-tx-2"))

	forward := utxostore.ConflictIntent{
		Kind:        utxostore.ConflictIntentForward,
		BlockHeight: 4242,
		TxHashes:    []chainhash.Hash{h1, h2},
		StartedAt:   1_700_000_000_000_000_000,
	}

	// No pending intents on a clean store.
	pending, err := db.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending, "store should start with no pending intents")

	// Begin → the intent becomes pending and round-trips its fields.
	require.NoError(t, db.BeginConflictIntent(ctx, forward))

	pending, err = db.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	got := pending[0]
	require.Equal(t, utxostore.ConflictIntentForward, got.Kind)
	require.Equal(t, uint32(4242), got.BlockHeight)
	require.Equal(t, int64(1_700_000_000_000_000_000), got.StartedAt)
	require.ElementsMatch(t, []chainhash.Hash{h1, h2}, got.TxHashes)
	require.Equal(t, forward.IntentID(), got.IntentID(), "round-tripped intent must hash to the same id")

	// Begin is idempotent on the deterministic id — re-begin (even with the
	// hashes in a different order) does not create a duplicate.
	reordered := forward
	reordered.TxHashes = []chainhash.Hash{h2, h1}
	require.NoError(t, db.BeginConflictIntent(ctx, reordered))

	pending, err = db.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "re-begin of the same intent must not duplicate")

	// A second, distinct intent (reverse, different hashes) coexists.
	reverse := utxostore.ConflictIntent{
		Kind:        utxostore.ConflictIntentReverse,
		BlockHeight: 99,
		TxHashes:    []chainhash.Hash{chainhash.HashH([]byte("wal-tx-3"))},
		StartedAt:   1_700_000_000_000_000_001,
	}
	require.NoError(t, db.BeginConflictIntent(ctx, reverse))

	pending, err = db.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 2)

	// Complete the forward intent → only the reverse remains.
	require.NoError(t, db.CompleteConflictIntent(ctx, forward.IntentID()))

	pending, err = db.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, reverse.IntentID(), pending[0].IntentID())

	// Completing an already-absent intent is a no-op.
	require.NoError(t, db.CompleteConflictIntent(ctx, forward.IntentID()))

	// Complete the reverse intent → store is clean again.
	require.NoError(t, db.CompleteConflictIntent(ctx, reverse.IntentID()))

	pending, err = db.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending)
}

// crashParentTx builds a standalone parent tx with one spendable output. The
// seed makes the txid unique across crash-recovery sub-cases that share one db.
func crashParentTx(seed byte) *bt.Tx {
	parent := bt.NewTx()
	// PreviousTxScript must be set so the tx is "extended" — the Aerospike
	// backend's Create computes fees/utxo hashes from it and rejects non-extended txs.
	in := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 200000,
		PreviousTxScript:   bscript.NewFromBytes([]byte{0x51}),
		SequenceNumber:     0xFFFFFFFF,
		UnlockingScript:    dummyUnlockingScript,
	}
	_ = in.PreviousTxIDAdd(&chainhash.Hash{seed, seed, seed, 0xCC})
	parent.Inputs = []*bt.Input{in}
	parent.Outputs = []*bt.Output{{Satoshis: 100000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}

	return parent
}

// crashSpendTx builds a tx spending parent[0]; outSats makes its txid unique.
func crashSpendTx(t *testing.T, parent *bt.Tx, outSats uint64) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	require.NoError(t, tx.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	tx.Inputs[0].UnlockingScript = dummyUnlockingScript
	tx.Outputs = []*bt.Output{{Satoshis: outSats, LockingScript: bscript.NewFromBytes([]byte{0x52})}}

	return tx
}

func crashConflicting(t *testing.T, ctx context.Context, db utxostore.Store, h *chainhash.Hash) bool {
	t.Helper()
	m, err := db.Get(ctx, h, fields.Conflicting)
	require.NoError(t, err)

	return m.Conflicting
}

func crashLocked(t *testing.T, ctx context.Context, db utxostore.Store, h *chainhash.Hash) bool {
	t.Helper()
	m, err := db.Get(ctx, h, fields.Locked)
	require.NoError(t, err)

	return m.Locked
}

func crashSpender(t *testing.T, ctx context.Context, db utxostore.Store, parent *bt.Tx) *chainhash.Hash {
	t.Helper()
	m, err := db.Get(ctx, parent.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)
	require.NotEmpty(t, m.SpendingDatas)

	if m.SpendingDatas[0] == nil {
		return nil
	}

	return m.SpendingDatas[0].TxID
}

// ConflictWALCrashRecovery is the per-step-boundary crash-restart harness for
// the conflict-resolution WAL (#861). For each step boundary of the forward
// (ProcessConflicting) and reverse (ReverseProcessConflicting) operations it
// reconstructs the on-disk partial state a SIGKILL would leave — the in-process
// rollback never runs on a kill — and then performs the startup replay (re-run
// the operation; forward seeds the processed-hashes map exactly as
// BlockAssembler.replayConflictIntent does). It asserts the UTXO state converges
// to the correct end-state with no operator action and the WAL is cleared.
//
// Runs against a bare store (no BlockAssembler), so it covers SQLite, Postgres
// and Aerospike via the shared backend test suites.
func ConflictWALCrashRecovery(t *testing.T, db utxostore.Store) {
	ctx := context.Background()
	require.NoError(t, db.SetBlockHeight(20))

	// --- Forward: ProcessConflicting end-state is winner=non-conflicting,
	// loser=conflicting, parent[0] spent by winner, parent unlocked. ---
	forwardCases := []struct {
		name  string
		seed  byte
		build func(t *testing.T, parent, txW, txL *bt.Tx)
	}{
		{
			name: "forward_after_step1_mark", seed: 0x31,
			// losers marked, parent[0] still -> L, unlocked.
			build: func(t *testing.T, parent, txW, txL *bt.Tx) {
				_, _, err := db.SpendAndCreate(ctx, txL, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
				require.NoError(t, err)
				_, _, err = db.SetConflicting(ctx, []chainhash.Hash{*txW.TxIDChainHash(), *txL.TxIDChainHash()}, true)
				require.NoError(t, err)
			},
		},
		{
			name: "forward_after_step2_unspend_lock", seed: 0x32,
			// parent[0] -> nil, parent locked.
			build: func(t *testing.T, parent, txW, txL *bt.Tx) {
				_, _, err := db.SetConflicting(ctx, []chainhash.Hash{*txW.TxIDChainHash(), *txL.TxIDChainHash()}, true)
				require.NoError(t, err)
				require.NoError(t, db.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))
			},
		},
		{
			name: "forward_after_step3_spend", seed: 0x33,
			// parent[0] -> W, parent locked.
			build: func(t *testing.T, parent, txW, txL *bt.Tx) {
				_, _, err := db.SetConflicting(ctx, []chainhash.Hash{*txW.TxIDChainHash(), *txL.TxIDChainHash()}, true)
				require.NoError(t, err)
				_, _, err = db.SpendAndCreate(ctx, txW, db.GetBlockHeight()+1, utxostore.WithSpendOnly(), utxostore.WithIgnoreConflicting(true), utxostore.WithIgnoreLocked(true))
				require.NoError(t, err)
				require.NoError(t, db.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))
			},
		},
		{
			name: "forward_after_step4_unmark", seed: 0x34,
			// winner unmarked, parent[0] -> W, parent locked.
			build: func(t *testing.T, parent, txW, txL *bt.Tx) {
				_, _, err := db.SetConflicting(ctx, []chainhash.Hash{*txL.TxIDChainHash()}, true)
				require.NoError(t, err)
				_, _, err = db.SpendAndCreate(ctx, txW, db.GetBlockHeight()+1, utxostore.WithSpendOnly(), utxostore.WithIgnoreConflicting(true), utxostore.WithIgnoreLocked(true))
				require.NoError(t, err)
				require.NoError(t, db.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))
			},
		},
	}

	for _, tc := range forwardCases {
		t.Run(tc.name, func(t *testing.T) {
			parent := crashParentTx(tc.seed)
			_, _, err := db.SpendAndCreate(ctx, parent, 1, utxostore.WithCreateOnly())
			require.NoError(t, err)
			require.NoError(t, db.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))

			txL := crashSpendTx(t, parent, 90000)
			_, _, err = db.SpendAndCreate(ctx, txL, db.GetBlockHeight(), utxostore.WithCreateOnly())
			require.NoError(t, err)

			txW := crashSpendTx(t, parent, 80000)
			_, _, err = db.SpendAndCreate(ctx, txW, db.GetBlockHeight(), utxostore.WithConflicting(true), utxostore.WithCreateOnly())
			require.NoError(t, err)

			tc.build(t, parent, txW, txL)

			// Crash left the forward intent behind.
			intent := utxostore.ConflictIntent{Kind: utxostore.ConflictIntentForward, BlockHeight: 20, TxHashes: []chainhash.Hash{*txW.TxIDChainHash()}, StartedAt: 1}
			require.NoError(t, db.BeginConflictIntent(ctx, intent))

			// Replay: seed the processed-hashes map exactly as BlockAssembler does.
			seeded := map[chainhash.Hash]struct{}{*txW.TxIDChainHash(): {}}
			_, _, err = utxostore.ProcessConflicting(ctx, db, 20, chainhash.Hash{}, []chainhash.Hash{*txW.TxIDChainHash()}, seeded)
			require.NoError(t, err)
			require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))

			require.False(t, crashConflicting(t, ctx, db, txW.TxIDChainHash()), "winner must not be conflicting after replay")
			require.True(t, crashConflicting(t, ctx, db, txL.TxIDChainHash()), "loser must remain conflicting after replay")
			require.NotNil(t, crashSpender(t, ctx, db, parent))
			require.Equal(t, txW.TxIDChainHash().String(), crashSpender(t, ctx, db, parent).String(), "parent[0] must be spent by the winner")
			require.False(t, crashLocked(t, ctx, db, parent.TxIDChainHash()), "parent must be unlocked after replay")
		})
	}

	// --- Reverse: ReverseProcessConflicting end-state is demoted=conflicting,
	// parent[0] spent by the restored counter, counter=non-conflicting. ---
	reverseCases := []struct {
		name  string
		seed  byte
		build func(t *testing.T, parent, txD, txC *bt.Tx)
	}{
		{
			name: "reverse_after_mark_D", seed: 0x41,
			// D conflicting, parent[0] -> D (Unspend not yet run), C conflicting.
			build: func(t *testing.T, parent, txD, txC *bt.Tx) {
				_, _, err := db.SetConflicting(ctx, []chainhash.Hash{*txD.TxIDChainHash()}, true)
				require.NoError(t, err)
				_, _, err = db.SpendAndCreate(ctx, txD, db.GetBlockHeight()+1, utxostore.WithSpendOnly(), utxostore.WithIgnoreConflicting(true), utxostore.WithIgnoreLocked(true))
				require.NoError(t, err)
			},
		},
		{
			name: "reverse_after_unspend_D", seed: 0x42,
			// D conflicting, parent[0] -> nil, C conflicting.
			build: func(t *testing.T, parent, txD, txC *bt.Tx) {
				_, _, err := db.SetConflicting(ctx, []chainhash.Hash{*txD.TxIDChainHash()}, true)
				require.NoError(t, err)
			},
		},
		{
			name: "reverse_after_spend_C", seed: 0x43,
			// D conflicting, parent[0] -> C, C STILL conflicting (Unmark not yet run).
			build: func(t *testing.T, parent, txD, txC *bt.Tx) {
				_, _, err := db.SetConflicting(ctx, []chainhash.Hash{*txD.TxIDChainHash()}, true)
				require.NoError(t, err)
				_, _, err = db.SpendAndCreate(ctx, txC, db.GetBlockHeight()+1, utxostore.WithSpendOnly(), utxostore.WithIgnoreConflicting(true), utxostore.WithIgnoreLocked(true))
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range reverseCases {
		t.Run(tc.name, func(t *testing.T) {
			parent := crashParentTx(tc.seed)
			_, _, err := db.SpendAndCreate(ctx, parent, 1, utxostore.WithCreateOnly())
			require.NoError(t, err)
			require.NoError(t, db.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))

			// C: counter / original spender — conflicting + in parent.ConflictingChildren.
			txC := crashSpendTx(t, parent, 80000)
			_, _, err = db.SpendAndCreate(ctx, txC, db.GetBlockHeight(), utxostore.WithConflicting(true), utxostore.WithCreateOnly())
			require.NoError(t, err)

			// D: the demoted winner.
			txD := crashSpendTx(t, parent, 90000)
			_, _, err = db.SpendAndCreate(ctx, txD, db.GetBlockHeight(), utxostore.WithCreateOnly())
			require.NoError(t, err)

			tc.build(t, parent, txD, txC)

			intent := utxostore.ConflictIntent{Kind: utxostore.ConflictIntentReverse, BlockHeight: 20, TxHashes: []chainhash.Hash{*txD.TxIDChainHash()}, StartedAt: 1}
			require.NoError(t, db.BeginConflictIntent(ctx, intent))

			_, _, err = utxostore.ReverseProcessConflicting(ctx, db, 20, chainhash.Hash{}, []chainhash.Hash{*txD.TxIDChainHash()})
			require.NoError(t, err)
			require.NoError(t, db.CompleteConflictIntent(ctx, intent.IntentID()))

			require.True(t, crashConflicting(t, ctx, db, txD.TxIDChainHash()), "demoted tx must be conflicting after reverse replay")
			require.NotNil(t, crashSpender(t, ctx, db, parent))
			require.Equal(t, txC.TxIDChainHash().String(), crashSpender(t, ctx, db, parent).String(), "parent[0] must be spent by the restored counter")
			require.False(t, crashConflicting(t, ctx, db, txC.TxIDChainHash()), "restored counter must not be conflicting after reverse replay")
		})
	}
}

func Sanity(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	var resp *utxostore.SpendResponse

	var (
		err      error
		txs      = make([]*bt.Tx, 0, 1_000)
		spendTxs = make([]*bt.Tx, 0, 1_000)
	)

	for i := uint64(0); i < 1_000; i++ {
		stx := bt.NewTx()

		require.NoError(t, stx.FromUTXOs(&bt.UTXO{
			TxIDHash:      Tx.TxIDChainHash(),
			Vout:          0,
			LockingScript: Tx.Inputs[0].PreviousTxScript,
			Satoshis:      Tx.Inputs[0].PreviousTxSatoshis,
		}))

		err = stx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", i+2_000_000)
		require.NoError(t, err)

		_, _, err = db.SpendAndCreate(ctx, stx, 100, utxostore.WithCreateOnly())
		require.NoError(t, err)

		// create spending tx
		spentTx := bt.NewTx()
		require.NoError(t, spentTx.From(stx.TxIDChainHash().String(), 0, stx.Outputs[0].LockingScript.String(), stx.Outputs[0].Satoshis))
		require.NoError(t, spentTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", i))
		require.NoError(t, spentTx.ChangeToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", bt.NewFeeQuote()))

		_, _, err = db.SpendAndCreate(ctx, spentTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
		require.NoError(t, err)

		txs = append(txs, stx)
		spendTxs = append(spendTxs, spentTx)
	}

	for i := uint64(0); i < 1_000; i++ {
		stx := txs[i]
		spendingTx := spendTxs[i]

		utxoHash, err := util.UTXOHashFromOutput(stx.TxIDChainHash(), stx.Outputs[0], 0)
		require.NoError(t, err)

		resp, err = db.GetSpend(ctx, &utxostore.Spend{
			TxID:         stx.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash,
			SpendingData: spend.NewSpendingData(Hash, 0),
		})
		require.NoError(t, err)
		require.Equal(t, int(utxostore.Status_SPENT), resp.Status)
		require.NotNil(t, resp.SpendingData)
		require.NotNil(t, resp.SpendingData.TxID)
		require.Equal(t, spendingTx.TxIDChainHash().String(), resp.SpendingData.TxID.String())
	}
}

func Benchmark(b *testing.B, db utxostore.Store) {
	ctx := context.Background()

	spentTx := bt.NewTx()
	_ = spentTx.From(Tx.TxIDChainHash().String(), 0, Tx.Outputs[0].LockingScript.String(), Tx.Outputs[0].Satoshis)
	_ = spentTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)
	if err := spentTx.ChangeToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", bt.NewFeeQuote()); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, _, err := db.SpendAndCreate(ctx, Tx, 100, utxostore.WithCreateOnly())
		if err != nil {
			b.Fatal(err)
		}

		_, spends, err = db.SpendAndCreate(ctx, spentTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
		if err != nil {
			b.Fatal(err)
		}

		err = db.Unspend(ctx, spends)
		if err != nil {
			b.Fatal(err)
		}

		err = db.Delete(ctx, Tx.TxIDChainHash())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// SpendErrorTypes tests that spend operations return the correct error types for various
// failure modes. This is a parity test: both SQL and Aerospike must return the same error
// types for the same conditions.
//
// Covers:
//   - Spend non-existent parent tx → ErrTxNotFound
//   - Spend with UTXO hash mismatch → ErrUtxoHashMismatch
//   - Spend coinbase before maturity → ErrTxCoinbaseImmature
//   - Double spend → ErrSpent with ConflictingTxID
func SpendErrorTypes(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	t.Run("spend non-existent parent returns ErrTxNotFound", func(t *testing.T) {
		// Build a spending tx that references a parent that doesn't exist in the store.
		nonExistentHash, _ := chainhash.NewHashFromStr("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		nonExistentSpendTx := bt.NewTx()
		require.NoError(t, nonExistentSpendTx.From(nonExistentHash.String(), 0, Tx.Outputs[0].LockingScript.String(), Tx.Outputs[0].Satoshis))
		require.NoError(t, nonExistentSpendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, resultSpends, err := db.SpendAndCreate(ctx, nonExistentSpendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.NotEmpty(t, resultSpends)
		require.ErrorIs(t, resultSpends[0].Err, errors.ErrTxNotFound)
	})

	t.Run("spend with utxo hash mismatch returns ErrUtxoHashMismatch", func(t *testing.T) {
		// Create a unique tx for this sub-test
		uniqueTx := newTestTx(t, 3_000_000)

		_, _, err := db.SpendAndCreate(ctx, uniqueTx, 1000, utxostore.WithCreateOnly())
		require.NoError(t, err)
		defer func() { _ = db.Delete(ctx, uniqueTx.TxIDChainHash()) }()

		// Build a spending tx but tamper with the PreviousTxScript so the UTXO hash
		// computed at spend time doesn't match what's stored.
		tamperedSpendTx := bt.NewTx()
		require.NoError(t, tamperedSpendTx.From(
			uniqueTx.TxIDChainHash().String(), 0,
			Tx.Outputs[1].LockingScript.String(), // wrong locking script → wrong UTXO hash
			uniqueTx.Outputs[0].Satoshis,
		))
		require.NoError(t, tamperedSpendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, resultSpends, err := db.SpendAndCreate(ctx, tamperedSpendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.NotEmpty(t, resultSpends)
		require.ErrorIs(t, resultSpends[0].Err, errors.ErrUtxoHashMismatch)
	})

	t.Run("spend coinbase before maturity returns ErrTxCoinbaseImmature", func(t *testing.T) {
		// Create a coinbase tx. The coinbase_spending_height is set to blockHeight + COINBASE_MATURITY.
		// With blockHeight=1000, spending height will be 1000 + 100 = 1100 typically.
		coinbaseTx := bt.NewTx()
		coinbaseInput := &bt.Input{
			PreviousTxOutIndex: 0xFFFFFFFF,
			SequenceNumber:     0xFFFFFFFF,
			UnlockingScript:    bscript.NewFromBytes([]byte{0x04, 0xff, 0xff, 0x00, 0x1d}), // coinbase data
		}
		// Coinbase inputs have all-zero PreviousTxID
		require.NoError(t, coinbaseInput.PreviousTxIDAdd(&chainhash.Hash{}))
		coinbaseTx.Inputs = []*bt.Input{coinbaseInput}
		require.NoError(t, coinbaseTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5_000_000_000))

		_, _, err := db.SpendAndCreate(ctx, coinbaseTx, 1000, utxostore.WithSetCoinbase(true), utxostore.WithCreateOnly())
		require.NoError(t, err)
		defer func() { _ = db.Delete(ctx, coinbaseTx.TxIDChainHash()) }()

		// Try to spend at the same block height (before maturity).
		// CoinbaseMaturity in test settings is 1, so coinbaseSpendingHeight = 1000 + 1 = 1001.
		// Spending at blockHeight <= coinbaseSpendingHeight should fail.
		coinbaseSpendTx := bt.NewTx()
		require.NoError(t, coinbaseSpendTx.From(
			coinbaseTx.TxIDChainHash().String(), 0,
			coinbaseTx.Outputs[0].LockingScript.String(),
			coinbaseTx.Outputs[0].Satoshis,
		))
		require.NoError(t, coinbaseSpendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, resultSpends, err := db.SpendAndCreate(ctx, coinbaseSpendTx, 1000, utxostore.WithSpendOnly())
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.NotEmpty(t, resultSpends)
		// The coinbase is immature — check for the correct error type
		require.ErrorIs(t, resultSpends[0].Err, errors.ErrTxCoinbaseImmature)
	})

	t.Run("double spend returns ErrSpent with ConflictingTxID", func(t *testing.T) {
		// Create a unique tx for this sub-test using a fresh transaction
		uniqueTx := newTestTx(t, 5_000_000)

		_, _, err := db.SpendAndCreate(ctx, uniqueTx, 1000, utxostore.WithCreateOnly())
		require.NoError(t, err)
		defer func() { _ = db.Delete(ctx, uniqueTx.TxIDChainHash()) }()

		// First spend succeeds
		spendTx1 := bt.NewTx()
		require.NoError(t, spendTx1.From(
			uniqueTx.TxIDChainHash().String(), 0,
			uniqueTx.Outputs[0].LockingScript.String(),
			uniqueTx.Outputs[0].Satoshis,
		))
		require.NoError(t, spendTx1.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, _, err = db.SpendAndCreate(ctx, spendTx1, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
		require.NoError(t, err)

		// Second spend with a DIFFERENT spending tx → double spend
		spendTx2 := bt.NewTx()
		require.NoError(t, spendTx2.From(
			uniqueTx.TxIDChainHash().String(), 0,
			uniqueTx.Outputs[0].LockingScript.String(),
			uniqueTx.Outputs[0].Satoshis,
		))
		require.NoError(t, spendTx2.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000)) // different amount → different txid

		_, resultSpends, err := db.SpendAndCreate(ctx, spendTx2, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.NotEmpty(t, resultSpends)
		require.ErrorIs(t, resultSpends[0].Err, errors.ErrSpent)
		// The conflicting tx ID should be the first spending tx
		require.NotNil(t, resultSpends[0].ConflictingTxID)
		require.Equal(t, spendTx1.TxIDChainHash().String(), resultSpends[0].ConflictingTxID.String())
	})
}

// GetSpendNotFound tests that GetSpend returns a SpendResponse with Status_NOT_FOUND
// (and nil error) when the referenced UTXO doesn't exist. This is a behavioral contract:
// not-found is a status, not an error.
func GetSpendNotFound(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	nonExistentHash, _ := chainhash.NewHashFromStr("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	fakeUTXOHash, _ := chainhash.NewHashFromStr("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")

	resp, err := db.GetSpend(ctx, &utxostore.Spend{
		TxID:     nonExistentHash,
		Vout:     0,
		UTXOHash: fakeUTXOHash,
	})
	require.NoError(t, err, "GetSpend for non-existent tx must return nil error, not an error")
	require.NotNil(t, resp)
	require.Equal(t, int(utxostore.Status_NOT_FOUND), resp.Status)
}

// SetBlockHeightZero tests that SetBlockHeight(0) returns an InvalidArgumentError.
// Block height zero is invalid because it would break DAH calculations and maturity checks.
func SetBlockHeightZero(t *testing.T, db utxostore.Store) {
	err := db.SetBlockHeight(0)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrInvalidArgument)
}

// SetBlockStateContract pins the write side of the GetBlockState snapshot
// guarantee on a real store (issue 1443): after one SetBlockState call the
// snapshot AND the single-field getters must all agree. Height matters most
// here: stores keep it in a separate atomic that DAH and maturity logic read
// directly, so a SetBlockState that updated only the snapshot pair would
// silently freeze those readers at a stale height while every pair-based test
// stayed green. Height zero is rejected exactly like SetBlockHeight(0).
//
// The store's block state is restored on the way out, so this is safe to run
// against a suite-wide shared store: later cases that derive a height from
// GetBlockHeight() see what they would have seen without it. A store that
// started at height zero cannot be restored — zero is not a settable height —
// so run this case last against such a store.
func SetBlockStateContract(t *testing.T, db utxostore.Store) {
	prior := db.GetBlockState()
	if prior.Height > 0 {
		defer func() {
			require.NoError(t, db.SetBlockState(prior.Height, prior.MedianTime))
		}()
	}

	require.NoError(t, db.SetBlockState(4242, 1700000042))

	state := db.GetBlockState()
	require.Equal(t, uint32(4242), state.Height)
	require.Equal(t, uint32(1700000042), state.MedianTime)
	require.Equal(t, uint32(4242), db.GetBlockHeight(), "single-field height reader must stay in step with the snapshot")
	require.Equal(t, uint32(1700000042), db.GetMedianBlockTime(), "single-field median reader must stay in step with the snapshot")

	err := db.SetBlockState(0, 1700000042)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrInvalidArgument)
}

// SetBlockStateSnapshotUnderConcurrency pins the read side of the same
// guarantee on a real store: GetBlockState must return a pair some single
// writer actually published, never one assembled from two separate reads.
//
// The holder is already covered in isolation by TestBlockStateHolderSnapshot,
// but nothing there stops a store's own GetBlockState from going back to two
// independent atomic loads — which is exactly what issue 1443 was. That
// regression would keep the sequential contract above green, because torn
// pairs only appear while a writer is mid-update. So: one writer publishing
// pairs with the fixed relation MedianTime == Height + 1000, readers checking
// the relation holds on every snapshot they observe.
//
// Restores the prior block state on the way out, with the same height-zero
// caveat as SetBlockStateContract.
func SetBlockStateSnapshotUnderConcurrency(t *testing.T, db utxostore.Store) {
	prior := db.GetBlockState()
	if prior.Height > 0 {
		defer func() {
			require.NoError(t, db.SetBlockState(prior.Height, prior.MedianTime))
		}()
	}

	const (
		baseHeight = uint32(5_000_000)
		iterations = uint32(20_000)
		offset     = uint32(1000)
	)

	require.NoError(t, db.SetBlockState(baseHeight, baseHeight+offset))

	var (
		wg        sync.WaitGroup
		tornMu    sync.Mutex
		torn      []utxostore.BlockState
		samples   atomic.Uint64
		tornFound atomic.Bool
	)

	stop := make(chan struct{})

	for r := 0; r < 4; r++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				got := db.GetBlockState()
				samples.Add(1)

				// Recorded and asserted after wg.Wait: testify's FailNow is only
				// valid on the goroutine running the test.
				if got.MedianTime != got.Height+offset {
					tornMu.Lock()
					torn = append(torn, got)
					tornMu.Unlock()

					tornFound.Store(true)

					return
				}
			}
		}()
	}

	// Recorded rather than asserted inside the loop: a FailNow here would skip
	// close(stop) and leave the readers spinning for the rest of the run.
	var writeErr error

	// The writer keeps going until the readers have actually sampled, rather
	// than stopping at a fixed count and asserting the sample total afterwards.
	// On a single-processor run the readers may not be scheduled at all while a
	// fixed-length writer loop runs, which would leave the torn-pair assertion
	// vacuously true; asserting the count instead would fail the case on
	// correct code. Gating the loop keeps it meaningful on one core either way.
	//
	// Note the tear itself is only reliably observable with GOMAXPROCS > 1: at
	// -cpu=1 a reader can only interleave with the writer where the scheduler
	// preempts it, so this case guards the regression in CI, not on one core.
	const minSamples = uint64(1_000)

	deadline := time.Now().Add(30 * time.Second)

	for i := baseHeight + 1; ; i++ {
		if writeErr = db.SetBlockState(i, i+offset); writeErr != nil {
			break
		}

		// Readers exit on the first torn pair, so stop chasing minSamples once
		// the case has already failed — otherwise a real tear costs a stall.
		if tornFound.Load() {
			break
		}

		if i-baseHeight >= iterations && samples.Load() >= minSamples {
			break
		}

		if time.Now().After(deadline) {
			break
		}
	}

	close(stop)
	wg.Wait()

	require.NoError(t, writeErr)
	require.Empty(t, torn, "GetBlockState returned torn pairs: %v", torn)
	// Vacuity guard: the assertion above also holds for a reader loop that never
	// ran. The writer loop gates on this, so reaching zero means the readers
	// were starved for the full deadline rather than merely descheduled.
	require.NotZero(t, samples.Load(), "readers observed no snapshots at all")
}

// SetLockedBehavior tests the SetLocked lifecycle:
//   - Locking a tx makes its outputs report Status_LOCKED via GetSpend
//   - Spending a locked tx returns ErrTxLocked
//   - Unlocking a tx restores normal spendability
func SetLockedBehavior(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	// Create a unique tx for this test
	uniqueTx := newTestTx(t, 6_000_000)

	_, _, err := db.SpendAndCreate(ctx, uniqueTx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)
	defer func() { _ = db.Delete(ctx, uniqueTx.TxIDChainHash()) }()

	utxoHash, err := util.UTXOHashFromOutput(uniqueTx.TxIDChainHash(), uniqueTx.Outputs[0], 0)
	require.NoError(t, err)

	spendObj := &utxostore.Spend{
		TxID:     uniqueTx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: utxoHash,
	}

	// Verify initial state is OK (spendable)
	resp, err := db.GetSpend(ctx, spendObj)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_OK), resp.Status)

	// Lock the transaction
	txHash := *uniqueTx.TxIDChainHash()
	err = db.SetLocked(ctx, []chainhash.Hash{txHash}, true)
	require.NoError(t, err)

	// Verify it's now LOCKED
	resp, err = db.GetSpend(ctx, spendObj)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_LOCKED), resp.Status)

	// Spending a locked tx should fail with ErrTxLocked
	lockedSpendTx := bt.NewTx()
	require.NoError(t, lockedSpendTx.From(
		uniqueTx.TxIDChainHash().String(), 0,
		uniqueTx.Outputs[0].LockingScript.String(),
		uniqueTx.Outputs[0].Satoshis,
	))
	require.NoError(t, lockedSpendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	_, resultSpends, err := db.SpendAndCreate(ctx, lockedSpendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrUtxoError)
	require.NotEmpty(t, resultSpends)
	require.ErrorIs(t, resultSpends[0].Err, errors.ErrTxLocked)

	// Unlock the transaction
	err = db.SetLocked(ctx, []chainhash.Hash{txHash}, false)
	require.NoError(t, err)

	// Verify it's back to OK
	resp, err = db.GetSpend(ctx, spendObj)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_OK), resp.Status)

	// Now spending should succeed
	_, _, err = db.SpendAndCreate(ctx, lockedSpendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err)
}

// SetConflictingBehavior tests the SetConflicting lifecycle:
//   - Creating a tx as conflicting makes its outputs report Status_CONFLICTING via GetSpend
//   - Spending a conflicting tx returns ErrTxConflicting
//   - Conflicting flag is visible via Get metadata
//
// Note: SetConflicting(false) to clear conflicting is not tested here because the
// implementation internally starts a write transaction and calls s.Get/s.GetSpend
// on the main pool, which deadlocks with SQLite's concurrency model. The existing
// Conflicting() shared test covers the creation path; this test adds GetSpend and
// error-type verification.
func SetConflictingBehavior(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	// Need a parent for Tx's input so conflicting child tracking works
	_, _, err := db.SpendAndCreate(ctx, ParentTx, 999, utxostore.WithCreateOnly())
	require.NoError(t, err)
	defer func() { _ = db.Delete(ctx, ParentTx.TxIDChainHash()) }()

	// Create Tx marked as conflicting — Tx's input references ParentTx
	_, _, err = db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithConflicting(true), utxostore.WithCreateOnly())
	require.NoError(t, err)
	defer func() { _ = db.Delete(ctx, Tx.TxIDChainHash()) }()

	utxoHash, err := util.UTXOHashFromOutput(Tx.TxIDChainHash(), Tx.Outputs[0], 0)
	require.NoError(t, err)

	spendObj := &utxostore.Spend{
		TxID:     Tx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: utxoHash,
	}

	// Verify it's CONFLICTING via GetSpend
	resp, err := db.GetSpend(ctx, spendObj)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_CONFLICTING), resp.Status)

	// Verify via Get metadata
	txMeta, err := db.Get(ctx, Tx.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	assert.True(t, txMeta.Conflicting)

	// Spending should fail with ErrTxConflicting
	conflictingSpendTx := bt.NewTx()
	require.NoError(t, conflictingSpendTx.From(
		Tx.TxIDChainHash().String(), 0,
		Tx.Outputs[0].LockingScript.String(),
		Tx.Outputs[0].Satoshis,
	))
	require.NoError(t, conflictingSpendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	_, resultSpends, err := db.SpendAndCreate(ctx, conflictingSpendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrUtxoError)
	require.NotEmpty(t, resultSpends)
	require.ErrorIs(t, resultSpends[0].Err, errors.ErrTxConflicting)

	// Verify parent has this tx as a conflicting child
	txMeta, err = db.Get(ctx, Tx.Inputs[0].PreviousTxIDChainHash(), fields.Conflicting, fields.ConflictingChildren)
	require.NoError(t, err)
	assert.False(t, txMeta.Conflicting, "parent itself should not be conflicting")
	require.Len(t, txMeta.ConflictingChildren, 1)
	require.Equal(t, Tx.TxIDChainHash().String(), txMeta.ConflictingChildren[0].String())
}

// SetMinedUnminedSince tests that:
//   - An unmined tx has UnminedSince != 0 (set to current block height at creation)
//   - After SetMinedMulti with OnLongestChain=true, UnminedSince becomes 0
//   - MarkTransactionsOnLongestChain(false) sets UnminedSince to current block height
//   - MarkTransactionsOnLongestChain(true) clears UnminedSince back to 0
func SetMinedUnminedSince(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	err := db.SetBlockHeight(200)
	require.NoError(t, err)

	// Create Tx as unmined. Pass blockHeight=200 as the height when it was received.
	// Since no WithMinedBlockInfo option is provided, unmined_since will be set to 200.
	_, _, err = db.SpendAndCreate(ctx, Tx, 200, utxostore.WithCreateOnly())
	require.NoError(t, err)
	defer func() { _ = db.Delete(ctx, Tx.TxIDChainHash()) }()

	// Unmined tx should have UnminedSince set (non-zero) to the block height at creation
	txMeta, err := db.Get(ctx, Tx.TxIDChainHash())
	require.NoError(t, err)
	assert.Equal(t, uint32(200), txMeta.UnminedSince, "unmined tx should have UnminedSince set to creation block height")

	// Mine the tx on the longest chain
	txHash := Tx.TxIDChainHash()
	_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, utxostore.MinedBlockInfo{
		BlockID:        500,
		BlockHeight:    200,
		SubtreeIdx:     1,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	// After mining on longest chain, UnminedSince should be 0
	txMeta, err = db.Get(ctx, Tx.TxIDChainHash())
	require.NoError(t, err)
	assert.Zero(t, txMeta.UnminedSince, "mined tx on longest chain should have UnminedSince=0")

	// Mark as NOT on longest chain
	err = db.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, false)
	require.NoError(t, err)

	// UnminedSince should now be non-zero (set to current block height)
	txMeta, err = db.Get(ctx, Tx.TxIDChainHash())
	require.NoError(t, err)
	assert.NotZero(t, txMeta.UnminedSince, "tx not on longest chain should have UnminedSince set")

	// Mark back as on longest chain
	err = db.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, true)
	require.NoError(t, err)

	// UnminedSince should be cleared again
	txMeta, err = db.Get(ctx, Tx.TxIDChainHash())
	require.NoError(t, err)
	assert.Zero(t, txMeta.UnminedSince, "tx back on longest chain should have UnminedSince=0")
}

// SpendIdempotent tests that spending the same UTXO with the same spending transaction
// is idempotent — the second spend should not return an error because the spending data
// matches what's already stored.
func SpendIdempotent(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	// Create a unique tx for this test
	uniqueTx := newTestTx(t, 7_000_000)

	_, _, err := db.SpendAndCreate(ctx, uniqueTx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)
	defer func() { _ = db.Delete(ctx, uniqueTx.TxIDChainHash()) }()

	// Build a spending tx
	idempotentSpendTx := bt.NewTx()
	require.NoError(t, idempotentSpendTx.From(
		uniqueTx.TxIDChainHash().String(), 0,
		uniqueTx.Outputs[0].LockingScript.String(),
		uniqueTx.Outputs[0].Satoshis,
	))
	require.NoError(t, idempotentSpendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	// First spend succeeds
	_, _, err = db.SpendAndCreate(ctx, idempotentSpendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err)

	// Second spend with the SAME spending tx should be idempotent (no error)
	_, _, err = db.SpendAndCreate(ctx, idempotentSpendTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err, "re-spending with the same spending tx should be idempotent")
}

// SetMinedWithSpent tests the full lifecycle: create → spend all outputs → mine.
// This exercises the path where DAH should be set (fully spent + mined + on longest chain).
// While we can't observe DAH directly through the Store interface, this test verifies
// the observable state is correct after the sequence.
func SetMinedWithSpent(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	err := db.SetBlockHeight(300)
	require.NoError(t, err)

	// Create Tx as unmined
	_, _, err = db.SpendAndCreate(ctx, Tx, 0, utxostore.WithCreateOnly())
	require.NoError(t, err)
	defer func() { _ = db.Delete(ctx, Tx.TxIDChainHash()) }()

	// Spend ALL outputs of Tx
	for vout, output := range Tx.Outputs {
		spendingTx := bt.NewTx()
		require.NoError(t, spendingTx.From(
			Tx.TxIDChainHash().String(), uint32(vout),
			output.LockingScript.String(),
			output.Satoshis,
		))
		require.NoError(t, spendingTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, _, err = db.SpendAndCreate(ctx, spendingTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
		require.NoError(t, err, "spending output %d should succeed", vout)
	}

	// Mine the tx on the longest chain
	txHash := Tx.TxIDChainHash()
	blockIDsMap, err := db.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, utxostore.MinedBlockInfo{
		BlockID:        600,
		BlockHeight:    300,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)
	require.Len(t, blockIDsMap, 1)
	require.Equal(t, []uint32{600}, blockIDsMap[*txHash])

	// Verify the tx is properly mined with all correct metadata
	txMeta, err := db.Get(ctx, Tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{600}, txMeta.BlockIDs)
	require.Equal(t, []uint32{300}, txMeta.BlockHeights)
	require.Equal(t, []int{0}, txMeta.SubtreeIdxs)
	assert.Zero(t, txMeta.UnminedSince, "mined tx should have UnminedSince=0")

	// Mine it in a second block — verify accumulation
	blockIDsMap, err = db.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, utxostore.MinedBlockInfo{
		BlockID:        601,
		BlockHeight:    301,
		SubtreeIdx:     1,
		OnLongestChain: true,
	})
	require.NoError(t, err)
	require.Len(t, blockIDsMap, 1)
	require.Equal(t, []uint32{600, 601}, blockIDsMap[*txHash])

	txMeta, err = db.Get(ctx, Tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{600, 601}, txMeta.BlockIDs)
	require.Equal(t, []uint32{300, 301}, txMeta.BlockHeights)
	require.Equal(t, []int{0, 1}, txMeta.SubtreeIdxs)
}

// MinedThenSpendAllPrunes covers the full delete-at-height lifecycle that
// production nodes depend on for disk reclamation:
//
//  1. Create a tx marked as mined on the longest chain (block_ids populated).
//     At this point its outputs are still unspent, so DAH should be NULL.
//  2. Spend every output one by one via the normal Spend() path — this is
//     the common production case (a tx is mined, then over time its outputs
//     are consumed by child txs).
//  3. Advance block height past any plausible DAH and run the pruner.
//  4. Assert the tx is gone — which is only possible if DAH was set during
//     spend (or via some other DAH path) and picked up by the pruner.
//
// If DAH is never set on a mined-then-spent tx, the pruner has nothing to
// delete and the tx accumulates forever — the disk-bloat bug this test
// guards against.
//
// The caller provides a started, ready pruner service because different
// backends have different readiness requirements (e.g. aerospike builds
// a secondary index asynchronously).
func MinedThenSpendAllPrunes(t *testing.T, db utxostore.Store, prunerSvc pruner.Service) {
	ctx := context.Background()

	require.NotNil(t, prunerSvc, "pruner service must be supplied and started by the caller")

	const mineHeight uint32 = 1000
	require.NoError(t, db.SetBlockHeight(mineHeight))

	// Parent is referenced by Tx's input; create first (unmined is fine for this test).
	_, _, err := db.SpendAndCreate(ctx, ParentTx, mineHeight-1, utxostore.WithCreateOnly())
	require.NoError(t, err)
	defer func() { _ = db.Delete(ctx, ParentTx.TxIDChainHash()) }()

	// Create Tx as UNMINED, then transition to mined via SetMinedMulti — this is the
	// real production flow (tx arrives, gets validated, later a block includes it).
	// Creating directly with WithMinedBlockInfo would skip the mined-transition path
	// that the disk-bloat bug was observed under.
	_, _, err = db.SpendAndCreate(ctx, Tx, mineHeight, utxostore.WithCreateOnly())
	require.NoError(t, err)
	defer func() { _ = db.Delete(ctx, Tx.TxIDChainHash()) }()

	txHash := Tx.TxIDChainHash()

	// Transition to mined on the longest chain. After this, block_ids is populated
	// and unmined_since is NULL, but all outputs are still unspent — so DAH stays
	// NULL (SetMinedMulti only sets DAH when outputs happen to already be spent).
	_, err = db.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, utxostore.MinedBlockInfo{
		BlockID:        100,
		BlockHeight:    mineHeight,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	// Sanity: tx is retrievable before spending.
	_, err = db.Get(ctx, txHash)
	require.NoError(t, err, "tx must be retrievable before spending")

	// Spend every output via the normal Spend path — the exact path production uses.
	for i, out := range Tx.Outputs {
		spendTx := bt.NewTx()
		require.NoError(t, spendTx.From(
			txHash.String(), uint32(i),
			out.LockingScript.String(),
			out.Satoshis,
		))
		require.NoError(t, spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
		_, _, err = db.SpendAndCreate(ctx, spendTx, mineHeight+1, utxostore.WithSpendOnly())
		require.NoError(t, err, "spending output %d should succeed", i)
	}

	// Advance block height well past any plausible DAH and run the pruner.
	// DAH = blockHeight + 1 + retention; pruneHeight is chosen high enough that
	// any reasonable retention value is cleared.
	const pruneHeight uint32 = 1_000_000
	require.NoError(t, db.SetBlockHeight(pruneHeight))

	_, err = prunerSvc.Prune(ctx, pruneHeight, "<MinedThenSpendAllPrunes>")
	require.NoError(t, err)

	// The tx must no longer be retrievable. If DAH was never set, the pruner had
	// nothing to delete, Get returns the tx, and the assertion fails — which is
	// exactly the disk-bloat regression this test guards against.
	_, err = db.Get(ctx, txHash)
	require.Error(t, err, "tx must be deleted by pruner after all outputs are spent on a mined, on-longest-chain tx")
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "expected ErrTxNotFound, got %v", err)
}

// newSpendAndCreateTx builds an extended transaction spending output `vout` of Tx.
// The satoshi amount makes the txid unique per test case.
func newSpendAndCreateTx(t *testing.T, vout uint32, satoshis uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      Tx.TxIDChainHash(),
		Vout:          vout,
		LockingScript: Tx.Outputs[vout].LockingScript,
		Satoshis:      Tx.Outputs[vout].Satoshis,
	}))
	tx.Inputs[0].UnlockingScript = dummyUnlockingScript
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis))

	return tx
}

// SpendAndCreate proves the combined call spends the tx's inputs and creates its
// outputs in one operation.
func SpendAndCreate(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, _, err := db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)

	spendingTx := newSpendAndCreateTx(t, 0, 1001)
	_ = db.Delete(ctx, spendingTx.TxIDChainHash())

	md, resultSpends, err := db.SpendAndCreate(ctx, spendingTx, db.GetBlockHeight()+1)
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Len(t, resultSpends, 1)

	// the parent output is spent by spendingTx
	resp, err := db.GetSpend(ctx, testSpend0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_SPENT), resp.Status)
	require.NotNil(t, resp.SpendingData)
	require.Equal(t, spendingTx.TxIDChainHash().String(), resp.SpendingData.TxID.String())

	// spendingTx's own outputs were created
	created, err := db.Get(ctx, spendingTx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, spendingTx.TxIDChainHash().String(), created.Tx.TxID())
}

// SpendAndCreateCreateOnly proves WithCreateOnly skips the spend phase entirely.
func SpendAndCreateCreateOnly(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	md, resultSpends, err := db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Nil(t, resultSpends)

	resp, err := db.Get(ctx, Tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, Tx.TxIDChainHash().String(), resp.Tx.TxID())

	// creating again surfaces ErrTxExists, same as the old Create
	_, _, err = db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.ErrorIs(t, err, errors.ErrTxExists)
}

// SpendAndCreateSpendOnly proves WithSpendOnly spends the inputs without creating
// the transaction itself.
func SpendAndCreateSpendOnly(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, _, err := db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)

	spendingTx := newSpendAndCreateTx(t, 1, 2001)
	_ = db.Delete(ctx, spendingTx.TxIDChainHash())

	md, resultSpends, err := db.SpendAndCreate(ctx, spendingTx, db.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err)
	require.Nil(t, md)
	require.Len(t, resultSpends, 1)

	// the parent output is spent
	utxoHash1, err := util.UTXOHashFromOutput(Tx.TxIDChainHash(), Tx.Outputs[1], 1)
	require.NoError(t, err)

	resp, err := db.GetSpend(ctx, &utxostore.Spend{TxID: Tx.TxIDChainHash(), Vout: 1, UTXOHash: utxoHash1})
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_SPENT), resp.Status)

	// but the spending tx was NOT created
	_, err = db.Get(ctx, spendingTx.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound)
}

// SpendAndCreateTxExistsKeepsSpends proves the documented ErrTxExists contract:
// when the create phase finds the tx already stored, the error is returned WITHOUT
// rolling back the spends made in the spend phase.
func SpendAndCreateTxExistsKeepsSpends(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, _, err := db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)

	spendingTx := newSpendAndCreateTx(t, 2, 3001)
	_ = db.Delete(ctx, spendingTx.TxIDChainHash())

	// pre-create the spending tx without spending its input
	_, _, err = db.SpendAndCreate(ctx, spendingTx, db.GetBlockHeight()+1, utxostore.WithCreateOnly())
	require.NoError(t, err)

	// combined call: spend phase succeeds, create phase hits ErrTxExists
	_, resultSpends, err := db.SpendAndCreate(ctx, spendingTx, db.GetBlockHeight()+1)
	require.ErrorIs(t, err, errors.ErrTxExists)
	require.Len(t, resultSpends, 1)

	// the spend must still be in place
	utxoHash2, err := util.UTXOHashFromOutput(Tx.TxIDChainHash(), Tx.Outputs[2], 2)
	require.NoError(t, err)

	resp, err := db.GetSpend(ctx, &utxostore.Spend{TxID: Tx.TxIDChainHash(), Vout: 2, UTXOHash: utxoHash2})
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_SPENT), resp.Status)
}

// SpendAndCreateSpendErrorSurfacesPerInput proves a spend-phase failure carries
// per-input errors in the returned spends (conflict detection contract) and that
// the create phase never runs.
func SpendAndCreateSpendErrorSurfacesPerInput(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, _, err := db.SpendAndCreate(ctx, Tx, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)

	winner := newSpendAndCreateTx(t, 3, 4001)
	loser := newSpendAndCreateTx(t, 3, 4002)
	_ = db.Delete(ctx, winner.TxIDChainHash())
	_ = db.Delete(ctx, loser.TxIDChainHash())

	_, _, err = db.SpendAndCreate(ctx, winner, db.GetBlockHeight()+1)
	require.NoError(t, err)

	_, resultSpends, err := db.SpendAndCreate(ctx, loser, db.GetBlockHeight()+1)
	require.ErrorIs(t, err, errors.ErrUtxoError)
	require.Len(t, resultSpends, 1)
	require.ErrorIs(t, resultSpends[0].Err, errors.ErrSpent)
	require.Equal(t, winner.TxIDChainHash().String(), resultSpends[0].ConflictingTxID.String())

	// the loser must not have been created
	_, err = db.Get(ctx, loser.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound)
}

// SpendAndCreateInvalidOptions proves combining WithCreateOnly and WithSpendOnly
// is rejected.
func SpendAndCreateInvalidOptions(t *testing.T, db utxostore.Store) {
	_, _, err := db.SpendAndCreate(context.Background(), Tx, 1000, utxostore.WithCreateOnly(), utxostore.WithSpendOnly())
	require.ErrorIs(t, err, errors.ErrInvalidArgument)
}
