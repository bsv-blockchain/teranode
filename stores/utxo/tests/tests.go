package tests

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

var (
	tx, _   = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	spendTx = &bt.Tx{
		Version:  1,
		LockTime: 0,
		Inputs: []*bt.Input{
			{
				PreviousTxOutIndex: 0,
				SequenceNumber:     0,
				PreviousTxScript:   tx.Outputs[0].LockingScript,
				PreviousTxSatoshis: tx.Outputs[0].Satoshis,
			},
		},
		Outputs: []*bt.Output{
			{
				Satoshis:      tx.Outputs[0].Satoshis,
				LockingScript: tx.Outputs[0].LockingScript,
			},
			{
				Satoshis:      tx.Outputs[1].Satoshis,
				LockingScript: tx.Outputs[1].LockingScript,
			},
		},
	}
	utxoHash0, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	testSpend0   = &utxostore.Spend{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: TXHash,
	}
	TXHash  = tx.TxIDChainHash()
	Hash, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	spends  = []*utxostore.Spend{{
		TxID:         TXHash,
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: Hash,
	}}
)

func Store(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, err := db.Create(ctx, tx, 1000)
	require.NoError(t, err)

	resp, err := db.Get(ctx, testSpend0.TxID)
	require.NoError(t, err)
	require.Equal(t, testSpend0.TxID.String(), resp.Tx.TxID())

	_, err = db.Create(context.Background(), tx, 1000)
	require.Error(t, err, errors.ErrTxExists)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
	_, err = db.Spend(context.Background(), spendTx)
	require.NoError(t, err)

	_, err = db.Create(context.Background(), tx, 1000)
	require.Error(t, err, errors.ErrSpent)
}

func Spend(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, err := db.Create(ctx, tx, 1000)
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
	_, err = db.Spend(ctx, spendTx)
	require.NoError(t, err)

	resp, err := db.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, tx.TxIDChainHash().String(), resp.Tx.TxID())

	spendTx2 := spendTx.Clone()
	spendTx2.Outputs = spendTx2.Outputs[1:]

	// try to spend with different txid
	spends, err := db.Spend(context.Background(), spendTx2)
	require.ErrorIs(t, err, errors.ErrTxInvalid)

	// check the individual spend error
	require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
	require.Equal(t, spendTx.TxIDChainHash().String(), spends[0].ConflictingTxID.String())
}

func Restore(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, err := db.Create(ctx, tx, 1000)
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
	_, err = db.Spend(ctx, spendTx)
	require.NoError(t, err)

	// try to reset the utxo
	err = db.UnSpend(ctx, spends)
	require.NoError(t, err)

	resp, err := db.Get(ctx, testSpend0.TxID)
	require.NoError(t, err)
	require.Equal(t, testSpend0.TxID.String(), resp.Tx.TxID())
}

func Freeze(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings()

	_, err := db.Create(ctx, tx, 1000)
	require.NoError(t, err)

	err = db.FreezeUTXOs(ctx, spends, tSettings)
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
	spends, err := db.Spend(ctx, spendTx)
	require.ErrorIs(t, err, errors.ErrTxInvalid)
	require.ErrorIs(t, spends[0].Err, errors.ErrFrozen)

	resp, err := db.GetSpend(ctx, testSpend0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_FROZEN), resp.Status)

	err = db.UnFreezeUTXOs(ctx, spends, tSettings)
	require.NoError(t, err)

	resp, err = db.GetSpend(ctx, testSpend0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_OK), resp.Status)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
	_, err = db.Spend(ctx, spendTx)
	require.NoError(t, err)

	resp, err = db.GetSpend(ctx, testSpend0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_SPENT), resp.Status)
	require.Equal(t, spendTx.TxIDChainHash().String(), resp.SpendingTxID.String())
}

func ReAssign(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings()

	err := db.SetBlockHeight(101)
	require.NoError(t, err)

	_, err = db.Create(ctx, tx, 0)
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

	privKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	newLockingScript, err := bscript.NewP2PKHFromPubKeyBytes(privKey.PubKey().SerialiseCompressed())
	require.NoError(t, err)

	newOutput := &bt.Output{
		Satoshis:      tx.Outputs[0].Satoshis,
		LockingScript: newLockingScript,
	}

	// create a new transaction with a new transaction ID
	spendTx2 := spendTx.Clone()
	_ = spendTx2.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
	spendTx2.Inputs[0].PreviousTxSatoshis = newOutput.Satoshis
	spendTx2.Inputs[0].PreviousTxScript = newOutput.LockingScript

	utxoHash2, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), newOutput, 0)
	testSpend1 := &utxostore.Spend{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash2,
		SpendingTxID: spendTx2.TxIDChainHash(),
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
	resp, err := db.GetSpend(ctx, testSpend0)
	require.Error(t, err)

	resp, err = db.GetSpend(ctx, testSpend1)
	require.NoError(t, err)
	require.Equal(t, int(utxostore.Status_OK), resp.Status)
	require.Nil(t, resp.SpendingTxID)

	// try to spend the old utxo, should fail
	_, err = db.Spend(ctx, spendTx)
	require.Error(t, err)

	// try to spend the new utxo, should fail, block height not reached
	_, err = db.Spend(ctx, spendTx2)
	require.Error(t, err)

	err = db.SetBlockHeight(1101)
	require.NoError(t, err)

	// try to spend the new utxo, should succeed
	_, err = db.Spend(ctx, spendTx2)
	require.NoError(t, err)
}

func Conflicting(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, err := db.Create(ctx, tx, 1000, utxostore.WithConflicting(true))
	require.NoError(t, err)

	spends, err := db.Spend(ctx, spendTx)
	require.ErrorIs(t, err, errors.ErrTxInvalid)
	require.ErrorIs(t, spends[0].Err, errors.ErrTxConflicting)

	// TODO unset conflicting and test again
	_ = err
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
		err = stx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", i+2_000_000)
		require.NoError(t, err)

		_, err = db.Create(ctx, stx, 100)
		require.NoError(t, err)

		// create spending tx
		spentTx := bt.NewTx()
		_ = spentTx.From(stx.TxIDChainHash().String(), 0, stx.Outputs[0].LockingScript.String(), stx.Outputs[0].Satoshis)
		_ = spentTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", i)
		_ = spentTx.ChangeToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", &bt.FeeQuote{})

		_, err = db.Spend(ctx, spentTx)
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
			SpendingTxID: Hash,
		})
		require.NoError(t, err)
		require.Equal(t, int(utxostore.Status_SPENT), resp.Status)
		require.Equal(t, spendingTx.TxIDChainHash().String(), resp.SpendingTxID.String())
	}
}

func Benchmark(b *testing.B, db utxostore.Store) {
	ctx := context.Background()

	spentTx := bt.NewTx()
	_ = spentTx.From(tx.TxIDChainHash().String(), 0, tx.Outputs[0].LockingScript.String(), tx.Outputs[0].Satoshis)
	_ = spentTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)
	_ = spentTx.ChangeToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", &bt.FeeQuote{})

	for i := 0; i < b.N; i++ {
		_, err := db.Create(ctx, tx, 100)
		if err != nil {
			b.Fatal(err)
		}

		spends, err = db.Spend(ctx, spentTx)
		if err != nil {
			b.Fatal(err)
		}

		err = db.UnSpend(ctx, spends)
		if err != nil {
			b.Fatal(err)
		}

		err = db.Delete(ctx, tx.TxIDChainHash())
		if err != nil {
			b.Fatal(err)
		}
	}
}
