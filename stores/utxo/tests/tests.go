package tests

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	_, err := db.Create(ctx, Tx, 1000)
	require.NoError(t, err)

	resp, err := db.Get(ctx, testSpend0.TxID)
	require.NoError(t, err)
	require.Equal(t, testSpend0.TxID.String(), resp.Tx.TxID())

	_, err = db.Create(context.Background(), Tx, 1000)
	require.Error(t, err, errors.ErrTxExists)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	_, err = db.Spend(context.Background(), spendTx, db.GetBlockHeight()+1)
	require.NoError(t, err)

	_, err = db.Create(context.Background(), Tx, 1000)
	require.Error(t, err, errors.ErrSpent)
}

func Spend(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, err := db.Create(ctx, Tx, 1000)
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	_, err = db.Spend(ctx, spendTx, db.GetBlockHeight()+1)
	require.NoError(t, err)

	resp, err := db.Get(ctx, Tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, Tx.TxIDChainHash().String(), resp.Tx.TxID())

	spendTx2 := spendTx.Clone()
	spendTx2.Outputs = spendTx2.Outputs[1:]

	// try to spend with different txid
	spends, err := db.Spend(context.Background(), spendTx2, db.GetBlockHeight()+1)
	require.ErrorIs(t, err, errors.ErrUtxoError)

	// check the individual spend error
	require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
	require.Equal(t, spendTx.TxIDChainHash().String(), spends[0].ConflictingTxID.String())
}

func Restore(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	_, err := db.Create(ctx, Tx, 1000)
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	_, err = db.Spend(ctx, spendTx, db.GetBlockHeight()+1)
	require.NoError(t, err)

	// try to reset the utxo
	err = db.Unspend(ctx, spends, false)
	require.NoError(t, err)

	resp, err := db.Get(ctx, testSpend0.TxID)
	require.NoError(t, err)
	require.Equal(t, testSpend0.TxID.String(), resp.Tx.TxID())
}

func Freeze(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)

	_, err := db.Create(ctx, Tx, 1000)
	require.NoError(t, err)

	err = db.FreezeUTXOs(ctx, spends, tSettings)
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())
	spends, err := db.Spend(ctx, spendTx, db.GetBlockHeight()+1)
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
	_, err = db.Spend(ctx, spendTx, db.GetBlockHeight()+1)
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

	_, err = db.Create(ctx, Tx, 0)
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
	_, err = db.Spend(ctx, spendTx, db.GetBlockHeight()+1)
	require.Error(t, err)

	// try to spend the new utxo, should fail, block height not reached
	_, err = db.Spend(ctx, spendTx2, db.GetBlockHeight()+1)
	require.Error(t, err)

	err = db.SetBlockHeight(1101)
	require.NoError(t, err)

	// try to spend the new utxo, should succeed
	_, err = db.Spend(ctx, spendTx2, db.GetBlockHeight()+1)
	require.NoError(t, err)
}

func SetMined(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	err := db.SetBlockHeight(101)
	require.NoError(t, err)

	_, err = db.Create(ctx, Tx, 0)
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

	_, err := db.Create(ctx, ParentTx, 999)
	require.NoError(t, err)

	_, err = db.Create(ctx, Tx, 1000, utxostore.WithConflicting(true))
	require.NoError(t, err)

	_ = spendTx.Inputs[0].PreviousTxIDAdd(Tx.TxIDChainHash())

	spends, err := db.Spend(ctx, spendTx, db.GetBlockHeight()+1)
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

		_, err = db.Create(ctx, stx, 100)
		require.NoError(t, err)

		// create spending tx
		spentTx := bt.NewTx()
		_ = spentTx.From(stx.TxIDChainHash().String(), 0, stx.Outputs[0].LockingScript.String(), stx.Outputs[0].Satoshis)
		_ = spentTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", i)
		_ = spentTx.ChangeToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", &bt.FeeQuote{})

		_, err = db.Spend(ctx, spentTx, db.GetBlockHeight()+1)
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
	_ = spentTx.ChangeToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", &bt.FeeQuote{})

	for i := 0; i < b.N; i++ {
		_, err := db.Create(ctx, Tx, 100)
		if err != nil {
			b.Fatal(err)
		}

		spends, err = db.Spend(ctx, spentTx, db.GetBlockHeight()+1)
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
