// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bn/models"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
)

var (
	// TX has 1 input and 10 outputs
	tx, _ = bt.NewTxFromString("010000000000000000ef01c0f6beed3f280acac9e3268b3a4b6cecac6160f84f750fdd2f8eac06284d960a000000006a47304402206b2782cc5b4a1d68d34f36df0241964bbc23eca0d2d8d698407429541993b063022016954b628894df8f6295097403148c3d7ae84097b538ab3c46cba2727f6deafd4121030ca32438b798eda7d8a818f108340a85bf77fefe24850979ac5dd7e15000ee1affffffff80746802000000001976a914f13bf914962276da063784e9e8b7ecbd59b20bf888ac0a002d3101000000001976a914954dede73fba730977b8630e3f7c93024b33795f88ac404b4c00000000001976a914e429e73ad33123c1a7248f660a162f0098fb819988ac80841e00000000001976a914df7974fdbb7890e0a608f923ef59112c475c078688ac80841e00000000001976a91422f9476db77bcad3998a9d4f96dbcaa2c9ef507288aca0860100000000001976a9143729fa58808bf6db6bf69e15adc96e0f20c26e6a88ac50c30000000000001976a91417accfc5f92836427c14299c51abbdbaedb791ce88ac204e0000000000001976a91462a4e3fab0ef92f1c130681aa657f8c858b59def88ac10270000000000001976a9149928c96c401b326f93043ce1434680ac502f487b88aca00a0000000000001976a9146ed6d5942deab79b654c1b31b86c3e62a7b5e61c88ac1528ab00000000001976a914239bae4bd2abf49a0a493b962cc0c027936b1b4788ac00000000")
)

func TestNode_AddToConsensusBlacklist(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings()

	t.Run("empty", func(t *testing.T) {
		utxoStore := memory.New(ulogger.TestLogger{})
		node := NewNodeConfig(ulogger.TestLogger{}, nil, utxoStore, nil, nil, nil, tSettings)

		_, err := node.AddToConsensusBlacklist(ctx, nil)
		require.Error(t, err)
	})

	t.Run("freeze", func(t *testing.T) {
		utxoStore := memory.New(ulogger.TestLogger{})
		_ = utxoStore.SetBlockHeight(101)
		_, err := utxoStore.Create(ctx, tx, 101)
		require.NoError(t, err)

		node := NewNodeConfig(ulogger.TestLogger{}, nil, utxoStore, nil, nil, nil, tSettings)

		funds := []models.Fund{{
			TxOut: models.TxOut{
				TxId: tx.TxIDChainHash().String(),
				Vout: 0,
			},
			EnforceAtHeight: []models.Enforce{
				{
					Start: 101,
					Stop:  999999999,
				},
			},
		}}

		response, err := node.AddToConsensusBlacklist(ctx, funds)
		require.NoError(t, err)

		// check response
		require.Equal(t, 0, len(response.NotProcessed))

		utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
		require.NoError(t, err)

		// check that the utxo is frozen
		utxoSpend, err := utxoStore.GetSpend(ctx, &utxo.Spend{
			TxID:     tx.TxIDChainHash(),
			Vout:     0,
			UTXOHash: utxoHash,
		})
		require.NoError(t, err)

		// check that the utxo is frozen = 5
		require.Equal(t, 5, utxoSpend.Status)

		// try again, should return an error
		response, err = node.AddToConsensusBlacklist(ctx, funds)
		require.NoError(t, err)

		// check response
		require.Equal(t, 1, len(response.NotProcessed))
		require.Contains(t, response.NotProcessed[0].Reason, "is already frozen")
	})

	t.Run("unfreeze", func(t *testing.T) {
		utxoStore := memory.New(ulogger.TestLogger{})
		_ = utxoStore.SetBlockHeight(101)
		_, err := utxoStore.Create(ctx, tx, 101)
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings()

		utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
		require.NoError(t, err)

		err = utxoStore.FreezeUTXOs(ctx, []*utxo.Spend{{
			TxID:     tx.TxIDChainHash(),
			Vout:     0,
			UTXOHash: utxoHash,
		}}, tSettings)
		require.NoError(t, err)

		// make sure that the utxo is frozen
		utxoSpend, err := utxoStore.GetSpend(ctx, &utxo.Spend{
			TxID:     tx.TxIDChainHash(),
			Vout:     0,
			UTXOHash: utxoHash,
		})
		require.NoError(t, err)

		// check that the utxo is unfrozen = 0
		require.Equal(t, 5, utxoSpend.Status)

		node := NewNodeConfig(ulogger.TestLogger{}, nil, utxoStore, nil, nil, nil, tSettings)

		funds := []models.Fund{{
			TxOut: models.TxOut{
				TxId: tx.TxIDChainHash().String(),
				Vout: 0,
			},
			EnforceAtHeight: []models.Enforce{
				{
					Start: 100,
					Stop:  100, // below current height
				},
			},
		}}

		response, err := node.AddToConsensusBlacklist(ctx, funds)
		require.NoError(t, err)

		// check response
		require.Equal(t, 0, len(response.NotProcessed))

		// check that the utxo is unfrozen
		utxoSpend, err = utxoStore.GetSpend(ctx, &utxo.Spend{
			TxID:     tx.TxIDChainHash(),
			Vout:     0,
			UTXOHash: utxoHash,
		})
		require.NoError(t, err)

		// check that the utxo is unfrozen = 0
		require.Equal(t, 0, utxoSpend.Status)

		// try again, should return an error
		response, err = node.AddToConsensusBlacklist(ctx, funds)
		require.NoError(t, err)

		// check response
		require.Equal(t, 1, len(response.NotProcessed))
		require.Contains(t, response.NotProcessed[0].Reason, "is not frozen")
	})
}

func TestNode_getAddToConsensusBlacklistResponse(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings()

	t.Run("empty", func(t *testing.T) {
		node := NewNodeConfig(ulogger.TestLogger{}, nil, nil, nil, nil, nil, tSettings)

		response, err := node.AddToConsensusBlacklist(ctx, nil)
		require.Error(t, err)
		require.Nil(t, response)
	})

	t.Run("re-assign tx input 0", func(t *testing.T) {
		utxoStore := memory.New(ulogger.TestLogger{})
		_ = utxoStore.SetBlockHeight(101)
		_, err := utxoStore.Create(ctx, tx, 101)
		require.NoError(t, err)

		node := NewNodeConfig(ulogger.TestLogger{}, nil, utxoStore, nil, nil, nil, tSettings)

		btTx := tx.Clone()
		_ = btTx

		confiscationTransaction := bt.Tx{}

		// Generate a random private key
		privateKey, err := bec.NewPrivateKey(bec.S256())
		require.NoError(t, err)

		// create a p2pkh locking script
		lockingScript, err := bscript.NewP2PKHFromPubKeyBytes(privateKey.PubKey().SerialiseCompressed())
		require.NoError(t, err)

		// create an input, that replicates the original output, except the locking script which is going to be replaced
		err = confiscationTransaction.FromUTXOs([]*bt.UTXO{{
			TxIDHash:       tx.TxIDChainHash(),
			Vout:           0,
			LockingScript:  lockingScript,
			Satoshis:       tx.Outputs[0].Satoshis,
			SequenceNumber: bt.DefaultSequenceNumber,
		}}...)
		require.NoError(t, err)

		// send the output to a different (random) address
		_ = confiscationTransaction.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", tx.Outputs[0].Satoshis)

		unlockerGetter := unlocker.Getter{PrivateKey: privateKey}

		err = confiscationTransaction.FillAllInputs(ctx, &unlockerGetter)
		require.NoError(t, err)

		confiscation := []models.ConfiscationTransactionDetails{{
			ConfiscationTransaction: models.ConfiscationTransaction{
				EnforceAtHeight: 102,
				Hex:             confiscationTransaction.String(),
			},
		}}

		response, err := node.AddToConfiscationTransactionWhitelist(ctx, confiscation)
		require.NoError(t, err)

		// check response
		require.Equal(t, 1, len(response.NotProcessed))
		require.Contains(t, response.NotProcessed[0].Reason, "is not frozen")

		utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
		require.NoError(t, err)

		newUtxoHash, err := util.UTXOHashFromInput(confiscationTransaction.Inputs[0])
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings()

		// freeze the utxo
		err = utxoStore.FreezeUTXOs(ctx, []*utxo.Spend{{
			TxID:     tx.TxIDChainHash(),
			Vout:     0,
			UTXOHash: utxoHash,
		}}, tSettings)
		require.NoError(t, err)

		// try again, should NOT return an error, and should succeed
		response, err = node.AddToConfiscationTransactionWhitelist(ctx, confiscation)
		require.NoError(t, err)

		// check response
		require.Equal(t, 0, len(response.NotProcessed))

		// check that the utxo has been re-assigned and unfrozen
		utxoSpend, err := utxoStore.GetSpend(ctx, &utxo.Spend{
			TxID:     tx.TxIDChainHash(),
			Vout:     0,
			UTXOHash: newUtxoHash,
		})
		require.NoError(t, err)

		// check that the utxo is unfrozen = 0
		require.Equal(t, int(utxo.Status_UNSPENDABLE), utxoSpend.Status)
		require.Nil(t, utxoSpend.SpendingData)
	})
}
