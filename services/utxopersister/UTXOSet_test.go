// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"context"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	hash1 = chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})
	// hash2 = chainhash.HashH([]byte{0x05, 0x06, 0x07, 0x08, 0x09})
	// ctx   = context.Background()

	// TX has 1 input and 10 outputs
	txid  = "9797ceee1543d53db03f5cedc877f638119cddb6f2f469af70504d1e1ccecebd"
	tx, _ = bt.NewTxFromString("010000000000000000ef01c0f6beed3f280acac9e3268b3a4b6cecac6160f84f750fdd2f8eac06284d960a000000006a47304402206b2782cc5b4a1d68d34f36df0241964bbc23eca0d2d8d698407429541993b063022016954b628894df8f6295097403148c3d7ae84097b538ab3c46cba2727f6deafd4121030ca32438b798eda7d8a818f108340a85bf77fefe24850979ac5dd7e15000ee1affffffff80746802000000001976a914f13bf914962276da063784e9e8b7ecbd59b20bf888ac0a002d3101000000001976a914954dede73fba730977b8630e3f7c93024b33795f88ac404b4c00000000001976a914e429e73ad33123c1a7248f660a162f0098fb819988ac80841e00000000001976a914df7974fdbb7890e0a608f923ef59112c475c078688ac80841e00000000001976a91422f9476db77bcad3998a9d4f96dbcaa2c9ef507288aca0860100000000001976a9143729fa58808bf6db6bf69e15adc96e0f20c26e6a88ac50c30000000000001976a91417accfc5f92836427c14299c51abbdbaedb791ce88ac204e0000000000001976a91462a4e3fab0ef92f1c130681aa657f8c858b59def88ac10270000000000001976a9149928c96c401b326f93043ce1434680ac502f487b88aca00a0000000000001976a9146ed6d5942deab79b654c1b31b86c3e62a7b5e61c88ac1528ab00000000001976a914239bae4bd2abf49a0a493b962cc0c027936b1b4788ac00000000")
)

func TestPadUTXOs(t *testing.T) {
	utxos := make([]*UTXO, 3)

	utxos[0] = &UTXO{
		Index: uint32(0),
		Value: uint64(0),
	}

	utxos[1] = &UTXO{
		Index: uint32(5),
		Value: uint64(5),
	}

	utxos[2] = &UTXO{
		Index: uint32(11),
		Value: uint64(11),
	}

	padded := PadUTXOsWithNil(utxos)

	assert.Equal(t, 12, len(padded))

	assert.Equal(t, utxos[0], padded[0])
	assert.Nil(t, padded[1])
	assert.Nil(t, padded[2])
	assert.Nil(t, padded[3])
	assert.Nil(t, padded[4])
	assert.Equal(t, utxos[1], padded[5])
	assert.Nil(t, padded[6])
	assert.Nil(t, padded[7])
	assert.Nil(t, padded[8])
	assert.Nil(t, padded[8])
	assert.Nil(t, padded[10])
	assert.Equal(t, utxos[2], padded[11])

	for i, u := range padded {
		if u == nil {
			t.Logf("%d: nil", i)
		} else {
			t.Logf("%d: %d", i, u.Index)
		}
	}
}

func TestNewUTXOSet(t *testing.T) {
	store := memory.New()

	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)

	ud1, err := NewUTXOSet(ctx, ulogger.TestLogger{}, tSettings, store, &hash1, 0)
	require.NoError(t, err)

	ud1.blockHeight = 10

	err = ud1.ProcessTx(tx)
	require.NoError(t, err)

	for i := uint32(0); i < 5; i++ {
		err = ud1.delete(&UTXODeletion{*tx.TxIDChainHash(), i})
		require.NoError(t, err)
	}

	err = ud1.Close()
	require.NoError(t, err)

	checkAdditions(t, ud1)
	checkDeletions(t, ud1)
}

func checkAdditions(t *testing.T, ud *UTXOSet) {
	ctx := context.Background()

	r, err := ud.GetUTXOAdditionsReader(ctx)
	require.NoError(t, err)

	defer r.Close()

	for {
		utxoWrapper, err := NewUTXOWrapperFromReader(context.Background(), r)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}

		require.NoError(t, err)
		assert.Equal(t, tx.TxIDChainHash().String(), utxoWrapper.TxID.String())
		assert.Equal(t, uint32(10), utxoWrapper.Height)
		assert.False(t, utxoWrapper.Coinbase)

		for i, utxo := range utxoWrapper.UTXOs {
			// nolint:gosec
			assert.Equal(t, uint32(i), utxo.Index)
			assert.Equal(t, tx.Outputs[i].Satoshis, utxo.Value)
			assert.True(t, tx.Outputs[i].LockingScript.EqualsBytes(utxo.Script))
		}
	}
}

func checkDeletions(t *testing.T, ud *UTXOSet) {
	r, err := ud.GetUTXODeletionsReader(context.Background())
	require.NoError(t, err)

	defer r.Close()

	// _, err = fileformat.ReadHeader(r)
	// // require.NoError(t, err)

	// Read the deletion caused by the processTX of tx
	_, err = NewUTXODeletionFromReader(r)
	require.NoError(t, err)

	// assert.Equal(t, tx.Inputs[0].PreviousTxID(), utxoDeletion.TxID)

	for i := 0; i < 5; i++ {
		utxoDeletion, err := NewUTXODeletionFromReader(r)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}

		require.NoError(t, err)
		assert.Equal(t, tx.TxIDChainHash().String(), utxoDeletion.TxID.String())
		// nolint:gosec
		assert.Equal(t, uint32(i), utxoDeletion.Index)
	}
}
