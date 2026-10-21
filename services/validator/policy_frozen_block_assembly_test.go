package validator

import (
	"context"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// spendsPolicyFrozenCoin is what keeps a block-context transaction that spends a coin this
// node has frozen out of its own block assembly (issue #1422): IgnorePolicyFreeze lets the
// spend through for the block, not for the template.
func TestSpendsPolicyFrozenCoin(t *testing.T) {
	ctx := context.Background()
	v, _ := newGuardTestValidator(t, "policyfrozenba", 100, 101)

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	parent := bt.NewTx()
	parentInput := &bt.Input{PreviousTxSatoshis: 10_000, PreviousTxScript: script, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, parentInput.PreviousTxIDAdd(&chainhash.Hash{1}))
	parent.Inputs = append(parent.Inputs, parentInput)
	parent.Outputs = []*bt.Output{{Satoshis: 1000, LockingScript: script}, {Satoshis: 1000, LockingScript: script}}

	_, _, err = v.utxoStore.SpendAndCreate(ctx, parent, 101, utxo.WithCreateOnly())
	require.NoError(t, err)

	frozenHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
	require.NoError(t, err)

	// A window still ahead: the consensus tier does not bite yet, the policy tier does.
	require.NoError(t, v.utxoStore.FreezeUTXOs(ctx, []*utxo.Spend{{
		TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: frozenHash, FreezeFrom: 500, FreezeUntil: 600,
	}}, v.settings))

	spending := func(hash *chainhash.Hash, vout uint32) *bt.Tx {
		tx := bt.NewTx()
		input := &bt.Input{PreviousTxOutIndex: vout, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
		require.NoError(t, input.PreviousTxIDAdd(hash))
		tx.Inputs = append(tx.Inputs, input)

		return tx
	}

	require.True(t, v.spendsPolicyFrozenCoin(ctx, spending(parent.TxIDChainHash(), 0), "frozen"))
	require.False(t, v.spendsPolicyFrozenCoin(ctx, spending(parent.TxIDChainHash(), 1), "unfrozen"))

	unknown := chainhash.HashH([]byte("unknown parent"))
	require.False(t, v.spendsPolicyFrozenCoin(ctx, spending(&unknown, 0), "unknown"), "an unknown parent holds no freeze")
}
