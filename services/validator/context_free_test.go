package validator

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestCheckTransactionContextFree(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	policy := tSettings.Policy
	postGenesis := tSettings.ChainCfgParams.GenesisActivationHeight
	preGenesis := postGenesis - 1

	check := func(tx *bt.Tx) error {
		return CheckTransactionContextFree(tx, tSettings, postGenesis)
	}

	valid := func(t *testing.T) *bt.Tx {
		tx := newPolicyRejectedTestTx(t, 32)
		require.NoError(t, check(tx))

		return tx
	}

	t.Run("a well-formed tx passes", func(t *testing.T) {
		valid(t)
	})

	t.Run("coinbase", func(t *testing.T) {
		tx := bt.NewTx()
		require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
		require.NoError(t, tx.AddOpReturnOutput([]byte{1}))
		require.True(t, tx.IsCoinbase())
		require.True(t, errors.Is(check(tx), errors.ErrTxInvalid))
	})

	t.Run("no inputs", func(t *testing.T) {
		tx := valid(t)
		tx.Inputs = nil
		require.True(t, errors.Is(check(tx), errors.ErrTxInvalid))
	})

	t.Run("no outputs", func(t *testing.T) {
		tx := valid(t)
		tx.Outputs = nil
		require.True(t, errors.Is(check(tx), errors.ErrTxInvalid))
	})

	t.Run("duplicate input", func(t *testing.T) {
		tx := valid(t)
		dup := *tx.Inputs[0]
		tx.Inputs = append(tx.Inputs, &dup)
		require.True(t, errors.Is(check(tx), errors.ErrTxInvalid))
	})

	t.Run("null prevout", func(t *testing.T) {
		tx := valid(t)
		null := bt.NewTx()
		require.NoError(t, null.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
		tx.Inputs = append(tx.Inputs, null.Inputs[0])
		require.True(t, errors.Is(check(tx), errors.ErrTxInvalid))
	})

	t.Run("output value too large", func(t *testing.T) {
		tx := valid(t)
		tx.Outputs[0].Satoshis = MaxSatoshis + 1
		require.True(t, errors.Is(check(tx), errors.ErrTxInvalid))
	})

	t.Run("larger than the policy max tx size", func(t *testing.T) {
		tx := valid(t)
		small := *tSettings
		smallPolicy := *policy
		smallPolicy.MaxTxSizePolicy = tx.Size() - 1
		small.Policy = &smallPolicy
		require.True(t, errors.Is(CheckTransactionContextFree(tx, &small, postGenesis), errors.ErrTxPolicy))
	})

	// the consensus rules that need no parent apply whatever the policy, by protocol era
	unlimitedPolicy := *tSettings
	unlimited := *policy
	unlimited.MaxTxSizePolicy = 0
	unlimitedPolicy.Policy = &unlimited

	t.Run("oversize before Genesis, with no policy size limit", func(t *testing.T) {
		tx := valid(t)
		require.NoError(t, tx.AddOpReturnOutput(make([]byte, 1_000_000)))

		require.True(t, errors.Is(CheckTransactionContextFree(tx, &unlimitedPolicy, preGenesis), errors.ErrTxInvalid))
		require.NoError(t, CheckTransactionContextFree(tx, &unlimitedPolicy, postGenesis))
	})

	t.Run("too many sigops before Genesis", func(t *testing.T) {
		tx := valid(t)

		checksigs := bytes.Repeat([]byte{bscript.OpCHECKSIG}, 20_001)
		tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: bscript.NewFromBytes(checksigs)})

		require.True(t, errors.Is(CheckTransactionContextFree(tx, &unlimitedPolicy, preGenesis), errors.ErrTxInvalid))
		require.NoError(t, CheckTransactionContextFree(tx, &unlimitedPolicy, postGenesis))
	})
}
