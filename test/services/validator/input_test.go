package validator

import (
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bk/bec"
	"github.com/stretchr/testify/require"
)

func TestCheckInputsWithDuplicateInputs(t *testing.T) {
	privKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	td := daemon.TestDaemon{}

	parentTx := td.CreateTransactionWithOptions(t,
		daemon.WithSkipCheck(),
		daemon.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	tx1 := td.CreateTransactionWithOptions(t,
		daemon.WithInput(parentTx, 0, privKey),
		daemon.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	tx2 := td.CreateTransactionWithOptions(t,
		daemon.WithInput(parentTx, 0, privKey),
		daemon.WithInput(parentTx, 0, privKey),
		daemon.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	tSettings := test.CreateBaseTestSettings()

	tv := validator.NewTxValidator(
		ulogger.TestLogger{},
		tSettings,
	)

	// Check for duplicate inputs
	err = tv.ValidateTransaction(tx1, 0, &validator.Options{
		SkipPolicyChecks: true,
	})
	require.NoError(t, err)

	// Check for duplicate inputs
	err = tv.ValidateTransaction(tx2, 0, &validator.Options{
		SkipPolicyChecks: true,
	})

	require.Error(t, err, "Expected error due to duplicate inputs")
}
