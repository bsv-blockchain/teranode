package seeder

import (
	"context"
	"net/url"
	"testing"

	utxo_factory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/stretchr/testify/require"
)

func TestExtend(t *testing.T) {
	tx, err := bt.NewTxFromString("0100000001266f75f74d5d10b44f1f9c42d9d8149e1466a3de0ef4c9ccecbe94bbce51f84a000000006a473044022028b04e77eef87871b5e0704056e1793def562381e6b938a90c5d3f8f9fd05cde0220273f22cd2c077b56a04e500f09a9bc75d7c5fc218193976b7c34bfd79ae33d2f41210371dc4cc740476dabd8eaeefb8b678dd71aa8a58700accd5376ea4dd55c4b2823ffffffff04a0860100000000001976a9142f8752f8d52e001a06e90c92080cbb0190c220dc88ac50c30000000000001976a9144580b1033ebb3fce27989601115726bb85648ec688ac901a0000000000001976a914679ada63b578d10418fd8fcdb1c0a23cfc1e411988acc70b0000000000001976a914f8b4d6d51a72ff9594825114698d49188a078a4688ac00000000")
	require.NoError(t, err)

	ctx := context.Background()

	storeURL, err := url.Parse("aerospike://localhost:3000/test?set=utxo&externalStore=file:///external")
	require.NoError(t, err)

	utxoStore, err := utxo_factory.NewStore(ctx, ulogger.TestLogger{}, storeURL, "test", false)
	require.NoError(t, err)

	outpoints := make([]*meta.PreviousOutput, len(tx.Inputs))

	for i, input := range tx.Inputs {
		outpoints[i] = &meta.PreviousOutput{
			PreviousTxID: *input.PreviousTxIDChainHash(),
			Vout:         input.PreviousTxOutIndex,
		}
	}

	err = utxoStore.PreviousOutputsDecorate(ctx, outpoints)
	require.NoError(t, err)

	for i, input := range tx.Inputs {
		input.PreviousTxScript = bscript.NewFromBytes(outpoints[i].LockingScript)
		input.PreviousTxSatoshis = outpoints[i].Satoshis
	}

}
