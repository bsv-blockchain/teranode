package utxopersister

import (
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestRunUtxoPersisterToHeight_RequiresDirectMode(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Block.UTXOPersisterDirect = false

	err := RunUtxoPersisterToHeight(logger, tSettings, 0, 300, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "direct mode")
}
