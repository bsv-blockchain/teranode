// Package utxo — single-source resolution of the reassigned-UTXO hold
// (reconciliation notes 64, open question 43).
package utxo

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

func TestReassignedSpendableBlocks(t *testing.T) {
	// No settings object at all: the constant.
	require.Equal(t, uint32(ReAssignedUtxoSpendableAfterBlocks), ReassignedSpendableBlocks(nil))

	// Settings present but key unset (zero): the constant.
	require.Equal(t, uint32(ReAssignedUtxoSpendableAfterBlocks),
		ReassignedSpendableBlocks(&settings.Settings{}))

	// Settings present with an explicit hold: the setting wins on every store.
	require.Equal(t, uint32(5), ReassignedSpendableBlocks(&settings.Settings{
		UtxoStore: settings.UtxoStoreSettings{ReAssignedUtxoSpendableAfterBlocks: 5},
	}))
}
