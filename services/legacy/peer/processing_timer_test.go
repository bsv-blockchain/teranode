package peer

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// TestShouldArmProcessingTimer covers the per-message processing-watchdog gate:
// the watchdog is skipped only for block messages when prefetch ingestion is
// active (budget > 0 AND off regression net) — regtest always takes the
// synchronous path, so it keeps the watchdog for blocks. Every other case arms.
func TestShouldArmProcessingTimer(t *testing.T) {
	tests := []struct {
		name   string
		cmd    string
		budget int64
		net    wire.BitcoinNet
		want   bool
	}{
		{"block, budget, mainnet -> not armed", wire.CmdBlock, 1, wire.MainNet, false},
		{"block, budget, regtest -> armed (regtest is synchronous)", wire.CmdBlock, 1, wire.RegTestNet, true},
		{"block, no budget, mainnet -> armed", wire.CmdBlock, 0, wire.MainNet, true},
		{"block, no budget, regtest -> armed", wire.CmdBlock, 0, wire.RegTestNet, true},
		{"tx, budget, mainnet -> armed", wire.CmdTx, 1, wire.MainNet, true},
		{"tx, budget, regtest -> armed", wire.CmdTx, 1, wire.RegTestNet, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldArmProcessingTimer(tt.cmd, tt.budget, tt.net))
		})
	}
}
