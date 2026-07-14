package peer

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// TestShouldArmProcessingTimer covers the per-message processing-watchdog gate
// across the full budget {0, positive} × net {mainnet, testnet, regtest} matrix.
// The watchdog is disarmed for block messages exactly when prefetch ingestion is
// active per the shared UseBlockPrefetchIngestion predicate (budget > 0 AND off
// regression net) — regtest always takes the synchronous path, so it keeps the
// watchdog for blocks. Non-block commands always arm, and the gate is asserted to
// track the predicate rather than a hand-copied rule so the read-loop and sync
// manager cannot drift apart.
func TestShouldArmProcessingTimer(t *testing.T) {
	budgets := []int64{0, 1}
	nets := []wire.BitcoinNet{wire.MainNet, wire.TestNet, wire.RegTestNet}

	for _, budget := range budgets {
		for _, net := range nets {
			prefetch := UseBlockPrefetchIngestion(budget, net)

			// The predicate is true exactly for a positive budget off regtest.
			require.Equal(t, budget > 0 && net != wire.RegTestNet, prefetch)

			// Blocks disarm the watchdog exactly when prefetch ingestion is active.
			require.Equal(t, !prefetch, shouldArmProcessingTimer(wire.CmdBlock, budget, net))

			// Every non-block command always arms, regardless of prefetch mode.
			require.True(t, shouldArmProcessingTimer(wire.CmdTx, budget, net))
		}
	}
}
