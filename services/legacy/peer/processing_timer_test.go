package peer

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// TestShouldArmProcessingTimer covers the per-message processing-watchdog gate
// across the net {mainnet, testnet, regtest} matrix. The watchdog is disarmed
// for block messages exactly when prefetch ingestion is active per the shared
// UseBlockPrefetchIngestion predicate (off regression net) — regtest always
// takes the synchronous path, so it keeps the watchdog for blocks. Non-block
// commands always arm, and the gate is asserted to track the predicate rather
// than a hand-copied rule so the two cannot drift apart.
func TestShouldArmProcessingTimer(t *testing.T) {
	nets := []wire.BitcoinNet{wire.MainNet, wire.TestNet, wire.RegTestNet}

	for _, net := range nets {
		prefetch := UseBlockPrefetchIngestion(net)

		// The predicate is true exactly off regtest.
		require.Equal(t, net != wire.RegTestNet, prefetch)

		// Blocks disarm the watchdog exactly when prefetch ingestion is active.
		require.Equal(t, !prefetch, shouldArmProcessingTimer(wire.CmdBlock, net))

		// Every non-block command always arms, regardless of prefetch mode.
		require.True(t, shouldArmProcessingTimer(wire.CmdTx, net))
	}
}
