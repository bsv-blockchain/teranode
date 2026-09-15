package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestUnrequestedBlockDuringCatchup_IsNotAGroundForDisconnect pins the change
// that lets the assignment ledger shed its forgive/reassert machinery.
//
// Below a checkpoint a block has already passed proof of work and the chain
// rejects it if it is wrong, so the disconnect buys nothing validation is not
// already buying, while costing us a supplier during the one phase where
// suppliers are scarce.
func TestUnrequestedBlockDuringCatchup_IsNotAGroundForDisconnect(t *testing.T) {
	sm := newRaceManager(t)

	require.False(t, sm.punishUnrequestedBlock(true),
		"while catching blocks an unasked-for block is free, not misbehaviour")
	require.True(t, sm.punishUnrequestedBlock(false),
		"at the tip the flood defence still applies")
}

func TestPunishUnrequestedBlock_NeverOnRegtest(t *testing.T) {
	sm := newRaceManager(t)
	sm.chainParams = regressionParamsForTest()

	require.False(t, sm.punishUnrequestedBlock(false),
		"the regression harness feeds unrequested blocks on purpose")

	_ = chainhash.Hash{}
}

// regressionParamsForTest returns the exact pointer the production check
// compares against. Pointer identity is load-bearing here and a copied value
// would not match, which is documented at manager.go's isRegtest.
func regressionParamsForTest() *chaincfg.Params {
	return &chaincfg.RegressionNetParams
}
