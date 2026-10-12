package blockvalidation

// TestCheckpointHeight_WriteReadAgree asserts invariant I3: the write side
// (blockchain.HighestCheckpointHeight, used by the fast-path fee=0 write) and the
// read side (model.HighestCheckpointHeight, used by checkBlockRewardAndFees on
// revalidation) return the same boundary for the same params.Checkpoints. Since
// blockchain.HighestCheckpointHeight now delegates to model.HighestCheckpointHeight
// there is a single implementation; this test guards that the delegation stays wired
// (a reintroduced divergent copy in either package would fail here) across several
// real and edge-case checkpoint sets.
//
// This file lives in services/blockvalidation (which imports both model and
// services/blockchain) because model cannot import services/blockchain (import cycle).

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
)

func TestCheckpointHeight_WriteReadAgree(t *testing.T) {
	cases := map[string][]chaincfg.Checkpoint{
		"mainnet":        chaincfg.MainNetParams.Checkpoints,
		"testnet":        chaincfg.TestNetParams.Checkpoints,
		"empty":          nil,
		"negative-only":  {{Height: -1}},
		"unordered":      {{Height: 500}, {Height: 100}, {Height: -1}, {Height: 300}},
		"single":         {{Height: 42}},
		"trailing-lower": {{Height: 1000}, {Height: 10}},
	}

	for name, cps := range cases {
		t.Run(name, func(t *testing.T) {
			writeSide := blockchain.HighestCheckpointHeight(cps)
			readSide := model.HighestCheckpointHeight(cps)
			require.Equal(t, readSide, writeSide,
				"fee write/read checkpoint height must be identical (I3) for %s", name)
		})
	}
}

// TestOperatorOverrideFence (T-B5 — invariant I2): asserts that quickValidateOutpointOnly
// returns FALSE for a block above the highest HARDCODED checkpoint height, even when the
// operator-supplied CatchupCheckpointHash/CatchupCheckpointHeight settings are set to a
// height above the hardcoded set (simulating an operator override that would otherwise widen
// the fast-path window). The fast path must be gated on ChainCfgParams.Checkpoints only
// (spec §2.2, invariant I2).
func TestOperatorOverrideFence(t *testing.T) {
	const hardcodedCheckpointHeight = uint32(1000)
	const aboveHardcoded = uint32(1500)
	const operatorOverrideHeight = int32(2000) // higher than hardcoded — the dangerous value

	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	// The store must report fast-path support so the gate turns on the store capability
	// (the mock defaults to false); this test is about the checkpoint/override fence.
	suite.MockUTXOStore.SupportsOutpointOnlySpendResult = true

	// Enable the outpoint-only fast path.
	suite.Server.blockValidation.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true

	// Set the HARDCODED checkpoint to 1000.
	setCheckpoints(t, suite, hardcodedCheckpointHeight)

	// Simulate the operator override: set CatchupCheckpointHeight above the hardcoded value.
	// If quickValidateOutpointOnly honoured this, it would engage at height 1500.
	suite.Server.blockValidation.settings.BlockValidation.CatchupCheckpointHeight = operatorOverrideHeight

	// Block at height 1500: above the hardcoded checkpoint but below the operator override.
	block := &model.Block{Height: aboveHardcoded}

	// The fast path must NOT engage: it uses ChainCfgParams.Checkpoints, not the operator override.
	got := suite.Server.blockValidation.quickValidateOutpointOnly(block)
	require.False(t, got,
		"quickValidateOutpointOnly must return false for height %d (above hardcoded checkpoint %d) "+
			"even when CatchupCheckpointHeight=%d (operator override must not widen the fast-path window; I2)",
		aboveHardcoded, hardcodedCheckpointHeight, operatorOverrideHeight)

	// Sanity: same block at height BELOW the hardcoded checkpoint must return true.
	blockBelow := &model.Block{Height: hardcodedCheckpointHeight - 1}
	gotBelow := suite.Server.blockValidation.quickValidateOutpointOnly(blockBelow)
	require.True(t, gotBelow,
		"quickValidateOutpointOnly must return true for height %d at or below hardcoded checkpoint %d",
		blockBelow.Height, hardcodedCheckpointHeight)
}

// TestQuickValidateOutpointOnly_StoreCapabilityGate asserts that quickValidateOutpointOnly
// engages only when the UTXO store itself reports fast-path support
// (store.SupportsOutpointOnlySpend()) — not by sniffing a URL scheme. A store that does
// not support it (e.g. Aerospike, pending Stage B) keeps the fast path OFF so un-decorated
// inputs are never handed to a store that would hard-error on them. Above the checkpoint it
// is off regardless of store capability.
func TestQuickValidateOutpointOnly_StoreCapabilityGate(t *testing.T) {
	const checkpointHeight = uint32(1000)
	const belowCheckpoint = uint32(500)
	const aboveCheckpoint = uint32(1500)

	tests := []struct {
		name          string
		storeSupports bool
		height        uint32
		want          bool
	}{
		{name: "store supports, below checkpoint", storeSupports: true, height: belowCheckpoint, want: true},
		{name: "store does not support, below checkpoint", storeSupports: false, height: belowCheckpoint, want: false},
		{name: "store supports, above checkpoint", storeSupports: true, height: aboveCheckpoint, want: false},
		{name: "store does not support, above checkpoint", storeSupports: false, height: aboveCheckpoint, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := NewCatchupTestSuite(t)
			defer suite.Cleanup()

			suite.Server.blockValidation.settings.BlockValidation.OutpointOnlyBelowCheckpoint = true
			setCheckpoints(t, suite, checkpointHeight)
			suite.MockUTXOStore.SupportsOutpointOnlySpendResult = tt.storeSupports

			block := &model.Block{Height: tt.height}
			got := suite.Server.blockValidation.quickValidateOutpointOnly(block)
			require.Equal(t, tt.want, got,
				"quickValidateOutpointOnly: storeSupports=%v, height=%d: want %v got %v",
				tt.storeSupports, tt.height, tt.want, got)
		})
	}
}

// TestCheckpointBoundary_B1_Deferred is a placeholder for the FULL T-B1 (spec §6):
// spend at checkpoint+1 of an output created at checkpoint−1, including a coinbase
// output for coinbase maturity. The full multi-block cross-checkpoint sync flow (two
// blocks, real catchup or block-validation pipeline with persisted subtree data)
// cannot be honestly expressed with the CatchupTestSuite mock store and stays deferred
// to e2e/smoketest. The CONSENSUS CORE of T-B1 — a coinbase minimally-created below the
// checkpoint still enforces coinbase maturity when spent outpoint-only — is now covered
// executably at the store layer by TestMinimalCreate_CoinbaseMaturity_OutpointOnlySpend
// (stores/utxo/sql). See spec §6 T-B1.
func TestCheckpointBoundary_B1_Deferred(t *testing.T) {
	t.Skip("full multi-block flow deferred to e2e/smoketest; consensus core covered by stores/utxo/sql.TestMinimalCreate_CoinbaseMaturity_OutpointOnlySpend; see spec §6 T-B1")
}

// TestCheckpointBoundary_B3_Deferred is a placeholder for T-B3 (spec §6):
// a fork diverging below the checkpoint is rejected (both catchup.go checkpoint
// verification and legacy headers-first). This test exercises existing checkpoint
// rejection machinery; confirming it still holds with the fast path enabled requires
// a real multi-header chain with a checkpoint mismatch at the fork height.
// Deferred to e2e/smoketest: see spec §6 T-B3.
func TestCheckpointBoundary_B3_Deferred(t *testing.T) {
	t.Skip("deferred to e2e/smoketest: T-B3 exercises catchup.go:771-773 checkpoint rejection; confirmed by existing integration harness, not exercisable with mock store; see spec §6 T-B3")
}

// TestCheckpointBoundary_B4_Deferred is a placeholder for T-B4 (spec §6):
// reconsiderblock on a below-checkpoint height succeeds (does not return BLOCK_INVALID).
// The underlying checkBlockRewardAndFees height-skip is directly unit-tested in Task 5.
// The full RPC path (handleReconsiderBlock → RevalidateBlock → block.Valid) requires a
// real persisted block with subtree files and a real block-validation pipeline.
// Deferred to e2e/smoketest: see spec §6 T-B4.
func TestCheckpointBoundary_B4_Deferred(t *testing.T) {
	t.Skip("deferred to e2e/smoketest: T-B4 full reconsiderblock RPC path needs real persisted block + subtree files; underlying checkBlockRewardAndFees height-skip covered by Task 5 unit test; see spec §6 T-B4")
}

// TestCheckpointBoundary_I2_Deferred is a placeholder for T-I2 (spec §6):
// first above-checkpoint block validates (full Block.Valid) after a flagged sync.
// This requires a real sync to the checkpoint height with the flag ON, followed by
// a real above-checkpoint block written with its own decorate+fees.
// Deferred to e2e/smoketest: see spec §6 T-I2.
func TestCheckpointBoundary_I2_Deferred(t *testing.T) {
	t.Skip("deferred to e2e/smoketest: T-I2 requires real sync to checkpoint + first above-checkpoint block validation with real subtree data; see spec §6 T-I2")
}
