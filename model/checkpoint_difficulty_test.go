package model

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

const mainnetHighestCheckpoint = uint32(945_000)

// TestSkipExpectedDifficulty_DeniesOnSyncedNode is the regression test for the
// free-proof-of-work step of GHSA-gggq-8f59-4jm9 as it survives on a synced node.
//
// The expected-nBits (DAA) rule is the only check that binds a block's declared
// difficulty to the chain it claims to extend. It was skipped for EVERY height in
// 1..945000 on a pure height test, so a fully synced node would accept a fork at
// 944,999 declaring the network minimum difficulty — seconds of GPU work instead
// of the real target at that height. The block's own PoW floor cannot substitute:
// it only bounds how easy a target may be, not whether it is the right one.
//
// A node that already holds the whole certified prefix has no legitimate reason
// to accept a NEW block inside it, so it must demand the real difficulty rule.
func TestSkipExpectedDifficulty_DeniesOnSyncedNode(t *testing.T) {
	cps := chaincfg.MainNetParams.Checkpoints

	require.False(t, SkipExpectedDifficulty(cps, 944_999, 964_784),
		"a synced node must apply the real expected-nBits rule to a new below-checkpoint block")
	require.False(t, SkipExpectedDifficulty(cps, 944_999, mainnetHighestCheckpoint),
		"a node exactly at the highest checkpoint has the whole prefix and must not skip")
}

// TestSkipExpectedDifficulty_AllowsDuringInitialSync is the other half of the
// contract, and the reason this predicate is not simply "always check".
//
// Re-deriving the expected difficulty over the historical prefix would require
// reproducing every retarget rule the chain has ever used exactly; that is what
// the pinned checkpoint hashes stand in for. A node still building the prefix
// must keep the skip, or initial block download fails on blocks that are in fact
// canonical.
func TestSkipExpectedDifficulty_AllowsDuringInitialSync(t *testing.T) {
	cps := chaincfg.MainNetParams.Checkpoints

	require.True(t, SkipExpectedDifficulty(cps, 1, 0),
		"a fresh node must keep the skip")
	require.True(t, SkipExpectedDifficulty(cps, 500_000, 499_999),
		"mid-sync must keep the skip")
	require.True(t, SkipExpectedDifficulty(cps, mainnetHighestCheckpoint, mainnetHighestCheckpoint-1),
		"the last block of the prefix must keep the skip")
}

// TestSkipExpectedDifficulty_DeniesAboveCheckpoint — above the prefix there is
// no checkpoint to stand in for the difficulty schedule, so the rule always runs.
func TestSkipExpectedDifficulty_DeniesAboveCheckpoint(t *testing.T) {
	cps := chaincfg.MainNetParams.Checkpoints

	require.False(t, SkipExpectedDifficulty(cps, mainnetHighestCheckpoint+1, 100))
	require.False(t, SkipExpectedDifficulty(cps, 1_000_000, 100))
}

// TestSkipExpectedDifficulty_DeniesGenesis mirrors BelowCheckpoint's mandatory
// height > 0 guard: a peer must not obtain the skip by declaring height 0.
func TestSkipExpectedDifficulty_DeniesGenesis(t *testing.T) {
	require.False(t, SkipExpectedDifficulty(chaincfg.MainNetParams.Checkpoints, 0, 0))
}

// TestSkipExpectedDifficulty_DeniesWithoutCheckpoints — regtest defines none, so
// there is nothing certifying any difficulty schedule and the rule always runs.
func TestSkipExpectedDifficulty_DeniesWithoutCheckpoints(t *testing.T) {
	require.False(t, SkipExpectedDifficulty(chaincfg.RegressionNetParams.Checkpoints, 1, 0))
	require.False(t, SkipExpectedDifficulty(nil, 1, 0))
}
