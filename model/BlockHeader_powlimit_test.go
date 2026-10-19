package model

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// nBitFor builds an NBit from a compact-target string as it appears in a block
// header (big-endian hex, e.g. "1d00ffff").
func nBitFor(t *testing.T, s string) NBit {
	t.Helper()

	nb, err := NewNBitFromString(s)
	require.NoError(t, err)

	return *nb
}

// TestHasMetPowLimit_RejectsFreeTarget is the regression test for the free
// proof-of-work half of GHSA-gggq-8f59-4jm9. The reported attack declared
// nBits=0x207fffff — a target of roughly 2^255, met by about every second nonce
// — and nothing compared that target against the network's own limit, so a
// fabricated header cost two hashes. HasMetTargetDifficulty cannot catch this on
// its own: it only asks whether the hash meets the target the attacker chose.
func TestHasMetPowLimit_RejectsFreeTarget(t *testing.T) {
	bh := &BlockHeader{Bits: nBitFor(t, "207fffff")}

	err := bh.HasMetPowLimit(&chaincfg.MainNetParams)
	require.Error(t, err, "mainnet must reject the minimum-difficulty target that made the reported attack free")
}

// TestHasMetPowLimit_AcceptsNetworkMinimum guards the other direction: the floor
// must not reject a block that declares exactly the network's own declared
// minimum difficulty. Mainnet's PowLimitBits is 0x1d00ffff (difficulty 1), which
// is what the earliest real blocks on the chain carry.
func TestHasMetPowLimit_AcceptsNetworkMinimum(t *testing.T) {
	bh := &BlockHeader{Bits: nBitFor(t, "1d00ffff")}

	require.NoError(t, bh.HasMetPowLimit(&chaincfg.MainNetParams),
		"the network's own minimum-difficulty bits must remain valid")
}

// TestHasMetPowLimit_AcceptsHarderThanLimit — a real mainnet header at any
// meaningful height declares a target far harder than the limit.
func TestHasMetPowLimit_AcceptsHarderThanLimit(t *testing.T) {
	bh := &BlockHeader{Bits: nBitFor(t, "1810b289")}

	require.NoError(t, bh.HasMetPowLimit(&chaincfg.MainNetParams))
}

// TestHasMetPowLimit_HonoursLooserNetworkLimit covers the networks whose two
// declarations of the limit disagree. STN pairs a 2^224 PowLimit with
// PowLimitBits of 0x207fffff. Enforcing PowLimit alone would reject STN's declared
// minimum. Regtest already has a roughly 2^255 PowLimit and is a compatibility
// control; only STN exercises the looser-of-two rule.
func TestHasMetPowLimit_HonoursLooserNetworkLimit(t *testing.T) {
	for _, params := range []*chaincfg.Params{&chaincfg.StnParams, &chaincfg.RegressionNetParams} {
		params := params

		t.Run(params.Name, func(t *testing.T) {
			bh := &BlockHeader{Bits: nBitFor(t, "207fffff")}

			require.NoError(t, bh.HasMetPowLimit(params),
				"%s declares PowLimitBits 0x207fffff, so that target must be accepted there", params.Name)
		})
	}
}

// TestHasMetPowLimit_MalformedBitsDeferToTargetCheck — NBit.CalculateTarget
// returns 0 for negative-encoded or overflowing nBits. A zero target is not a
// limit violation: no positive hash can satisfy it, so HasMetTargetDifficulty
// rejects it with a clearer error. The floor must not claim it as its own.
func TestHasMetPowLimit_MalformedBitsDeferToTargetCheck(t *testing.T) {
	bh := &BlockHeader{Bits: nBitFor(t, "20800000")}

	require.NoError(t, bh.HasMetPowLimit(&chaincfg.MainNetParams),
		"malformed nBits are HasMetTargetDifficulty's rejection, not the floor's")
}

// TestHasMetPowLimit_NilSafe — the floor is called on paths where params may be
// unset (unit tests, mis-wired settings). It must not panic, and an absent limit
// cannot be enforced.
func TestHasMetPowLimit_NilSafe(t *testing.T) {
	bh := &BlockHeader{Bits: nBitFor(t, "207fffff")}

	require.NoError(t, bh.HasMetPowLimit(nil))
	require.NoError(t, bh.HasMetPowLimit(&chaincfg.Params{}))

	var nilHeader *BlockHeader
	require.NoError(t, nilHeader.HasMetPowLimit(&chaincfg.MainNetParams))
}
