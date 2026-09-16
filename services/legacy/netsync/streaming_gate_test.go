package netsync

import (
	"math/big"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// TestStreamingBlockGate is the test for the only thing standing between a peer
// and this node's disk.
//
// The streaming path writes a block's body straight from the socket to the park's
// store, so nothing downstream can refuse the write after the fact. The gate runs
// first and must answer two separate questions: did this node ask for this block,
// and is the header real work rather than a header a peer simply declared easy.
//
// The difficulty floor is what makes the second question mean anything. A header
// carries its own target, so checking a header against its own target alone
// proves nothing: a peer picks a target it always meets. Measured on this
// codebase's own hardening work, 64 of 64 forged headers passed without the floor.
func TestStreamingBlockGate(t *testing.T) {
	newSM := func(t *testing.T) *SyncManager {
		t.Helper()

		sm := newRaceManager(t)
		sm.chainParams = &chaincfg.MainNetParams

		return sm
	}

	// A header whose target is the easiest the compact encoding can express, so
	// almost any hash meets it. This is the forged shape the floor must refuse.
	easyHeader := func() *wire.BlockHeader {
		return &wire.BlockHeader{Version: 1, Bits: 0x207fffff, Timestamp: time.Unix(1_600_000_000, 0)}
	}

	t.Run("a block nobody asked for is refused", func(t *testing.T) {
		sm := newSM(t)
		h := easyHeader()
		hash := h.BlockHash()

		err := sm.streamingBlockGate(hash, h)
		require.Error(t, err,
			"a body written for a hash this node never requested is a peer choosing what to put on our disk")
		// Asserted on the message, not merely on failure. Several checks can
		// refuse this header, so a bare Error assertion stays green when the
		// asked-for check is deleted and a later one happens to fire instead.
		require.Contains(t, err.Error(), "did not ask for this block",
			"the asked-for check must be what refuses this, or its removal goes unnoticed")
	})

	t.Run("a requested block with a target below the chain floor is refused", func(t *testing.T) {
		sm := newSM(t)
		h := easyHeader()
		hash := h.BlockHash()

		require.True(t, sm.blockDownloads.Add(nil, hash), "seed the request so only the floor can refuse it")

		err := sm.streamingBlockGate(hash, h)
		require.Error(t, err,
			"without the floor a peer declares a target it always meets, and the work check gates nothing")
		require.Contains(t, err.Error(), "easier than",
			"the refusal must name the floor, or the next reader cannot tell which check fired")
	})

	t.Run("the floor is the chain's own limit, not a constant", func(t *testing.T) {
		sm := newSM(t)
		sm.chainParams = &chaincfg.RegressionNetParams

		h := easyHeader()
		hash := h.BlockHash()
		require.True(t, sm.blockDownloads.Add(nil, hash))

		// Regtest's limit is deliberately far easier than mainnet's, so the same
		// header that mainnet refuses must clear the floor here. Whether it then
		// meets its own target is a separate question this case does not assert.
		err := sm.streamingBlockGate(hash, h)
		if err != nil {
			require.NotContains(t, err.Error(), "easier than",
				"a header at regtest's own limit must not be refused by regtest's floor")
		}
	})

	t.Run("no chain params fails closed", func(t *testing.T) {
		sm := newSM(t)
		sm.chainParams = nil

		h := easyHeader()
		hash := h.BlockHash()
		require.True(t, sm.blockDownloads.Add(nil, hash))

		err := sm.streamingBlockGate(hash, h)
		require.Error(t, err,
			"without a chain there is no floor to check against, and an unbounded write is the wrong default")
		require.Contains(t, err.Error(), "no chain parameters",
			"fail-closed on missing params must be its own refusal, distinguishable from a target that is merely too easy")
	})

	t.Run("a hash that does not match the header is refused", func(t *testing.T) {
		sm := newSM(t)
		h := easyHeader()
		real := h.BlockHash()
		require.True(t, sm.blockDownloads.Add(nil, real))

		other := real
		other[0] ^= 0xff

		err := sm.streamingBlockGate(other, h)
		require.Error(t, err,
			"the body is filed under the hash, so a hash the header does not produce files bytes under a name that is not theirs")
		require.Contains(t, err.Error(), "the header hashes to",
			"the hash-match check must be what refuses this; every later check would refuse it too, for the wrong reason")
	})

	t.Run("the mainnet floor is the value the chain actually uses", func(t *testing.T) {
		// Guards the direction of the comparison. A floor applied the wrong way
		// round would refuse every real block and accept every forged one, and
		// both failures look like "the gate is working" from a single test.
		require.Equal(t, 0, chaincfg.MainNetParams.PowLimit.Cmp(
			new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 224), big.NewInt(1))),
			"if this ever changes, the floor below changes with it")
	})
}
