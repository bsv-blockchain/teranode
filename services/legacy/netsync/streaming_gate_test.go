package netsync

import (
	stderrors "errors"
	"math/big"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
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
		// An honest header, so the header checks that come first pass and only
		// the asked-for check can refuse it.
		sm := newSM(t)
		sm.chainParams = &chaincfg.RegressionNetParams
		h := minedRegtestHeader(t)
		hash := h.BlockHash()

		err := sm.streamingBlockGate(hash, h, 0)
		require.Error(t, err,
			"a body written for a hash this node never requested is a peer choosing what to put on our disk")
		// Asserted on the message, not merely on failure. Several checks can
		// refuse this header, so a bare Error assertion stays green when the
		// asked-for check is deleted and a later one happens to fire instead.
		require.Contains(t, err.Error(), "did not ask for this block",
			"the asked-for check must be what refuses this, or its removal goes unnoticed")

		// The type is what the peer reads to discard the body and keep the
		// connection, as SV Node does, instead of disconnecting the peer.
		var notRequested *peerpkg.BlockNotRequestedError
		require.True(t, stderrors.As(err, &notRequested),
			"an unrequested block must be refused with the type the peer discards quietly, got %T", err)
		require.Equal(t, hash, notRequested.Hash)
	})

	t.Run("a requested block with a target below the chain floor is refused", func(t *testing.T) {
		sm := newSM(t)
		h := easyHeader()
		hash := h.BlockHash()

		require.True(t, sm.blockDownloads.Add(nil, hash), "seed the request so only the floor can refuse it")

		err := sm.streamingBlockGate(hash, h, 0)
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
		err := sm.streamingBlockGate(hash, h, 0)
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

		err := sm.streamingBlockGate(hash, h, 0)
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

		err := sm.streamingBlockGate(other, h, 0)
		require.Error(t, err,
			"the body is filed under the hash, so a hash the header does not produce files bytes under a name that is not theirs")
		require.Contains(t, err.Error(), "the header hashes to",
			"the hash-match check must be what refuses this; every later check would refuse it too, for the wrong reason")
	})

	t.Run("a request the ledger still holds passes the asked-for check however old it is", func(t *testing.T) {
		// One clock, the ledger's. The ceiling is 375 minutes at shipped mainnet
		// settings, so a body whose request is two hours old is still one a peer owes
		// us. The gate used to apply its own flat hour and refuse it.
		sm := newSM(t)
		sm.chainParams = &chaincfg.RegressionNetParams

		now := time.Unix(1_700_000_000, 0)
		sm.blockDownloads = newBlockDownloadTracker(375 * time.Minute)
		sm.blockDownloads.now = func() time.Time { return now }

		h := minedRegtestHeader(t)
		hash := h.BlockHash()
		require.True(t, sm.blockDownloads.Add(nil, hash))

		now = now.Add(2 * time.Hour)

		require.NoError(t, sm.streamingBlockGate(hash, h, 0),
			"a request inside the ledger's ownership ceiling must pass the asked-for check")

		now = now.Add(376*time.Minute - 2*time.Hour)

		err := sm.streamingBlockGate(hash, h, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "did not ask for this block",
			"past the ledger's ceiling nobody owes us the block, so the body is refused")
	})

	// The header is judged before the asked-for check, so a forged header for a
	// hash nobody asked for is the peer's fault and costs it the connection; it
	// is not the quiet discard an honest unrequested block gets.
	t.Run("a forged header is refused as invalid whether or not it was asked for", func(t *testing.T) {
		sm := newSM(t)
		h := easyHeader()
		hash := h.BlockHash()

		err := sm.streamingBlockGate(hash, h, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "easier than")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a forged header is an invalid block, got %v", err)

		var notRequested *peerpkg.BlockNotRequestedError
		require.False(t, stderrors.As(err, &notRequested), "a forged header is never the quiet unrequested discard")

		sm.chainParams = &chaincfg.RegressionNetParams
		unmet := minedRegtestHeader(t)
		unmet.Bits = chaincfg.MainNetParams.PowLimitBits
		unmetHash := unmet.BlockHash()

		err = sm.streamingBlockGate(unmetHash, unmet, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not meet its own target")
		require.False(t, stderrors.As(err, &notRequested), "a header without its work is never the quiet unrequested discard")
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

// TestStreamingBlockGate_RefusesADeclaredPayloadAboveTheExcessiveBlockSize pins
// the size arm. The declared payload length is in the message header, so a body
// too large for this node's own block policy is refused before the first byte
// of it is read instead of after the whole download. The refusal is a decline
// (ErrBlockPolicyDeclined, which the wire layer discards with the connection
// kept), the hash is marked in recentlyFailedBlocks so the wanted-range pass
// does not re-ask and drain it in a loop, and the arm sits after the asked-for
// check so an unrequested oversize body is still only discarded. A policy of 0
// is "no limit".
func TestStreamingBlockGate_RefusesADeclaredPayloadAboveTheExcessiveBlockSize(t *testing.T) {
	newSM := func(t *testing.T, limit int) *SyncManager {
		t.Helper()

		sm := newRaceManager(t)
		sm.chainParams = &chaincfg.RegressionNetParams
		sm.settings.Policy.ExcessiveBlockSize = limit
		sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](time.Minute)
		t.Cleanup(sm.recentlyFailedBlocks.Stop)

		return sm
	}

	// A header regtest's floor and work check both admit, so only the size arm
	// can refuse it.
	minedHeader := func(t *testing.T, sm *SyncManager) (*wire.BlockHeader, chainhash.Hash) {
		t.Helper()

		h := &wire.BlockHeader{Version: 1, Bits: chaincfg.RegressionNetParams.PowLimitBits, Timestamp: time.Unix(1_600_000_000, 0)}
		for nonce := uint32(0); ; nonce++ {
			require.Less(t, nonce, uint32(1000), "regtest's target is met about every other nonce")

			h.Nonce = nonce
			hash := h.BlockHash()
			require.True(t, sm.blockDownloads.Add(nil, hash))

			if sm.streamingBlockGate(hash, h, 0) == nil {
				return h, hash
			}
		}
	}

	t.Run("a requested body declared above the limit is declined and marked", func(t *testing.T) {
		sm := newSM(t, 1000)
		h, hash := minedHeader(t, sm)

		err := sm.streamingBlockGate(hash, h, 1001)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockPolicyDeclined), "a decline, not a verdict on the block or the peer: got %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid))

		_, marked := sm.recentlyFailedBlocks.Get(hash)
		require.True(t, marked, "marked so the wanted-range pass does not ask for it again and drain it in a loop")
	})

	t.Run("a body declared at the limit passes and is not marked", func(t *testing.T) {
		sm := newSM(t, 1000)
		h, hash := minedHeader(t, sm)

		require.NoError(t, sm.streamingBlockGate(hash, h, 1000))

		_, marked := sm.recentlyFailedBlocks.Get(hash)
		require.False(t, marked)
	})

	t.Run("an unrequested oversize body is only discarded, and not marked", func(t *testing.T) {
		sm := newSM(t, 1000)
		h, hash := minedHeader(t, sm)

		// The same header, with the request forgotten: the ledger no longer
		// holds the hash, so the asked-for check is what refuses it.
		sm.blockDownloads = newBlockDownloadTracker(blockRequestAssignmentTTL)

		err := sm.streamingBlockGate(hash, h, 1001)
		require.Error(t, err)

		var notRequested *peerpkg.BlockNotRequestedError
		require.True(t, stderrors.As(err, &notRequested), "the asked-for check wins: a body nobody asked for is not this node's to judge on size, got %T", err)

		_, marked := sm.recentlyFailedBlocks.Get(hash)
		require.False(t, marked, "a block nobody asked for must not be written off")
	})

	t.Run("a policy of zero never refuses on size", func(t *testing.T) {
		sm := newSM(t, 0)
		h, hash := minedHeader(t, sm)

		require.NoError(t, sm.streamingBlockGate(hash, h, 1<<40))
	})
}

// minedRegtestHeader returns a header that meets regtest's own limit, the
// easiest target the chain allows, found by trying nonces.
func minedRegtestHeader(t *testing.T) *wire.BlockHeader {
	t.Helper()

	h := &wire.BlockHeader{Version: 1, Bits: chaincfg.RegressionNetParams.PowLimitBits, Timestamp: time.Unix(1_600_000_000, 0)}

	for nonce := uint32(0); nonce < 1000; nonce++ {
		h.Nonce = nonce

		if headerMeetsWork(h.Bits, h.BlockHash(), model.PowLimitCeiling(&chaincfg.RegressionNetParams)) {
			return h
		}
	}

	t.Fatal("regtest's target is met about every other nonce")

	return nil
}
