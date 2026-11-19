package model

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// boundCoinbaseOnlyBlock builds a single-transaction block whose header merkle root IS its coinbase
// txid. That is a real binding — for a one-transaction block the merkle root is the coinbase txid —
// so ValidWithBinding reaches the binding without needing a subtree store. The coinbase pays more
// than the block subsidy, so validation fails BELOW the binding and the call reports a bound body
// together with a consensus verdict.
func boundCoinbaseOnlyBlock(t *testing.T, height uint32) *Block {
	t.Helper()

	// 50 BTC is the regtest subsidy at this height; GetCoinbaseParts is given twice that, so the
	// no-inflation check fails after the body has been bound.
	c1, c2, err := GetCoinbaseParts(height, 100_000_000_000, "/teranode/", []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"})
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromBytes(BuildCoinbase(c1, c2, "0000000000000000", "00000000"))
	require.NoError(t, err)
	require.True(t, coinbase.IsCoinbase())

	hdr := minedHeaderOnParent(t, 4, coinbase.TxIDChainHash(), &chainhash.Hash{})

	return &Block{
		Header:           hdr,
		CoinbaseTx:       coinbase,
		Subtrees:         []*chainhash.Hash{},
		TransactionCount: 1,
		SizeInBytes:      uint64(coinbase.Size()), //nolint:gosec
		Height:           height,
	}
}

// remineHeader grinds the nonce until the header meets its own declared target again, so a test that
// mutates the timestamp does not accidentally fail the proof-of-work step instead of the step it is
// about.
func remineHeader(t *testing.T, hdr *BlockHeader) {
	t.Helper()

	hdr.Nonce = 0

	for {
		if ok, _, _ := hdr.HasMetTargetDifficulty(); ok {
			return
		}

		hdr.Nonce++
		require.Less(t, hdr.Nonce, uint32(50_000_000), "could not re-mine the fixture header")
	}
}

func bindingTestSettings(t *testing.T) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.Checkpoints = nil

	return tSettings
}

func callValidWithBinding(t *testing.T, block *Block, tSettings *settings.Settings) (bool, bool, error) {
	t.Helper()

	return block.ValidWithBinding(context.Background(), ulogger.TestLogger{}, nil, nil,
		txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)
}

// TestBlock_ValidWithBinding_ReportsTheBinding asserts the fact itself: whether THIS invocation
// reconciled the body to the header's merkle root. A caller may treat a consensus failure as a
// verdict on the block HASH only when it did (bitcoin-sv/teranode#4844).
func TestBlock_ValidWithBinding_ReportsTheBinding(t *testing.T) {
	tSettings := bindingTestSettings(t)

	t.Run("false when the call returns above the binding", func(t *testing.T) {
		block := boundCoinbaseOnlyBlock(t, 1)

		// The contextual header checks run before the binding, so a timestamp past the two-hour
		// bound exits above it.
		block.Header.Timestamp = uint32(time.Now().Add(3 * time.Hour).Unix()) //nolint:gosec
		remineHeader(t, block.Header)

		ok, bound, err := callValidWithBinding(t, block, tSettings)
		require.False(t, ok)
		require.Error(t, err)
		require.ErrorContains(t, err, "two hours in the future")
		require.False(t, bound, "nothing has reconciled the body to the header at this point")
	})

	t.Run("true when the call reached the coinbase-only binding", func(t *testing.T) {
		block := boundCoinbaseOnlyBlock(t, 1)

		ok, bound, err := callValidWithBinding(t, block, tSettings)
		require.False(t, ok)
		require.Error(t, err)
		require.True(t, bound, "the coinbase-txid rule binds a single-transaction body to its header")
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a bound body's consensus failure is genuine invalidity, got: %v", err)
	})

	t.Run("false when subtrees are present and no subtree store is supplied", func(t *testing.T) {
		block := boundCoinbaseOnlyBlock(t, 1)
		block.Subtrees = []*chainhash.Hash{{0x01}}

		ok, bound, err := callValidWithBinding(t, block, tSettings)
		require.False(t, ok)
		require.Error(t, err)
		require.False(t, bound, "without a subtree store nothing loads the subtrees, so nothing binds them")
		// The overpaying coinbase still fails the no-inflation check, but on an unbound body that
		// failure is classified corrupt, never invalid.
		require.True(t, errors.IsBlockCorrupt(err), "got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an unbound body must not condemn the hash, got: %v", err)
	})
}

// TestBlock_ValidWithBinding_IsInvocationLocal cannot fail against the design it ships with: the
// binding fact is a return value, so it is invocation-local by construction. It is kept as a guard
// against ONE specific future refactor, because that refactor was actually proposed — holding the
// fact in a field on Block. Valid runs concurrently, from block validation's optimistic background
// goroutine and from block assembly, so a field there would let one invocation observe another's
// result, and an atomic would make the access safe while preserving exactly that bug. This test is
// what makes that change fail loudly. Both calls MUST use the same object, or it proves nothing.
func TestBlock_ValidWithBinding_IsInvocationLocal(t *testing.T) {
	tSettings := bindingTestSettings(t)

	block := boundCoinbaseOnlyBlock(t, 1)

	_, bound, err := callValidWithBinding(t, block, tSettings)
	require.Error(t, err)
	require.True(t, bound, "the first call reaches the binding")

	// Same object, now failing above the binding.
	block.Header.Timestamp = uint32(time.Now().Add(3 * time.Hour).Unix()) //nolint:gosec
	remineHeader(t, block.Header)

	_, bound, err = callValidWithBinding(t, block, tSettings)
	require.Error(t, err)
	require.False(t, bound, "the second call must report ITS OWN binding, not the first call's")
}

// The wrapper-equivalence test that used to sit here has been removed: Valid is a three-line
// delegation to ValidWithBinding, so comparing the two compared the implementation to itself and
// could not fail. The untouched call sites are covered by the suites that exercise them.
