package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// The quick-validation route never calls block.Valid, so checkQuickValidationCoinbase applies
// model.CoinbaseFinalityViolation itself (bitcoin-sv/teranode#4981). These tests run it over real
// stores (newPreBindHarness: sqlitememory UTXO and blockchain stores behind the ordinary
// LocalClient) on both entry points, and assert the end state: whether the block is in the
// blockchain store afterwards, not only which error came back.

// coinbaseFinalityBlock builds a coinbase-only block at preBindHeight that chains from genesis.
// With final set, the coinbase input carries the final sequence number, so the block is valid and
// must commit. Without it, the sequence is 0 and nLockTime equals the block height, which
// IsTransactionFinal treats as not yet reached, so the coinbase is non-final.
func (h *preBindHarness) coinbaseFinalityBlock(final bool) *model.Block {
	h.t.Helper()

	coinbase := preBindCoinbase(h.t, 0x41)
	coinbase.LockTime = preBindHeight

	if final {
		coinbase.Inputs[0].SequenceNumber = bt.DefaultSequenceNumber
	} else {
		coinbase.Inputs[0].SequenceNumber = 0
	}

	return h.newPreBindBlock(coinbase, nil, coinbase.TxIDChainHash(), 1)
}

// requireBlockStored reports whether the block reached the blockchain store.
func (h *preBindHarness) requireBlockStored(block *model.Block, want bool) {
	h.t.Helper()

	exists, err := h.chain.GetBlockExists(h.ctx, block.Hash())
	require.NoError(h.t, err)
	require.Equal(h.t, want, exists, "block stored in the blockchain store")
}

func requireNonFinalCoinbaseVerdict(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a bound non-final coinbase must condemn invalid, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err), "must NOT be corrupt")
	require.Contains(t, err.Error(), "bad-txns-nonfinal")
}

func TestQuickValidateBlock_CoinbaseFinality(t *testing.T) {
	t.Run("non-final coinbase is rejected and never stored", func(t *testing.T) {
		h := newPreBindHarness(t, nil)
		block := h.coinbaseFinalityBlock(false)

		requireNonFinalCoinbaseVerdict(t, h.bv.quickValidateBlock(h.ctx, block, "peer", ""))
		h.requireBlockStored(block, false)
	})

	t.Run("final coinbase commits", func(t *testing.T) {
		h := newPreBindHarness(t, nil)
		block := h.coinbaseFinalityBlock(true)

		require.NoError(t, h.bv.quickValidateBlock(h.ctx, block, "peer", ""))
		h.requireBlockStored(block, true)
	})
}

// quickValidateAsync drives the default catch-up entry point the way the catch-up loop does, with a
// running subtree write worker. quickValidateBlockAsync commits the block itself on success.
func (h *preBindHarness) quickValidateAsync(block *model.Block) error {
	h.t.Helper()

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	_, _, err := h.bv.quickValidateBlockAsync(h.ctx, block, "peer", "", writeJobsChan)

	close(writeJobsChan)
	require.NoError(h.t, g.Wait())

	return err
}

func TestQuickValidateBlockAsync_CoinbaseFinality(t *testing.T) {
	t.Run("non-final coinbase is rejected and never stored", func(t *testing.T) {
		h := newPreBindHarness(t, nil)
		block := h.coinbaseFinalityBlock(false)

		requireNonFinalCoinbaseVerdict(t, h.quickValidateAsync(block))
		h.requireBlockStored(block, false)
	})

	t.Run("final coinbase commits", func(t *testing.T) {
		h := newPreBindHarness(t, nil)
		block := h.coinbaseFinalityBlock(true)

		require.NoError(t, h.quickValidateAsync(block))
		h.requireBlockStored(block, true)
	})
}
