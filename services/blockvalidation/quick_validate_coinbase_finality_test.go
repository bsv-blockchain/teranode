package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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

// coinbaseLockedBlock builds a coinbase-only block at preBindHeight that chains from genesis, whose
// coinbase has the given nLockTime and input sequence 0, so finality turns on nLockTime alone. The
// header keeps newPreBindBlock's timestamp.
func (h *preBindHarness) coinbaseLockedBlock(lockTime uint32) *model.Block {
	h.t.Helper()

	const coinbaseNonce = 0x42 // preBindCoinbase nonce for these boundary blocks

	coinbase := preBindCoinbase(h.t, coinbaseNonce)
	coinbase.LockTime = lockTime
	coinbase.Inputs[0].SequenceNumber = 0

	return h.newPreBindBlock(coinbase, nil, coinbase.TxIDChainHash(), 1)
}

// coinbaseLockedBlockStampedAt is coinbaseLockedBlock with the header timestamp set to headerTime
// (the nonce is re-ground for it).
func (h *preBindHarness) coinbaseLockedBlockStampedAt(lockTime, headerTime uint32) *model.Block {
	h.t.Helper()

	block := h.coinbaseLockedBlock(lockTime)

	header := *block.Header
	header.Timestamp = headerTime
	header.Nonce = 0

	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}

		header.Nonce++
	}

	return &model.Block{
		Header:           &header,
		CoinbaseTx:       block.CoinbaseTx,
		Height:           preBindHeight,
		Subtrees:         []*chainhash.Hash(nil),
		TransactionCount: 1,
	}
}

// requirePreCSVHarness asserts the fixture precondition that the harness height is below CSVHeight.
func (h *preBindHarness) requirePreCSVHarness() {
	h.t.Helper()

	require.Less(h.t, preBindHeight, h.bv.settings.ChainCfgParams.CSVHeight, "fixture precondition: the harness height is pre-CSV")
}

// TestQuickValidateBlock_CoinbaseFinalityBoundaries checks the coinbase-finality boundaries on the
// quick-validation route.
//
// Spec: BV-BODY-019, BV-BODY-015 and BV-QUICK-031.
//
// Scenario: Given the quick-route harness, regtest (no checkpoints, so above the highest
// hardcoded checkpoint, where quick validation should still enforce coinbase finality) at
// preBindHeight H = 1, below regtest's CSVHeight. When the coinbase has sequence 0 and
// nLockTime = H-1, Then the block commits; with nLockTime = H+1, Then it is rejected
// ERR_BLOCK_INVALID and never stored. At this height H-1 is 0, which is also final by the
// nLockTime = 0 clause; the H-1 boundary at a height where the two differ is covered on the full
// route by TestValidateBlock_CoinbaseFinality_Service. Before CSV the time threshold is the
// block's own header time: When nLockTime equals the header time, Then the block is rejected and
// never stored; When it is one below the header time, Then the block commits.
func TestQuickValidateBlock_CoinbaseFinalityBoundaries(t *testing.T) {
	const (
		peerID         = "peer"
		noBaseURL      = ""
		blockIsStored  = true
		blockNotStored = false
	)

	t.Run("nLockTime == H-1 with sequence 0 commits", func(t *testing.T) {
		// Given
		harness := newPreBindHarness(t, nil)
		lockedUntilParentHeight := preBindHeight - 1
		block := harness.coinbaseLockedBlock(lockedUntilParentHeight)

		// When
		err := harness.bv.quickValidateBlock(harness.ctx, block, peerID, noBaseURL)

		// Then
		require.NoError(t, err)
		harness.requireBlockStored(block, blockIsStored)
	})

	t.Run("nLockTime == H+1 with sequence 0 is rejected and never stored", func(t *testing.T) {
		// Given
		harness := newPreBindHarness(t, nil)
		lockedUntilNextHeight := preBindHeight + 1
		block := harness.coinbaseLockedBlock(lockedUntilNextHeight)

		// When
		err := harness.bv.quickValidateBlock(harness.ctx, block, peerID, noBaseURL)

		// Then
		requireNonFinalCoinbaseVerdict(t, err)
		harness.requireBlockStored(block, blockNotStored)
	})

	headerTime := uint32(time.Now().Unix()) //nolint:gosec

	t.Run("pre-CSV: nLockTime equal to the header time is rejected and never stored", func(t *testing.T) {
		// Given
		harness := newPreBindHarness(t, nil)
		harness.requirePreCSVHarness()

		lockedUntilHeaderTime := headerTime
		block := harness.coinbaseLockedBlockStampedAt(lockedUntilHeaderTime, headerTime)

		// When
		err := harness.bv.quickValidateBlock(harness.ctx, block, peerID, noBaseURL)

		// Then
		requireNonFinalCoinbaseVerdict(t, err)
		harness.requireBlockStored(block, blockNotStored)
	})

	t.Run("pre-CSV: nLockTime one below the header time commits", func(t *testing.T) {
		// Given
		harness := newPreBindHarness(t, nil)
		harness.requirePreCSVHarness()

		lockedUntilJustBeforeHeaderTime := headerTime - 1
		block := harness.coinbaseLockedBlockStampedAt(lockedUntilJustBeforeHeaderTime, headerTime)

		// When
		err := harness.bv.quickValidateBlock(harness.ctx, block, peerID, noBaseURL)

		// Then
		require.NoError(t, err)
		harness.requireBlockStored(block, blockIsStored)
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
