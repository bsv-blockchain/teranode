package catchup

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestCollectPreviousHeadersIsNotAMedianTimePastWindow pins the trap that keeps the cached
// catchup run away from the median-time-past rule.
//
// collectPreviousHeaders returns only the headers preceding a block WITHIN its batch, so the
// early blocks of a batch get a 1..10-header run whose oldest entry is the common ancestor, not
// genesis. svnode cannot express that state — CBlockIndex::GetMedianTimePast walks pprev and
// stops early only at genesis — so CheckHeaderContextual refuses such a run rather than
// computing a median over too few blocks (issue #1467).
//
// Nothing feeds these headers to that check today: every caller passing CachedHeaders also sets
// DisableOptimisticMining, and the non-optimistic path re-fetches a full run from the store.
// This test exists so that if either of those changes, CI fails here rather than catchup failing
// in production. When issue #1499 makes the cache top up short windows from the store, this test
// should flip to asserting success.
func TestCollectPreviousHeadersIsNotAMedianTimePastWindow(t *testing.T) {
	// A batch from the middle of the chain: the first header's parent is a common ancestor, not
	// genesis — which is what catchup actually fetches.
	batch := createValidChain(t, 20)
	batch[0].HashPrevBlock = randomHash()
	for i := 1; i < len(batch); i++ {
		batch[i].HashPrevBlock = batch[i-1].Hash()
	}

	cache := &HeaderChainCache{}

	// Block at index 2 of the batch: two preceding headers, oldest of which is not genesis.
	const index = 2

	window := cache.collectPreviousHeaders(batch, index, 100)
	require.Len(t, window, index, "the batch cannot supply more than the headers preceding the block")
	require.True(t, window[0].Hash().IsEqual(batch[index-1].Hash()), "newest-first, anchored at the parent")
	require.False(t, window[len(window)-1].HashPrevBlock.IsEqual(&chainhash.Hash{}),
		"the oldest header of the window is the common ancestor, not genesis — the whole point of this test")

	block := childOf(t, batch[index-1])

	tSettings := test.CreateBaseTestSettings(t)
	mainParams := chaincfg.MainNetParams
	tSettings.ChainCfgParams = &mainParams

	err := block.CheckHeaderContextual(window, tSettings, ulogger.TestLogger{})
	require.Error(t, err, "a short run that does not reach genesis is not a usable median window")
	require.Contains(t, err.Error(), "does not reach genesis")

	// Retryable, not a verdict on the block: block validation persists invalid verdicts, and the
	// block is fine — our header context was not.
	require.True(t, errors.Is(err, errors.ErrProcessing))
	require.False(t, errors.Is(err, errors.ErrBlockInvalid))
}

// childOf builds a coinbase-only block anchored on parent, stamped well after it so only the
// window checks can fail. Height 1 keeps it below every BIP34/66/65 activation.
func childOf(t *testing.T, parent *model.BlockHeader) *model.Block {
	t.Helper()

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  parent.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      parent.Timestamp + 1000,
		Bits:           *bits,
		Nonce:          1,
	}

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	block, err := model.NewBlock(header, coinbase, []*chainhash.Hash{}, 1, 123, 1, 0)
	require.NoError(t, err)

	return block
}
