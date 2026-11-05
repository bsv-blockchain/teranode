package blockvalidation

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// These tests drive BV-BODY-019 (coinbase finality) through the full validation route of the
// service, BlockValidation.ValidateBlock with optimistic mining off, over a real sqlitememory
// blockchain store holding a real ancestor chain. The model tests in
// model/block_coinbase_finality_test.go pin the rule on Block.Valid with a chain the test hands in;
// here the service fetches the ancestor headers itself, so the time-lock cases also prove that the
// chain the service passes is what sets the lock-time cutoff (BV-BODY-015: the parent's
// median-time-past from CSVHeight, else the block's own timestamp).

// finalityAncestorCount is how many coinbase-only blocks the fixture stores on top of genesis
// before the candidate. The candidate is at height finalityAncestorCount+1.
const finalityAncestorCount = 5

// serviceFinalityChain is a blockchain store holding genesis plus finalityAncestorCount blocks
// with chosen timestamps, and the service validating against it.
type serviceFinalityChain struct {
	t          *testing.T
	ctx        context.Context
	bv         *BlockValidation
	client     blockchain.ClientI
	tip        *model.BlockHeader
	height     uint32 // the candidate's height
	headerTime uint32 // the candidate's header timestamp
	parentMTP  uint32 // median time past of the 11 blocks ending at the candidate's parent
}

// newServiceFinalityChain stores the ancestor chain and builds the service. csvHeight is copied
// into a private regtest parameter set, so the candidate is pre-CSV when csvHeight > height.
//
// The ancestors are spaced 600s apart, ending well before the candidate's header time, so the
// parent's median time past is strictly below the header time and a time-based nLockTime between
// the two is final before CSV and non-final from it.
func newServiceFinalityChain(t *testing.T, csvHeight uint32) *serviceFinalityChain {
	t.Helper()

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false // synchronous block.Valid path

	params := *tSettings.ChainCfgParams
	params.CSVHeight = csvHeight
	tSettings.ChainCfgParams = &params

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	genesis, _, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, tSettings.ChainCfgParams.GenesisHash.String(), genesis.Hash().String(), "fixture precondition: the store starts at genesis")

	now := uint32(time.Now().Unix()) //nolint:gosec
	base := now - 7200

	timestamps := []uint32{genesis.Timestamp}
	tip := genesis

	for i := uint32(1); i <= finalityAncestorCount; i++ {
		ts := base + i*600
		cb := coinbaseAtHeight(t, i)
		hdr := minedHeaderWithBits(t, tip.Hash(), cb.TxIDChainHash(), nBitsFrom(t, "207fffff"), ts)

		block, err := model.NewBlock(hdr, cb, []*chainhash.Hash{}, 1, uint64(cb.Size()), i, 0) //nolint:gosec
		require.NoError(t, err)
		// The ancestors stand in for blocks this node already validated: mined and subtrees set,
		// so the candidate does not wait on its parent's processing.
		require.NoError(t, client.AddBlock(ctx, block, "test",
			blockchainoptions.WithMinedSet(true), blockchainoptions.WithSubtreesSet(true)))

		timestamps = append(timestamps, ts)
		tip = hdr
	}

	best, _, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, tip.Hash().String(), best.Hash().String(), "fixture precondition: the ancestor chain is the best chain")

	h := &serviceFinalityChain{
		t:          t,
		ctx:        ctx,
		bv:         bv,
		client:     client,
		tip:        tip,
		height:     finalityAncestorCount + 1,
		headerTime: base + 6000,
		parentMTP:  medianTimePast(timestamps),
	}

	require.Less(t, h.parentMTP, h.headerTime, "fixture precondition: the parent MTP must be below the header time")

	return h
}

// medianTimePast is bitcoin-sv's GetMedianTimePast over the last (up to) 11 timestamps, oldest
// first: sort them and take the element at index n/2.
func medianTimePast(timestamps []uint32) uint32 {
	if len(timestamps) > 11 {
		timestamps = timestamps[len(timestamps)-11:]
	}

	sorted := append([]uint32(nil), timestamps...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	return sorted[len(sorted)/2]
}

// candidate builds a coinbase-only block on the tip at h.height, stamped h.headerTime, whose
// coinbase carries the given nLockTime and input sequence. A coinbase-only body is bound by its
// header (the merkle root is the coinbase txid), so a finality violation is INVALID (BV-BODY-011).
func (h *serviceFinalityChain) candidate(lockTime, sequence uint32) *model.Block {
	h.t.Helper()

	cb := coinbaseAtHeight(h.t, h.height)
	cb.LockTime = lockTime
	cb.Inputs[0].SequenceNumber = sequence

	hdr := minedHeaderWithBits(h.t, h.tip.Hash(), cb.TxIDChainHash(), nBitsFrom(h.t, "207fffff"), h.headerTime)

	block, err := model.NewBlock(hdr, cb, []*chainhash.Hash{}, 1, uint64(cb.Size()), h.height, 0) //nolint:gosec
	require.NoError(h.t, err)

	return block
}

// requireRejectedNonFinal asserts the AC-BV-BODY-019.1 outcome (ERR_BLOCK_INVALID, bitcoin-sv's
// bad-txns-nonfinal), that the verdict is persisted as an invalid record (BV-VERDICT-004), and that
// the block did not become the tip.
func (h *serviceFinalityChain) requireRejectedNonFinal(block *model.Block, err error) {
	h.t.Helper()

	require.Error(h.t, err, "a non-final coinbase must be rejected (BV-BODY-019)")
	require.True(h.t, errors.Is(err, errors.ErrBlockInvalid), "must be ERR_BLOCK_INVALID on a bound body, got: %v", err)
	require.False(h.t, errors.IsBlockCorrupt(err), "a bound body must not be classified corrupt")
	require.Contains(h.t, err.Error(), "bad-txns-nonfinal")

	_, meta, metaErr := h.client.GetBlockHeader(h.ctx, block.Hash())
	require.NoError(h.t, metaErr, "an INVALID verdict on a bound body is persisted (BV-VERDICT-004)")
	require.True(h.t, meta.Invalid, "the stored record must carry the invalid flag")

	best, _, bestErr := h.client.GetBestBlockHeader(h.ctx)
	require.NoError(h.t, bestErr)
	require.Equal(h.t, h.tip.Hash().String(), best.Hash().String(), "a rejected block must not become the tip")
}

// requireAccepted asserts the block was validated, stored valid and became the tip.
func (h *serviceFinalityChain) requireAccepted(block *model.Block, err error) {
	h.t.Helper()

	require.NoError(h.t, err, "a final coinbase must not be rejected on BV-BODY-019")

	_, meta, metaErr := h.client.GetBlockHeader(h.ctx, block.Hash())
	require.NoError(h.t, metaErr)
	require.False(h.t, meta.Invalid, "an accepted block must not be stored invalid")

	best, _, bestErr := h.client.GetBestBlockHeader(h.ctx)
	require.NoError(h.t, bestErr)
	require.Equal(h.t, block.Hash().String(), best.Hash().String(), "an accepted block extends the chain")
}

func (h *serviceFinalityChain) validate(block *model.Block) error {
	h.t.Helper()

	return h.bv.ValidateBlock(h.ctx, block, "http://localhost")
}

// TestValidateBlock_CoinbaseFinality_Service is AC-BV-BODY-019.1 on the service's full validation
// route: a block at height H whose coinbase has nLockTime = H and input sequence 0 is non-final and
// MUST be rejected ERR_BLOCK_INVALID, and the same coinbase with sequence 0xFFFFFFFF MUST NOT be
// rejected on that rule. The blocks are regtest, which has no checkpoints, so every height is above
// the highest hardcoded checkpoint as the AC requires. Regtest's CSVHeight (576) is above H, but
// the time-lock cutoff plays no part in a height-based nLockTime.
func TestValidateBlock_CoinbaseFinality_Service(t *testing.T) {
	csvHeight := test.CreateBaseTestSettings(t).ChainCfgParams.CSVHeight
	require.Greater(t, csvHeight, uint32(finalityAncestorCount+1), "fixture precondition: regtest CSV is not active at H")

	t.Run("AC-BV-BODY-019.1: nLockTime == H with sequence 0 is rejected INVALID", func(t *testing.T) {
		h := newServiceFinalityChain(t, csvHeight)
		block := h.candidate(h.height, 0)

		h.requireRejectedNonFinal(block, h.validate(block))
	})

	t.Run("AC-BV-BODY-019.1 control: nLockTime == H with sequence 0xFFFFFFFF is accepted", func(t *testing.T) {
		h := newServiceFinalityChain(t, csvHeight)
		block := h.candidate(h.height, bt.DefaultSequenceNumber)

		h.requireAccepted(block, h.validate(block))
	})

	t.Run("BV-BODY-019: nLockTime == H-1 with sequence 0 is accepted", func(t *testing.T) {
		h := newServiceFinalityChain(t, csvHeight)
		block := h.candidate(h.height-1, 0)

		h.requireAccepted(block, h.validate(block))
	})
}

// TestValidateBlock_CoinbaseTimeLockCutoff_Service is BV-BODY-019's time-based threshold on the
// service's full route: for nLockTime >= 500,000,000 the threshold is BV-BODY-015's lock-time
// cutoff, the block's own timestamp before CSVHeight and the parent's median time past from it.
// The parent's median time past comes from the ancestor chain the service reads from the blockchain
// store, so the post-CSV cases fail if the service does not pass that chain to block validation.
func TestValidateBlock_CoinbaseTimeLockCutoff_Service(t *testing.T) {
	const (
		preCSV  = uint32(1_000) // CSV not yet active at the candidate's height
		postCSV = uint32(1)     // CSV active at the candidate's height
	)

	between := func(h *serviceFinalityChain) uint32 { return h.parentMTP + (h.headerTime-h.parentMTP)/2 }

	t.Run("pre-CSV: nLockTime between the parent MTP and the header time is accepted", func(t *testing.T) {
		h := newServiceFinalityChain(t, preCSV)
		block := h.candidate(between(h), 0)

		h.requireAccepted(block, h.validate(block))
	})

	t.Run("pre-CSV: nLockTime equal to the header time is rejected INVALID", func(t *testing.T) {
		h := newServiceFinalityChain(t, preCSV)
		block := h.candidate(h.headerTime, 0)

		h.requireRejectedNonFinal(block, h.validate(block))
	})

	t.Run("post-CSV: nLockTime between the parent MTP and the header time is rejected INVALID", func(t *testing.T) {
		h := newServiceFinalityChain(t, postCSV)
		block := h.candidate(between(h), 0)

		h.requireRejectedNonFinal(block, h.validate(block))
	})

	t.Run("post-CSV: nLockTime equal to the parent MTP is rejected INVALID", func(t *testing.T) {
		h := newServiceFinalityChain(t, postCSV)
		block := h.candidate(h.parentMTP, 0)

		h.requireRejectedNonFinal(block, h.validate(block))
	})

	t.Run("post-CSV: nLockTime one below the parent MTP is accepted", func(t *testing.T) {
		h := newServiceFinalityChain(t, postCSV)
		block := h.candidate(h.parentMTP-1, 0)

		h.requireAccepted(block, h.validate(block))
	})
}
