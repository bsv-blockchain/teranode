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

// These tests drive coinbase finality through the full validation route of the service,
// BlockValidation.ValidateBlock with optimistic mining off, over a real sqlitememory blockchain
// store holding a real ancestor chain. The model tests in model/block_coinbase_finality_test.go pin
// the rule on Block.Valid with a chain the test hands in; here the service fetches the ancestor
// headers itself, so the time-lock cases also prove that the chain the service passes is what sets
// the lock-time cutoff (the parent's median time past from CSVHeight, else the block's own
// timestamp).

const (
	// coinbaseFinalityAncestorCount is how many coinbase-only blocks the fixture stores on top of
	// genesis before the candidate. The candidate is at height coinbaseFinalityAncestorCount+1.
	coinbaseFinalityAncestorCount = 5

	// coinbaseFinalityChainStartOffset is how many seconds before now the ancestor timestamps are
	// counted from.
	coinbaseFinalityChainStartOffset = uint32(7200)

	// coinbaseFinalityAncestorSpacing is the number of seconds between consecutive ancestors.
	coinbaseFinalityAncestorSpacing = uint32(600)

	// coinbaseFinalityCandidateTimeOffset is the candidate's header time in seconds after the chain
	// start: well after the last ancestor, so the parent's median time past is below it.
	coinbaseFinalityCandidateTimeOffset = uint32(6000)

	// coinbaseFinalityMedianTimeSpan is how many blocks bitcoin-sv's GetMedianTimePast looks at.
	coinbaseFinalityMedianTimeSpan = 11

	// coinbaseFinalityRegtestBits is the regtest proof-of-work limit every fixture header is mined to.
	coinbaseFinalityRegtestBits = "207fffff"

	// coinbaseFinalityPeerURL is the base URL the service is told the block came from.
	coinbaseFinalityPeerURL = "http://localhost"
)

// coinbaseFinalityChain is a blockchain store holding genesis plus coinbaseFinalityAncestorCount
// blocks with chosen timestamps, and the service validating against it.
type coinbaseFinalityChain struct {
	t          *testing.T
	ctx        context.Context
	bv         *BlockValidation
	client     blockchain.ClientI
	tip        *model.BlockHeader
	height     uint32 // the candidate's height
	headerTime uint32 // the candidate's header timestamp
	parentMTP  uint32 // median time past of the 11 blocks ending at the candidate's parent
}

// newCoinbaseFinalityChain stores the ancestor chain and builds the service. csvHeight is copied
// into a private regtest parameter set, so the candidate is pre-CSV when csvHeight > height.
//
// The ancestors are spaced coinbaseFinalityAncestorSpacing apart, ending well before the
// candidate's header time, so the parent's median time past is strictly below the header time and
// a time-based nLockTime between the two is final before CSV and non-final from it.
func newCoinbaseFinalityChain(t *testing.T, csvHeight uint32) *coinbaseFinalityChain {
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
	chainStart := now - coinbaseFinalityChainStartOffset

	timestamps := []uint32{genesis.Timestamp}
	tip := genesis

	for ancestorHeight := uint32(1); ancestorHeight <= coinbaseFinalityAncestorCount; ancestorHeight++ {
		ancestorTime := chainStart + ancestorHeight*coinbaseFinalityAncestorSpacing
		coinbase := coinbaseAtHeight(t, ancestorHeight)
		header := minedHeaderWithBits(t, tip.Hash(), coinbase.TxIDChainHash(), nBitsFrom(t, coinbaseFinalityRegtestBits), ancestorTime)

		block, err := model.NewBlock(header, coinbase, []*chainhash.Hash{}, 1, uint64(coinbase.Size()), ancestorHeight, 0) //nolint:gosec
		require.NoError(t, err)
		// The ancestors stand in for blocks this node already validated: mined and subtrees set,
		// so the candidate does not wait on its parent's processing.
		require.NoError(t, client.AddBlock(ctx, block, "test",
			blockchainoptions.WithMinedSet(true), blockchainoptions.WithSubtreesSet(true)))

		timestamps = append(timestamps, ancestorTime)
		tip = header
	}

	best, _, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, tip.Hash().String(), best.Hash().String(), "fixture precondition: the ancestor chain is the best chain")

	chain := &coinbaseFinalityChain{
		t:          t,
		ctx:        ctx,
		bv:         bv,
		client:     client,
		tip:        tip,
		height:     coinbaseFinalityAncestorCount + 1,
		headerTime: chainStart + coinbaseFinalityCandidateTimeOffset,
		parentMTP:  coinbaseFinalityMedianTimePast(timestamps),
	}

	require.Less(t, chain.parentMTP, chain.headerTime, "fixture precondition: the parent MTP must be below the header time")

	return chain
}

// coinbaseFinalityMedianTimePast is bitcoin-sv's GetMedianTimePast over the last (up to) 11
// timestamps, oldest first: sort them and take the element at index n/2.
func coinbaseFinalityMedianTimePast(timestamps []uint32) uint32 {
	if len(timestamps) > coinbaseFinalityMedianTimeSpan {
		timestamps = timestamps[len(timestamps)-coinbaseFinalityMedianTimeSpan:]
	}

	sorted := append([]uint32(nil), timestamps...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	return sorted[len(sorted)/2]
}

// timeBetweenParentMTPAndHeaderTime is a time-based nLockTime halfway between the parent's median
// time past and the candidate's header time: final before CSV, non-final from it.
func (chain *coinbaseFinalityChain) timeBetweenParentMTPAndHeaderTime() uint32 {
	return chain.parentMTP + (chain.headerTime-chain.parentMTP)/2
}

// candidateWithCoinbaseLock builds a coinbase-only block on the tip at chain.height, stamped
// chain.headerTime, whose coinbase carries the given nLockTime and input sequence. A coinbase-only
// body is bound by its header (the merkle root is the coinbase txid), so a finality violation is
// INVALID rather than corrupt.
func (chain *coinbaseFinalityChain) candidateWithCoinbaseLock(lockTime, sequence uint32) *model.Block {
	chain.t.Helper()

	coinbase := coinbaseAtHeight(chain.t, chain.height)
	coinbase.LockTime = lockTime
	coinbase.Inputs[0].SequenceNumber = sequence

	header := minedHeaderWithBits(chain.t, chain.tip.Hash(), coinbase.TxIDChainHash(), nBitsFrom(chain.t, coinbaseFinalityRegtestBits), chain.headerTime)

	block, err := model.NewBlock(header, coinbase, []*chainhash.Hash{}, 1, uint64(coinbase.Size()), chain.height, 0) //nolint:gosec
	require.NoError(chain.t, err)

	return block
}

// validateThroughService runs the block through the service's full validation route.
func (chain *coinbaseFinalityChain) validateThroughService(block *model.Block) error {
	chain.t.Helper()

	return chain.bv.ValidateBlock(chain.ctx, block, coinbaseFinalityPeerURL)
}

// requireRejectedAsNonFinal asserts the block was rejected ERR_BLOCK_INVALID with bitcoin-sv's
// bad-txns-nonfinal, that the verdict is persisted as an invalid record, and that the block did not
// become the tip.
func (chain *coinbaseFinalityChain) requireRejectedAsNonFinal(block *model.Block, err error) {
	chain.t.Helper()

	require.Error(chain.t, err, "a non-final coinbase must be rejected")
	require.True(chain.t, errors.Is(err, errors.ErrBlockInvalid), "must be ERR_BLOCK_INVALID on a bound body, got: %v", err)
	require.False(chain.t, errors.IsBlockCorrupt(err), "a bound body must not be classified corrupt")
	require.Contains(chain.t, err.Error(), "bad-txns-nonfinal")

	_, meta, metaErr := chain.client.GetBlockHeader(chain.ctx, block.Hash())
	require.NoError(chain.t, metaErr, "an INVALID verdict on a bound body is persisted")
	require.True(chain.t, meta.Invalid, "the stored record must carry the invalid flag")

	best, _, bestErr := chain.client.GetBestBlockHeader(chain.ctx)
	require.NoError(chain.t, bestErr)
	require.Equal(chain.t, chain.tip.Hash().String(), best.Hash().String(), "a rejected block must not become the tip")
}

// requireAcceptedAsNewTip asserts the block was validated, stored valid and became the tip.
func (chain *coinbaseFinalityChain) requireAcceptedAsNewTip(block *model.Block, err error) {
	chain.t.Helper()

	require.NoError(chain.t, err, "a final coinbase must not be rejected for finality")

	_, meta, metaErr := chain.client.GetBlockHeader(chain.ctx, block.Hash())
	require.NoError(chain.t, metaErr)
	require.False(chain.t, meta.Invalid, "an accepted block must not be stored invalid")

	best, _, bestErr := chain.client.GetBestBlockHeader(chain.ctx)
	require.NoError(chain.t, bestErr)
	require.Equal(chain.t, block.Hash().String(), best.Hash().String(), "an accepted block extends the chain")
}

// TestValidateBlock_CoinbaseFinality_Service checks the height-based coinbase lock on the service's
// full validation route.
//
// Spec: BV-BODY-019 and AC-BV-BODY-019.1.
//
// Scenario: Given a regtest chain of genesis plus 5 stored blocks and a candidate at height H = 6,
// below regtest's CSVHeight (576). When the candidate's coinbase has nLockTime = H and input
// sequence 0, Then the service rejects it ERR_BLOCK_INVALID, records it invalid and keeps the old
// tip. When the coinbase has nLockTime = H-1 and sequence 0, Then the block becomes the new tip.
// Regtest has no checkpoints, so every height is above the highest hardcoded checkpoint; the
// time-lock cutoff plays no part in a height-based nLockTime.
//
// Controls: the same nLockTime = H coinbase with the final sequence 0xFFFFFFFF is accepted, so the
// rejection is down to finality alone.
func TestValidateBlock_CoinbaseFinality_Service(t *testing.T) {
	const (
		nonFinalSequence = uint32(0)
		finalSequence    = bt.DefaultSequenceNumber // 0xFFFFFFFF
	)

	regtestCSVHeight := test.CreateBaseTestSettings(t).ChainCfgParams.CSVHeight
	require.Greater(t, regtestCSVHeight, uint32(coinbaseFinalityAncestorCount+1), "fixture precondition: regtest CSV is not active at H")

	t.Run("coinbase locked until the block's own height with sequence 0 is rejected as invalid", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, regtestCSVHeight)
		lockedUntilBlockHeight := chain.height
		block := chain.candidateWithCoinbaseLock(lockedUntilBlockHeight, nonFinalSequence)

		// When
		err := chain.validateThroughService(block)

		// Then
		chain.requireRejectedAsNonFinal(block, err)
	})

	t.Run("control: the same coinbase with the final sequence 0xFFFFFFFF is accepted", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, regtestCSVHeight)
		lockedUntilBlockHeight := chain.height
		block := chain.candidateWithCoinbaseLock(lockedUntilBlockHeight, finalSequence)

		// When
		err := chain.validateThroughService(block)

		// Then
		chain.requireAcceptedAsNewTip(block, err)
	})

	t.Run("coinbase locked until one below the block's height with sequence 0 is accepted", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, regtestCSVHeight)
		lockedUntilParentHeight := chain.height - 1
		block := chain.candidateWithCoinbaseLock(lockedUntilParentHeight, nonFinalSequence)

		// When
		err := chain.validateThroughService(block)

		// Then
		chain.requireAcceptedAsNewTip(block, err)
	})
}

// TestValidateBlock_CoinbaseTimeLockCutoff_Service checks the time-based coinbase lock on the
// service's full validation route.
//
// Spec: BV-BODY-019 and BV-BODY-015.
//
// Scenario: Given a regtest chain of genesis plus 5 stored blocks 600s apart and a candidate at
// height 6 stamped after them, so the parent's median time past (MTP) is below the candidate's
// header time. When the candidate's coinbase has a time-based nLockTime (>= 500,000,000) and input
// sequence 0, Then before CSV (CSVHeight 1,000) the cutoff is the header time: a lock between the
// parent MTP and the header time is accepted, a lock equal to the header time is rejected
// ERR_BLOCK_INVALID. From CSV (CSVHeight 1) the cutoff is the parent MTP: a lock between the
// parent MTP and the header time, or equal to the parent MTP, is rejected, and a lock one below the
// parent MTP is accepted. The parent MTP comes from the ancestor chain the service reads from the
// blockchain store, so the post-CSV cases fail if the service does not pass that chain to block
// validation.
func TestValidateBlock_CoinbaseTimeLockCutoff_Service(t *testing.T) {
	const (
		preCSV           = uint32(1_000) // CSV not yet active at the candidate's height
		postCSV          = uint32(1)     // CSV active at the candidate's height
		nonFinalSequence = uint32(0)
	)

	t.Run("pre-CSV: nLockTime between the parent MTP and the header time is accepted", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, preCSV)
		block := chain.candidateWithCoinbaseLock(chain.timeBetweenParentMTPAndHeaderTime(), nonFinalSequence)

		// When
		err := chain.validateThroughService(block)

		// Then
		chain.requireAcceptedAsNewTip(block, err)
	})

	t.Run("pre-CSV: nLockTime equal to the header time is rejected INVALID", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, preCSV)
		lockedUntilHeaderTime := chain.headerTime
		block := chain.candidateWithCoinbaseLock(lockedUntilHeaderTime, nonFinalSequence)

		// When
		err := chain.validateThroughService(block)

		// Then
		chain.requireRejectedAsNonFinal(block, err)
	})

	t.Run("post-CSV: nLockTime between the parent MTP and the header time is rejected INVALID", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, postCSV)
		block := chain.candidateWithCoinbaseLock(chain.timeBetweenParentMTPAndHeaderTime(), nonFinalSequence)

		// When
		err := chain.validateThroughService(block)

		// Then
		chain.requireRejectedAsNonFinal(block, err)
	})

	t.Run("post-CSV: nLockTime equal to the parent MTP is rejected INVALID", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, postCSV)
		lockedUntilParentMTP := chain.parentMTP
		block := chain.candidateWithCoinbaseLock(lockedUntilParentMTP, nonFinalSequence)

		// When
		err := chain.validateThroughService(block)

		// Then
		chain.requireRejectedAsNonFinal(block, err)
	})

	t.Run("post-CSV: nLockTime one below the parent MTP is accepted", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, postCSV)
		lockedUntilJustBeforeParentMTP := chain.parentMTP - 1
		block := chain.candidateWithCoinbaseLock(lockedUntilJustBeforeParentMTP, nonFinalSequence)

		// When
		err := chain.validateThroughService(block)

		// Then
		chain.requireAcceptedAsNewTip(block, err)
	})
}
