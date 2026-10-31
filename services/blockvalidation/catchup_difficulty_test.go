package blockvalidation

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// daaSettings returns test settings whose chain parameters are a copy of base, so the
// caller can pick a network that actually adjusts difficulty (and tweak fields first).
func daaSettings(t *testing.T, base chaincfg.Params) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	p := base
	p.DaaForkHeight = 0 // Synthetic chains exercise the modern DAA.
	tSettings.ChainCfgParams = &p

	return tSettings
}

// newDifficultyStore returns an empty sqlitememory blockchain store for tSettings' chain; the
// store inserts the genesis block itself.
func newDifficultyStore(t *testing.T, tSettings *settings.Settings) blockchain_store.Store {
	t.Helper()

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })

	return store
}

// storeDifficultyHeaders stores headers in order as main-chain blocks; the store takes each height
// from the parent. Only the header fields matter to the difficulty rules, so every block reuses the
// genesis coinbase, changed so the store does not take it for a second genesis.
func storeDifficultyHeaders(t *testing.T, tSettings *settings.Settings, store blockchain_store.Store, headers []*model.BlockHeader) {
	t.Helper()

	genesis, err := model.NewBlockFromMsgBlock(tSettings.ChainCfgParams.GenesisBlock, nil)
	require.NoError(t, err)

	genesis.CoinbaseTx.LockTime = 1

	for _, h := range headers {
		_, _, err = store.StoreBlock(t.Context(), &model.Block{Header: h, CoinbaseTx: genesis.CoinbaseTx, TransactionCount: 1}, "test")
		require.NoError(t, err)
	}
}

// honestDifficultyChain builds n headers on top of genesis, each carrying the bits the
// store-backed calculator demands for it, and returns them indexed by height (genesis at 0).
//
// The calculator reads ancestry through the store's SQL, not through the branch source the code
// under test uses, so it is an independent oracle for what every header must carry. timeAt gives
// the timestamp at each height.
func honestDifficultyChain(t *testing.T, tSettings *settings.Settings, n int, timeAt func(height uint32, prev []*model.BlockHeader) uint32) []*model.BlockHeader {
	t.Helper()

	store := newDifficultyStore(t, tSettings)

	d, err := blockchain.NewDifficulty(store, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	genesis, err := model.NewBlockFromMsgBlock(tSettings.ChainCfgParams.GenesisBlock, nil)
	require.NoError(t, err)

	headers := []*model.BlockHeader{genesis.Header}

	for height := uint32(1); height <= uint32(n); height++ { //nolint:gosec // a fixture size
		parent := headers[height-1]
		ts := timeAt(height, headers)

		bits, err := d.CalcNextWorkRequired(t.Context(), parent, height-1, int64(ts))
		require.NoError(t, err)

		h := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  parent.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      ts,
			Bits:           *bits,
			Nonce:          height,
		}

		storeDifficultyHeaders(t, tSettings, store, []*model.BlockHeader{h})

		headers = append(headers, h)
	}

	return headers
}

// irregularTime spaces blocks unevenly, on average faster than the target so the DAA moves off the
// pow limit, and every eleventh block repeats the timestamp of the block two back while the one
// between is later. That is the equal-time median-of-three case whose tie-break must match the
// store's ordering.
func irregularTime(height uint32, prev []*model.BlockHeader) uint32 {
	if height >= 2 && height%11 == 0 {
		return prev[height-2].Timestamp
	}

	return prev[height-1].Timestamp + 300 + (height*137)%400
}

// withTamperedBits returns a copy of h declaring bits instead of its own.
func withTamperedBits(t *testing.T, h *model.BlockHeader, bits string) *model.BlockHeader {
	t.Helper()

	nBits, err := model.NewNBitFromString(bits)
	require.NoError(t, err)
	require.NotEqual(t, h.Bits, *nBits, "fixture precondition: the tampered bits must differ")

	tampered := *h
	tampered.Bits = *nBits

	return &tampered
}

// daaFixture is an honest chain of 170 headers on mainnet rules with the modern DAA from genesis,
// and a node that has stored the first 160 of them.
func daaFixture(t *testing.T) (*settings.Settings, blockchain_store.Store, []*model.BlockHeader) {
	t.Helper()

	tSettings := daaSettings(t, chaincfg.MainNetParams)
	chain := honestDifficultyChain(t, tSettings, 170, irregularTime)

	powLimit := chain[0].Bits
	require.NotEqual(t, powLimit, chain[170].Bits, "fixture precondition: the DAA must have moved off the pow limit")

	trunk := newDifficultyStore(t, tSettings)
	storeDifficultyHeaders(t, tSettings, trunk, chain[1:161])

	return tSettings, trunk, chain
}

func validateFixtureRun(t *testing.T, tSettings *settings.Settings, trunk blockchain.HeaderSource, anchor *model.BlockHeader, headers []*model.BlockHeader) error {
	t.Helper()

	return validateHeaderChainDifficulty(t.Context(), ulogger.TestLogger{}, tSettings, trunk, anchor.Hash(), headers)
}

// TestValidateHeaderChainDifficulty_HonestRunAcrossTheStoredWindowPasses checks a run whose
// adjustment windows lie almost entirely in the store: every header gets the bits the store-backed
// calculator gave it.
func TestValidateHeaderChainDifficulty_HonestRunAcrossTheStoredWindowPasses(t *testing.T) {
	tSettings, trunk, chain := daaFixture(t)

	require.NoError(t, validateFixtureRun(t, tSettings, trunk, chain[160], chain[161:]))
}

// TestValidateHeaderChainDifficulty_HeaderExtendingTheTipWithEasyBitsIsRejected is the attack in
// bitcoin-sv/teranode#4582: a single header on top of our tip, mined against a target the peer
// chose. Its whole adjustment window is in the store, which the check used to skip.
func TestValidateHeaderChainDifficulty_HeaderExtendingTheTipWithEasyBitsIsRejected(t *testing.T) {
	tSettings, trunk, chain := daaFixture(t)

	err := validateFixtureRun(t, tSettings, trunk, chain[160], []*model.BlockHeader{withTamperedBits(t, chain[161], "207fffff")})
	require.Error(t, err)
	require.True(t, errors.IsMaliciousResponseError(err), "expected malicious response error, got %v", err)
	require.ErrorContains(t, err, "expected "+chain[161].Bits.String(), "the expected bits must be the store-backed calculator's")
}

// TestValidateHeaderChainDifficulty_ShortForkWithEasyBitsIsRejected offers a three-header fork
// rooted three blocks below the tip whose last header declares the pow limit.
func TestValidateHeaderChainDifficulty_ShortForkWithEasyBitsIsRejected(t *testing.T) {
	tSettings, trunk, chain := daaFixture(t)

	fork := []*model.BlockHeader{chain[158], chain[159], withTamperedBits(t, chain[160], chain[0].Bits.String())}

	err := validateFixtureRun(t, tSettings, trunk, chain[157], fork)
	require.Error(t, err)
	require.True(t, errors.IsMaliciousResponseError(err), "expected malicious response error, got %v", err)
}

// TestValidateHeaderChainDifficulty_UnjudgeableRunFailsClosed checks that a run the calculator
// cannot judge is refused without blaming the peer: an anchor the store does not hold, and a run
// that does not link to its anchor.
func TestValidateHeaderChainDifficulty_UnjudgeableRunFailsClosed(t *testing.T) {
	tSettings, trunk, chain := daaFixture(t)

	err := validateFixtureRun(t, tSettings, trunk, chain[165], chain[166:])
	require.Error(t, err, "an anchor the store does not hold")
	require.False(t, errors.IsMaliciousResponseError(err))

	err = validateFixtureRun(t, tSettings, trunk, chain[160], chain[162:])
	require.Error(t, err, "a run that does not link to its anchor")
	require.False(t, errors.IsMaliciousResponseError(err))
}

// TestValidateHeaderChainDifficulty_HistoricalTestnet runs real testnet headers from genesis, across
// the pre-DAA rules and testnet's minimum-difficulty rule, against a store holding only genesis.
func TestValidateHeaderChainDifficulty_HistoricalTestnet(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	params := chaincfg.TestNetParams
	tSettings.ChainCfgParams = &params

	data, err := os.ReadFile("../blockchain/testdata/testnet_headers_0_547.bin")
	require.NoError(t, err)
	require.Len(t, data, 548*80)

	headers := make([]*model.BlockHeader, 548)
	for i := range headers {
		headers[i], err = model.NewBlockHeaderFromBytes(data[i*80 : (i+1)*80])
		require.NoError(t, err)
	}

	trunk := newDifficultyStore(t, tSettings)

	require.NoError(t, validateFixtureRun(t, tSettings, trunk, headers[0], headers[1:]))

	headers[547] = withTamperedBits(t, headers[547], "207fffff")
	err = validateFixtureRun(t, tSettings, trunk, headers[0], headers[1:])
	require.True(t, errors.IsMaliciousResponseError(err), "expected malicious response error, got %v", err)
}

// TestValidateHeaderChainDifficulty_NoAdjustmentChain checks a chain that never adjusts difficulty
// (regtest) the way full-block validation does: each header must repeat its parent's bits.
func TestValidateHeaderChainDifficulty_NoAdjustmentChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	require.True(t, tSettings.ChainCfgParams.NoDifficultyAdjustment)

	chain := honestDifficultyChain(t, tSettings, 5, func(height uint32, prev []*model.BlockHeader) uint32 {
		return prev[height-1].Timestamp + 600
	})

	trunk := newDifficultyStore(t, tSettings)
	storeDifficultyHeaders(t, tSettings, trunk, chain[1:3])

	require.NoError(t, validateFixtureRun(t, tSettings, trunk, chain[2], chain[3:]))

	err := validateFixtureRun(t, tSettings, trunk, chain[2], []*model.BlockHeader{withTamperedBits(t, chain[3], "1d00ffff")})
	require.True(t, errors.IsMaliciousResponseError(err), "expected malicious response error, got %v", err)
}
