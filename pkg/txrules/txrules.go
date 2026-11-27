// Package txrules tells whether two block heights validate transactions under the same
// consensus rules, as the BDK script engine applies them. It sits below the validator and
// block assembly so both can use it.
package txrules

import (
	"math"
	"sort"
	"sync"

	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bsv-blockchain/go-chaincfg"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
)

// maxBlockHeight is BDK's mempool-height sentinel: every block height is below it.
const maxBlockHeight = math.MaxInt32

// flagEngineKey identifies a BDK engine by everything that sets its flags: the
// network and the two activation heights teranode overrides.
type flagEngineKey struct {
	network   string
	genesis   uint32
	chronicle uint32
}

// flagEngines caches one BDK engine per flagEngineKey for SameTxRules.
var flagEngines sync.Map

// BDKChainName maps a teranode network name to the BDK chain name, or "" for a network
// BDK does not know.
func BDKChainName(params *chaincfg.Params) string {
	return map[string]string{
		"mainnet":     "main",
		"stn":         "stn",
		"tstn":        "tstn",
		"teratestnet": "teratestnet",
		"testnet":     "test",
		"regtest":     "regtest",
	}[params.Name]
}

// SameTxRules reports whether a transaction valid at height a is checked under the
// same consensus rules at height b: BDK computes the same consensus script flags
// at both, and no BIP68 activation (ChainCfgParams.CSVHeight) lies between them.
// The flags come from BDK itself rather than a list of chaincfg heights: BDK
// enables P2SH at a height chaincfg does not carry, and UAHF and DAA one block
// after their fork heights. A network BDK does not know returns false.
func SameTxRules(params *chaincfg.Params, a, b uint32) bool {
	lo, hi := min(a, b), max(a, b)
	if csv := params.CSVHeight; csv > lo && csv <= hi {
		return false
	}

	se := flagEngine(params)
	if se == nil {
		return false
	}

	// BDK reads MaxInt32 as its mempool-height sentinel and aborts the process on it, so
	// no height at or above it reaches the engine.
	if hi >= maxBlockHeight {
		return false
	}

	loH, err := safeconversion.Uint32ToInt32(lo)
	if err != nil {
		return false
	}

	hiH, err := safeconversion.Uint32ToInt32(hi)
	if err != nil {
		return false
	}

	// Two UTXO heights, pre- and post-Genesis: the per-input flags depend on them.
	for _, utxoHeight := range []int32{0, loH} {
		if se.CalculateFlags(utxoHeight, loH, true) != se.CalculateFlags(utxoHeight, hiH, true) {
			return false
		}
	}

	return true
}

// EraBounds returns the lowest and highest heights that share the transaction rules of
// height h (SameTxRules), so a caller can test many heights against h with two integer
// comparisons. Rules only change at activation heights and never revert, so the set is one
// contiguous range, found by binary search. ok is false for a network BDK does not know.
func EraBounds(params *chaincfg.Params, h uint32) (lo, hi uint32, ok bool) {
	if !SameTxRules(params, h, h) {
		return 0, 0, false
	}

	// Lowest x in [0, h] with SameTxRules(x, h).
	lo = uint32(sort.Search(int(h)+1, func(x int) bool { return SameTxRules(params, uint32(x), h) })) //nolint:gosec // x <= h <= MaxInt32

	// Highest x in [h, maxBlockHeight) with SameTxRules(h, x): one below the first that
	// differs, or maxBlockHeight-1 past the last activation.
	span := maxBlockHeight - 1 - int(h)
	first := sort.Search(span+1, func(i int) bool { return !SameTxRules(params, h, h+uint32(i)) }) //nolint:gosec // i <= span
	hi = h + uint32(first) - 1                                                                     //nolint:gosec // first >= 1, since SameTxRules(h, h)

	return lo, hi, true
}

// UnminedValidatedHeight returns the height an unmined transaction was validated at, read
// from its unmined_since, or 0 when that is unknown. A transaction is created, and validated,
// at its unmined_since, unless a reorg or an invalidation moved it back: then unmined_since
// is backdated to the tip less the unmined retention, or 1 (utxo.BackdatedUnminedSince), and
// says nothing about the rules it was validated under. Such a value, 1 or at least the
// retention below the tip, is unknown; a transaction created that long ago is due for
// pruning anyway.
func UnminedValidatedHeight(unminedSince, tip, retention uint32) uint32 {
	if unminedSince > 1 && (tip <= retention || unminedSince > tip-retention) {
		return unminedSince
	}

	return 0
}

// Era is the range of heights that share the transaction rules of one block height
// (EraBounds), for testing many heights against that block with integer comparisons.
type Era struct {
	lo, hi      uint32
	blockHeight uint32
	known       bool
}

// EraOf returns the era of blockHeight. On a network BDK does not know, no height shares it.
func EraOf(params *chaincfg.Params, blockHeight uint32) Era {
	lo, hi, ok := EraBounds(params, blockHeight)

	return Era{lo: lo, hi: hi, blockHeight: blockHeight, known: ok}
}

// Holds reports what the verdict a stored transaction got at one of provenHeights proves for
// a block at the era's block height:
//   - rules: some proving height checks the transaction under the same rules, so its
//     scripts verify the same way;
//   - height: such a height is not above the block either. Coinbase maturity and
//     height-based finality only grow with the height, so a verdict at h holds for them
//     above it, not below.
//
// Time-based finality is not covered: it depends on the median time of the block's own
// branch, which no height bounds (LockTimeIsTime). A 0 in provenHeights is an unknown
// height and proves nothing.
func (e Era) Holds(provenHeights []uint32) (rules, height bool) {
	if !e.known {
		return false, false
	}

	for _, h := range provenHeights {
		if h == 0 || h < e.lo || h > e.hi {
			continue
		}

		rules = true

		if h <= e.blockHeight {
			return true, true
		}
	}

	return rules, false
}

// lockTimeThreshold is where a lock time stops being a block height and becomes a time.
const lockTimeThreshold = 500_000_000

// LockTimeIsTime reports whether lockTime is a time rather than a block height. A
// transaction locked to a time is final against the median time of the block's branch,
// which a verdict reached on another branch does not bound.
func LockTimeIsTime(lockTime uint32) bool {
	return lockTime >= lockTimeThreshold
}

// flagEngine returns the cached BDK engine for params, creating it on first use.
func flagEngine(params *chaincfg.Params) *bdkscript.TxValidator {
	key := flagEngineKey{
		network:   BDKChainName(params),
		genesis:   params.GenesisActivationHeight,
		chronicle: params.ChronicleActivationHeight,
	}

	if se, ok := flagEngines.Load(key); ok {
		return se.(*bdkscript.TxValidator)
	}

	if key.network == "" {
		return nil
	}

	se := bdkscript.NewTxValidator(key.network)
	if se == nil {
		return nil
	}

	genesis, err := safeconversion.Uint32ToInt32(key.genesis)
	if err != nil || se.SetGenesisActivationHeight(genesis) != nil {
		return nil
	}

	chronicle, err := safeconversion.Uint32ToInt32(key.chronicle)
	if err != nil || se.SetChronicleActivationHeight(chronicle) != nil {
		return nil
	}

	actual, _ := flagEngines.LoadOrStore(key, se)

	return actual.(*bdkscript.TxValidator)
}
