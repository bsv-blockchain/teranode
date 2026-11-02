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
