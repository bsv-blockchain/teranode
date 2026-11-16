package txrules

import (
	"math"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestSameTxRules pins the heights where BDK's consensus script flags change, as
// probed from BDK itself, not the chaincfg activation heights: P2SH is not in
// chaincfg, and UAHF and DAA take effect one block after their fork height.
func TestSameTxRules(t *testing.T) {
	tests := []struct {
		name   string
		params *chaincfg.Params
		a, b   uint32
		want   bool
	}{
		{"same height", &chaincfg.RegressionNetParams, 150, 150, true},
		{"competitor at the tip height", &chaincfg.RegressionNetParams, 150, 151, true},
		{"block ahead of the tip, same era", &chaincfg.RegressionNetParams, 160, 151, true},
		{"regtest P2SH", &chaincfg.RegressionNetParams, 1, 2, false},
		{"just below Genesis", &chaincfg.RegressionNetParams, 99, 100, false},
		{"below Genesis, tip far above", &chaincfg.RegressionNetParams, 99, 151, false},
		{"at Genesis", &chaincfg.RegressionNetParams, 100, 101, true},
		{"just below Chronicle", &chaincfg.RegressionNetParams, 199, 200, false},
		{"order does not matter", &chaincfg.RegressionNetParams, 200, 199, false},
		{"across CSV", &chaincfg.RegressionNetParams, 575, 576, false},
		{"across BIP66", &chaincfg.RegressionNetParams, 1250, 1251, false},
		{"across BIP65", &chaincfg.RegressionNetParams, 1350, 1351, false},
		{"mainnet P2SH", &chaincfg.MainNetParams, 173805, 173806, false},
		{"mainnet UAHF height itself is pre-UAHF", &chaincfg.MainNetParams, 478557, 478558, true},
		{"mainnet first UAHF block", &chaincfg.MainNetParams, 478558, 478559, false},
		{"mainnet DAA height itself is pre-DAA", &chaincfg.MainNetParams, 504030, 504031, true},
		{"mainnet first DAA block", &chaincfg.MainNetParams, 504031, 504032, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, SameTxRules(tt.params, tt.a, tt.b))
		})
	}
}

// TestSameTxRules_BIP68FollowsCSVHeight pins the BIP68 term: teranode applies
// BIP68 from ChainCfgParams.CSVHeight, independently of BDK's CSV script flag.
func TestSameTxRules_BIP68FollowsCSVHeight(t *testing.T) {
	params := chaincfg.RegressionNetParams
	params.CSVHeight = 700

	require.False(t, SameTxRules(&params, 699, 700))
	require.True(t, SameTxRules(&params, 700, 701))
}

// TestEraBounds pins the range of heights that share the rules of a given height, at the
// boundaries TestSameTxRules pins.
func TestEraBounds(t *testing.T) {
	tests := []struct {
		name   string
		params *chaincfg.Params
		h      uint32
		lo, hi uint32
	}{
		{"regtest between Genesis and Chronicle", &chaincfg.RegressionNetParams, 150, 100, 199},
		{"regtest at Genesis", &chaincfg.RegressionNetParams, 100, 100, 199},
		{"regtest between CSV and BIP66", &chaincfg.RegressionNetParams, 1000, 576, 1250},
		{"regtest past every activation", &chaincfg.RegressionNetParams, 5000, 1351, math.MaxInt32 - 1},
		{"mainnet first UAHF block", &chaincfg.MainNetParams, 478559, 478559, 504031},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lo, hi, ok := EraBounds(tt.params, tt.h)
			require.True(t, ok)
			require.Equal(t, tt.lo, lo)
			require.Equal(t, tt.hi, hi)
		})
	}

	_, _, ok := EraBounds(&chaincfg.Params{Name: "unknown"}, 150)
	require.False(t, ok, "a network BDK does not know has no bounds")
}

// TestSameTxRules_MempoolSentinelHeight pins that a height at BDK's mempool sentinel is
// refused before it reaches the engine, which aborts the process on it.
func TestSameTxRules_MempoolSentinelHeight(t *testing.T) {
	require.False(t, SameTxRules(&chaincfg.RegressionNetParams, 150, math.MaxInt32))
	require.False(t, SameTxRules(&chaincfg.RegressionNetParams, math.MaxUint32, 150))
}

// TestUnminedValidatedHeight pins when unmined_since still names the height a tx was
// validated at: a reorg or an invalidation backdates it to the tip less the unmined
// retention, or 1 (utxo.BackdatedUnminedSince), and then it names nothing.
func TestUnminedValidatedHeight(t *testing.T) {
	tests := []struct {
		name                         string
		unminedSince, tip, retention uint32
		want                         uint32
	}{
		{"mined on the longest chain", 0, 1000, 144, 0},
		{"backdated to 1", 1, 100, 144, 0},
		{"tip within the retention", 99, 100, 144, 99},
		{"backdated to the tip less the retention", 856, 1000, 144, 0},
		{"older than the retention", 800, 1000, 144, 0},
		{"within the retention", 857, 1000, 144, 857},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, UnminedValidatedHeight(tt.unminedSince, tt.tip, tt.retention))
		})
	}
}

// TestVerdictHolds pins when a verdict a tx got at one of provenHeights still holds for a
// block at another height: the same transaction rules (scripts) and no lower height
// (coinbase maturity and height finality only grow with the height).
// Regtest: Genesis at 100, Chronicle at 200.
func TestVerdictHolds(t *testing.T) {
	params := &chaincfg.RegressionNetParams

	tests := []struct {
		name          string
		provenHeights []uint32
		blockHeight   uint32
		want          bool
	}{
		{"same height", []uint32{150}, 150, true},
		{"lower height, same rules", []uint32{120}, 199, true},
		{"validated below Genesis, block above", []uint32{99}, 101, false},
		{"validated below Chronicle, block above", []uint32{199}, 200, false},
		{"validated above the block, same rules", []uint32{160}, 150, false},
		{"validated above Chronicle, block below", []uint32{200}, 199, false},
		{"one proving height is enough", []uint32{99, 150}, 160, true},
		{"no proving height", nil, 150, false},
		{"an unknown height proves nothing", []uint32{0}, 50, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, VerdictHolds(params, tt.provenHeights, tt.blockHeight))
		})
	}
}

// TestLockTimeIsTime pins the height/time split of a lock time.
func TestLockTimeIsTime(t *testing.T) {
	require.False(t, LockTimeIsTime(0))
	require.False(t, LockTimeIsTime(499_999_999), "a block height")
	require.True(t, LockTimeIsTime(500_000_000), "a time")
}
