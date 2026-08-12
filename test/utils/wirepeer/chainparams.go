package wirepeer

import (
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
)

// WithChainParams returns a settings override that mutates the daemon's chain
// parameters.
//
// This is safe, and the reason is worth stating because the code looks like it
// should not be. chaincfg.GetChainParams returns a pointer to a package-level
// struct - &RegressionNetParams under SETTINGS_CONTEXT=test - which every daemon
// in the process would otherwise share, so mutating it would rewrite consensus
// parameters for every other test in the binary. daemon.NewTestDaemon already
// prevents that: it copies RegressionNetParams and repoints ChainCfgParams at the
// copy (see "Create a copy of RegressionNetParams to avoid race conditions" in
// daemon/test_daemon.go) before it calls SettingsOverrideFunc, so an override
// lands on a copy the daemon owns.
//
// That ordering is the entire basis of this helper's safety and is invisible from
// any call site, so TestWirePeerChainParamsAreIsolated locks it in rather than
// leaving it to this comment.
//
// Note the same copy also sets CoinbaseMaturity to 1, so ports do not inherit
// regtest's 100-block maturity.
func WithChainParams(mutate func(*chaincfg.Params)) func(*settings.Settings) {
	return func(s *settings.Settings) {
		mutate(s.ChainCfgParams)
	}
}

// WithGenesisActivationHeight moves the Genesis fork to height.
//
// Upstream does this with -genesisactivationheight, and 58 of the 279 scripts use
// it: they put the fork a few blocks ahead so one short chain covers both sides of
// it. Under SETTINGS_CONTEXT=test the network is regtest, where Genesis activates
// at 10000, so a port that wants the far side of the fork has to move it rather
// than mine to it.
//
// Teranode honours the height: services/validator/ScriptVerifierGoBDK.go passes it
// to the script engine via SetGenesisActivationHeight, and the UTXO store consults
// it when deciding whether an output is spendable.
func WithGenesisActivationHeight(height uint32) func(*settings.Settings) {
	return WithChainParams(func(p *chaincfg.Params) {
		p.GenesisActivationHeight = height
	})
}

// WithChronicleActivationHeight moves the Chronicle fork to height, the companion
// to WithGenesisActivationHeight and safe for the same reason.
//
// Upstream does this with -chronicleactivationheight. Under SETTINGS_CONTEXT=test
// the network is regtest, where go-chaincfg puts Chronicle at 15000 and marks it
// "temporary and subject to change", so a port that wants the far side of it has to
// move it.
//
// Teranode honours the height the same way it honours Genesis:
// services/validator/ScriptVerifierGoBDK.go passes it to the script engine via
// SetChronicleActivationHeight.
//
// Chronicle sits above Genesis, so a port moving both must keep that order or it is
// describing a chain that cannot exist.
func WithChronicleActivationHeight(height uint32) func(*settings.Settings) {
	return WithChainParams(func(p *chaincfg.Params) {
		p.ChronicleActivationHeight = height
	})
}
