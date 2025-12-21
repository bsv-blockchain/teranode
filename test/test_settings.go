package test

import (
	"net/url"

	"github.com/bsv-blockchain/teranode/settings"
)

// SystemTestSettings returns a settings override function that configures
// settings equivalent to what "dev.system.test" context provided.
// Tests can compose this with additional overrides as needed.
// Note: Service toggles (Start*) are controlled via TestOptions in daemon.NewTestDaemon,
// not through the Settings struct. Use EnableRPC, EnableP2P, etc. in TestOptions instead.
func SystemTestSettings() func(*settings.Settings) {
	return func(s *settings.Settings) {
		s.BlockChain.StoreURL = mustParseURL("sqlite:///blockchain")
		s.Coinbase.Store = mustParseURL("sqlitememory:///coinbase")
		s.Coinbase.P2PStaticPeers = []string{}
		s.Coinbase.WaitForPeers = false

		// Tracing - disabled for faster test execution
		s.TracingEnabled = false
	}
}

func mustParseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic("invalid URL in test settings: " + rawURL + ": " + err.Error())
	}
	return u
}

// SystemTestSettingsWithBlockAssemblyDisabled returns system test settings
// with block assembly disabled. Useful for tests that don't need block assembly.
//
// Note: Use TestOptions.EnableBlockAssembly = false instead of this function
// to control whether block assembly service starts.
func SystemTestSettingsWithBlockAssemblyDisabled() func(*settings.Settings) {
	return ComposeSettings(
		SystemTestSettings(),
		func(s *settings.Settings) {
			s.BlockAssembly.Disabled = true
		},
	)
}

// SystemTestSettingsWithCoinbaseDisabled returns system test settings
// with coinbase disabled. Useful for tests that don't need coinbase tracking.
//
// Note: Use TestOptions to control which services start. This function exists
// for consistency but has no effect on the Settings struct.
func SystemTestSettingsWithCoinbaseDisabled() func(*settings.Settings) {
	return ComposeSettings(
		SystemTestSettings(),
		func(s *settings.Settings) {
			// Coinbase service start is controlled via TestOptions, not Settings.
			// This function is kept for API compatibility but doesn't modify settings.
		},
	)
}

// SystemTestSettingsWithPolicyOverrides returns system test settings
// with specific policy overrides for testing edge cases.
func SystemTestSettingsWithPolicyOverrides(maxTxSize, maxScriptSize, maxScriptNumLength int64) func(*settings.Settings) {
	return ComposeSettings(
		SystemTestSettings(),
		func(s *settings.Settings) {
			if maxTxSize > 0 {
				s.Policy.MaxTxSizePolicy = int(maxTxSize)
			}
			if maxScriptSize > 0 {
				s.Policy.MaxScriptSizePolicy = int(maxScriptSize)
			}
			if maxScriptNumLength > 0 {
				s.Policy.MaxScriptNumLengthPolicy = int(maxScriptNumLength)
			}
		},
	)
}

// ComposeSettings combines multiple settings override functions into one.
// This allows tests to compose base settings with test-specific overrides.
//
// Example:
//
//	daemon.NewTestDaemon(t, daemon.TestOptions{
//	    EnableRPC: true,
//	    SettingsOverrideFunc: test.ComposeSettings(
//	        test.SystemTestSettings(),
//	        func(s *settings.Settings) {
//	            s.TracingEnabled = true
//	            s.TracingSampleRate = 1.0
//	        },
//	    ),
//	})
func ComposeSettings(overrides ...func(*settings.Settings)) func(*settings.Settings) {
	return func(s *settings.Settings) {
		for _, override := range overrides {
			if override != nil {
				override(s)
			}
		}
	}
}
