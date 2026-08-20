package legacy

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExcessiveBlockSizeUserAgentComment(t *testing.T) {
	// Wipe test args.
	os.Args = []string{"bsvd"}

	cfg, _, err := loadConfig(ulogger.TestLogger{}, 4294967296)
	if err != nil {
		t.Fatal("Failed to load configuration")
	}

	if len(cfg.UserAgentComments) != 1 {
		t.Fatal("Expected EB UserAgentComment")
	}

	uac := cfg.UserAgentComments[0]
	uacExpected := "EB4000.0"
	if uac != uacExpected {
		t.Fatalf("Expected UserAgentComments to contain %s but got %s", uacExpected, uac)
	}

	// Custom excessive block size.
	os.Args = []string{"bsvd", "--excessiveblocksize=64000000"}

	cfg, _, err = loadConfig(ulogger.TestLogger{}, 4294967296)
	if err != nil {
		t.Fatal("Failed to load configuration")
	}

	if len(cfg.UserAgentComments) != 1 {
		t.Fatal("Expected EB UserAgentComment")
	}

	// loadConfig's advertised EB now tracks the enforced policy limit passed
	// in (4294967296, capped at maxWireBlockPayload), not the CLI flag: the
	// CLI parsing path is dead code (see TestLoadConfigAdvertisesEnforcedExcessiveBlockSize).
	uac = cfg.UserAgentComments[0]
	uacExpected = "EB4000.0"
	if uac != uacExpected {
		t.Fatalf("Expected UserAgentComments to contain %s but got %s", uacExpected, uac)
	}
}

// TestAdvertisedExcessiveBlockSizeTracksEnforcedPolicy confirms the advertised
// excessive block size is derived from the limit block validation actually
// enforces (settings.Policy.ExcessiveBlockSize), capped at the largest block
// message the legacy wire path accepts, so the user agent can never claim a
// block size this node would reject.
func TestAdvertisedExcessiveBlockSizeTracksEnforcedPolicy(t *testing.T) {
	tests := []struct {
		name     string
		policy   int
		expected uint64
	}{
		{"policy below the wire block payload cap is advertised verbatim", 64000000, 64000000},
		{"policy equal to the wire block payload cap", maxWireBlockPayload, maxWireBlockPayload},
		{"the 10GiB shipped settings.conf default is capped", 10737418240, maxWireBlockPayload},
		{"the 4GiB Go struct fallback default is capped", 4294967296, maxWireBlockPayload},
		{"a disabled policy limit falls back to the wire block payload cap", 0, maxWireBlockPayload},
		{"a negative policy limit falls back to the wire block payload cap", -1, maxWireBlockPayload},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, advertisedExcessiveBlockSize(tt.policy))
		})
	}
}

// TestLoadConfigAdvertisesEnforcedExcessiveBlockSize confirms the derivation is
// wired into loadConfig, i.e. that the EB user agent comment follows the
// enforced policy limit rather than a hard-coded constant.
//
// Note: the enforced limit is passed in rather than set via os.Args, because
// loadConfig's CLI parsing path is dead code (the flags.NewIniParser and
// parser.Parse calls are commented out), so loadConfig always builds cfg from
// its hard-coded struct defaults regardless of os.Args.
func TestLoadConfigAdvertisesEnforcedExcessiveBlockSize(t *testing.T) {
	// Wipe test args.
	os.Args = []string{"bsvd"}

	// A policy limit below the wire cap is what gets advertised.
	cfg, _, err := loadConfig(ulogger.TestLogger{}, 64000000)
	require.NoError(t, err)
	require.Equal(t, uint64(64000000), cfg.ExcessiveBlockSize)
	require.Equal(t, []string{"EB64.0"}, cfg.UserAgentComments)

	// Blocks we mine must stay inside the size we accept.
	require.LessOrEqual(t, cfg.BlockMaxSize, cfg.ExcessiveBlockSize-1000)

	// The 4GiB Go struct fallback default exceeds the largest block message
	// the legacy wire path accepts, so the advertisement is capped there.
	cfg, _, err = loadConfig(ulogger.TestLogger{}, 4294967296)
	require.NoError(t, err)
	require.Equal(t, uint64(maxWireBlockPayload), cfg.ExcessiveBlockSize)
	require.Equal(t, []string{"EB4000.0"}, cfg.UserAgentComments)

	// The 10GiB shipped settings.conf default likewise exceeds the wire cap.
	cfg, _, err = loadConfig(ulogger.TestLogger{}, 10737418240)
	require.NoError(t, err)
	require.Equal(t, uint64(maxWireBlockPayload), cfg.ExcessiveBlockSize)
	require.Equal(t, []string{"EB4000.0"}, cfg.UserAgentComments)
}

// TestBlockMaxSizeNeverExceedsExcessiveBlockSize confirms the invariant stated
// in loadConfig's comment above the clamp -- "never mine a block bigger than
// the excessive blocksize we accept" -- holds even for a policy
// excessiveblocksize at or below blockMaxSizeMin (1000), where the floor
// applied to blockMaxSizeMax must not itself push BlockMaxSize above
// ExcessiveBlockSize.
func TestBlockMaxSizeNeverExceedsExcessiveBlockSize(t *testing.T) {
	os.Args = []string{"bsvd"}

	tests := []struct {
		name            string
		policy          int
		expectedEB      uint64
		expectedMaxSize uint64
	}{
		{"policy below blockMaxSizeMin", 999, 999, 999},
		{"policy equal to blockMaxSizeMin", 1000, 1000, 1000},
		{"policy at the 1000-byte margin boundary", 2000, 2000, 1000},
		{"policy just above the margin boundary", 2001, 2001, 1001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, _, err := loadConfig(ulogger.TestLogger{}, tt.policy)
			require.NoError(t, err)
			require.Equal(t, tt.expectedEB, cfg.ExcessiveBlockSize)
			require.Equal(t, tt.expectedMaxSize, cfg.BlockMaxSize)
			require.LessOrEqual(t, cfg.BlockMaxSize, cfg.ExcessiveBlockSize)
		})
	}
}

func TestCreateDefaultConfigFile(t *testing.T) {
	// Setup a temporary directory
	tmpDir, err := ioutil.TempDir("", "bsvd")
	if err != nil {
		t.Fatalf("Failed creating a temporary directory: %v", err)
	}

	testpath := filepath.Join(tmpDir, "test.conf")

	// Clean-up
	defer func() {
		os.Remove(testpath)
		os.Remove(tmpDir)
	}()
	// err = createDefaultConfigFile(testpath)
	//
	//	if err != nil {
	//		t.Fatalf("Failed to create a default config file: %v", err)
	//	}
	//
	// content, err := ioutil.ReadFile(testpath)
	//
	//	if err != nil {
	//		t.Fatalf("Failed to read generated default config file: %v", err)
	//	}
}

func Test_setConfigValuesFromSettings(t *testing.T) {
	settings := map[string]string{
		"legacy_config_ShowVersion":             "true",              // bool
		"legacy_config_DataDir":                 "/tmp/test",         // string
		"legacy_config_AddPeers":                "peer1|peer2|peer3", // []string
		"legacy_config_MaxPeers":                "12",                // int
		"legacy_config_MinSyncPeerNetworkSpeed": "12345",             // uint64
		"legacy_config_BanDuration":             "23s",               // time.Duration (int64 natively)
		"legacy_config_BanThreshold":            "37",                // uint32
		"legacy_config_MinRelayTxFee":           "0.3",               // float64
		"legacy_config_SigCacheMaxSize":         "125",               // uint
	}
	testCfg := &config{}
	setConfigValuesFromSettings(ulogger.TestLogger{}, settings, testCfg)

	assert.True(t, testCfg.ShowVersion)
	assert.Equal(t, "/tmp/test", testCfg.DataDir)
	assert.Equal(t, []string{"peer1", "peer2", "peer3"}, testCfg.AddPeers)
	assert.Equal(t, 12, testCfg.MaxPeers)
	assert.Equal(t, uint64(12345), testCfg.MinSyncPeerNetworkSpeed)
	assert.Equal(t, 23*time.Second, testCfg.BanDuration)
	assert.Equal(t, uint32(37), testCfg.BanThreshold)
	assert.Equal(t, 0.3, testCfg.MinRelayTxFee)
	assert.Equal(t, uint(125), testCfg.SigCacheMaxSize)
}
