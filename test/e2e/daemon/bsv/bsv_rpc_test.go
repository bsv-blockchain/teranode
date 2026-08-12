package bsv

import (
	"encoding/json"
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// oneGigabyte matches upstream's ONE_GIGABYTE in test_framework/cdefs.py, which is
// a round decimal gigabyte rather than 2^30. Kept as upstream defines it so the
// value being asserted is the same one.
const oneGigabyte = 1000000000

// smallBlockMaxSize is upstream's other setblockmaxsize argument. Ten bytes is
// absurd as a policy, which is the point: it proves the reported number comes from
// configuration rather than from a plausible-looking default.
const smallBlockMaxSize = 10

// TestBSVRPC ports bsv-rpc.py, which checks that the maximum mined block size is
// reported and that changing it takes effect.
//
// Upstream reads getinfo()['maxminedblocksize'], then calls setblockmaxsize twice
// and re-reads it. Teranode reports the value but has no setter: getinfo publishes
// it under a "policy" sub-object rather than at the top level
// (services/rpc/handlers.go handleGetInfo), and setblockmaxsize is not in the RPC
// dispatch table at all.
//
// The substance of the upstream test is not the RPC, though - it is whether the
// reported number tracks the configured one. So this port configures the policy
// instead of setting it over RPC, using upstream's own two values, and requires
// getinfo to report each. That reproduces what the assertions were checking while
// being honest that the route is different.
//
// Reproduced from upstream:
//   - getinfo reports a maximum mined block size at all
//   - it equals the configured policy, for a value as large as upstream's
//     ONE_GIGABYTE
//   - and for a value as small as upstream's 10, so the number is genuinely read
//     from configuration
//
// Not reproduced, all waived in registry.yaml:
//   - the field is at the top level of getinfo: Teranode nests it under "policy"
//   - the initial value equals REGTEST_DEFAULT_MAX_GENERATED_BLOCK_SIZE_AFTER:
//     Teranode's blockmaxsize defaults to 0 in settings.conf, so the two projects
//     simply have different defaults and there is nothing to compare
//   - setblockmaxsize(10) changes it at runtime: the RPC does not exist
//   - setblockmaxsize(ONE_GIGABYTE) changes it at runtime: likewise
func TestBSVRPC(t *testing.T) {
	// Each case needs its own daemon, because the policy is read from settings at
	// startup - there is no setter to call, which is the whole point of the waiver.
	for _, tc := range []struct {
		name string
		size int
	}{
		{"a gigabyte", oneGigabyte},
		{"ten bytes", smallBlockMaxSize},
	} {
		t.Run("getinfo reports the configured max mined block size: "+tc.name, func(t *testing.T) {
			// P2P is enabled for speed: handleGetInfo also asks the peer services for
			// their connection counts, and without P2P running that wait dominates the
			// call. See the getpeerinfo-stalls-without-p2p-service gap.
			td := wirepeer.NewLegacyDaemonWithP2P(t, func(s *settings.Settings) {
				s.Policy.BlockMaxSize = tc.size
			})
			defer td.Stop(t)

			require.Equal(t, tc.size, maxMinedBlockSize(t, td),
				"getinfo should report the configured blockmaxsize")
		})
	}
}

// maxMinedBlockSize reads policy.maxminedblocksize out of getinfo.
//
// The nesting is Teranode's, not upstream's: handleGetInfo groups the policy
// numbers under a "policy" key, where bitcoin-sv returns them flat. Decoding the
// nested shape explicitly is what makes that difference visible here rather than
// hidden behind a helper that pretends the layouts match.
func maxMinedBlockSize(t *testing.T, td *daemon.TestDaemon) int {
	t.Helper()

	resp, err := td.CallRPC(td.Ctx, "getinfo", nil)
	require.NoError(t, err, "getinfo RPC")

	var envelope struct {
		Result struct {
			Policy struct {
				MaxMinedBlockSize *int `json:"maxminedblocksize"`
			} `json:"policy"`
		} `json:"result"`
	}

	require.NoError(t, json.Unmarshal([]byte(resp), &envelope), "decode getinfo: %s", resp)
	require.NotNil(t, envelope.Result.Policy.MaxMinedBlockSize,
		"getinfo has no policy.maxminedblocksize; if the field moved, this port and its "+
			"waivers in registry.yaml need revisiting: %s", resp)

	return *envelope.Result.Policy.MaxMinedBlockSize
}
