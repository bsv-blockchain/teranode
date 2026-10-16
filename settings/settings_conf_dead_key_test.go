package settings

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This test guards the exact bug class it ships alongside: a settings.conf
// base key that looks correct by case/underscore analogy to a real,
// key-tagged setting but does not actually match it, so gocore's exact-case,
// exact-string lookup (see (*gocore.Configuration).getInternal) silently
// ignores it forever. That is precisely what happened to securityLevelGRPC:
// it was typed by analogy to the genuinely-camelCase SecurityLevelHTTP
// (`key:"securityLevelHTTP"`), but the real key is `security_level_grpc`, so
// the settings.conf line at its default value of 0 was harmless - until an
// operator edited it to 1 expecting TLS on inter-service gRPC and got silent
// plaintext instead.
//
// This is deliberately narrower than "every settings.conf key resolves to
// something real": most of settings.conf's ~400 base keys are legitimate
// non-setting entries (gocore ${VAR} interpolation targets, keys read
// outside the reflection-tagged Settings struct, the startXxx bootstrap
// pattern), and telling those apart from a genuinely dead key needs
// per-key investigation, not a blanket assertion - see
// TestSettingsConfHasNoDeadKeys below, which makes the same tradeoff for a
// related but different check (keys asserted to stay dead once removed,
// rather than keys asserted to resolve).
//
// Instead, this test targets only the specific, mechanically-detectable
// signature of the securityLevelGRPC bug: a settings.conf key whose
// lowercase/underscore-stripped form exactly matches a real key tag's
// normalized form, while the settings.conf key itself does not match that
// tag exactly. A key with no real-key lookalike at all (e.g. DATADIR,
// clientName, startPruner) never trips this check.

// settingsConfDeadKeyException documents a settings.conf base key that
// normalizes to the same string as a real ExportMetadata() key tag but is
// not a broken variant of it (or, where noted, is a broken variant that was
// found by this check but is being tracked separately rather than fixed
// here - see the reason for each).
type settingsConfDeadKeyException struct {
	key    string
	reason string
}

var settingsConfDeadKeyExceptions = []settingsConfDeadKeyException{
	{
		key: "P2P_PORT",
		reason: "ALL_CAPS interpolation constant, referenced via ${P2P_PORT} elsewhere in settings.conf " +
			"(p2p_port and coinbase_p2p_static_peers.dev both expand it); a distinct naming convention from " +
			"the real p2p_port setting it collides with after normalization, not a broken variant of it",
	},
	{
		key: "P2P_PORT_COINBASE",
		reason: "ALL_CAPS interpolation constant, referenced via ${P2P_PORT_COINBASE} elsewhere in " +
			"settings.conf (p2p_port_coinbase.dev/.docker/.operator all expand it); distinct from the real " +
			"p2p_port_coinbase setting it collides with after normalization, not a broken variant of it",
	},
	{
		key: "LOG_LEVEL",
		reason: "sits in the same ALL_CAPS docker-context-override block as COINBASE_RPC_USER and " +
			"COINBASE_HTTP_PORT (settings.conf, just after coinbase_rpc_url); no Go code reads this literal " +
			"key so it may be unused, but it follows that block's naming convention rather than being a typo " +
			"of the real logLevel setting - flagged for the settings owner to triage as a possible separate " +
			"cleanup, not fixed here to keep this change to the securityLevelGRPC bug",
	},
	{
		key: "COINBASE_GRPC_ADDRESS",
		reason: "same ALL_CAPS docker-context-override block as LOG_LEVEL above; docker-compose-3blasters.yml " +
			"sets it as a container env var, but no Go code reads this literal key and gocore's env lookup is " +
			"exact-case, so the override appears inert - flagged for the settings owner, not fixed here",
	},
}

// settingsConfPath returns the repo-root settings.conf path relative to this
// test file.
func settingsConfPath(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "unable to determine test file location")

	repoRoot := filepath.Dir(filepath.Dir(thisFile))

	return filepath.Join(repoRoot, "settings.conf")
}

// settingsConfBaseKeys returns the set of distinct base keys (the part
// before the first '.', which strips any ".context" suffix) assigned
// anywhere in settings.conf.
func settingsConfBaseKeys(t *testing.T) map[string]bool {
	t.Helper()

	f, err := os.Open(settingsConfPath(t))
	require.NoError(t, err, "opening settings.conf")
	defer f.Close()

	keys := make(map[string]bool)

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		eq := strings.Index(line, "=")
		if eq < 0 {
			continue
		}

		left := strings.TrimSpace(line[:eq])
		if left == "" {
			continue
		}

		base := left
		if dot := strings.Index(left, "."); dot >= 0 {
			base = left[:dot]
		}

		keys[base] = true
	}
	require.NoError(t, sc.Err(), "scanning settings.conf")

	return keys
}

// normalizeSettingsKey lowercases a key and strips underscores, so
// security_level_grpc and securityLevelGRPC (and SecurityLevelGrpc, etc.)
// all normalize to the same string.
func normalizeSettingsKey(key string) string {
	return strings.ToLower(strings.ReplaceAll(key, "_", ""))
}

// TestSettingsConfNoShadowedKeys guards against the securityLevelGRPC bug
// class: a settings.conf key that is never read because it only matches a
// real key tag after case/underscore normalization, not exactly. See the
// package-level doc comment above for why this check is intentionally
// narrower than "every settings.conf key resolves".
func TestSettingsConfNoShadowedKeys(t *testing.T) {
	exempt := make(map[string]bool, len(settingsConfDeadKeyExceptions))
	for _, e := range settingsConfDeadKeyExceptions {
		require.NotEmpty(t, e.reason, "exception for %q must document why it is exempt", e.key)
		exempt[e.key] = true
	}

	confKeys := settingsConfBaseKeys(t)

	reg := (&Settings{}).ExportMetadata()

	realExact := make(map[string]bool, len(reg.Settings))
	realByNormalized := make(map[string]string, len(reg.Settings))

	for _, s := range reg.Settings {
		realExact[s.Key] = true

		norm := normalizeSettingsKey(s.Key)
		if _, ok := realByNormalized[norm]; !ok {
			realByNormalized[norm] = s.Key
		}
	}

	for confKey := range confKeys {
		if realExact[confKey] || exempt[confKey] {
			continue
		}

		norm := normalizeSettingsKey(confKey)

		real, collides := realByNormalized[norm]
		if !collides {
			continue
		}

		t.Errorf("settings.conf key %q does not match any Settings key tag exactly, but normalizes "+
			"(lowercased, underscores stripped) to the same string as the real key %q. Because gocore's "+
			"config lookup is exact-case and exact-string, %q is never read - this is the securityLevelGRPC "+
			"bug class. Either correct the settings.conf key to %q, or - if %q is a genuinely distinct, "+
			"intentionally-named key - add a reasoned entry to settingsConfDeadKeyExceptions",
			confKey, real, confKey, real, confKey)
	}
}

// settingsConfRemovedKeys maps each base key deleted from settings.conf
// because nothing reads it to the PR that deleted it. A rebase that resolves a
// settings.conf conflict by keeping both sides restores such keys without any
// failure: #1744 put back the legacy_outpointBatcher* keys that #1699 removed.
var settingsConfRemovedKeys = map[string]string{
	"legacy_outpointBatcherConcurrency":     "#1699",
	"legacy_outpointBatcherSize":            "#1699",
	"legacy_spendBatcherConcurrency":        "#1699",
	"legacy_spendBatcherSize":               "#1699",
	"legacy_storeBatcherConcurrency":        "#1699",
	"legacy_storeBatcherSize":               "#1699",
	"blockvalidation_subtree_fetch_timeout": "#1744",
	"coinbase_Store":                        "#1744",
	"http_sign_response":                    "#1744",
	"JAEGER_PORT":                           "#1744",
	"kafka_unitTest":                        "#1744",
	"min_block_height_for_e2e":              "#1744",
	"p2p_bestblock_topic":                   "#1744",
	"p2p_handshake_topic":                   "#1744",
	"p2p_mining_on_topic":                   "#1744",
	"p2p_optimise_retries":                  "#1744",
	"p2p_shared_key":                        "#1744",
	"p2p_sync_stuck_retry_threshold":        "#1744",
	"PK4":                                   "#1744",
	"PK5":                                   "#1744",
	"PK6":                                   "#1744",
	"spent_utxo_ttl":                        "#1744",
	"startCoinbase":                         "#1744",
	"startFaucet":                           "#1744",
	"test_run_mode":                         "#1744",
	"use_cgo_signer":                        "#1744",
	"utxostore_dbTimeoutMillis":             "#1744",
	"utxostore_outpointBatcherConcurrency":  "#1744",
}

// TestSettingsConfHasNoDeadKeys fails when a key listed in
// settingsConfRemovedKeys is assigned in settings.conf again, in any context.
func TestSettingsConfHasNoDeadKeys(t *testing.T) {
	confKeys := settingsConfBaseKeys(t)

	for key, pr := range settingsConfRemovedKeys {
		if confKeys[key] {
			t.Errorf("settings.conf assigns %q, which %s removed because nothing reads it. Delete it again; "+
				"if code now reads it, remove it from settingsConfRemovedKeys", key, pr)
		}
	}
}
