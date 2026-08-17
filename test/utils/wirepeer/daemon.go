package wirepeer

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// NewLegacyDaemon starts a TestDaemon configured for BSV-wire tests: the legacy
// service listening on a free loopback port, RPC enabled so ports can inspect
// node state (listbanned, getblockchaininfo, ...), and a local validator so
// transaction submission does not need a separate validator service.
//
// Every port that talks wire should start here rather than assembling its own
// TestOptions, so that a change in what the harness requires is made in one
// place. Pass extra setting overrides for anything test-specific.
func NewLegacyDaemon(t *testing.T, extra ...func(*settings.Settings)) *daemon.TestDaemon {
	t.Helper()

	return newDaemon(t, false, extra...)
}

// NewLegacyDaemonWithP2P is NewLegacyDaemon with the modern P2P service running
// alongside the legacy one.
//
// Only ports that need it should use this. There are two known reasons to:
//
//   - Correctness, for the ban-administration RPCs: handleSetBan's ban leg lives
//     in the P2P service, so with the legacy service alone every setban call
//     fails (see the setban-address-format gap).
//   - Speed, for any port that polls a peer RPC: the RPC service is handed a P2P
//     client whether or not the service is running, and waits out its ~10s of
//     gRPC retries before answering. getpeerinfo measures 9.76s without P2P and
//     0.5ms with it, for the same answer (see the
//     getpeerinfo-stalls-without-p2p-service gap).
//
// Running both services makes listbanned report each ban twice, so callers must
// compare distinct addresses rather than lengths. getpeerinfo is unaffected: the
// P2P leg contributes entries only for peers actually connected over libp2p.
func NewLegacyDaemonWithP2P(t *testing.T, extra ...func(*settings.Settings)) *daemon.TestDaemon {
	t.Helper()

	return newDaemon(t, true, extra...)
}

func newDaemon(t *testing.T, enableP2P bool, extra ...func(*settings.Settings)) *daemon.TestDaemon {
	t.Helper()

	requireSettingsContext(t)

	overrides := append([]func(*settings.Settings){
		func(s *settings.Settings) {
			s.Validator.UseLocalValidator = true

			// Teranode caches getpeerinfo, getbestblockhash, getchaintips and
			// friends for 10 seconds. bitcoin-sv does not, so upstream tests read
			// an RPC immediately after acting and expect the new answer. Caching
			// turns those into 10-second waits at best and false failures at
			// worst, so ports run against uncached RPC.
			s.RPC.CacheEnabled = false
		},
	}, extra...)

	return daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableLegacy:         true,
		EnableRPC:            true,
		EnableValidator:      true,
		EnableP2P:            enableP2P,
		SettingsOverrideFunc: LegacySettings(t, overrides...),
	})
}

// requireSettingsContext fails fast if SETTINGS_CONTEXT is unset.
//
// gocore resolves the settings context once, on the first Config() call, and
// ui/dashboard's package init makes that call before any test code runs — so
// setting the variable from init or TestMain is too late, and the only thing
// that works is having it in the environment at process start. Left unset, the
// context defaults to "dev", which points at Postgres and Aerospike, and the
// daemon dies 30 seconds later with "failed to create postgres schema". This
// check turns that into an immediate, actionable failure.
func requireSettingsContext(t *testing.T) {
	t.Helper()

	if os.Getenv("SETTINGS_CONTEXT") == "" {
		t.Fatalf("wirepeer: SETTINGS_CONTEXT is unset, so settings would default to the \"dev\" " +
			"context (Postgres/Aerospike) and the daemon would fail to start. Run with " +
			"SETTINGS_CONTEXT=test, or use `make bsvporttest`. It cannot be set from Go: gocore " +
			"reads it before test code runs.")
	}
}

// ListenAddr is the daemon's legacy BSV-wire listen address, for tests that need
// to reason about the address the node sees them connecting from.
func ListenAddr(t *testing.T, td *daemon.TestDaemon) string {
	t.Helper()

	return listenAddr(t, td)
}

// ListBanned returns the node's ban list via the listbanned RPC. Legacy-service
// bans are asynchronous (server.BanPeer hands off to a channel), so callers
// should poll with WaitForBan rather than reading this once.
func ListBanned(t *testing.T, td *daemon.TestDaemon) []string {
	t.Helper()

	resp, err := td.CallRPC(td.Ctx, "listbanned", nil)
	require.NoError(t, err, "listbanned RPC")

	// The RPC returns the standard {result, error, id} envelope.
	var envelope struct {
		Result []string `json:"result"`
	}

	require.NoError(t, json.Unmarshal([]byte(resp), &envelope), "decode listbanned response: %s", resp)

	return envelope.Result
}

// TryListBanned is ListBanned without the assertions, returning nil on any
// failure.
//
// Use it inside require.Eventually and require.Never conditions. testify runs
// those conditions in goroutines that can still be in flight after the call has
// returned its verdict, so a condition that asserts can fail an already-passing
// test when a straggler catches the daemon mid-shutdown.
func TryListBanned(td *daemon.TestDaemon) []string {
	resp, err := td.CallRPC(td.Ctx, "listbanned", nil)
	if err != nil {
		return nil
	}

	var envelope struct {
		Result []string `json:"result"`
	}

	if json.Unmarshal([]byte(resp), &envelope) != nil {
		return nil
	}

	return envelope.Result
}

// ClearBanned empties the node's ban list via the clearbanned RPC, so a test can
// provoke a ban and then carry on using the same daemon. Ports need this because
// a ban is recorded against the loopback address and would otherwise lock the
// test out of the node for the rest of the run.
func ClearBanned(t *testing.T, td *daemon.TestDaemon) {
	t.Helper()

	_, err := td.CallRPC(td.Ctx, "clearbanned", nil)
	require.NoError(t, err, "clearbanned RPC")

	require.Eventually(t, func() bool {
		return len(TryListBanned(td)) == 0
	}, 10*time.Second, 100*time.Millisecond, "ban list should be empty after clearbanned")
}

// WaitForBan polls listbanned until an entry contains addr, and fails if it does
// not appear within timeout. Use it after provoking a ban; asserting on a single
// listbanned read races the legacy server's ban channel.
func WaitForBan(t *testing.T, td *daemon.TestDaemon, addr string, timeout time.Duration) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(td.Ctx, timeout)
	defer cancel()

	var last []string

	for {
		last = ListBanned(t, td)

		for _, banned := range last {
			// Entries may carry a subnet suffix, e.g. "127.0.0.1/32".
			if banned == addr || strings.HasPrefix(banned, addr+"/") {
				return last
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("wirepeer: %s did not appear in listbanned within %s; ban list: %v", addr, timeout, last)

			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
}
