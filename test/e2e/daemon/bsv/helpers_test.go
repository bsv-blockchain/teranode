package bsv

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// setLegacyConfigForTest overrides a legacy_config_* settings key for the
// duration of t, and restores it afterwards.
//
// Some legacy settings are not reachable through SettingsOverrideFunc at all:
// services/legacy/peer_server.go newServer reads them out of the gocore config
// map (setConfigValuesFromSettings), not out of settings.Settings. That map is
// process-global and read once at server start, so an override has to be in place
// before the daemon starts and taken back out afterwards - otherwise it leaks into
// every later test in the package, where it would surface as an unrelated failure.
//
// Both hazards are guarded rather than trusted, because both fail silently: an
// override that does not land makes the test pass for the wrong reason, and one
// that is not taken back out fails a later, unrelated test. Cleanups run
// last-in-first-out, so the check registered first here runs after the restore
// registered second - no dependence on test ordering.
//
// Values are read back through GetAll, which is the map newServer actually reads,
// and which prefers the environment over the config map - so an override of a key
// that is also an environment variable would be inert. That is checked too.
func setLegacyConfigForTest(t *testing.T, key, value string) {
	t.Helper()

	require.Empty(t, os.Getenv(key),
		"%s is set in the environment, which gocore prefers over the config map, so this override "+
			"would never reach the daemon", key)

	before := gocore.Config().GetAll()[key]

	t.Cleanup(func() {
		require.Equal(t, before, gocore.Config().GetAll()[key],
			"%s was not restored; it would leak into every later test in this package", key)
	})

	t.Cleanup(func() { gocore.Config().Set(key, before) })

	gocore.Config().Set(key, value)

	require.Equal(t, value, gocore.Config().GetAll()[key],
		"%s did not take the override, so the daemon would not see it", key)
}

// chainTip mirrors one element of the getchaintips RPC result. The field names
// match bitcoin-sv's, which is what makes upstream assertions portable
// one-for-one; see services/rpc/handlers.go handleGetchaintips.
type chainTip struct {
	Height    int64  `json:"height"`
	Hash      string `json:"hash"`
	BranchLen int    `json:"branchlen"`
	Status    string `json:"status"`
}

// peerInfo mirrors the fields of one getpeerinfo entry that upstream tests
// assert on. Teranode returns many more; the port only decodes what it uses.
type peerInfo struct {
	ID        int32  `json:"id"`
	Addr      string `json:"addr"`
	SubVer    string `json:"subver"`
	BytesSent uint64 `json:"bytessent"`
	BytesRecv uint64 `json:"bytesrecv"`
	Inbound   bool   `json:"inbound"`
}

// byID indexes a getpeerinfo result so a test can follow one specific peer's
// counters across calls. getpeerinfo's ordering is not contractual, so tests
// must not index the slice positionally.
func byID(infos []peerInfo) map[int32]peerInfo {
	out := make(map[int32]peerInfo, len(infos))
	for _, info := range infos {
		out[info.ID] = info
	}

	return out
}

// peers calls getpeerinfo and decodes the result.
func peers(t *testing.T, td *daemon.TestDaemon) []peerInfo {
	t.Helper()

	resp, err := td.CallRPC(td.Ctx, "getpeerinfo", nil)
	require.NoError(t, err, "getpeerinfo RPC")

	var envelope struct {
		Result []peerInfo `json:"result"`
	}

	require.NoError(t, json.Unmarshal([]byte(resp), &envelope), "decode getpeerinfo: %s", resp)

	return envelope.Result
}

// tryChainTips is chainTips without the assertions, and tryPeers is peers
// without them. See tryBestBlockHash for why polling conditions must not
// assert.
func tryChainTips(td *daemon.TestDaemon) []chainTip {
	resp, err := td.CallRPC(td.Ctx, "getchaintips", nil)
	if err != nil {
		return nil
	}

	var envelope struct {
		Result []chainTip `json:"result"`
	}

	if json.Unmarshal([]byte(resp), &envelope) != nil {
		return nil
	}

	return envelope.Result
}

func tryPeers(td *daemon.TestDaemon) []peerInfo {
	resp, err := td.CallRPC(td.Ctx, "getpeerinfo", nil)
	if err != nil {
		return nil
	}

	var envelope struct {
		Result []peerInfo `json:"result"`
	}

	if json.Unmarshal([]byte(resp), &envelope) != nil {
		return nil
	}

	return envelope.Result
}

// chainTips calls getchaintips and decodes the result.
func chainTips(t *testing.T, td *daemon.TestDaemon) []chainTip {
	t.Helper()

	resp, err := td.CallRPC(td.Ctx, "getchaintips", nil)
	require.NoError(t, err, "getchaintips RPC")

	var envelope struct {
		Result []chainTip `json:"result"`
	}

	require.NoError(t, json.Unmarshal([]byte(resp), &envelope), "decode getchaintips: %s", resp)

	return envelope.Result
}

// activeTip returns the single tip with status "active", failing if there is not
// exactly one. Every assertion about the chain's head depends on that being
// unambiguous, so it is worth checking rather than assuming.
func activeTip(t *testing.T, tips []chainTip) chainTip {
	t.Helper()

	var active []chainTip

	for _, tip := range tips {
		if tip.Status == "active" {
			active = append(active, tip)
		}
	}

	require.Len(t, active, 1, "expected exactly one active tip, got %+v", tips)

	return active[0]
}

// bestBlockHash calls getbestblockhash and decodes the result.
func bestBlockHash(t *testing.T, td *daemon.TestDaemon) string {
	t.Helper()

	resp, err := td.CallRPC(td.Ctx, "getbestblockhash", nil)
	require.NoError(t, err, "getbestblockhash RPC")

	var envelope struct {
		Result string `json:"result"`
	}

	require.NoError(t, json.Unmarshal([]byte(resp), &envelope), "decode getbestblockhash: %s", resp)

	return envelope.Result
}

// tryBestBlockHash is bestBlockHash without the assertions, returning "" on any
// failure.
//
// Use this inside require.Eventually and require.Never conditions. testify runs
// those conditions in goroutines that can still be in flight after the call has
// returned its verdict, so a condition that asserts can fail an already-passing
// test when a straggler catches the daemon mid-shutdown. Returning "" instead
// lets the condition simply read as "not the value we want".
func tryBestBlockHash(td *daemon.TestDaemon) string {
	resp, err := td.CallRPC(td.Ctx, "getbestblockhash", nil)
	if err != nil {
		return ""
	}

	var envelope struct {
		Result string `json:"result"`
	}

	if json.Unmarshal([]byte(resp), &envelope) != nil {
		return ""
	}

	return envelope.Result
}

// tryBestHeight is bestHeight without the assertion, returning 0 on failure.
// See tryBestBlockHash for why polling conditions must not assert.
func tryBestHeight(td *daemon.TestDaemon) uint32 {
	_, meta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	if err != nil {
		return 0
	}

	return meta.Height
}

// bestHeight reads the chain tip height from the blockchain client rather than
// over RPC, because getblockcount is handleUnimplemented in Teranode.
func bestHeight(t *testing.T, td *daemon.TestDaemon) uint32 {
	t.Helper()

	_, meta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err, "GetBestBlockHeader")

	return meta.Height
}
