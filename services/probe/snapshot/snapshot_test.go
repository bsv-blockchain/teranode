package snapshot

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotJSONContract(t *testing.T) {
	normal := 0.5
	drain := 12.5
	lag := int64(3)
	at := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)

	s := Snapshot{
		Time:         at,
		Node:         "n1",
		FSM:          "RUNNING",
		Tip:          10,
		PeerTip:      11,
		DrainSeconds: &drain,
		DrainGrowing: true,
		Flows:        []Flow{{ID: "f", From: "a", To: "b", Label: "l", Lag: &lag}},
		Alarms: []Alarm{{
			ID: "a1", Stage: "cache", Title: "t", Body: "b", TextID: "x", Severity: "warn",
			RaisedAt: at, StartOfChain: true, Snoozed: true, Drilldowns: []Link{{Label: "l", Path: "/p"}},
		}},
		Calibration: Calibration{Ready: true, Until: at, Fraction: 1},
		Sources:     []SourceHealth{{Name: "metrics", OK: true}, {Name: "aerospike", OK: false, AgeSec: 61.5}},
		Kafka:       []TopicLag{{Topic: "subtrees", Lag: &lag}, {Topic: "rejectedtx"}},
	}
	s.Cache.HitRate = Reading{Value: 0.9, Status: StatusOK, Normal: &normal, Note: "Warming", AgeSec: 4}
	s.Chain.BlocksWaiting = []uint32{1, 2}
	s.Stores.Aerospike.Configured = true

	out, err := json.Marshal(s)
	require.NoError(t, err)

	r := `{"v":0,"st":""}`
	st := func(names ...string) string {
		parts := make([]string, len(names))
		for i, n := range names {
			parts[i] = `"` + n + `":` + r
		}

		return strings.Join(parts, ",")
	}
	store := `{"configured":%C,` + st("diskFill", "sizeGB", "readP99Sec", "readsPerSec", "writesPerSec", "commitsPerSec") + `}`

	golden := `{"time":"2026-10-28T10:00:00Z","node":"n1","fsm":"RUNNING","tip":10,"peerTip":11,` +
		`"ingress":{` + st("propagationTxPerSec", "subtreeIntakePerSec", "peers", "rejectedPerSec", "subtreeQueue", "subtreeQueueOldestSec") + `},` +
		`"validator":{` + st("validatedPerSec", "latencyP99Sec", "queueFill", "spendBatchFill", "errorsPerSec") + `},` +
		`"cache":{"hitRate":{"v":0.9,"st":"ok","normal":0.5,"note":"Warming","age":4},` +
		st("missesPerSec", "evictionsPerSec", "insertionsPerSec", "fill", "currentGenFrac", "previousGenFrac", "entriesMillions", "sizeGB", "capacityGB") + `},` +
		`"assembly":{` + st("paused", "tipLagBlocks", "queueHeadAgeSec", "transactions", "ingestQueueFill", "subtreeFill") + `},` +
		`"chain":{` + st("forkDepth", "persisterBehind", "subscribersDropped", "peerHeight", "blocksWaitingCount") + `,"blocksWaiting":[1,2]},` +
		`"stores":{"aerospike":` + strings.Replace(store, "%C", "true", 1) + `,"postgres":` + strings.Replace(store, "%C", "false", 1) + `},` +
		`"lanes":{"tx":{` + st("alreadySeenPerSec") + `},` +
		`"subtrees":{` + st("alreadyKnownPerSec", "completedPerSec", "missingTxsPerSec", "txsFetchedAvg") + `},` +
		`"blocks":{` + st("announcedTotal", "validatedTotal", "missingSubtrees", "missingSubtreesPerSec") + `},` +
		`"mining":{` + st("candidatesPerSec", "candidateAgeSec", "solutionsTotal") + `}},` +
		`"blobs":{"subtree":{` + st("readsPerSec", "writesPerSec", "errorsPerSec", "readP99Sec", "writeP99Sec") + `},` +
		`"block":{` + st("readsPerSec", "writesPerSec", "errorsPerSec", "readP99Sec", "writeP99Sec") + `}},` +
		`"kafka":[{"topic":"subtrees","lag":3},{"topic":"rejectedtx","lag":null}],` +
		`"flows":[{"id":"f","from":"a","to":"b","label":"l","rate":{"v":0,"st":""},"lag":3}],` +
		`"alarms":[{"id":"a1","stage":"cache","title":"t","body":"b","textId":"x","severity":"warn","raisedAt":"2026-10-28T10:00:00Z","startOfChain":true,"snoozed":true,"drilldowns":[{"label":"l","path":"/p"}]}],` +
		`"calibration":{"ready":true,"until":"2026-10-28T10:00:00Z","fraction":1},` +
		`"drainSeconds":12.5,"drainGrowing":true,` +
		`"sources":[{"name":"metrics","ok":true},{"name":"aerospike","ok":false,"age":61.5}]}`

	require.Equal(t, golden, string(out))
}

func TestReadingOmitsEmpty(t *testing.T) {
	out, err := json.Marshal(Reading{Value: 1, Status: StatusIdle})
	require.NoError(t, err)
	require.JSONEq(t, `{"v":1,"st":"idle"}`, string(out))
}

func TestLookupResolvesJSONPaths(t *testing.T) {
	s := NewSnapshot("n")

	r, ok := s.Lookup("cache.hitRate")
	require.True(t, ok)

	r.Value = 0.75
	require.Equal(t, 0.75, s.Cache.HitRate.Value)

	r, ok = s.Lookup("validator.queueFill")
	require.True(t, ok)
	require.Same(t, &s.Validator.QueueFill, r)

	r, ok = s.Lookup("stores.aerospike.readP99Sec")
	require.True(t, ok)
	require.Same(t, &s.Stores.Aerospike.ReadP99Sec, r)

	for _, key := range []string{"cache.nope", "nope.hitRate", "cache", "chain.blocksWaiting", "stores.aerospike.configured", "node", ""} {
		r, ok = s.Lookup(key)
		require.False(t, ok, key)
		require.Nil(t, r, key)
	}
}

func TestNewSnapshotHasAllFlowsInOrder(t *testing.T) {
	s := NewSnapshot("node-1")
	require.Equal(t, "node-1", s.Node)

	want := []string{
		FlowTxInput, FlowSubtrees, FlowTxMeta, FlowLookups, FlowValidated,
		FlowBlocks, FlowBlocksFinal, FlowUTXOOps, FlowBlockWrites,
	}
	require.Len(t, s.Flows, len(want))

	for i, id := range want {
		require.Equal(t, id, s.Flows[i].ID)
		require.NotEmpty(t, s.Flows[i].From)
		require.NotEmpty(t, s.Flows[i].To)
		require.Equal(t, id, s.Flows[i].Label)
		require.Equal(t, StatusNoData, s.Flows[i].Rate.Status)
		require.Same(t, &s.Flows[i], s.Flow(id))
	}

	require.Nil(t, s.Flow("missing"))
	require.NotNil(t, s.Sources)
	require.Empty(t, s.Sources)
}

func TestLookupResolvesFlowRates(t *testing.T) {
	s := NewSnapshot("n")

	r, ok := s.Lookup("flows." + FlowTxMeta + ".rate")
	require.True(t, ok)
	require.Same(t, &s.Flow(FlowTxMeta).Rate, r)

	for _, key := range []string{"flows", "flows." + FlowTxMeta, "flows.missing.rate", "flows." + FlowTxMeta + ".lag", "flows." + FlowTxMeta + ".rate.v"} {
		r, ok = s.Lookup(key)
		require.False(t, ok, key)
		require.Nil(t, r, key)
	}
}

func TestEachReadingVisitsEveryReading(t *testing.T) {
	s := NewSnapshot("n")

	stageReadings := 6 + 5 + 10 + 6 + 5 + 12 + 10
	storeReadings := 2 * 6
	flowReadings := len(s.Flows)

	seen := make(map[string]*Reading)

	s.EachReading(func(key string, r *Reading) {
		_, dup := seen[key]
		require.False(t, dup, key)

		seen[key] = r
	})

	require.Len(t, seen, stageReadings+storeReadings+flowReadings)

	require.Same(t, &s.Ingress.PropagationTxPerSec, seen["ingress.propagationTxPerSec"])
	require.Same(t, &s.Cache.CapacityGB, seen["cache.capacityGB"])
	require.Same(t, &s.Chain.SubscribersDropped, seen["chain.subscribersDropped"])
	require.Same(t, &s.Stores.Postgres.CommitsPerSec, seen["stores.postgres.commitsPerSec"])
	require.Same(t, &s.Flow(FlowBlockWrites).Rate, seen["flows."+FlowBlockWrites+".rate"])

	for key, r := range seen {
		looked, ok := s.Lookup(key)
		require.True(t, ok, key)
		require.Same(t, r, looked, key)
	}
}
