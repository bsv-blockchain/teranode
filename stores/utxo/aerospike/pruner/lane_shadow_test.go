package pruner

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// laneMetric reads one series of a lane pruner counter from the default registry, 0 when absent.
func laneMetric(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() != name {
			continue
		}

	metrics:
		for _, m := range f.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}

			for k, v := range labels {
				if got[k] != v {
					continue metrics
				}
			}

			return m.GetCounter().GetValue()
		}
	}

	return 0
}

func laneVerdicts(t *testing.T, today, lanes, reason string) float64 {
	return laneMetric(t, "teranode_lane_pruner_verdicts_total", map[string]string{
		"store": lanereader.PruneStoreAerospike, "today": today, "lanes": lanes, "reason": reason})
}

func lanePageReads(t *testing.T) float64 {
	return laneMetric(t, "teranode_lane_pruner_extra_reads_total", map[string]string{"store": lanereader.PruneStoreAerospike, "kind": "page"})
}

// laneTestIndex is a main chain of heights 1 to 300, block ID = height, each block taking one filler
// position in lane 250 so no block lacks rows, and block h also taking took[h]; and for each height h
// in forks a block off the main chain, ID 10,000 + h, on block h-1, taking forks[h]. Settle depth 100,
// so blocks at or below 200 are folded into the floors.
func laneTestIndex(t testing.TB, took, forks map[uint32][]uint64) *lane.Index {
	t.Helper()

	hash := func(id uint32) chainhash.Hash {
		var h chainhash.Hash
		binary.LittleEndian.PutUint32(h[:], id)
		h[31] = 3

		return h
	}

	idx := lane.NewIndex(100)

	for h := uint32(1); h <= 300; h++ {
		byLane := map[uint8]*roaring64.Bitmap{250: roaring64.BitmapOf(uint64(h))}

		for _, p := range took[h] {
			l, pos := lane.Unpack(p)
			if byLane[l] == nil {
				byLane[l] = roaring64.New()
			}

			byLane[l].Add(pos)
		}

		var rows []lane.MapRow

		for l, bm := range byLane {
			r, err := lane.BuildMapRows(l, bm)
			require.NoError(t, err)

			rows = append(rows, r...)
		}

		require.NoError(t, idx.AddBlock(lane.IndexBlock{ID: h, Height: h, Hash: hash(h), Parent: hash(h - 1)}, rows))
	}

	for h, positions := range forks {
		bm := roaring64.New()

		var rows []lane.MapRow

		for _, p := range positions {
			l, pos := lane.Unpack(p)
			bm.Clear()
			bm.Add(pos)

			r, err := lane.BuildMapRows(l, bm)
			require.NoError(t, err)

			rows = append(rows, r...)
		}

		require.NoError(t, idx.AddBlock(lane.IndexBlock{ID: 10_000 + h, Height: h, Hash: hash(10_000 + h), Parent: hash(h - 1)}, rows))
	}

	require.NoError(t, idx.SetTip(hash(300)))

	return idx
}

// spentUtxo is a utxos-bin entry spent by spender's input 0; unspentUtxo one not spent.
func spentUtxo(seed string, spender chainhash.Hash) []byte {
	h := chainhash.HashH([]byte(seed))

	b := make([]byte, 0, 68)
	b = append(b, h[:]...)
	b = append(b, spender[:]...)

	return append(b, 0, 0, 0, 0)
}

func slotPositions(positions ...uint64) []byte {
	return lane.AppendPositions(nil, positions)
}

func installLaneIndex(t testing.TB, idx *lane.Index) {
	t.Helper()

	lanereader.InstallIndex(ulogger.TestLogger{}, idx, 100)
	t.Cleanup(func() { lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil) })
}

// Positions: parents in lane 7, spenders in lane 9.
var (
	laneSettledParent = lane.Pack(7, 500) // block 150: below the floor
	laneSettled       = lane.Pack(9, 40)  // block 190: below the floor
	laneShallow       = lane.Pack(9, 41)  // block 250: 50 deep
	laneUnmined       = lane.Pack(9, 99)  // no block
	laneUnminedParent = lane.Pack(7, 900) // no block
	laneForkOnly      = lane.Pack(9, 77)  // only fork block 10,250
)

func laneIndexForTests(t testing.TB) *lane.Index {
	return laneTestIndex(t, map[uint32][]uint64{
		150: {laneSettledParent},
		190: {laneSettled},
		250: {laneShallow},
		// Lane 7 and 9 also hold positions above the ones judged unmined, so those are unmined, not
		// older than the index.
		260: {lane.Pack(7, 1000), lane.Pack(9, 1000)},
	}, map[uint32][]uint64{250: {laneForkOnly}})
}

// TestLaneShadowJudgesEveryDecisionOnAerospike runs the pruner over real records at shadow and checks
// each record's verdict is counted beside today's decision, a paged parent once with its page's slots.
func TestLaneShadowJudgesEveryDecisionOnAerospike(t *testing.T) {
	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx)
	test.SkipIfContainerUnavailable(t, err)

	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)

	namespace, set := "test", "test"

	tSettings := createTestSettings()
	tSettings.GlobalBlockHeightRetention = 288
	// One worker and one chunk, so the paged parent's page is still there when its master is judged.
	// With several workers, another may delete the page first, and the master counts spender_unread
	// (TestLaneShadowUnreadPage).
	tSettings.Pruner.UTXOPartitionQueries = 1
	tSettings.Pruner.UTXOChunkSize = 1000

	service, err := NewService(tSettings, Options{
		Logger:        ulogger.New("test"),
		Client:        client,
		ExternalStore: memory.New(),
		Namespace:     namespace,
		Set:           set,
		IndexWaiter:   &MockIndexWaiter{Client: client, Namespace: namespace, Set: set},
	})
	require.NoError(t, err)

	service.Start(ctx)
	require.Eventually(t, service.indexReady.Load, 30*time.Second, 100*time.Millisecond, "delete_at_height index not ready")

	installLaneIndex(t, laneIndexForTests(t))

	const pruneHeight = 5

	writePolicy := aerospike.NewWritePolicy(0, 0)

	put := func(name string, num uint32, bins aerospike.BinMap) *aerospike.Key {
		txID := chainhash.HashH([]byte(name))
		key, kerr := aerospike.NewKey(namespace, set, uaerospike.CalculateKeySourceInternal(&txID, num))
		require.NoError(t, kerr)

		bins[fields.TxID.String()] = txID.CloneBytes()
		bins[fields.DeleteAtHeight.String()] = pruneHeight
		require.NoError(t, client.Put(writePolicy, key, bins))

		return key
	}

	signed := func(p uint64) int { return int(lane.ToSigned(p)) }

	spenderA := chainhash.HashH([]byte("spender a"))
	spenderB := chainhash.HashH([]byte("spender b"))

	utxos, slots, lanePos := fields.Utxos.String(), fields.SpendingLanePositions.String(), fields.LanePosition.String()
	totalExtra, unmined, conflicting := fields.TotalExtraRecs.String(), fields.UnminedSince.String(), fields.Conflicting.String()

	// Both rules delete: parent and spender below the floor.
	put("settled", 0, aerospike.BinMap{totalExtra: 0, lanePos: signed(laneSettledParent),
		utxos: []interface{}{spentUtxo("s0", spenderA)}, slots: slotPositions(laneSettled)})
	// A zero-output parent, mined deep: both delete.
	put("zero-output", 0, aerospike.BinMap{totalExtra: 0, lanePos: signed(laneSettledParent), utxos: []interface{}{nil}})
	// Today deletes, lanes keeps: the spender is 50 deep.
	put("shallow", 0, aerospike.BinMap{totalExtra: 0, lanePos: signed(laneSettledParent),
		utxos: []interface{}{spentUtxo("sh0", spenderA), spentUtxo("sh1", spenderB)}, slots: slotPositions(laneSettled, laneShallow)})
	// Today keeps it (unmined), lanes would delete: the dangerous direction, counted.
	put("unmined", 0, aerospike.BinMap{totalExtra: 0, unmined: 3, lanePos: signed(laneSettledParent),
		utxos: []interface{}{spentUtxo("u0", spenderA)}, slots: slotPositions(laneSettled)})
	// A spender with no position: cannot judge.
	put("no-position", 0, aerospike.BinMap{totalExtra: 0, lanePos: signed(laneSettledParent),
		utxos: []interface{}{spentUtxo("np0", spenderA)}})
	// A paged parent: the master's spender is settled, the page's is unmined, so lanes keeps.
	put("paged", 0, aerospike.BinMap{totalExtra: 1, lanePos: signed(laneSettledParent),
		utxos: []interface{}{spentUtxo("p0", spenderA)}, slots: slotPositions(laneSettled)})
	put("paged", 1, aerospike.BinMap{utxos: []interface{}{spentUtxo("p1", spenderB)}, slots: slotPositions(laneUnmined)})
	// A spender only a fork block took: today deletes, lanes keeps.
	put("fork-spender", 0, aerospike.BinMap{totalExtra: 0, lanePos: signed(laneSettledParent),
		utxos: []interface{}{spentUtxo("f0", spenderA)}, slots: slotPositions(laneForkOnly)})
	// A conflict loser: today deletes it by its delete-at height, lanes keeps it, never mined.
	put("conflicting", 0, aerospike.BinMap{totalExtra: 0, unmined: 3, conflicting: true, lanePos: signed(laneUnminedParent)})

	before := map[string]float64{
		"both delete": laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled),
		"shallow":     laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonSpenderShallow),
		"lanes only":  laneVerdicts(t, lanereader.PruneKeep, lanereader.PruneDelete, lanereader.ReasonSettled),
		"no position": laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonNoPosition),
		"paged":       laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonSpenderUnmined),
		"conflicting": laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonConflicting),
		"fork":        laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonSpenderOffChain),
		"page reads":  lanePageReads(t),
	}

	pruneCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err = service.Prune(pruneCtx, pruneHeight, "<test-hash>")
	require.NoError(t, err)

	require.Equal(t, before["both delete"]+2, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled), "settled and zero-output")
	require.Equal(t, before["shallow"]+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonSpenderShallow))
	require.Equal(t, before["lanes only"]+1, laneVerdicts(t, lanereader.PruneKeep, lanereader.PruneDelete, lanereader.ReasonSettled), "the unmined record today keeps")
	require.Equal(t, before["no position"]+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonNoPosition))
	require.Equal(t, before["paged"]+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonSpenderUnmined), "the paged parent, once, by its page's slot")
	require.Equal(t, before["conflicting"]+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonConflicting))
	require.Equal(t, before["fork"]+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonSpenderOffChain))
	require.Equal(t, before["page reads"]+1, lanePageReads(t))
}

// pagedMasterChunk is a chunk of one mined master with one page, which today deletes.
func pagedMasterChunk(t *testing.T, s *Service) []*aerospike.Result {
	t.Helper()

	txID := chainhash.HashH([]byte("paged master"))
	key, err := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySourceInternal(&txID, 0))
	require.NoError(t, err)

	return []*aerospike.Result{{Record: &aerospike.Record{Key: key, Bins: aerospike.BinMap{
		s.fieldTxID:                           txID.CloneBytes(),
		s.fieldExternal:                       false,
		s.fieldTotalExtraRecs:                 1,
		fields.LanePosition.String():          int(lane.ToSigned(laneSettledParent)),
		s.fieldUtxos:                          []interface{}{spentUtxo("pm0", chainhash.HashH([]byte("spender a")))},
		fields.SpendingLanePositions.String(): slotPositions(laneSettled),
	}}}}
}

// TestLaneShadowAtOffAddsNothing: at off the query reads exactly today's bins, the chunk makes no
// extra call, and nothing is counted.
func TestLaneShadowAtOffAddsNothing(t *testing.T) {
	lanereader.Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil)
	require.Nil(t, lanereader.Current())

	svc := newTestService(t)
	svc.fieldConflicting = fields.Conflicting.String()

	require.Equal(t, []string{svc.fieldTxID, svc.fieldExternal, svc.fieldTotalExtraRecs, svc.fieldUnminedSince, svc.fieldConflicting},
		svc.queryBinNames(), "today's bins exactly")

	svc.defensiveEnabled = true
	require.Equal(t, []string{svc.fieldTxID, svc.fieldExternal, svc.fieldTotalExtraRecs, svc.fieldUnminedSince, svc.fieldConflicting, svc.fieldUtxos},
		svc.queryBinNames())
	svc.defensiveEnabled = false

	calls := 0
	svc.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) error {
		calls++

		return nil
	}

	before := laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled)

	processed, _, err := svc.processRecordChunk(context.Background(), 1000, pagedMasterChunk(t, svc))
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Zero(t, calls, "no page read at off")
	require.Equal(t, before, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled))
}

// TestLaneShadowReadsPagesBeforeTheDelete: at shadow the query adds the lane bins, and a paged master's
// page is read, once, in a batch before the delete batch.
func TestLaneShadowReadsPagesBeforeTheDelete(t *testing.T) {
	installLaneIndex(t, laneIndexForTests(t))

	svc := newTestService(t)
	svc.fieldConflicting = fields.Conflicting.String()
	svc.laneRetention = 288

	require.Equal(t, []string{svc.fieldTxID, svc.fieldExternal, svc.fieldTotalExtraRecs, svc.fieldUnminedSince, svc.fieldConflicting,
		fields.LanePosition.String(), fields.SpendingLanePositions.String(), svc.fieldUtxos}, svc.queryBinNames())

	svc.defensiveEnabled = true
	require.Equal(t, []string{svc.fieldTxID, svc.fieldExternal, svc.fieldTotalExtraRecs, svc.fieldUnminedSince, svc.fieldConflicting, svc.fieldUtxos,
		fields.LanePosition.String(), fields.SpendingLanePositions.String()}, svc.queryBinNames(), "defensive mode reads utxos already")
	svc.defensiveEnabled = false

	// Deletes run, so the order of the page read and the delete batch shows.
	svc.settings.Pruner.SkipDeletions = false

	var (
		pageReads []aerospike.BatchRecordIfc
		calls     []string
	)

	svc.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		if _, isRead := records[0].(*aerospike.BatchRead); !isRead {
			calls = append(calls, "delete")

			for _, r := range records {
				r.BatchRec().ResultCode = types.OK
			}

			return nil
		}

		calls = append(calls, "read")
		pageReads = append(pageReads, records...)

		for _, r := range records {
			br := r.BatchRec()
			br.Record = &aerospike.Record{Key: br.Key, Bins: aerospike.BinMap{
				svc.fieldUtxos:                        []interface{}{spentUtxo("pg0", chainhash.HashH([]byte("spender b")))},
				fields.SpendingLanePositions.String(): slotPositions(laneShallow),
			}}
		}

		return nil
	}

	shallow := laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonSpenderShallow)
	utxosBins := laneMetric(t, "teranode_lane_pruner_extra_reads_total", map[string]string{"store": lanereader.PruneStoreAerospike, "kind": "utxos_bin"})

	processed, _, err := svc.processRecordChunk(context.Background(), 1000, pagedMasterChunk(t, svc))
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Len(t, pageReads, 1, "one page read")
	require.Equal(t, utxosBins+1, laneMetric(t, "teranode_lane_pruner_extra_reads_total", map[string]string{"store": lanereader.PruneStoreAerospike, "kind": "utxos_bin"}),
		"outside defensive mode the record's utxos bin was read for the rule")
	require.Equal(t, []string{"read", "delete"}, calls, "the page is read before the delete batch")
	require.Equal(t, shallow+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneKeep, lanereader.ReasonSpenderShallow), "judged by the page's slot")
}

// TestLaneShadowUnreadPage: a page that cannot be read leaves its master cannot_judge spender_unread.
func TestLaneShadowUnreadPage(t *testing.T) {
	installLaneIndex(t, laneIndexForTests(t))

	svc := newTestService(t)
	svc.fieldConflicting = fields.Conflicting.String()
	svc.laneRetention = 288

	svc.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		for _, r := range records {
			r.BatchRec().Err = aerospike.ErrKeyNotFound
		}

		return nil
	}

	unread := laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonSpenderUnread)

	_, _, err := svc.processRecordChunk(context.Background(), 1000, pagedMasterChunk(t, svc))
	require.NoError(t, err)
	require.Equal(t, unread+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonSpenderUnread))
}

func TestAppendSlotSpenders(t *testing.T) {
	a := chainhash.HashH([]byte("a"))
	b := chainhash.HashH([]byte("b"))

	unspent := chainhash.HashH([]byte("unspent"))

	got, whole := appendSlotSpenders(nil, []interface{}{spentUtxo("0", a), unspent[:], nil, spentUtxo("3", b)},
		slotPositions(laneSettled, 0, 0))

	require.True(t, whole, "nil, unspent and spent entries are all well formed")
	require.Equal(t, []lanereader.PruneSpender{{Hash: a, Pos: laneSettled}, {Hash: b, Pos: lane.None}}, got,
		"output 3 is past the end of the positions bin")

	got, _ = appendSlotSpenders(nil, []interface{}{spentUtxo("0", a)}, []byte{1, 2, 3})
	require.Equal(t, []lanereader.PruneSpender{{Hash: a, Pos: lane.None}}, got, "a malformed bin gives no positions")

	got, _ = appendSlotSpenders(nil, []interface{}{spentUtxo("0", a)}, append(slotPositions(laneSettled), 0, 0, 0, 0))
	require.Equal(t, []lanereader.PruneSpender{{Hash: a, Pos: lane.None}}, got,
		"a bin that is not whole positions gives none, even where its first eight bytes are one")

	got, _ = appendSlotSpenders(nil, []interface{}{spentUtxo("0", a)}, slotPositions(laneSettled, laneSettled))
	require.Equal(t, []lanereader.PruneSpender{{Hash: a, Pos: lane.None}}, got, "a bin with more positions than outputs gives none")

	got, whole = appendSlotSpenders(nil, []interface{}{spentUtxo("0", subtreepkg.FrozenBytesTxHash), spentUtxo("1", a)},
		slotPositions(0, laneSettled))
	require.True(t, whole)
	require.Equal(t, []lanereader.PruneSpender{{Hash: a, Pos: laneSettled}}, got, "a frozen output has no spender")

	for _, torn := range []interface{}{spentUtxo("0", a)[:40], append(spentUtxo("0", a), 0), "not bytes"} {
		got, whole = appendSlotSpenders(nil, []interface{}{torn, spentUtxo("1", b)}, nil)
		require.False(t, whole, "a torn entry's spender is unknown")
		require.Equal(t, []lanereader.PruneSpender{{Hash: b, Pos: lane.None}}, got)
	}

	require.Equal(t, lane.None, binLanePosition(nil))
	require.Equal(t, laneSettled, binLanePosition(int(lane.ToSigned(laneSettled))))
	require.Equal(t, laneSettled, binLanePosition(lane.ToSigned(laneSettled)))
}

// TestLaneShadowJudgesAChunkTodayKeepsWhole: a chunk whose every record today keeps as unmined is
// judged too, the direction that must stay at zero.
func TestLaneShadowJudgesAChunkTodayKeepsWhole(t *testing.T) {
	installLaneIndex(t, laneIndexForTests(t))

	svc := newTestService(t)
	svc.fieldConflicting = fields.Conflicting.String()
	svc.laneRetention = 288
	svc.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) error { return nil }

	chunk := pagedMasterChunk(t, svc)
	chunk[0].Record.Bins[svc.fieldTotalExtraRecs] = 0
	chunk[0].Record.Bins[svc.fieldUnminedSince] = 3

	lanesOnly := laneVerdicts(t, lanereader.PruneKeep, lanereader.PruneDelete, lanereader.ReasonSettled)

	processed, _, err := svc.processRecordChunk(context.Background(), 1000, chunk)
	require.NoError(t, err)
	require.Zero(t, processed, "today keeps the unmined record")
	require.Equal(t, lanesOnly+1, laneVerdicts(t, lanereader.PruneKeep, lanereader.PruneDelete, lanereader.ReasonSettled))
}

// TestLaneShadowCountsAFailedChunkUnjudged: a chunk that fails before its delete batch, here on an
// external record whose blob cannot be parsed, counts its records unjudged.
func TestLaneShadowCountsAFailedChunkUnjudged(t *testing.T) {
	installLaneIndex(t, laneIndexForTests(t))

	svc := newTestService(t)
	svc.fieldConflicting = fields.Conflicting.String()
	svc.laneRetention = 288
	svc.external = memory.New()

	chunk := pagedMasterChunk(t, svc)
	chunk[0].Record.Bins[svc.fieldTotalExtraRecs] = 0
	chunk[0].Record.Bins[svc.fieldExternal] = true

	txID := chainhash.Hash(chunk[0].Record.Bins[svc.fieldTxID].([]byte))
	require.NoError(t, svc.external.Set(context.Background(), txID.CloneBytes(), fileformat.FileTypeTx, []byte{0xff}))

	chunkErrors := laneMetric(t, "teranode_lane_pruner_unjudged_total", map[string]string{"store": lanereader.PruneStoreAerospike, "reason": "chunk_error"})

	_, _, err := svc.processRecordChunk(context.Background(), 1000, chunk)
	require.Error(t, err)
	require.Equal(t, chunkErrors+1, laneMetric(t, "teranode_lane_pruner_unjudged_total", map[string]string{"store": lanereader.PruneStoreAerospike, "reason": "chunk_error"}))
}

// laneShadowTestService is newTestService at shadow with the lane index installed.
func laneShadowTestService(t *testing.T) *Service {
	t.Helper()

	installLaneIndex(t, laneIndexForTests(t))

	svc := newTestService(t)
	svc.fieldConflicting = fields.Conflicting.String()
	svc.laneRetention = 288

	return svc
}

// TestLaneShadowSkipsARecordTheQueryReturnedWithAnError: a record that came with an error is counted
// unjudged, even when it carries bins.
func TestLaneShadowSkipsARecordTheQueryReturnedWithAnError(t *testing.T) {
	svc := laneShadowTestService(t)

	chunk := pagedMasterChunk(t, svc)
	chunk[0].Record.Bins[svc.fieldTotalExtraRecs] = 0
	chunk[0].Err = aerospike.ErrTimeout

	recordErrors := laneMetric(t, "teranode_lane_pruner_unjudged_total", map[string]string{"store": lanereader.PruneStoreAerospike, "reason": "record_error"})
	settled := laneVerdicts(t, lanereader.PruneKeep, lanereader.PruneDelete, lanereader.ReasonSettled)

	_, _, err := svc.processRecordChunk(context.Background(), 1000, chunk)
	require.NoError(t, err)
	require.Equal(t, recordErrors+1, laneMetric(t, "teranode_lane_pruner_unjudged_total", map[string]string{"store": lanereader.PruneStoreAerospike, "reason": "record_error"}))
	require.Equal(t, settled, laneVerdicts(t, lanereader.PruneKeep, lanereader.PruneDelete, lanereader.ReasonSettled), "not judged")
}

// TestLaneShadowJudgesAMasterWithoutTotalExtraRecs: a master from a writer that sets no
// totalExtraRecs bin, stored under the bare txid, is judged as a master.
func TestLaneShadowJudgesAMasterWithoutTotalExtraRecs(t *testing.T) {
	svc := laneShadowTestService(t)

	chunk := pagedMasterChunk(t, svc)
	delete(chunk[0].Record.Bins, svc.fieldTotalExtraRecs)

	settled := laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled)

	processed, _, err := svc.processRecordChunk(context.Background(), 1000, chunk)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, settled+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneDelete, lanereader.ReasonSettled))
}

// TestLaneShadowCountsADeletedPageWithoutItsMaster: a page returned alone is deleted by today's
// rule, but cannot be judged as a transaction without its master.
func TestLaneShadowCountsADeletedPageWithoutItsMaster(t *testing.T) {
	svc := laneShadowTestService(t)
	svc.readMastersFn = func(keys []*aerospike.Key) ([]masterRead, error) {
		require.Len(t, keys, 1)
		return []masterRead{{err: aerospike.ErrKeyNotFound}}, nil
	}
	chunk := pagedMasterChunk(t, svc)
	txID := chainhash.Hash(chunk[0].Record.Bins[svc.fieldTxID].([]byte))
	pageKey, keyErr := aerospike.NewKey(svc.namespace, svc.set, uaerospike.CalculateKeySourceInternal(&txID, 1))
	require.NoError(t, keyErr)
	chunk[0].Record.Key = pageKey
	delete(chunk[0].Record.Bins, svc.fieldTotalExtraRecs)

	before := laneMetric(t, "teranode_lane_pruner_unjudged_total", map[string]string{"store": lanereader.PruneStoreAerospike, "reason": "page_without_master"})
	processed, _, err := svc.processRecordChunk(context.Background(), 1000, chunk)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, before+1, laneMetric(t, "teranode_lane_pruner_unjudged_total", map[string]string{"store": lanereader.PruneStoreAerospike, "reason": "page_without_master"}))

	svc.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) error { return aerospike.ErrTimeout }
	withMaster := append(pagedMasterChunk(t, svc), chunk[0])
	shadow := svc.newLaneShadow(withMaster)
	for _, rec := range withMaster {
		shadow.markDeleted(rec)
	}
	shadow.run()
	require.Equal(t, before+1, laneMetric(t, "teranode_lane_pruner_unjudged_total", map[string]string{"store": lanereader.PruneStoreAerospike, "reason": "page_without_master"}),
		"a page returned with its master is not counted twice")
}

// TestLaneShadowCountsAFailedPageBatch: when the page-read batch fails, its master cannot be judged
// settled, and the failure is counted apart.
func TestLaneShadowCountsAFailedPageBatch(t *testing.T) {
	svc := laneShadowTestService(t)
	svc.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) error { return aerospike.ErrTimeout }

	pageErrors := laneMetric(t, "teranode_lane_pruner_extra_reads_total", map[string]string{"store": lanereader.PruneStoreAerospike, "kind": "page_error"})
	unread := laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonSpenderUnread)

	_, _, err := svc.processRecordChunk(context.Background(), 1000, pagedMasterChunk(t, svc))
	require.NoError(t, err)
	require.Equal(t, pageErrors+1, laneMetric(t, "teranode_lane_pruner_extra_reads_total", map[string]string{"store": lanereader.PruneStoreAerospike, "kind": "page_error"}))
	require.Equal(t, unread+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonSpenderUnread))
}

// TestLaneShadowPageReadTimeoutDoesNotHoldDelete: a stalled optional read must leave the real
// delete batch free to run, and the incomplete page must make its master unread.
func TestLaneShadowPageReadTimeoutDoesNotHoldDelete(t *testing.T) {
	svc := laneShadowTestService(t)
	svc.settings.Pruner.SkipDeletions = false
	svc.batchPolicy = aerospike.NewBatchPolicy()
	svc.batchPolicy.TotalTimeout = 5 * time.Minute

	readDone := make(chan struct{}, 2)
	var readCalls atomic.Int32
	deleted := false
	var readTimeout, deleteTimeout time.Duration
	svc.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		if _, isRead := records[0].(*aerospike.BatchRead); isRead {
			defer func() { readDone <- struct{}{} }()
			readCalls.Add(1)
			readTimeout = policy.TotalTimeout
			time.Sleep(3 * time.Second)
			return aerospike.ErrTimeout
		}

		deleted = true
		deleteTimeout = policy.TotalTimeout
		for _, r := range records {
			r.BatchRec().ResultCode = types.OK
		}

		return nil
	}

	unread := laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonSpenderUnread)
	start := time.Now()
	processed, _, err := svc.processRecordChunk(context.Background(), 1000, pagedMasterChunk(t, svc))
	elapsed := time.Since(start)
	processedAgain, _, secondErr := svc.processRecordChunk(context.Background(), 1000, pagedMasterChunk(t, svc))
	for i := int32(0); i < readCalls.Load(); i++ {
		<-readDone
	}

	require.NoError(t, err)
	require.NoError(t, secondErr)
	require.Equal(t, 1, processed)
	require.Equal(t, 1, processedAgain)
	require.EqualValues(t, 1, readCalls.Load(), "a timed-out read cannot accumulate another background read")
	require.True(t, deleted, "the real delete batch ran")
	require.Equal(t, time.Second, readTimeout, "the optional read uses a short policy")
	require.Equal(t, 5*time.Minute, deleteTimeout, "the real delete retains its configured policy")
	require.Less(t, elapsed, 2*time.Second, "the optional read did not hold deletion")
	require.Equal(t, unread+2, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonSpenderUnread))
}

// TestLaneShadowCountsATornMasterUnread: a master whose utxos bin holds a torn entry cannot be judged
// settled, since that output's spender is unknown.
func TestLaneShadowCountsATornMasterUnread(t *testing.T) {
	svc := laneShadowTestService(t)

	chunk := pagedMasterChunk(t, svc)
	chunk[0].Record.Bins[svc.fieldTotalExtraRecs] = 0
	chunk[0].Record.Bins[svc.fieldUtxos] = []interface{}{spentUtxo("pm0", chainhash.HashH([]byte("spender a")))[:40]}

	unread := laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonSpenderUnread)

	shadow := svc.newLaneShadow(chunk)
	shadow.markDeleted(chunk[0])
	shadow.run()

	require.Equal(t, unread+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonSpenderUnread))
}

// TestLaneShadowCountsNoUtxosBinInDefensiveMode: in defensive mode the query reads the utxos bin for
// today's child check, so it is not an extra read.
func TestLaneShadowCountsNoUtxosBinInDefensiveMode(t *testing.T) {
	svc := laneShadowTestService(t)
	svc.defensiveEnabled = true

	chunk := pagedMasterChunk(t, svc)
	chunk[0].Record.Bins[svc.fieldTotalExtraRecs] = 0

	utxosBins := laneMetric(t, "teranode_lane_pruner_extra_reads_total", map[string]string{"store": lanereader.PruneStoreAerospike, "kind": "utxos_bin"})
	settled := laneVerdicts(t, lanereader.PruneKeep, lanereader.PruneDelete, lanereader.ReasonSettled)

	svc.newLaneShadow(chunk).run()

	require.Equal(t, settled+1, laneVerdicts(t, lanereader.PruneKeep, lanereader.PruneDelete, lanereader.ReasonSettled))
	require.Equal(t, utxosBins, laneMetric(t, "teranode_lane_pruner_extra_reads_total", map[string]string{"store": lanereader.PruneStoreAerospike, "kind": "utxos_bin"}))
}

// TestLaneShadowReadsNoPageBeforeTheIndexIsReady: before the index's first snapshot every verdict is
// not_ready, so a paged master's pages are not read for it.
func TestLaneShadowReadsNoPageBeforeTheIndexIsReady(t *testing.T) {
	installLaneIndex(t, lane.NewIndex(100))

	svc := newTestService(t)
	svc.fieldConflicting = fields.Conflicting.String()
	svc.laneRetention = 288
	svc.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		_, isRead := records[0].(*aerospike.BatchRead)
		require.False(t, isRead, "no page read before the index is ready")

		return nil
	}

	pages := laneMetric(t, "teranode_lane_pruner_extra_reads_total", map[string]string{"store": lanereader.PruneStoreAerospike, "kind": "page"})
	notReady := laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonNotReady)

	chunk := pagedMasterChunk(t, svc)
	shadow := svc.newLaneShadow(chunk)
	shadow.markDeleted(chunk[0])
	shadow.run()

	require.Equal(t, notReady+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonNotReady))
	require.Equal(t, pages, laneMetric(t, "teranode_lane_pruner_extra_reads_total", map[string]string{"store": lanereader.PruneStoreAerospike, "kind": "page"}))
}

// TestLaneShadowCountsATornPageUnread: a page whose utxos bin holds a torn entry leaves its master
// cannot_judge, since that output's spender is unknown.
func TestLaneShadowCountsATornPageUnread(t *testing.T) {
	svc := laneShadowTestService(t)
	svc.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		for _, r := range records {
			br := r.BatchRec()
			br.Record = &aerospike.Record{Key: br.Key, Bins: aerospike.BinMap{
				svc.fieldUtxos: []interface{}{spentUtxo("pg0", chainhash.HashH([]byte("spender b")))[:40]},
			}}
		}

		return nil
	}

	unread := laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonSpenderUnread)

	chunk := pagedMasterChunk(t, svc)
	shadow := svc.newLaneShadow(chunk)
	shadow.markDeleted(chunk[0])
	shadow.run()

	require.Equal(t, unread+1, laneVerdicts(t, lanereader.PruneDelete, lanereader.PruneCannotJudge, lanereader.ReasonSpenderUnread))
}
