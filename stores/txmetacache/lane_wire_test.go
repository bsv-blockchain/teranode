package txmetacache

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// laneTestData is a record with two distinct parents, one of them spent twice, and a fee of 423
// satoshis, whose MetaBytes begin with the same two bytes as the lane encoding.
func laneTestData(t *testing.T) *meta.Data {
	t.Helper()

	tx := bt.NewTx()

	for i, parent := range []chainhash.Hash{{1}, {2}, {1}} {
		in := &bt.Input{PreviousTxOutIndex: uint32(i)} //nolint:gosec // i is 0 to 2
		require.NoError(t, in.PreviousTxIDAdd(&parent))
		tx.Inputs = append(tx.Inputs, in)
	}

	inpoints, err := subtree.NewTxInpointsFromTx(tx)
	require.NoError(t, err)
	require.Len(t, inpoints.ParentTxHashes, 2)

	return &meta.Data{
		TxInpoints:   inpoints,
		Fee:          423,
		SizeInBytes:  250,
		InBlock:      true,
		LanePosition: lane.Pack(200, 41_000),
	}
}

// The lane encoding holds MetaBytes whole, so a receiver gets exactly what a plain ADD entry carries,
// without a copy, plus the positions.
func TestLaneMetaContentIsTheMetaBytes(t *testing.T) {
	d := laneTestData(t)
	parents := []uint64{lane.Pack(3, 1), lane.Pack(4, 2)}

	content, err := d.LaneMetaBytesInto(nil, parents)
	require.NoError(t, err)

	misses := testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode))

	legacy, err := LaneMetaContent(content)
	require.NoError(t, err)
	require.Equal(t, misses, testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode)),
		"an entry that decodes is not a miss")

	var full meta.Data
	gotParents, err := meta.NewDataFromLaneMetaBytes(content, &full)
	require.NoError(t, err)
	own := full.LanePosition

	want, err := d.MetaBytes()
	require.NoError(t, err)
	require.Equal(t, want, legacy)
	require.Equal(t, content[0], want[0], "a fee of 423 satoshis begins like the lane magic, which is why the action byte marks the format")
	require.Equal(t, d.LanePosition, own)
	require.Equal(t, parents, gotParents)
	require.Equal(t, len(legacy), cap(legacy), "an append to the MetaBytes cannot write into the positions")

	var back meta.Data
	require.NoError(t, meta.NewMetaDataFromBytes(legacy, &back))
	require.True(t, back.InBlock)
	require.Equal(t, uint64(423), back.Fee)
}

// A lane entry that does not decode is an error for the receiver to drop, and a counted miss.
func TestLaneMetaContentCountsWhatDoesNotDecode(t *testing.T) {
	d := laneTestData(t)

	content, err := d.LaneMetaBytesInto(nil, []uint64{lane.Pack(3, 1), lane.Pack(4, 2)})
	require.NoError(t, err)

	before := testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode))

	for name, bad := range map[string][]byte{
		"truncated":      content[:len(content)-1],
		"one byte extra": append(append([]byte(nil), content...), 0),
		"zero position":  append(append([]byte(nil), content[:len(content)-8]...), 0, 0, 0, 0, 0, 0, 0, 0),
		"legacy content": func() []byte { b, _ := d.MetaBytes(); return b }(),
		"empty":          nil,
		"zero own":       laneMetaWith(content, func(b []byte) { clear(b[2:10]) }),
		// Byte 26 holds the flags (stores/utxo/meta/data_lane.go).
		"unknown flag": laneMetaWith(content, func(b []byte) { b[26] |= 0b100000 }),
	} {
		_, err := LaneMetaContent(bad)
		require.Error(t, err, name)
	}

	require.Equal(t, before+7, testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode)))
}

// The sender counts every entry at shadow, lane, own or legacy, and the reason for each one that
// goes without the parents' positions.
func TestCountLaneTxMetaSent(t *testing.T) {
	lanes := prometheusLaneTxMetaMessages.WithLabelValues("sent", "lane")
	own := prometheusLaneTxMetaMessages.WithLabelValues("sent", "own")
	legacy := prometheusLaneTxMetaMessages.WithLabelValues("sent", "legacy")
	parentMissing := prometheusLaneTxMetaMisses.WithLabelValues(LaneMissParent)
	ownMissing := prometheusLaneTxMetaMisses.WithLabelValues(LaneMissOwn)

	beforeLane, beforeOwn, beforeLegacy := testutil.ToFloat64(lanes), testutil.ToFloat64(own), testutil.ToFloat64(legacy)
	beforeParent, beforeOwnMiss := testutil.ToFloat64(parentMissing), testutil.ToFloat64(ownMissing)
	noReason := prometheusLaneTxMetaMisses.WithLabelValues("")
	beforeNoReason := testutil.ToFloat64(noReason)

	CountLaneTxMetaSent("", false)
	CountLaneTxMetaSent(LaneMissParent, true)
	CountLaneTxMetaSent(LaneMissOwn, false)

	require.Equal(t, beforeLane+1, testutil.ToFloat64(lanes))
	require.Equal(t, beforeOwn+1, testutil.ToFloat64(own))
	require.Equal(t, beforeLegacy+1, testutil.ToFloat64(legacy))
	require.Equal(t, beforeParent+1, testutil.ToFloat64(parentMissing))
	require.Equal(t, beforeOwnMiss+1, testutil.ToFloat64(ownMissing))
	require.Equal(t, beforeNoReason, testutil.ToFloat64(noReason), "a full lane entry is no miss")
}

// A consumer counts each entry under its own name, by the format the entry came in.
func TestLaneTxMetaReceivedCountsByFormat(t *testing.T) {
	lanes := prometheusLaneTxMetaMessages.WithLabelValues("test-consumer", "lane")
	legacy := prometheusLaneTxMetaMessages.WithLabelValues("test-consumer", "legacy")
	beforeLane, beforeLegacy := testutil.ToFloat64(lanes), testutil.ToFloat64(legacy)

	own := prometheusLaneTxMetaMessages.WithLabelValues("test-consumer", "own")
	beforeOwn := testutil.ToFloat64(own)

	received := NewLaneTxMetaReceived("test-consumer")
	received.Count(true)
	received.Count(true)
	received.Count(false)
	received.CountOwn()

	require.Equal(t, beforeLane+2, testutil.ToFloat64(lanes))
	require.Equal(t, beforeLegacy+1, testutil.ToFloat64(legacy))
	require.Equal(t, beforeOwn+1, testutil.ToFloat64(own))
}

// An ADDOwn entry gives the receiver the MetaBytes a plain ADD entry carries, without a copy.
func TestOwnCacheContentIsTheMetaBytes(t *testing.T) {
	d := laneTestData(t)

	content, err := d.CacheBytes()
	require.NoError(t, err)

	misses := testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode))

	legacy, err := OwnCacheContent(content)
	require.NoError(t, err)
	require.Equal(t, misses, testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode)),
		"an entry that checks out is not a miss")

	want, err := d.MetaBytes()
	require.NoError(t, err)
	require.Equal(t, want, legacy)
	require.Same(t, &content[0], &legacy[0])
}

// An ADDOwn entry that does not check out is an error for the receiver to drop, and a counted miss.
func TestOwnCacheContentCountsWhatDoesNotCheckOut(t *testing.T) {
	d := laneTestData(t)

	content, err := d.CacheBytes()
	require.NoError(t, err)

	plain, err := d.MetaBytes()
	require.NoError(t, err)

	before := testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode))

	for name, bad := range map[string][]byte{
		"plain MetaBytes": plain,
		"truncated":       content[:len(content)-1],
		"zero position":   append(append([]byte(nil), plain...), 0, 0, 0, 0, 0, 0, 0, 0),
	} {
		_, err := OwnCacheContent(bad)
		require.Error(t, err, name)
	}

	require.Equal(t, before+3, testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode)))
}

// The view allocates nothing, so a receiver pays no more for a lane entry than for a plain one.
func TestLaneMetaContentDoesNotAllocate(t *testing.T) {
	d := laneTestData(t)

	content, err := d.LaneMetaBytesInto(nil, []uint64{lane.Pack(3, 1), lane.Pack(4, 2)})
	require.NoError(t, err)

	require.Zero(t, testing.AllocsPerRun(1000, func() {
		if _, err := LaneMetaContent(content); err != nil {
			t.Fatal(err)
		}
	}))
}

// With room for the whole message in dst, building an entry's cache form allocates nothing.
func TestAppendLaneCacheContentDoesNotAllocate(t *testing.T) {
	d := laneTestData(t)

	content, err := d.LaneMetaBytesInto(nil, []uint64{lane.Pack(3, 1), lane.Pack(4, 2)})
	require.NoError(t, err)

	dst := make([]byte, 0, len(content))

	require.Zero(t, testing.AllocsPerRun(1000, func() {
		if _, err := AppendLaneCacheContent(dst[:0], content); err != nil {
			t.Fatal(err)
		}
	}))
}

// At shadow a receiver keeps the lane entry's own position after its MetaBytes: the cache form.
func TestAppendLaneCacheContentIsTheCacheForm(t *testing.T) {
	d := laneTestData(t)

	content, err := d.LaneMetaBytesInto(nil, []uint64{lane.Pack(3, 1), lane.Pack(4, 2)})
	require.NoError(t, err)

	want, err := d.CacheBytes()
	require.NoError(t, err)

	got, err := AppendLaneCacheContent(make([]byte, 0, len(content)), content)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.LessOrEqual(t, len(got), len(content), "the cache form is never longer than the entry")

	before := testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode))

	dst := []byte{1}
	got, err = AppendLaneCacheContent(dst, content[:len(content)-1])
	require.Error(t, err)
	require.Equal(t, []byte{1}, got)
	require.Equal(t, before+1, testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode)))
}

// laneMetaWith returns a copy of content changed by change.
func laneMetaWith(content []byte, change func([]byte)) []byte {
	b := append([]byte(nil), content...)
	change(b)

	return b
}
