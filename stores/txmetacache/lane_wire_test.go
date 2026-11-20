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

	legacy, own, gotParents, err := LaneMetaContent(content)
	require.NoError(t, err)

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
		"legacy content": func() []byte { b, _ := d.MetaBytes(); return b }(),
		"empty":          nil,
	} {
		_, _, _, err := LaneMetaContent(bad)
		require.Error(t, err, name)
	}

	require.Equal(t, before+3, testutil.ToFloat64(prometheusLaneTxMetaMisses.WithLabelValues(LaneMissDecode)))
}

// The sender counts every entry at shadow, lane or legacy, and the reason for each legacy one.
func TestCountLaneTxMetaSent(t *testing.T) {
	lanes := prometheusLaneTxMetaMessages.WithLabelValues("sent", "lane")
	legacy := prometheusLaneTxMetaMessages.WithLabelValues("sent", "legacy")
	parentMissing := prometheusLaneTxMetaMisses.WithLabelValues(LaneMissParent)

	beforeLane, beforeLegacy, beforeMiss := testutil.ToFloat64(lanes), testutil.ToFloat64(legacy), testutil.ToFloat64(parentMissing)

	CountLaneTxMetaSent("")
	CountLaneTxMetaSent(LaneMissParent)

	require.Equal(t, beforeLane+1, testutil.ToFloat64(lanes))
	require.Equal(t, beforeLegacy+1, testutil.ToFloat64(legacy))
	require.Equal(t, beforeMiss+1, testutil.ToFloat64(parentMissing))
}
