package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func u32(v uint32) *uint32 { return &v }
func ptrInt(v int) *int    { return &v }

// TestChildDAHFollowUps_Collect pins, without a server, which master DAH transitions queue a
// child-record follow-up and that, within one batch, the LAST state per txid wins so a set and an
// unset for the same txid can never be applied in the wrong order.
func TestChildDAHFollowUps_Collect(t *testing.T) {
	a := chainhash.HashH([]byte("a"))
	b := chainhash.HashH([]byte("b"))

	type step struct {
		hash  *chainhash.Hash
		state SetMinedState
	}

	ext := func(old, nw *uint32) SetMinedState {
		return SetMinedState{External: true, TotalExtraRecs: ptrInt(2), OldDeleteAtHeight: old, NewDeleteAtHeight: nw}
	}

	tests := []struct {
		name      string
		steps     []step
		wantSets  []childDAHItem
		wantUnset []childDAHItem
	}{
		{name: "non-external master queues nothing",
			steps: []step{{&a, SetMinedState{External: false, TotalExtraRecs: ptrInt(2), NewDeleteAtHeight: u32(10)}}}},
		{name: "no totalExtraRecs (pagination record) queues nothing",
			steps: []step{{&a, SetMinedState{External: true, NewDeleteAtHeight: u32(10)}}}},
		{name: "nil -> X is a set",
			steps:    []step{{&a, ext(nil, u32(10))}},
			wantSets: []childDAHItem{{TxID: &a, ChildCount: 2, DeleteAtHeight: 10}}},
		{name: "X -> Y is a set of the new value",
			steps:    []step{{&a, ext(u32(5), u32(10))}},
			wantSets: []childDAHItem{{TxID: &a, ChildCount: 2, DeleteAtHeight: 10}}},
		{name: "X -> X queues nothing", steps: []step{{&a, ext(u32(10), u32(10))}}},
		{name: "nil -> nil queues nothing", steps: []step{{&a, ext(nil, nil)}}},
		{name: "X -> nil is an unset (DeleteAtHeight 0)",
			steps:     []step{{&a, ext(u32(5), nil)}},
			wantUnset: []childDAHItem{{TxID: &a, ChildCount: 2, DeleteAtHeight: 0}}},
		{name: "set then unset for the same txid: unset wins",
			steps:     []step{{&a, ext(nil, u32(10))}, {&a, ext(u32(10), nil)}},
			wantUnset: []childDAHItem{{TxID: &a, ChildCount: 2, DeleteAtHeight: 0}}},
		{name: "unset then set for the same txid: set wins",
			steps:    []step{{&a, ext(u32(5), nil)}, {&a, ext(nil, u32(10))}},
			wantSets: []childDAHItem{{TxID: &a, ChildCount: 2, DeleteAtHeight: 10}}},
		{name: "different txids are independent",
			steps:     []step{{&a, ext(nil, u32(10))}, {&b, ext(u32(5), nil)}},
			wantSets:  []childDAHItem{{TxID: &a, ChildCount: 2, DeleteAtHeight: 10}},
			wantUnset: []childDAHItem{{TxID: &b, ChildCount: 2, DeleteAtHeight: 0}}},
		{name: "a later no-change keeps the earlier transition",
			steps:    []step{{&a, ext(nil, u32(10))}, {&a, ext(u32(10), u32(10))}},
			wantSets: []childDAHItem{{TxID: &a, ChildCount: 2, DeleteAtHeight: 10}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var f childDAHFollowUps
			for _, st := range tc.steps {
				f.collect(st.hash, st.state)
			}

			require.Equal(t, tc.wantSets, f.sets())
			require.Equal(t, tc.wantUnset, f.unsets())
		})
	}
}

// TestChunkChildDAHItems pins the bounded batching of child-record writes: items are grouped so a
// chunk holds about maxPages pages, an item larger than the cap travels alone, and order is kept.
func TestChunkChildDAHItems(t *testing.T) {
	h := func(b byte) *chainhash.Hash { v := chainhash.HashH([]byte{b}); return &v }
	it := func(b byte, pages int) childDAHItem {
		return childDAHItem{TxID: h(b), ChildCount: pages, DeleteAtHeight: 1}
	}

	tests := []struct {
		name  string
		items []childDAHItem
		max   int
		want  [][]int // child counts per chunk
	}{
		{name: "empty", max: 10},
		{name: "fits in one chunk", items: []childDAHItem{it(1, 3), it(2, 4)}, max: 10, want: [][]int{{3, 4}}},
		{name: "splits when the cap would be exceeded", items: []childDAHItem{it(1, 6), it(2, 6), it(3, 2)}, max: 10, want: [][]int{{6}, {6, 2}}},
		{name: "oversized item travels alone", items: []childDAHItem{it(1, 2), it(2, 50), it(3, 2)}, max: 10, want: [][]int{{2}, {50}, {2}}},
		{name: "exact fit", items: []childDAHItem{it(1, 5), it(2, 5), it(3, 5)}, max: 10, want: [][]int{{5, 5}, {5}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got [][]int

			for _, chunk := range chunkChildDAHItems(tc.items, tc.max) {
				var counts []int
				for _, item := range chunk {
					counts = append(counts, item.ChildCount)
				}

				got = append(got, counts)
			}

			require.Equal(t, tc.want, got)
		})
	}
}

// TestExpiredPreservationRestoreBins pins the mapping used to put a master back after a failed
// child follow-up: the queried preserveUntil, and the old DAH as an int, or a nil bin (delete)
// when there was none.
func TestExpiredPreservationRestoreBins(t *testing.T) {
	t.Run("old DAH nil deletes the DAH bin", func(t *testing.T) {
		bins := expiredPreservationRestoreBins(190, nil)
		require.Len(t, bins, 2)
		require.Equal(t, "preserveUntil", bins[0].Name)
		require.Equal(t, 190, bins[0].Value.GetObject())
		require.Equal(t, "deleteAtHeight", bins[1].Name)
		require.Nil(t, bins[1].Value.GetObject())
	})

	t.Run("old DAH restored as int", func(t *testing.T) {
		bins := expiredPreservationRestoreBins(190, u32(5))
		require.Equal(t, 5, bins[1].Value.GetObject())
	})
}
