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
