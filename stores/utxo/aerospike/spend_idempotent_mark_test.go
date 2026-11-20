package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"
)

// TestMarkIdempotentSpendsOnlyMarksThisRecordsItems: the idempotent list comes
// back from the Lua or from the native dispatcher, which lives outside this
// repo. An index that is in range for the whole batch but belongs to another
// record's spends names an item that may already have been completed and
// published, so it must be ignored rather than flagged.
func TestMarkIdempotentSpendsOnlyMarksThisRecordsItems(t *testing.T) {
	batch := make([]*batchSpend, 4)
	for i := range batch {
		batch[i] = &batchSpend{}
	}

	// This record was asked about items 2 and 3 only.
	batchByKey := []aerospike.MapValue{{"idx": 2}, {"idx": 3}}

	// The heights come back from the same dispatcher: one for an index of another record, one for
	// this record's own match, and one for an index that was not reported idempotent.
	markIdempotentSpends([]int{1, 3, 9, -1}, map[int]uint32{1: 1100, 2: 1150, 3: 1200}, batchByKey, batch)

	require.False(t, batch[0].idempotent)
	require.False(t, batch[1].idempotent, "index 1 is in the batch but belongs to another record")
	require.False(t, batch[2].idempotent, "not reported idempotent")
	require.True(t, batch[3].idempotent, "this record's own idempotent match is flagged")

	require.True(t, batch[3].hasRecordedHeight)
	require.Equal(t, uint32(1200), batch[3].recordedHeight, "the match carries the height its spend recorded")

	require.False(t, batch[1].hasRecordedHeight, "a height reported for another record's index is ignored")
	require.False(t, batch[2].hasRecordedHeight, "a height for an index not reported idempotent is ignored")

	// a match reported without a height has none, so it counts as historical
	other := []*batchSpend{{}, {}, {}, {}}
	markIdempotentSpends([]int{3}, nil, batchByKey, other)
	require.True(t, other[3].idempotent)
	require.False(t, other[3].hasRecordedHeight)
}
