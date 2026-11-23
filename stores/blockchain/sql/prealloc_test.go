package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPreallocBounds pins the allocation guard on the height-range and
// count-driven queries. These are reachable on unauthenticated gRPC methods, so
// a caller-supplied range must never become a caller-supplied allocation:
// {startHeight: 2, endHeight: 0} used to underflow uint32 subtraction to
// 4294967295 and attempt a ~34GB make().
func TestPreallocBounds(t *testing.T) {
	require.Equal(t, 1, preallocForRange(2, 0), "a reversed range must not underflow")
	require.Equal(t, 1, preallocForRange(0, 0))
	require.Equal(t, 11, preallocForRange(10, 20))
	require.Equal(t, maxResultPrealloc, preallocForRange(0, 4294967294), "a huge ordered range must be clamped")

	require.Equal(t, 1, preallocFor(0))
	require.Equal(t, 100, preallocFor(100))
	require.Equal(t, maxResultPrealloc, preallocFor(1<<40), "a huge requested count must be clamped")
}
