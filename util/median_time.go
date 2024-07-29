package util

import (
	"sort"

	"github.com/bitcoin-sv/ubsv/errors"
)

// timeSorter implements sort.Interface to allow a slice of timestamps to
// be sorted.
type timeSorter []int64

// Len returns the number of timestamps in the slice.  It is part of the
// sort.Interface implementation.
func (s timeSorter) Len() int {
	return len(s)
}

// Swap swaps the timestamps at the passed indices.  It is part of the
// sort.Interface implementation.
func (s timeSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less returns whether the timstamp with index i should sort before the
// timestamp with index j.  It is part of the sort.Interface implementation.
func (s timeSorter) Less(i, j int) bool {
	return s[i] < s[j]
}

// medianTimeBlocks is the number of previous blocks which should be
// used to calculate the median time used to validate block timestamps.
const medianTimeBlocks = 11

// CalcPastMedianTime calculates the median time of the previous few blocks
// prior to, and including, the block node.
//
// This function is safe for concurrent access.
func CalcPastMedianTime(timestamps []int64) (int64, error) {
	if len(timestamps) > medianTimeBlocks {
		return 0, errors.New(errors.ERR_PROCESSING, "too many timestamps for median time calculation")
	}

	sort.Sort(timeSorter(timestamps))

	// NOTE: The consensus rules incorrectly calculate the median for even
	// numbers of blocks.  A true median averages the middle two elements
	// for a set with an even number of elements in it.   Since the constant
	// for the previous number of blocks to be used is odd, this is only an
	// issue for a few blocks near the beginning of the chain.  I suspect
	// this is an optimization even though the result is slightly wrong for
	// a few of the first blocks since after the first few blocks, there
	// will always be an odd number of blocks in the set per the constant.
	//
	// This code follows suit to ensure the same rules are used, however, be
	// aware that should the medianTimeBlocks constant ever be changed to an
	// even number, this code will be wrong.
	return timestamps[len(timestamps)/2], nil
}
