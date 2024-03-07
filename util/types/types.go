package types

// define bucket type enum with 3 types, unallocated, pre-allocated and trimmed
type BucketType int

const (
	Unallocated BucketType = iota
	Preallocated
	Trimmed
)
