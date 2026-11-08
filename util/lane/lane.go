// Package lane defines the lane position, an 8-byte number every new
// transaction record is given once, in the same store write that creates it.
//
// The high byte is the lane: one per process that creates records, handed out
// by the blockchain service. The low seven bytes are the position: the record's
// place in that lane, counting up by one per record. A lane position is unique
// and never changes, so later work can say which block took a record by keeping
// a range of positions per lane per block, instead of writing to the record.
//
// Positions start at 1. Lane 0 is a real lane, so the packed value 0 can only
// mean "no lane position": a record created with the feature off, or before it
// existed. Stores keep 0 as an absent bin or a NULL column.
//
// Every store that holds the value uses a signed 64-bit integer (Aerospike
// integer bins, Postgres BIGINT, SQLite INTEGER). Lanes 128 to 255 set the top
// bit, so they are stored as negative numbers. ToSigned and FromSigned convert
// bit for bit; never convert with a range check or a clamp.
package lane

import "fmt"

const (
	// None is the packed value of "no lane position".
	None uint64 = 0

	// positionBits is the width of the position, the low seven bytes.
	positionBits = 56

	// MaxPosition is the highest position a lane can hold.
	MaxPosition uint64 = 1<<positionBits - 1

	// MaxLane is the highest lane number.
	MaxLane = 255

	// FirstPosition is the first position ever handed out in a lane.
	FirstPosition uint64 = 1
)

// Pack builds a lane position from a lane and a position. It panics if the
// position does not fit in seven bytes, because a wider one would silently
// change the lane.
func Pack(lane uint8, position uint64) uint64 {
	if position > MaxPosition {
		panic(fmt.Sprintf("lane position %d does not fit in 7 bytes", position))
	}

	return uint64(lane)<<positionBits | position
}

// Unpack splits a lane position into its lane and position.
func Unpack(v uint64) (lane uint8, position uint64) {
	return uint8(v >> positionBits), v & MaxPosition
}

// Lane returns the lane of a lane position.
func Lane(v uint64) uint8 {
	return uint8(v >> positionBits)
}

// Valid reports whether v names a real position: any lane, position 1 or more.
func Valid(v uint64) bool {
	return v&MaxPosition != 0
}

// ToSigned converts a lane position to the signed form the stores keep.
func ToSigned(v uint64) int64 {
	return int64(v) //nolint:gosec // bit-preserving by design, see the package comment
}

// FromSigned converts the signed form a store keeps back to a lane position.
func FromSigned(v int64) uint64 {
	return uint64(v) //nolint:gosec // bit-preserving by design, see the package comment
}
