// Package lane defines the lane position, an 8-byte number a new transaction
// record is given once, in the same store write that creates it, when the daemon
// runs utxostore_laneMigration at shadow or later.
//
// The high byte is the lane: one per process that creates records, held under a
// lease from the blockchain service. The low seven bytes are the position: the
// record's place in that lane. Within a grant, new positions count up by one per
// take. A position a store proved no record carries can be returned and given out
// again, only inside the grant it came from (Allocator.Return), so takes within a
// grant need not rise. A lease lapses unless renewed; its holder stops giving out
// positions before the service can close it and give the lane to another process,
// and every grant in a lane starts above the lane's last one, so no two records
// carry the same position in a lane, and the grants rise in the order they were
// recorded. A lane position is unique among the records created under the grants
// recorded in one blockchain database and is written once, so later work can say
// which block took a record by keeping, per lane per block, the range of positions
// it took and the holes in that range, instead of writing to the record. A range
// alone does not say it: a position returned and reused can be written in a later
// block, below a position an earlier block took, and is a hole in the earlier one.
// Restoring an older blockchain database could reuse positions, which is a listed
// limit of this phase.
//
// Positions start at 1. Lane 0 is a real lane, so the packed value 0 can only
// mean "no lane position": a record created with the migration off, before it
// existed, or by a tool that opens its own store. At shadow, the daemon refuses
// to start with a UTXO store that cannot write positions, such as null or pebble. A read
// also returns 0 for a utxoset record whose row carrying the position has aged
// out. Stores keep 0 as an absent bin or a NULL column.
//
// Every store that holds the value uses a signed 64-bit integer (Aerospike
// integer bins, Postgres BIGINT, SQLite INTEGER). Lanes 128 to 255 set the top
// bit, so they are stored as negative numbers. ToSigned and FromSigned convert
// bit for bit; never convert with a range check or a clamp.
package lane

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

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

// LeaseID names one lease: one process's claim on one lane, which lapses unless renewed. It is
// random, so a process that restarts can never be mistaken for its earlier self.
type LeaseID [16]byte

// String formats the lease ID as a UUID, the form the blockchain database keeps on Postgres.
func (id LeaseID) String() string {
	return uuid.UUID(id).String()
}

// IsZero reports whether id is the zero value, which no lease ever has.
func (id LeaseID) IsZero() bool {
	return id == LeaseID{}
}

// NewLeaseID returns a random lease ID.
func NewLeaseID() (LeaseID, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return LeaseID{}, err
	}

	return LeaseID(u), nil
}

// Grant is a stretch of positions in a leased lane that the holder may use: First to Last
// inclusive. The blockchain service records it before the holder uses it. Each grant in a lane
// starts one above that lane's newest grant, whoever held it, or, when its request settles that
// newest grant as the same lease's own, right after the settled last taken position.
type Grant struct {
	Lane  uint8
	First uint64
	Last  uint64
}

// Len is the number of positions in the grant.
func (g Grant) Len() uint64 {
	if g.Last < g.First {
		return 0
	}

	return g.Last - g.First + 1
}

// Settles finishes a grant the holder has left: the grant's first position and its final number,
// which is the grant's last position if it was used up, or the highest position taken if it aged
// out, lowered past any positions at the top of that range that came back unwritten (Allocator.Return)
// and were not given out again. No position of that grant above the final number carries a record. The holder
// sends it on its next grant request and on its release, and the service writes it on that hand-out
// row or close row. A grant is settled only once it has been used; LastTaken is zero in a pair the
// holder sends only when every position the grant gave out came back, so none carries a record. A
// grant never used gets no settles pair: positions above the release's LastTaken were never given
// out, or came back, and a lapsed close is taken to have used every grant of its lease. The zero
// value settles nothing.
type Settles struct {
	First     uint64
	LastTaken uint64
}

// Renewal renews a lease and reports what the holder has taken. It is the plain-Go form of the
// blockchain service's renewal request.
type Renewal struct {
	Lease LeaseID
	Lane  uint8
	// Rate is positions taken per second, measured by the holder.
	Rate uint64
	// WantGrant asks for the next grant.
	WantGrant bool
	// Settles finishes the most recent grant the holder has left and not yet reported (an older one
	// may follow on a later request or the release). It is sent with a grant request, and the
	// service records it on the new grant's hand-out row.
	Settles Settles
}

// RenewResult answers a renewal. Closed means the lease is closed, nothing was recorded, and the
// holder must open a new lease. Granted means Grant is the next grant.
type RenewResult struct {
	Closed  bool
	Granted bool
	Grant   Grant
}

// Release ends a lease cleanly.
type Release struct {
	Lease LeaseID
	Lane  uint8
	// LastTaken is the highest position the lease gave out in the lane that may carry a record:
	// positions at the top that came back unwritten (Allocator.Return) are left out, so it can fall
	// below the current grant, and is zero when none was given out or every one came back.
	LastTaken uint64
	// Settles finishes the oldest grant left and not yet reported; a newer one left after it is
	// settled by LastTaken.
	Settles Settles
}

// LeaseSource opens, renews and releases lane leases. The blockchain client implements it.
type LeaseSource interface {
	OpenLaneLease(ctx context.Context, holder string) (LeaseID, Grant, error)
	RenewLaneLease(ctx context.Context, r Renewal) (RenewResult, error)
	ReleaseLaneLease(ctx context.Context, r Release) (alreadyClosed bool, err error)
}
