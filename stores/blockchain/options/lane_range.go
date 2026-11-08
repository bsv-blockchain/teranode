package options

import "time"

// LaneRangeRequest is one request for a range of lane positions. It lives here,
// next to the other store parameter types, so the Store interface and the SQL
// store can share it without an import cycle.
type LaneRangeRequest struct {
	// Holder names the process asking, recorded with the hand-out.
	Holder string
	// WantLane is the lane the holder asks for. It only counts when HasLane is true.
	WantLane uint8
	HasLane  bool
	// Size is the number of positions to hand out.
	Size uint64
	// Height is the best block height at hand-out, recorded for operators.
	Height uint32
	// FreeAfter is how long a lane must be idle before another holder can have it.
	FreeAfter time.Duration
	// Now is the hand-out time. Injected so tests can move the clock.
	Now time.Time
}
