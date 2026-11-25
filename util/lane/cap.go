package lane

import "time"

// CapGrant is one lane's first grant handed out at or above a height: block assembly's restart
// window starts at it (the lanes design, "Job two: block assembly's restart"). Positions only rise
// within a lane, so a position of the lane below First came from a grant handed out below that
// height.
type CapGrant struct {
	Lane uint8
	// First is the grant's first position in the lane, unpacked.
	First uint64
	// Height is the chain tip the blockchain service recorded when it handed the grant out.
	Height uint32
	// HandedAt is when the blockchain service recorded the grant, on the database's clock.
	HandedAt time.Time
}

// Below reports whether the packed lane position pos is below the grant's first position in its
// lane: from a grant handed out before the grant's height.
func (g CapGrant) Below(pos uint64) bool {
	_, p := Unpack(pos)

	return p < g.First
}
