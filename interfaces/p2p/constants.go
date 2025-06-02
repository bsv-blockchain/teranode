// Package p2p defines interfaces and constants for P2P networking
package p2p

// BanReason represents the reason for banning a peer
type BanReason string

// String returns the string representation of the ban reason
func (r BanReason) String() string {
	return string(r)
}

// Ban reasons
const (
	// ReasonInvalidBlock is used when a peer sends an invalid block
	ReasonInvalidBlock BanReason = "invalid-block"
)
