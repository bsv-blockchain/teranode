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

	// ReasonCorruptBlockBody scores a peer that served a corrupt block body for a valid
	// block hash (bitcoin-sv/teranode#4692). Unlike ReasonInvalidBlock the block is NOT persisted
	// invalid; the peer is struck and the body re-downloaded, mirroring svnode's
	// CorruptionOrDoS stance of punishing the connection without failing the header. Lives here in
	// the dependency-free interfaces package so callers (e.g. services/blockvalidation) do not need
	// to import the whole services/p2p server package for one string. The underscore value must
	// match the services/blockchain peer_registry ReasonPoints key and the services/p2p AddBanScore
	// switch. String value: "corrupt_block_body".
	ReasonCorruptBlockBody BanReason = "corrupt_block_body"
)
