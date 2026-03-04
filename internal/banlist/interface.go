package banlist

import (
	"context"
	"net"
	"time"
)

// Interface defines the contract for IP/subnet ban list functionality.
// Implementations must be safe for concurrent use.
type Interface interface {
	// IsBanned checks if an IP address is currently banned.
	// Accepts bare IPs or host:port format.
	IsBanned(ipStr string) bool

	// Add adds an IP or CIDR subnet to the ban list with an expiration time.
	Add(ctx context.Context, ipOrSubnet string, expirationTime time.Time) error

	// Remove removes an IP or subnet from the ban list.
	Remove(ctx context.Context, ipOrSubnet string) error

	// ListBanned returns all currently banned IPs and subnets.
	ListBanned() []string

	// Subscribe returns a channel that receives ban events.
	Subscribe() chan BanEvent

	// Unsubscribe removes a subscription to ban events.
	Unsubscribe(ch chan BanEvent)

	// Init initializes the ban list (creates tables, loads from DB).
	Init(ctx context.Context) error

	// Clear removes all entries from the ban list.
	Clear()
}

// BanInfo contains information about a banned peer.
type BanInfo struct {
	ExpirationTime time.Time
	Subnet         *net.IPNet
}

// BanEvent represents a ban-related event in the system.
type BanEvent struct {
	Action string
	PeerID string
	IP     string
	Subnet *net.IPNet
	Reason string
}
