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

	// Remove removes bans from the ban list. A request containing "/" is an
	// explicit CIDR and removes only that exact raw key. Any other valid request
	// is a host and removes every stored host entry for the same IP, whatever its
	// port, spelling or IPv4-mapped form; CIDR rules are never removed by a host
	// request, so a covering CIDR can keep the host banned. A valid request that
	// matches nothing, confirmed against the database, succeeds.
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
