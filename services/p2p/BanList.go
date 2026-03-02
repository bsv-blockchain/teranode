package p2p

// Package p2p provides peer-to-peer networking functionality for the Teranode system.
// The ban management subsystem in this package implements mechanisms to protect
// the network from malicious or misbehaving peers by temporarily or permanently
// restricting their access to the node.
//
// The ban system supports:
// - IP-based and subnet-based banning
// - Time-limited bans with automatic expiration
// - Persistent storage of ban information across restarts
// - Event notifications for ban-related actions
// - Thread-safe operations for concurrent access

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/usql"
)

var (
	banListInstance *BanList
	banListOnce     sync.Once
)

// BanInfo contains information about a banned peer.
// It encapsulates all the data needed to track and manage a ban entry,
// including when the ban should expire and what network range it applies to.
//
// The struct supports both individual IP bans and subnet-based bans through
// the Subnet field. For individual IP bans, the subnet mask is set to /32 (IPv4)
// or /128 (IPv6) to represent a single address.
type BanInfo struct {
	ExpirationTime time.Time  // Time when the ban expires
	Subnet         *net.IPNet // Subnet information for the ban (can represent single IP or CIDR range)
}

// BanEvent represents a ban-related event in the system.
// It is used for notifying subscribers about changes to the ban list,
// allowing other components to react to ban-related activities.
//
// BanEvents are published through channels created via the Subscribe method,
// and can be used for logging, metrics collection, and coordination with
// other network components like the P2P node.
type BanEvent struct {
	Action string     // Action type ("add" for new bans or "remove" for unbanning)
	PeerID string     // Peer ID involved in the ban action (primary identifier)
	IP     string     // IP address (for logging/backward compatibility)
	Subnet *net.IPNet // Subnet information if the ban applies to a network range
	Reason string     // Optional reason for the ban
}

// BanListI defines the interface for peer banning functionality.
// This interface abstracts the implementation details of ban management,
// allowing different implementations to be used (such as in-memory, database-backed,
// or distributed implementations) while providing a consistent API.
//
// The interface supports checking, adding, removing, and listing banned peers,
// with operations to manage both individual IP addresses and subnets.
// Thread-safety is guaranteed by implementations of this interface.
type BanListI interface {
	// IsBanned checks if a peer is banned by its IP address.
	// This is a fast, thread-safe operation suitable for frequent checks
	// during network operations.
	//
	// Parameters:
	// - ipStr: The IP address to check as a string
	//
	// Returns true if the IP is currently banned, false otherwise
	IsBanned(ipStr string) bool

	// Add adds an IP or subnet to the ban list with an expiration time.
	// For individual IPs, the ipOrSubnet parameter should be a valid IP address.
	// For subnets, it should be in CIDR notation (e.g., "192.168.1.0/24").
	//
	// Parameters:
	// - ctx: Context for the operation, allowing for cancellation
	// - ipOrSubnet: IP address or subnet to ban
	// - expirationTime: When the ban should expire; use a far future time for permanent bans
	//
	// Returns an error if the IP/subnet cannot be parsed or the ban cannot be applied
	Add(ctx context.Context, ipOrSubnet string, expirationTime time.Time) error

	// Remove removes an IP or subnet from the ban list.
	// The format for ipOrSubnet is the same as in the Add method.
	//
	// Parameters:
	// - ctx: Context for the operation, allowing for cancellation
	// - ipOrSubnet: IP address or subnet to unban
	//
	// Returns an error if the operation fails or the IP/subnet was not banned
	Remove(ctx context.Context, ipOrSubnet string) error

	// ListBanned returns a list of all currently banned IP addresses and subnets.
	// The list includes both individual IPs and subnet ranges currently in effect.
	//
	// Returns a slice of strings representing banned IPs and subnets
	ListBanned() []string

	// Subscribe returns a channel to receive ban events
	Subscribe() chan BanEvent

	// Unsubscribe removes a subscription to ban events
	Unsubscribe(ch chan BanEvent)

	// Init initializes the ban list and starts any background processes
	Init(ctx context.Context) error

	// Clear removes all entries from the ban list and cleans up resources
	Clear()
}

// BanList manages the list of banned peers and related operations.
type BanList struct {
	db          *usql.DB                   // Database connection
	engine      util.SQLEngine             // SQL engine type
	logger      ulogger.Logger             // Logger instance
	bannedPeers map[string]BanInfo         // Map of banned peers
	subscribers map[chan BanEvent]struct{} // Map of ban event subscribers
	mu          sync.RWMutex               // Mutex for thread-safe operations
}

// GetBanList retrieves or creates a singleton instance of BanList.
// Parameters:
//   - ctx: Context for the operation
//   - logger: Logger instance
//   - tSettings: Configuration settings
//
// Returns:
//   - *BanList: The ban list instance
//   - chan BanEvent: Channel for ban events
//   - error: Any error encountered
func GetBanList(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (*BanList, chan BanEvent, error) {
	var eventChan chan BanEvent

	banListOnce.Do(func() {
		var err error

		banListInstance, err = newBanList(logger, tSettings)
		if err != nil {
			logger.Errorf("Failed to create BanList: %v", err)

			banListInstance = nil

			return
		}

		err = banListInstance.Init(ctx)
		if err != nil {
			logger.Errorf("Failed to initialise BanList: %v", err)

			banListInstance = nil
		}
	})

	if banListInstance == nil {
		return nil, nil, errors.New(errors.ERR_ERROR, "Failed to initialise BanList")
	}

	eventChan = banListInstance.Subscribe()

	return banListInstance, eventChan, nil
}

func newBanList(logger ulogger.Logger, tSettings *settings.Settings) (*BanList, error) {
	blockchainStoreURL := tSettings.BlockChain.StoreURL
	if blockchainStoreURL == nil {
		return nil, errors.NewConfigurationError("no blockchain_store setting found")
	}

	store, err := blockchain.NewStore(logger, blockchainStoreURL, tSettings)
	if err != nil {
		return nil, errors.NewStorageError("failed to create blockchain store: %s", err)
	}

	engine := store.GetDBEngine()
	if engine != util.Postgres && engine != util.Sqlite && engine != util.SqliteMemory {
		return nil, errors.NewStorageError("unsupported database engine: %s", engine)
	}

	return &BanList{
		db:          store.GetDB(),
		engine:      engine,
		logger:      logger,
		bannedPeers: make(map[string]BanInfo),
		subscribers: make(map[chan BanEvent]struct{}),
	}, nil
}

func (b *BanList) Init(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)

	defer cancel()

	if err = b.createTables(ctx); err != nil {
		return errors.NewProcessingError("failed to create banlist tables", err)
	}

	if err = b.loadFromDatabase(ctx); err != nil {
		return errors.NewProcessingError("failed to load banlist from database", err)
	}

	return nil
}

// Add adds an IP or subnet to the ban list.
// map of banned ips with their expiration time
// if the ip or subnet is already banned, the expiration time will be updated
// if the ip or subnet is not banned, it will be added to the map
// ipOrSubnet can be an IP address or a subnet in CIDR notation
// Parameters:
//   - ctx: Context for the operation
//   - ipOrSubnet: IP address or subnet to ban
//   - expirationTime: When the ban should expire
//
// Returns:
//   - error: Any error encountered during the operation
func (b *BanList) Add(ctx context.Context, ipOrSubnet string, expirationTime time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	subnet, err := parseAddress(ipOrSubnet)
	if err != nil {
		b.logger.Errorf("error parsing ip or subnet: %v", err)
		return err
	}

	banInfo := BanInfo{
		ExpirationTime: expirationTime,
		Subnet:         subnet,
	}

	b.bannedPeers[ipOrSubnet] = banInfo

	// Notify subscribers asynchronously after state is updated
	// Copy event data to avoid race conditions
	event := BanEvent{Action: "add", IP: ipOrSubnet, Subnet: subnet}
	go func() {
		b.notifySubscribersAsync(event)
	}()

	return b.savePeerToDatabase(ctx, ipOrSubnet, banInfo)
}

// Remove removes an IP or subnet from the ban list.
// Parameters:
//   - ctx: Context for the operation
//   - ipOrSubnet: IP address or subnet to unban
//
// Returns:
//   - error: Any error encountered during the operation
func (b *BanList) Remove(ctx context.Context, ipOrSubnet string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	subnet, err := parseAddress(ipOrSubnet)
	if err != nil {
		b.logger.Errorf("Invalid IP address or subnet: %s", ipOrSubnet)
		return err
	}

	if _, ok := b.bannedPeers[ipOrSubnet]; !ok {
		return nil // Not found, nothing to do
	}

	delete(b.bannedPeers, ipOrSubnet)

	// Notify subscribers asynchronously after state is updated
	// Copy event data to avoid race conditions
	event := BanEvent{Action: "remove", IP: ipOrSubnet, Subnet: subnet}
	go func() {
		b.notifySubscribersAsync(event)
	}()

	return b.removePeerFromDatabase(ctx, ipOrSubnet)
}

// IsBanned checks if a given IP address is currently banned.
// Parameters:
//   - ipStr: IP address to check
//
// Returns:
//   - bool: true if the IP is banned, false otherwise
func (b *BanList) IsBanned(ipStr string) bool {
	if ipStr == "" {
		return false
	}

	// Strip port from IP address (handles both IPv4 and IPv6)
	host, _, err := net.SplitHostPort(ipStr)
	if err == nil {
		// Successfully split host and port
		ipStr = host
	}
	// If SplitHostPort fails, assume it's just an IP without port

	// First try direct lookup in our map
	b.mu.RLock()
	if info, exists := b.bannedPeers[ipStr]; exists {
		isBanned := info.ExpirationTime.After(time.Now())
		b.mu.RUnlock()

		return isBanned
	}
	b.mu.RUnlock()

	// Try to parse the IP address
	ip := net.ParseIP(ipStr)
	if ip == nil {
		// Not a valid IP address - no need to log this for test cases
		b.logger.Errorf("Invalid IP address passed to IsBanned: %s", ipStr)
		return false
	}

	// Check each subnet
	b.mu.RLock()

	// First, collect keys to delete without modifying the map
	var (
		expiredKeys []string
		isBanned    bool
	)

	for key, info := range b.bannedPeers {
		// Mark expired bans for deletion
		if !info.ExpirationTime.After(time.Now()) {
			expiredKeys = append(expiredKeys, key)
			continue
		}

		// Skip simple IP bans (not subnets)
		if !strings.Contains(key, "/") {
			continue
		}

		// Check if IP is in this subnet
		if info.Subnet != nil && info.Subnet.Contains(ip) {
			isBanned = true
			break
		}
	}
	b.mu.RUnlock()

	// If we found any expired entries, clean them up now with a write lock
	if len(expiredKeys) > 0 {
		b.mu.Lock()
		for _, key := range expiredKeys {
			// Double-check the entry is still expired (it might have been updated)
			if info, exists := b.bannedPeers[key]; exists && !info.ExpirationTime.After(time.Now()) {
				delete(b.bannedPeers, key)
			}
		}
		b.mu.Unlock()
	}

	return isBanned
}

// ListBanned returns a list of all banned IP addresses and subnets
func (b *BanList) ListBanned() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	banned := make([]string, 0, len(b.bannedPeers))

	for key := range b.bannedPeers {
		banned = append(banned, key)
	}

	return banned
}

// createTables initializes the database schema for persistent ban storage.
// This method creates the necessary tables to store ban information across node restarts.
// The bans table stores the ban key, expiration time, and subnet information for each banned entity.
//
// The table schema includes:
//   - key: Primary key identifying the banned IP or subnet
//   - expiration_time: Timestamp when the ban expires
//   - subnet: CIDR notation of the banned network range
//
// This method is called during ban list initialization and is thread-safe.
//
// Parameters:
//   - ctx: Context for the database operation, used for cancellation and timeout control
//
// Returns:
//   - Error if table creation fails, nil on success
func (b *BanList) createTables(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS bans (
            key TEXT PRIMARY KEY,
            expiration_time TIMESTAMP WITH TIME ZONE,
            subnet TEXT
        )
    `)

	return err
}

// savePeerToDatabase persists a ban entry to the database for long-term storage.
// This method ensures that ban information survives node restarts by storing it
// in the persistent database. It handles both individual IP bans and subnet bans.
//
// The method uses an INSERT OR REPLACE pattern to handle updates to existing bans,
// allowing ban duration extensions without creating duplicate entries.
//
// Parameters:
//   - ctx: Context for the database operation, used for cancellation and timeout control
//   - key: Unique identifier for the ban (IP address or subnet in CIDR notation)
//   - info: Ban information including expiration time and subnet details
//
// Returns:
//   - Error if the database operation fails, nil on success
func (b *BanList) savePeerToDatabase(ctx context.Context, key string, info BanInfo) error {
	_, err := b.db.ExecContext(ctx, `
        INSERT INTO bans (key, expiration_time, subnet)
        VALUES ($1, $2, $3)
        ON CONFLICT (key) DO UPDATE
        SET expiration_time = $2, subnet = $3
    `, key, info.ExpirationTime.Format(time.RFC3339), info.Subnet.String())

	if err != nil {
		return errors.NewProcessingError("failed to save peer to database", err)
	}

	return nil
}

func (b *BanList) removePeerFromDatabase(ctx context.Context, key string) error {
	_, err := b.db.ExecContext(ctx, "DELETE FROM bans WHERE key = $1", key)
	if err != nil {
		return errors.NewProcessingError("failed to remove peer from database", err)
	}

	return nil
}

func (b *BanList) loadFromDatabase(ctx context.Context) error {
	rows, err := b.db.QueryContext(ctx, "SELECT key, expiration_time, subnet FROM bans")
	if err != nil {
		_ = b.db.Close()
		return err
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			var key string

			var expirationTimeStr string

			var expirationTime time.Time

			var subnetStr string

			err := rows.Scan(&key, &expirationTimeStr, &subnetStr)
			if err != nil {
				return err
			}

			expirationTime, err = time.Parse(time.RFC3339, expirationTimeStr)
			if err != nil {
				b.logger.Errorf("Error parsing expiration time %s: %v", expirationTimeStr, err)
				continue
			}

			_, subnet, err := net.ParseCIDR(subnetStr)
			if err != nil {
				b.logger.Errorf("Error parsing subnet %s: %v", subnetStr, err)
				continue
			}

			b.bannedPeers[key] = BanInfo{
				ExpirationTime: expirationTime,
				Subnet:         subnet,
			}
		}
	}

	return rows.Err()
}

// Subscribe creates and returns a new channel for ban events.
// Returns:
//   - chan BanEvent: Channel that will receive ban events
func (b *BanList) Subscribe() chan BanEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan BanEvent, 100) // Buffered channel to prevent blocking
	b.subscribers[ch] = struct{}{}

	return ch
}

// Unsubscribe removes a subscriber from receiving ban events.
// Parameters:
// - ch: Channel to unsubscribe
// Note: The subscriber is responsible for closing the channel after unsubscribing.
func (b *BanList) Unsubscribe(ch chan BanEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Don't close the channel here - let the subscriber close it
	// to avoid race conditions with notifySubscribers
	delete(b.subscribers, ch)
}

// notifySubscribersAsync notifies all subscribers about a ban event asynchronously.
// This is safe to call from a goroutine.
// util.SafeSend is called directly (without spawning goroutines) to avoid goroutine proliferation.
// SafeSend provides both panic recovery and non-blocking sends (select with default),
// so each subscriber's closed or full channel won't affect others or block execution.
func (b *BanList) notifySubscribersAsync(event BanEvent) {
	// Make a copy of subscribers to avoid long lock
	b.mu.RLock()
	subscribers := make([]chan BanEvent, 0, len(b.subscribers))
	for ch := range b.subscribers {
		subscribers = append(subscribers, ch)
	}
	b.mu.RUnlock()

	// Notify each subscriber without holding the lock
	// util.SafeSend handles panic recovery and non-blocking sends,
	// preventing closed/full channels from affecting other subscribers
	for _, ch := range subscribers {
		util.SafeSend(ch, event)
	}
}

// Clear removes all entries from the ban list and cleans up resources.
func (b *BanList) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Clear the in-memory maps
	b.bannedPeers = make(map[string]BanInfo)
	b.subscribers = make(map[chan BanEvent]struct{})
	// Note: We don't close subscriber channels - they'll be garbage collected
	// This avoids any race condition with goroutines trying to send to them

	// Clear the database table
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := b.db.ExecContext(ctx, "DELETE FROM bans")
	if err != nil {
		b.logger.Errorf("Failed to clear bans table: %v", err)
	}
}

func parseAddress(ipOrSubnet string) (subnet *net.IPNet, err error) {
	if strings.Contains(ipOrSubnet, "/") {
		// It's a subnet
		_, subnet, err = net.ParseCIDR(ipOrSubnet)
		if err != nil {
			return nil, errors.New(errors.ERR_INVALID_SUBNET, fmt.Sprintf("can't parse subnet: %s", ipOrSubnet))
		}

		return subnet, nil
	} else {
		// Strip port from IP address (handles both IPv4 and IPv6)
		if host, _, err := net.SplitHostPort(ipOrSubnet); err == nil {
			ipOrSubnet = host
		}
		// If SplitHostPort fails, assume it's just an IP without port
		// It's an IP address
		ip := net.ParseIP(ipOrSubnet)
		if ip == nil {
			return nil, errors.New(errors.ERR_INVALID_IP, fmt.Sprintf("can't parse IP: %s", ipOrSubnet))
		}

		var cidr string
		if ip.To4() != nil {
			cidr = fmt.Sprintf("%s/32", ipOrSubnet)
		} else {
			cidr = fmt.Sprintf("%s/128", ipOrSubnet)
		}

		_, subnet, err = net.ParseCIDR(cidr)
		if err != nil {
			return nil, errors.New(errors.ERR_INVALID_IP, fmt.Sprintf("can't create subnet from IP: %s", ipOrSubnet))
		}

		return subnet, nil
	}
}
