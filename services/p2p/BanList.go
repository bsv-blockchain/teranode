// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/usql"
)

var (
	banListInstance *BanList
	banListOnce     sync.Once
)

// BanInfo contains information about a banned peer.
type BanInfo struct {
	ExpirationTime time.Time  // Time when the ban expires
	Subnet         *net.IPNet // Subnet information for the ban
}

// BanEvent represents a ban-related event in the system.
type BanEvent struct {
	Action string     // Action type ("add" or "remove")
	IP     string     // IP address involved in the ban
	Subnet *net.IPNet // Subnet information if applicable
}

// BanListI defines the interface for peer banning functionality
type BanListI interface {
	// IsBanned checks if a peer is banned
	IsBanned(ipStr string) bool

	// Add adds an IP or subnet to the ban list
	Add(ctx context.Context, ipOrSubnet string, expirationTime time.Time) error

	// Remove removes an IP or subnet from the ban list
	Remove(ctx context.Context, ipOrSubnet string) error

	// ListBanned returns a list of all banned IP addresses and subnets
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

	go b.notifySubscribers(BanEvent{Action: "add", IP: ipOrSubnet, Subnet: subnet})

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

	go b.notifySubscribers(BanEvent{Action: "remove", IP: ipOrSubnet, Subnet: subnet})

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

	// If it contains a port, strip it off
	if strings.Contains(ipStr, ":") {
		ipStr = strings.Split(ipStr, ":")[0]
	}

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

func (b *BanList) notifySubscribers(event BanEvent) {
	// Make a copy of subscribers to avoid long lock and reduce chance of race conditions
	b.mu.RLock()
	subscribers := make([]chan BanEvent, 0, len(b.subscribers))

	for ch := range b.subscribers {
		subscribers = append(subscribers, ch)
	}
	b.mu.RUnlock()

	// Notify each subscriber without holding the lock
	for _, ch := range subscribers {
		// Use a closure to safely send to each channel
		func(ch chan BanEvent) {
			defer func() {
				if r := recover(); r != nil {
					// If we hit a closed channel or other panic, log and continue
					b.logger.Warnf("Failed to send notification: %v", r)
					// Remove the subscriber if its channel is closed
					if _, ok := r.(error); ok && strings.Contains(r.(error).Error(), "closed channel") {
						b.mu.Lock()
						delete(b.subscribers, ch)
						b.mu.Unlock()
					}
				}
			}()

			select {
			case ch <- event:
				b.logger.Debugf("Successfully notified subscriber about %s\n", event.IP)
			default:
				b.logger.Warnf("Skipped notification for %s due to full channel", event.IP)
			}
		}(ch)
	}

	b.logger.Debugf("Finished notifying subscribers for %s\n", event.IP)
}

// Clear removes all entries from the ban list and cleans up resources.
func (b *BanList) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Clear the in-memory map
	b.bannedPeers = make(map[string]BanInfo)
	for ch := range b.subscribers {
		// Drain the channel before closing
		for len(ch) > 0 {
			<-ch
		}

		close(ch)
	}

	b.subscribers = make(map[chan BanEvent]struct{})

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
		if strings.Contains(ipOrSubnet, ":") {
			// remove port
			ipOrSubnet = strings.Split(ipOrSubnet, ":")[0]
		}
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
