package banlist

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

// BanList manages the list of banned IPs/subnets with database persistence.
type BanList struct {
	db          *usql.DB
	engine      util.SQLEngine
	logger      ulogger.Logger
	bannedPeers map[string]BanInfo
	subscribers map[chan BanEvent]struct{}
	mu          sync.RWMutex
	stopCh      chan struct{}
}

// New creates a new BanList instance backed by the given database.
func New(db *usql.DB, engine util.SQLEngine, logger ulogger.Logger) *BanList {
	return &BanList{
		db:          db,
		engine:      engine,
		logger:      logger,
		bannedPeers: make(map[string]BanInfo),
		subscribers: make(map[chan BanEvent]struct{}),
		stopCh:      make(chan struct{}),
	}
}

// NewFromSettings creates a BanList from application settings by opening the
// blockchain store and extracting the DB handle and engine type.
func NewFromSettings(logger ulogger.Logger, tSettings *settings.Settings) (*BanList, error) {
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

	return New(store.GetDB(), engine, logger), nil
}

func (b *BanList) Init(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := b.createTables(ctx); err != nil {
		return errors.NewProcessingError("failed to create banlist tables", err)
	}

	if err := b.loadFromDatabase(ctx); err != nil {
		return errors.NewProcessingError("failed to load banlist from database", err)
	}

	return nil
}

// StartPeriodicReload starts a background goroutine that reloads the ban list
// from the database at the given interval, enabling cross-service consistency.
func (b *BanList) StartPeriodicReload(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-b.stopCh:
				return
			case <-ticker.C:
				if err := b.reloadFromDatabase(); err != nil {
					b.logger.Errorf("failed to reload ban list from database: %v", err)
				}
			}
		}
	}()
}

// Stop shuts down the periodic reload goroutine.
func (b *BanList) Stop() {
	select {
	case <-b.stopCh:
		// already closed
	default:
		close(b.stopCh)
	}
}

// reloadFromDatabase re-reads the bans table and replaces the in-memory map.
func (b *BanList) reloadFromDatabase() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rows, err := b.db.QueryContext(ctx, "SELECT key, expiration_time, subnet FROM bans")
	if err != nil {
		return err
	}
	defer rows.Close()

	newPeers := make(map[string]BanInfo)
	for rows.Next() {
		var key, expirationTimeStr, subnetStr string

		if err := rows.Scan(&key, &expirationTimeStr, &subnetStr); err != nil {
			return err
		}

		expirationTime, err := time.Parse(time.RFC3339, expirationTimeStr)
		if err != nil {
			b.logger.Errorf("error parsing expiration time %s: %v", expirationTimeStr, err)
			continue
		}

		_, subnet, err := net.ParseCIDR(subnetStr)
		if err != nil {
			b.logger.Errorf("error parsing subnet %s: %v", subnetStr, err)
			continue
		}

		newPeers[key] = BanInfo{
			ExpirationTime: expirationTime,
			Subnet:         subnet,
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	b.mu.Lock()
	b.bannedPeers = newPeers
	b.mu.Unlock()

	return nil
}

func (b *BanList) Add(ctx context.Context, ipOrSubnet string, expirationTime time.Time) error {
	subnet, err := parseAddress(ipOrSubnet)
	if err != nil {
		b.logger.Errorf("error parsing ip or subnet: %v", err)
		return err
	}

	banInfo := BanInfo{
		ExpirationTime: expirationTime,
		Subnet:         subnet,
	}

	b.mu.Lock()
	b.bannedPeers[ipOrSubnet] = banInfo
	b.mu.Unlock()

	event := BanEvent{Action: "add", IP: ipOrSubnet, Subnet: subnet}
	go func() {
		b.notifySubscribersAsync(event)
	}()

	return b.savePeerToDatabase(ctx, ipOrSubnet, banInfo)
}

func (b *BanList) Remove(ctx context.Context, ipOrSubnet string) error {
	subnet, err := parseAddress(ipOrSubnet)
	if err != nil {
		b.logger.Errorf("invalid IP address or subnet: %s", ipOrSubnet)
		return err
	}

	b.mu.Lock()
	if _, ok := b.bannedPeers[ipOrSubnet]; !ok {
		b.mu.Unlock()
		return nil
	}
	delete(b.bannedPeers, ipOrSubnet)
	b.mu.Unlock()

	event := BanEvent{Action: "remove", IP: ipOrSubnet, Subnet: subnet}
	go func() {
		b.notifySubscribersAsync(event)
	}()

	return b.removePeerFromDatabase(ctx, ipOrSubnet)
}

func (b *BanList) IsBanned(ipStr string) bool {
	if ipStr == "" {
		return false
	}

	// Strip port from IP address
	host, _, err := net.SplitHostPort(ipStr)
	if err == nil {
		ipStr = host
	}

	// Direct lookup
	b.mu.RLock()
	if info, exists := b.bannedPeers[ipStr]; exists {
		isBanned := info.ExpirationTime.After(time.Now())
		b.mu.RUnlock()

		return isBanned
	}
	b.mu.RUnlock()

	ip := net.ParseIP(ipStr)
	if ip == nil {
		b.logger.Errorf("invalid IP address passed to IsBanned: %s", ipStr)
		return false
	}

	// Check subnets
	b.mu.RLock()
	var (
		expiredKeys []string
		isBanned    bool
	)

	for key, info := range b.bannedPeers {
		if !info.ExpirationTime.After(time.Now()) {
			expiredKeys = append(expiredKeys, key)
			continue
		}

		if !strings.Contains(key, "/") {
			continue
		}

		if info.Subnet != nil && info.Subnet.Contains(ip) {
			isBanned = true
			break
		}
	}
	b.mu.RUnlock()

	// Clean up expired entries
	if len(expiredKeys) > 0 {
		b.mu.Lock()
		for _, key := range expiredKeys {
			if info, exists := b.bannedPeers[key]; exists && !info.ExpirationTime.After(time.Now()) {
				delete(b.bannedPeers, key)
			}
		}
		b.mu.Unlock()
	}

	return isBanned
}

func (b *BanList) ListBanned() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	banned := make([]string, 0, len(b.bannedPeers))
	for key := range b.bannedPeers {
		banned = append(banned, key)
	}

	return banned
}

func (b *BanList) Subscribe() chan BanEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan BanEvent, 100)
	b.subscribers[ch] = struct{}{}

	return ch
}

func (b *BanList) Unsubscribe(ch chan BanEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.subscribers, ch)
}

func (b *BanList) Clear() {
	b.mu.Lock()
	b.bannedPeers = make(map[string]BanInfo)
	b.subscribers = make(map[chan BanEvent]struct{})
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := b.db.ExecContext(ctx, "DELETE FROM bans")
	if err != nil {
		b.logger.Errorf("failed to clear bans table: %v", err)
	}
}

// BannedPeers returns the internal banned peers map for testing.
func (b *BanList) BannedPeers() map[string]BanInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cp := make(map[string]BanInfo, len(b.bannedPeers))
	for k, v := range b.bannedPeers {
		cp[k] = v
	}
	return cp
}

func (b *BanList) notifySubscribersAsync(event BanEvent) {
	b.mu.RLock()
	subscribers := make([]chan BanEvent, 0, len(b.subscribers))
	for ch := range b.subscribers {
		subscribers = append(subscribers, ch)
	}
	b.mu.RUnlock()

	for _, ch := range subscribers {
		util.SafeSend(ch, event)
	}
}

func (b *BanList) createTables(ctx context.Context) error {
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
				b.logger.Errorf("error parsing expiration time %s: %v", expirationTimeStr, err)
				continue
			}

			_, subnet, err := net.ParseCIDR(subnetStr)
			if err != nil {
				b.logger.Errorf("error parsing subnet %s: %v", subnetStr, err)
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

// LoadFromDatabase is exported for testing.
func (b *BanList) LoadFromDatabase(ctx context.Context) error {
	return b.loadFromDatabase(ctx)
}

// FormatBanListInfo returns a formatted string with ban list statistics.
func FormatBanListInfo(banList Interface) string {
	banned := banList.ListBanned()
	return fmt.Sprintf("ban list active with %d entries", len(banned))
}
