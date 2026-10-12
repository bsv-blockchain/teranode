package banlist

import (
	"context"
	"database/sql"
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
	// operationMu orders complete administrative operations across their SQL
	// and map phases. Acquire it before mu and never while holding mu.
	operationMu sync.Mutex
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
// It holds operationMu from query to publication, so a snapshot read before a
// local Add, Remove or Clear cannot be installed after it.
func (b *BanList) reloadFromDatabase() error {
	b.operationMu.Lock()
	defer b.operationMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	newPeers, err := b.readDatabaseSnapshot(ctx)
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.bannedPeers = newPeers
	b.mu.Unlock()

	return nil
}

// readDatabaseSnapshot reads and decodes every persisted ban without locking
// or publishing; the caller holds operationMu and publishes on success only.
// Rows with unparseable expiration or key are logged and skipped, and networks
// are reconstructed from the raw key to repair legacy host rows.
func (b *BanList) readDatabaseSnapshot(ctx context.Context) (map[string]BanInfo, error) {
	rows, err := b.db.QueryContext(ctx, "SELECT key, expiration_time, subnet FROM bans")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	peers := make(map[string]BanInfo)

	for rows.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var key, expirationTimeStr, subnetStr string

		if err := rows.Scan(&key, &expirationTimeStr, &subnetStr); err != nil {
			return nil, err
		}

		expirationTime, err := time.Parse(time.RFC3339, expirationTimeStr)
		if err != nil {
			b.logger.Errorf("error parsing expiration time %s: %v", expirationTimeStr, err)
			continue
		}

		// Reconstruct from the raw key to repair legacy host networks without rewriting rows.
		subnet, err := parseAddress(key)
		if err != nil {
			b.logger.Errorf("error parsing ban key %s: %v", key, err)
			continue
		}

		peers[key] = BanInfo{
			ExpirationTime: expirationTime,
			Subnet:         subnet,
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	return peers, nil
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

	// Held across the memory-first update, event scheduling and save, so a
	// local Remove or reload cannot interleave with this Add's SQL phase.
	b.operationMu.Lock()
	defer b.operationMu.Unlock()

	b.mu.Lock()
	b.bannedPeers[ipOrSubnet] = banInfo
	b.mu.Unlock()

	event := BanEvent{Action: "add", IP: ipOrSubnet, Subnet: subnet}
	go func() {
		b.notifySubscribersAsync(event)
	}()

	return b.savePeerToDatabase(ctx, ipOrSubnet, banInfo)
}

// Remove deletes bans selected by ipOrSubnet from SQL and memory. An explicit
// CIDR request (containing "/") removes only that exact raw key. Any other
// valid request is a host request and removes every slashless raw key naming
// the same IP, whatever its port, spelling, IPv4-mapped form or expiry. Absence
// is confirmed in SQL, so a valid request matching nothing succeeds without
// events. Memory and events are published only after the transaction commits.
func (b *BanList) Remove(ctx context.Context, ipOrSubnet string) error {
	requested, err := parseAddress(ipOrSubnet)
	if err != nil {
		b.logger.Errorf("invalid IP address or subnet: %s", ipOrSubnet)
		return err
	}

	exactCIDR := strings.Contains(ipOrSubnet, "/")
	matches := b.removalMatcher(ipOrSubnet, exactCIDR, requested)

	b.operationMu.Lock()
	defer b.operationMu.Unlock()

	b.mu.RLock()
	var memoryKeys []string
	for key := range b.bannedPeers {
		if matches(key) {
			memoryKeys = append(memoryKeys, key)
		}
	}
	b.mu.RUnlock()

	// Final values come from the last callback run, which is the committed one.
	var selectedKeys, sqlDeletedKeys []string

	err = b.db.RetryTx(ctx, nil, func(tx *sql.Tx) error {
		selectedKeys, sqlDeletedKeys = nil, nil

		persistedKeys, err := selectPersistedBanKeys(ctx, tx, ipOrSubnet, exactCIDR, matches)
		if err != nil {
			return err
		}

		candidates := unionKeys(memoryKeys, persistedKeys)
		deleted := make([]string, 0, len(candidates))

		for _, key := range candidates {
			result, err := tx.ExecContext(ctx, "DELETE FROM bans WHERE key = $1", key)
			if err != nil {
				return err
			}

			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}

			if affected > 0 {
				deleted = append(deleted, key)
			}
		}

		selectedKeys, sqlDeletedKeys = candidates, deleted

		return nil
	})
	if err != nil {
		return errors.NewProcessingError("failed to remove peer from database", err)
	}

	removedKeys := make(map[string]struct{}, len(selectedKeys))
	for _, key := range sqlDeletedKeys {
		removedKeys[key] = struct{}{}
	}

	b.mu.Lock()
	for _, key := range selectedKeys {
		if _, present := b.bannedPeers[key]; present {
			delete(b.bannedPeers, key)
			removedKeys[key] = struct{}{}
		}
	}
	b.mu.Unlock()

	for key := range removedKeys {
		subnet, err := parseAddress(key)
		if err != nil {
			continue
		}

		event := BanEvent{Action: "remove", IP: key, Subnet: subnet}
		go func() {
			b.notifySubscribersAsync(event)
		}()
	}

	return nil
}

// removalMatcher reports whether a raw key belongs to a removal request. An
// explicit CIDR request matches only its exact raw key. A host request matches
// slashless keys whose parsed IP equals the requested IP; identity comes from
// the raw key, never from stored subnet text.
func (b *BanList) removalMatcher(request string, exactCIDR bool, requested *net.IPNet) func(key string) bool {
	if exactCIDR {
		return func(key string) bool { return key == request }
	}

	return func(key string) bool {
		if strings.Contains(key, "/") {
			return false
		}

		subnet, err := parseAddress(key)
		if err != nil {
			b.logger.Errorf("skipping invalid ban key %s during removal: %v", key, err)
			return false
		}

		return subnet.IP.Equal(requested.IP)
	}
}

// selectPersistedBanKeys reads matching raw keys inside tx. Host requests scan
// keys only, so expired, malformed or stale row data cannot affect selection.
// Rows are closed before the caller issues deletes; driver errors are returned
// unwrapped so RetryTx can classify them.
func selectPersistedBanKeys(ctx context.Context, tx *sql.Tx, request string, exactCIDR bool, matches func(string) bool) ([]string, error) {
	var (
		rows *sql.Rows
		err  error
	)

	if exactCIDR {
		rows, err = tx.QueryContext(ctx, "SELECT key FROM bans WHERE key = $1", request)
	} else {
		rows, err = tx.QueryContext(ctx, "SELECT key FROM bans")
	}

	if err != nil {
		return nil, err
	}

	var keys []string

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			_ = rows.Close()
			return nil, err
		}

		if matches(key) {
			keys = append(keys, key)
		}
	}

	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	return keys, nil
}

func unionKeys(groups ...[]string) []string {
	seen := make(map[string]struct{})

	var keys []string

	for _, group := range groups {
		for _, key := range group {
			if _, duplicate := seen[key]; !duplicate {
				seen[key] = struct{}{}
				keys = append(keys, key)
			}
		}
	}

	return keys
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

	// One decision time for the whole lookup, including the cleanup recheck.
	now := time.Now()

	// Direct lookup; an expired exact entry falls through so it cannot mask an
	// active covering ban.
	b.mu.RLock()
	if info, exists := b.bannedPeers[ipStr]; exists && info.ExpirationTime.After(now) {
		b.mu.RUnlock()

		return true
	}
	b.mu.RUnlock()

	ip := net.ParseIP(ipStr)
	if ip == nil {
		b.logger.Errorf("invalid IP address passed to IsBanned: %s", ipStr)
		return false
	}

	// Check every active network, whatever the spelling of its raw key.
	b.mu.RLock()
	var (
		expiredKeys []string
		isBanned    bool
	)

	for key, info := range b.bannedPeers {
		if !info.ExpirationTime.After(now) {
			expiredKeys = append(expiredKeys, key)
			continue
		}

		if info.Subnet != nil && info.Subnet.Contains(ip) {
			isBanned = true
			break
		}
	}
	b.mu.RUnlock()

	// Clean up expired entries, rechecking the current entry so a renewal made
	// after the read above survives.
	if len(expiredKeys) > 0 {
		b.mu.Lock()
		for _, key := range expiredKeys {
			if info, exists := b.bannedPeers[key]; exists && !info.ExpirationTime.After(now) {
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
	// Held across both phases so no local operation observes a half-done Clear.
	b.operationMu.Lock()
	defer b.operationMu.Unlock()

	b.mu.Lock()
	b.bannedPeers = make(map[string]BanInfo)
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

// Advisory lock ID for banlist schema creation serialization across pods.
const banlistSchemaLockID int64 = 7_265_726_098 // "tera" + "bl" in ASCII-ish

func (b *BanList) createTables(ctx context.Context) error {
	if b.engine == util.Postgres {
		return usql.WithAdvisoryLock(ctx, b.db, banlistSchemaLockID, func() error {
			return b.createTablesUnlocked(ctx)
		})
	}

	return b.createTablesUnlocked(ctx)
}

func (b *BanList) createTablesUnlocked(ctx context.Context) error {
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

// loadFromDatabase merges every persisted ban into the in-memory map. Rows are
// published only after the complete read succeeds, under operationMu.
func (b *BanList) loadFromDatabase(ctx context.Context) error {
	b.operationMu.Lock()
	defer b.operationMu.Unlock()

	peers, err := b.readDatabaseSnapshot(ctx)
	if err != nil {
		return err
	}

	b.mu.Lock()
	for key, info := range peers {
		b.bannedPeers[key] = info
	}
	b.mu.Unlock()

	return nil
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
