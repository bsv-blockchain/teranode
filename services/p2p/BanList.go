package p2p

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
)

var (
	banListInstance *BanList
	banListOnce     sync.Once
)

type BanInfo struct {
	ExpirationTime time.Time
	Subnet         *net.IPNet
}

type BanEvent struct {
	Action string // "add" or "remove"
	IP     string
	Subnet *net.IPNet
}

type BanList struct {
	db          *usql.DB
	engine      util.SQLEngine
	logger      ulogger.Logger
	bannedPeers map[string]BanInfo
	subscribers map[chan BanEvent]struct{}

	mu sync.RWMutex
}

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

	store, err := blockchain.NewStore(logger, blockchainStoreURL)
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

// map of banned ips with their expiration time
// if the ip or subnet is already banned, the expiration time will be updated
// if the ip or subnet is not banned, it will be added to the map
// ipOrSubnet can be an IP address or a subnet in CIDR notation
func (b *BanList) Add(ctx context.Context, ipOrSubnet string, expirationTime time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var subnet *net.IPNet

	var err error

	var key string

	if strings.Contains(ipOrSubnet, "/") {
		_, subnet, err = net.ParseCIDR(ipOrSubnet)
		if err != nil {
			b.logger.Errorf("error parsing ip or subnet: %v", err)

			return err
		}

		key = subnet.String()
	} else {
		ip := net.ParseIP(ipOrSubnet)
		if ip == nil {
			b.logger.Errorf("Can't parse IP or subnet: %s", ipOrSubnet)

			return err
		}

		if ip.To4() != nil {
			_, subnet, err = net.ParseCIDR(fmt.Sprintf("%s/32", ipOrSubnet))
		} else {
			_, subnet, err = net.ParseCIDR(fmt.Sprintf("%s/128", ipOrSubnet))
		}

		if err != nil {
			b.logger.Errorf("error creating subnet for IP: %v", err)
			return err
		}

		key = ipOrSubnet
	}

	banInfo := BanInfo{
		ExpirationTime: expirationTime,
		Subnet:         subnet,
	}

	go b.notifySubscribers(BanEvent{Action: "add", IP: key, Subnet: subnet})

	b.bannedPeers[key] = banInfo

	return b.savePeerToDatabase(ctx, key, banInfo)
}
func (b *BanList) Remove(ctx context.Context, ipOrSubnet string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var key string

	var subnet *net.IPNet

	var err error

	if strings.Contains(ipOrSubnet, "/") {
		// It's a subnet
		_, subnet, err = net.ParseCIDR(ipOrSubnet)
		if err != nil {
			return errors.New(errors.ERR_INVALID_SUBNET, fmt.Sprintf("can't parse subnet: %s", ipOrSubnet))
		}

		key = subnet.String()
		delete(b.bannedPeers, key)
	} else {
		// It's an IP address
		ip := net.ParseIP(ipOrSubnet)
		if ip == nil {
			return errors.New(errors.ERR_INVALID_IP, fmt.Sprintf("can't parse IP: %s", ipOrSubnet))
		}

		if ip.To4() != nil {
			_, subnet, _ = net.ParseCIDR(fmt.Sprintf("%s/32", ipOrSubnet))
		} else {
			_, subnet, _ = net.ParseCIDR(fmt.Sprintf("%s/128", ipOrSubnet))
		}

		key = ipOrSubnet
		delete(b.bannedPeers, key)
	}

	_, err = b.db.ExecContext(ctx, "DELETE FROM bans WHERE key = $1", key)

	go b.notifySubscribers(BanEvent{Action: "remove", IP: key, Subnet: subnet})

	return err
}

// IsBanned checks if a given IP address is banned
func (b *BanList) IsBanned(ipStr string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// ipStr may contain a port, so we need to split it
	host, _, err := net.SplitHostPort(ipStr)
	if err != nil {
		// if SplitHostPort fails, it means there's no port, so use the original string
		host = ipStr
	}

	ip := net.ParseIP(host)
	if ip == nil {
		b.logger.Errorf("Invalid IP address: %s", host)
		return false
	}

	now := time.Now()

	for key, banInfo := range b.bannedPeers {
		// First, check if the ban has expired
		if now.After(banInfo.ExpirationTime) {
			delete(b.bannedPeers, key)
			continue
		}

		// Check if it's a direct IP match
		if key == host {
			return true
		}

		// Check if the IP is in the subnet
		if banInfo.Subnet.Contains(ip) {
			return true
		}
	}

	return false
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
    `, key, info.ExpirationTime, info.Subnet.String())

	if err != nil {
		return errors.NewProcessingError("failed to save peer to database", err)
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

			var expirationTime time.Time

			var subnetStr string

			err := rows.Scan(&key, &expirationTime, &subnetStr)
			if err != nil {
				return err
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

func (b *BanList) Subscribe() chan BanEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan BanEvent, 100) // Buffered channel to prevent blocking
	b.subscribers[ch] = struct{}{}

	return ch
}

func (b *BanList) Unsubscribe(ch chan BanEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subscribers, ch)
	close(ch)
}

func (b *BanList) notifySubscribers(event BanEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.subscribers {
		select {
		case ch <- event:
			b.logger.Debugf("Successfully notified subscriber about %s\n", event.IP)
		default:
			b.logger.Warnf("Skipped notification for %s due to full channel", event.IP)
		}
	}

	b.logger.Debugf("Finished notifying subscribers for %s\n", event.IP)
}

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
