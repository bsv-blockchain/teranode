package p2p

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
	"github.com/ordishs/gocore"
)

type BanInfo struct {
	ExpirationTime time.Time
	Subnet         *net.IPNet
}

type BanList struct {
	db          *usql.DB
	engine      util.SQLEngine
	logger      ulogger.Logger
	bannedPeers map[string]BanInfo
}

func NewBanList(logger ulogger.Logger) (*BanList, error) {
	blockchainStoreURL, err, found := gocore.Config().GetURL("blockchain_store")
	if err != nil {
		return nil, errors.NewConfigurationError("failed to get blockchain_store setting", err)
	}

	if !found {
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
	}, nil
}

func (b *BanList) Init(ctx context.Context) (err error) {
	if err = b.createTables(ctx); err != nil {
		return errors.NewProcessingError("failed to create banlist tables", err)
	}

	if err = b.LoadFromDatabase(ctx); err != nil {
		return errors.NewProcessingError("failed to load banlist from database", err)
	}

	return nil
}

// map of banned ips with their expiration time
// if the ip or subnet is already banned, the expiration time will be updated
// if the ip or subnet is not banned, it will be added to the map
// ipOrSubnet can be an IP address or a subnet in CIDR notation
func (b *BanList) Add(ctx context.Context, ipOrSubnet string, expirationTime time.Time) error {
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

		key = ipOrSubnet
	}

	if err != nil {
		return err
	}

	banInfo := BanInfo{
		ExpirationTime: expirationTime,
		Subnet:         subnet,
	}

	b.bannedPeers[key] = banInfo

	return b.SavePeerToDatabase(ctx, key, banInfo)
}
func (b *BanList) Remove(ctx context.Context, ipOrSubnet string) error {
	var key string

	if strings.Contains(ipOrSubnet, "/") {
		// It's a subnet
		_, subnet, err := net.ParseCIDR(ipOrSubnet)
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

		key = ipOrSubnet
		delete(b.bannedPeers, key)
	}

	_, err := b.db.ExecContext(ctx, "DELETE FROM bans WHERE key = $1", key)

	return err
}

// IsBanned checks if a given IP address is banned
func (b *BanList) IsBanned(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		b.logger.Errorf("Invalid IP address: %s", ipStr)
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
		if key == ipStr {
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
	_, err := b.db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS bans (
            key TEXT PRIMARY KEY,
            expiration_time TIMESTAMP WITH TIME ZONE,
            subnet TEXT
        )
    `)

	return err
}

func (b *BanList) SavePeerToDatabase(ctx context.Context, key string, info BanInfo) error {
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

func (b *BanList) LoadFromDatabase(ctx context.Context) error {
	rows, err := b.db.QueryContext(ctx, "SELECT key, expiration_time, subnet FROM bans")
	if err != nil {
		_ = b.db.Close()
		return err
	}
	defer rows.Close()

	for rows.Next() {
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

	return rows.Err()
}
