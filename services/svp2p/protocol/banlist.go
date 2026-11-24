package protocol

import (
	"encoding/json"
	"net"
	"os"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

var ErrNotBanned = errors.New(errors.ERR_NOT_FOUND, "svp2p: host not banned")

type BanEntry struct {
	Host  string    `json:"host"`
	Until time.Time `json:"until"`
}

// BanList holds manual bans by IP or CIDR subnet, persisted as JSON when a
// path is configured. Automatic misbehavior-driven bans arrive in a later
// phase; this is the peer_api Ban/Unban surface.
type BanList struct {
	mu      sync.Mutex
	path    string
	entries map[string]banEntry
}

type banEntry struct {
	until time.Time
	ipnet *net.IPNet // nil for single-IP entries
	ip    net.IP     // nil for CIDR entries
}

func NewBanList(path string) (*BanList, error) {
	bl := &BanList{path: path, entries: make(map[string]banEntry)}

	if path == "" {
		return bl, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return bl, nil
		}

		return nil, errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot read ban list", err)
	}

	var stored []BanEntry
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot parse ban list", err)
	}

	for _, e := range stored {
		entry, err := parseBanHost(e.Host)
		if err != nil {
			continue // drop unparseable entries rather than refuse to start
		}

		entry.until = e.Until
		bl.entries[e.Host] = entry
	}

	return bl, nil
}

func parseBanHost(host string) (banEntry, error) {
	if _, ipnet, err := net.ParseCIDR(host); err == nil {
		return banEntry{ipnet: ipnet}, nil
	}

	if ip := net.ParseIP(host); ip != nil {
		return banEntry{ip: ip}, nil
	}

	return banEntry{}, errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: invalid ban host %q", host)
}

func (b *BanList) Add(host string, until time.Time) error {
	entry, err := parseBanHost(host)
	if err != nil {
		return err
	}

	entry.until = until

	b.mu.Lock()
	defer b.mu.Unlock()

	b.entries[host] = entry

	return b.persistLocked()
}

func (b *BanList) Remove(host string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.entries[host]; !ok {
		return ErrNotBanned
	}

	delete(b.entries, host)

	return b.persistLocked()
}

func (b *BanList) IsBanned(ipPort string) bool {
	host, _, err := net.SplitHostPort(ipPort)
	if err != nil {
		host = ipPort // tolerate a bare IP without port
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	for key, e := range b.entries {
		if now.After(e.until) {
			delete(b.entries, key)
			continue
		}

		if e.ipnet != nil && e.ipnet.Contains(ip) {
			return true
		}

		if e.ip != nil && e.ip.Equal(ip) {
			return true
		}
	}

	return false
}

func (b *BanList) List() []BanEntry {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]BanEntry, 0, len(b.entries))
	for host, e := range b.entries {
		out = append(out, BanEntry{Host: host, Until: e.until})
	}

	return out
}

func (b *BanList) Clear() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.entries = make(map[string]banEntry)

	return b.persistLocked()
}

func (b *BanList) persistLocked() error {
	if b.path == "" {
		return nil
	}

	stored := make([]BanEntry, 0, len(b.entries))
	for host, e := range b.entries {
		stored = append(stored, BanEntry{Host: host, Until: e.until})
	}

	data, err := json.Marshal(stored)
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot encode ban list", err)
	}

	if err := os.WriteFile(b.path, data, 0o600); err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "svp2p: cannot write ban list", err)
	}

	return nil
}
