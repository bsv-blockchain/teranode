package banlist

import (
	"container/heap"
	"net"
	"net/netip"
	"time"
)

// banIndex is the lookup structure IsBanned reads instead of scanning every
// ban. An entry whose network is a single address is filed under that address
// in hosts, so a lookup reads only the bucket for the queried address. Every
// other entry is a real subnet and sits in networks, which a lookup scans.
// expiries orders entries by expiration time so a lookup can drop expired
// entries without walking the rest.
//
// The index only selects candidates. IsBanned still decides each candidate
// with Subnet.Contains, so the index can make a lookup cheaper but cannot
// change its answer, provided every entry that contains an address is either
// in that address's host bucket or in networks (see hostAddr).
//
// banIndex is not safe for concurrent use; BanList guards it with mu, together
// with the bannedPeers map it indexes.
type banIndex struct {
	hosts    map[netip.Addr]map[string]struct{}
	networks map[string]struct{}
	expiries expiryHeap
}

func newBanIndex() banIndex {
	return banIndex{
		hosts:    make(map[netip.Addr]map[string]struct{}),
		networks: make(map[string]struct{}),
	}
}

// buildBanIndex indexes every entry of peers. Reload builds the index for a
// fresh snapshot with this before taking mu, so the lock is held only for the
// swap.
func buildBanIndex(peers map[string]BanInfo) banIndex {
	ix := newBanIndex()

	for key, info := range peers {
		ix.file(key, info)
		ix.expiries = append(ix.expiries, expiryItem{key: key, at: info.ExpirationTime})
	}

	heap.Init(&ix.expiries)

	return ix
}

// add indexes a new or replaced entry. The caller has already unindexed any
// previous entry under key.
func (ix *banIndex) add(key string, info BanInfo) {
	ix.file(key, info)
	heap.Push(&ix.expiries, expiryItem{key: key, at: info.ExpirationTime})
}

// file records key in its host bucket or among the networks. An entry with
// no network can never match a lookup and is not filed.
func (ix *banIndex) file(key string, info BanInfo) {
	if info.Subnet == nil {
		return
	}

	addr, isHost := hostAddr(info.Subnet)
	if !isHost {
		ix.networks[key] = struct{}{}
		return
	}

	bucket := ix.hosts[addr]
	if bucket == nil {
		bucket = make(map[string]struct{}, 1)
		ix.hosts[addr] = bucket
	}

	bucket[key] = struct{}{}
}

// remove unindexes the entry info stored under key. Its expiry item stays in
// the heap; the purge skips items whose key is gone or was renewed, and
// compact drops them in bulk.
func (ix *banIndex) remove(key string, info BanInfo) {
	if info.Subnet == nil {
		return
	}

	addr, isHost := hostAddr(info.Subnet)
	if !isHost {
		delete(ix.networks, key)
		return
	}

	bucket := ix.hosts[addr]
	delete(bucket, key)

	if len(bucket) == 0 {
		delete(ix.hosts, addr)
	}
}

// dueBy reports whether some indexed entry may have expired at now.
func (ix *banIndex) dueBy(now time.Time) bool {
	return len(ix.expiries) > 0 && !ix.expiries[0].at.After(now)
}

// hostAddr returns the single address a network covers, unmapped to IPv4 when
// it is IPv4-mapped, and whether the network covers exactly one address.
//
// net.IPNet.Contains normalises both the network and the queried address to
// four bytes when they are IPv4 or IPv4-mapped IPv6, and otherwise compares
// sixteen-byte forms. With an all-ones mask it is therefore true exactly when
// the queried address, unmapped, equals this address, unmapped; a sixteen-byte
// non-IPv4 address under a four-byte mask matches nothing at all. Any network
// that is not all-ones is treated as a subnet and scanned.
func hostAddr(n *net.IPNet) (netip.Addr, bool) {
	ones, bits := n.Mask.Size()
	if bits == 0 || ones != bits {
		return netip.Addr{}, false
	}

	addr, ok := netip.AddrFromSlice(n.IP)
	if !ok {
		return netip.Addr{}, false
	}

	return addr.Unmap(), true
}

// lookupAddr is the host-bucket key for a parsed query address.
func lookupAddr(ip net.IP) (netip.Addr, bool) {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.Addr{}, false
	}

	return addr.Unmap(), true
}

// expiryItem is one entry's expiration as it was when the entry was indexed.
type expiryItem struct {
	at  time.Time
	key string
}

// expiryHeap is a min-heap of expiryItem by time, for container/heap.
type expiryHeap []expiryItem

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *expiryHeap) Push(x any) { *h = append(*h, x.(expiryItem)) }

func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]

	return item
}
