package banlist

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// requireIndexConsistent recomputes the lookup index from bannedPeers and
// requires the live one to match it, and requires every entry to have an
// expiry item at its current expiration. Entries without a network are
// skipped: they never match a lookup, and one test writes such an entry into
// the map directly.
func requireIndexConsistent(t *testing.T, bl *BanList) {
	t.Helper()

	bl.mu.RLock()
	defer bl.mu.RUnlock()

	wantHosts := make(map[netip.Addr]map[string]struct{})
	wantNetworks := make(map[string]struct{})
	pending := make(map[string]time.Time)

	for key, info := range bl.bannedPeers {
		if info.Subnet == nil {
			continue
		}

		pending[key] = info.ExpirationTime

		if addr, isHost := hostAddr(info.Subnet); isHost {
			if wantHosts[addr] == nil {
				wantHosts[addr] = make(map[string]struct{})
			}

			wantHosts[addr][key] = struct{}{}
		} else {
			wantNetworks[key] = struct{}{}
		}
	}

	require.Equal(t, wantHosts, bl.index.hosts, "host buckets must match the map")
	require.Equal(t, wantNetworks, bl.index.networks, "networks must match the map")

	for _, item := range bl.index.expiries {
		if at, ok := pending[item.key]; ok && at.Equal(item.at) {
			delete(pending, item.key)
		}
	}

	require.Empty(t, pending, "every entry needs an expiry item at its current expiration")
}

// isBannedByScan is the pre-index IsBanned decision, kept as the reference
// the index must agree with: a direct hit on the raw key, else any active
// network that contains the address.
func isBannedByScan(peers map[string]BanInfo, ipStr string, now time.Time) bool {
	if ipStr == "" {
		return false
	}

	if host, _, err := net.SplitHostPort(ipStr); err == nil {
		ipStr = host
	}

	if info, exists := peers[ipStr]; exists && info.ExpirationTime.After(now) {
		return true
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	for _, info := range peers {
		if info.ExpirationTime.After(now) && info.Subnet != nil && info.Subnet.Contains(ip) {
			return true
		}
	}

	return false
}

// parityQueries covers every spelling the index has to fold together: bare
// and host:port forms, IPv4-mapped IPv6, non-canonical IPv6, addresses under
// subnets, expired bans, clean addresses and unparseable input.
var parityQueries = []string{
	"192.0.2.7", "192.0.2.7:8333", "::ffff:192.0.2.7", "[::ffff:192.0.2.7]:18444",
	"198.51.100.9", "[::ffff:198.51.100.9]:8333", "198.51.100.9:1",
	"203.0.113.5", "::ffff:203.0.113.5", "203.0.113.6", "[::ffff:203.0.113.6]:8333",
	"10.1.2.3", "::ffff:10.1.2.3", "10.255.255.255",
	"2001:db8::7", "[2001:db8::7]:8333", "2001:0DB8:0000:0000:0000:0000:0000:0007",
	"2001:db8::9", "2001:DB8::0009",
	"2001:db8:1::5", "[2001:db8:1:ffff::1]:8333",
	"192.0.2.40", "198.51.100.77", "2001:db8::42", "2001:db8:2::1",
	"192.0.2.8", "2001:db9::1", "8.8.8.8", "::1",
	"", "not-an-ip", "192.0.2.7/32", "fe80::1%eth0",
}

// parityEntries is a ban list mixing single hosts in every spelling, host-sized
// CIDRs, real subnets and expired entries.
func parityEntries(now time.Time) map[string]time.Time {
	active, expired := now.Add(time.Hour), now.Add(-time.Hour)

	return map[string]time.Time{
		"192.0.2.7":                  active,
		"[::ffff:198.51.100.9]:8333": active,
		"203.0.113.5/32":             active,
		"::ffff:203.0.113.6/128":     active,
		"10.0.0.0/8":                 active,
		"2001:db8::7":                active,
		"2001:0DB8::0009":            active,
		"2001:db8:1::/48":            active,
		"192.0.2.40":                 expired,
		"198.51.100.0/24":            expired,
		"2001:db8::42":               expired,
		"2001:db8:2::/64":            expired,
	}
}

func requireMatchesScan(t *testing.T, bl *BanList, step string) {
	t.Helper()

	snapshot := bl.BannedPeers()
	now := time.Now()

	for _, query := range parityQueries {
		require.Equal(t, isBannedByScan(snapshot, query, now), bl.IsBanned(query), "%s: query=%q", step, query)
	}

	requireIndexConsistent(t, bl)
}

// The index must give the same answer as the linear scan it replaces, for
// every spelling, after Add, after a restart and after a periodic reload, and
// through host removal, exact-CIDR removal and Clear.
func TestBanList_IndexMatchesLinearScan(t *testing.T) {
	for _, mode := range []string{"add", "startup", "reload"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			bl := newTestBanList(t)

			for key, until := range parityEntries(time.Now()) {
				require.NoError(t, bl.Add(ctx, key, until))
			}

			if mode != "add" {
				bl = loadEnforcementFixture(t, bl, mode)
			}

			requireMatchesScan(t, bl, "loaded")

			for key, info := range bl.BannedPeers() {
				require.True(t, info.ExpirationTime.After(time.Now()), "a lookup must purge expired entries from memory, found %q", key)
			}

			// A host request removes every slashless spelling of the address.
			require.NoError(t, bl.Remove(ctx, "::ffff:198.51.100.9"))
			require.False(t, bl.IsBanned("198.51.100.9"))
			requireMatchesScan(t, bl, "host removed")

			// A host request leaves a host-sized CIDR in force.
			require.NoError(t, bl.Remove(ctx, "203.0.113.6"))
			require.True(t, bl.IsBanned("203.0.113.6"))
			requireMatchesScan(t, bl, "host request against a CIDR")

			// An exact CIDR request removes it.
			require.NoError(t, bl.Remove(ctx, "::ffff:203.0.113.6/128"))
			require.False(t, bl.IsBanned("203.0.113.6"))
			requireMatchesScan(t, bl, "exact CIDR removed")

			// A renewal of an expired spelling re-enters the index.
			require.NoError(t, bl.Add(ctx, "192.0.2.40", time.Now().Add(time.Hour)))
			require.True(t, bl.IsBanned("::ffff:192.0.2.40"))
			requireMatchesScan(t, bl, "renewed")

			bl.Clear()
			requireMatchesScan(t, bl, "cleared")
			require.False(t, bl.IsBanned("10.1.2.3"))
		})
	}
}

// Single-address bans, in any spelling or as a host-sized CIDR, are filed by
// address; only real subnets are left for a lookup to scan.
func TestBanList_IndexScansOnlySubnets(t *testing.T) {
	ctx := context.Background()
	bl := newTestBanList(t)
	until := time.Now().Add(time.Hour)

	for _, key := range []string{
		"192.0.2.7", "192.0.2.7:8333", "::ffff:192.0.2.7", "203.0.113.5/32",
		"::ffff:203.0.113.6/128", "2001:db8::7", "[2001:0DB8::7]:8333", "2001:db8::9/128",
		"10.0.0.0/8", "2001:db8:1::/48",
	} {
		require.NoError(t, bl.Add(ctx, key, until))
	}

	require.Equal(t, map[string]struct{}{"10.0.0.0/8": {}, "2001:db8:1::/48": {}}, bl.index.networks)
	require.Len(t, bl.index.hosts[netip.MustParseAddr("192.0.2.7")], 3, "every spelling of a mapped or bare IPv4 host shares one bucket")
	require.Len(t, bl.index.hosts[netip.MustParseAddr("2001:db8::7")], 2, "IPv6 spellings share one bucket")
	require.Contains(t, bl.index.hosts, netip.MustParseAddr("203.0.113.6"), "a mapped /128 is an IPv4 host")
	require.Contains(t, bl.index.hosts, netip.MustParseAddr("2001:db8::9"))
}

// Renewals and removals leave stale expiry items behind; the heap must not
// grow without bound under repeated renewal of one ban.
func TestBanList_ExpiryHeapStaysBounded(t *testing.T) {
	bl := New(nil, "", ulogger.TestLogger{})
	info := BanInfo{Subnet: &net.IPNet{IP: net.IPv4(192, 0, 2, 7).To4(), Mask: net.CIDRMask(32, 32)}}

	bl.mu.Lock()
	defer bl.mu.Unlock()

	for i := 0; i < 10_000; i++ {
		info.ExpirationTime = time.Now().Add(time.Duration(i+1) * time.Hour)
		bl.putLocked("192.0.2.7", info)
	}

	require.LessOrEqual(t, len(bl.index.expiries), 2*len(bl.bannedPeers)+64)
}

// A clean lookup must cost the same however many single hosts are banned.
// Entries are written through the indexing helper, not SQL, so the benchmark
// measures the lookup alone.
func BenchmarkBanList_IsBannedClean(b *testing.B) {
	for _, hosts := range []int{1_000, 10_000, 100_000} {
		for _, subnets := range []int{0, 8} {
			b.Run(fmt.Sprintf("hosts=%d/subnets=%d", hosts, subnets), func(b *testing.B) {
				bl := populatedBanList(hosts, subnets)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					if bl.IsBanned("192.0.2.1:8333") {
						b.Fatal("the probe address must be clean")
					}
				}
			})
		}
	}
}

func populatedBanList(hosts, subnets int) *BanList {
	bl := New(nil, "", ulogger.TestLogger{})
	until := time.Now().Add(24 * time.Hour)

	bl.mu.Lock()
	defer bl.mu.Unlock()

	for i := 0; i < hosts; i++ {
		ip := net.IPv4(100, byte(64+i>>16), byte(i>>8), byte(i)).To4()
		bl.putLocked(ip.String(), BanInfo{ExpirationTime: until, Subnet: &net.IPNet{IP: ip, Mask: net.CIDRMask(32, 32)}})
	}

	for i := 0; i < subnets; i++ {
		_, subnet, _ := net.ParseCIDR(fmt.Sprintf("172.16.%d.0/24", i))
		bl.putLocked(subnet.String(), BanInfo{ExpirationTime: until, Subnet: subnet})
	}

	return bl
}
