package banlist

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// enforcementCase pairs a raw ban key with literal expectations. network is the
// membership network the key must produce; stored is the subnet text baseline
// Add persisted for the key (legacy row content, used by persistence tests).
type enforcementCase struct {
	name         string
	key          string
	explicitCIDR bool
	network      string
	stored       string
	banned       []string
	allowed      []string
}

// enforcementCases covers every supported address form. Host cases share
// equivalent-spelling queries and neighbour/cross-family negative controls so a
// broadened host network (such as legacy mapped "::/32") is caught. Explicit
// CIDR cases keep net.ParseCIDR prefix semantics, including mapped spellings.
func enforcementCases() []enforcementCase {
	ipv4HostBanned := []string{
		"192.0.2.7", "192.0.2.7:8333", "192.0.2.7:18444",
		"::ffff:192.0.2.7", "::FFFF:192.0.2.7", "::ffff:c000:207",
		"0:0:0:0:0:ffff:c000:0207", "[::ffff:192.0.2.7]:8333",
	}
	ipv4HostAllowed := []string{
		"192.0.2.8", "192.0.2.8:8333", "::ffff:192.0.2.8", "[::ffff:192.0.2.8]:8333",
		"::", "::1", "::2", "[::1]:8333", "2001:db8::7",
	}
	ipv6HostBanned := []string{
		"2001:db8::7", "2001:DB8::7", "2001:0db8:0000:0000:0000:0000:0000:0007",
		"[2001:db8::7]:8333", "[2001:0DB8::0007]:18444",
	}
	ipv6HostAllowed := []string{
		"2001:db8::8", "[2001:db8::8]:8333", "2001:db8::", "2001:db8::7:0", "::7",
		"192.0.2.7", "::ffff:192.0.2.7",
	}

	return []enforcementCase{
		{
			name: "ipv4", key: "192.0.2.7",
			network: "192.0.2.7/32", stored: "192.0.2.7/32",
			banned: ipv4HostBanned, allowed: ipv4HostAllowed,
		},
		{
			name: "ipv4_port", key: "192.0.2.7:8333",
			network: "192.0.2.7/32", stored: "192.0.2.7/32",
			banned: ipv4HostBanned, allowed: ipv4HostAllowed,
		},
		{
			name: "expanded_uppercase_ipv6", key: "2001:0DB8:0000:0000:0000:0000:0000:0007",
			network: "2001:db8::7/128", stored: "2001:db8::7/128",
			banned: ipv6HostBanned, allowed: ipv6HostAllowed,
		},
		{
			name: "ipv6_port", key: "[2001:db8::7]:8333",
			network: "2001:db8::7/128", stored: "2001:db8::7/128",
			banned: ipv6HostBanned, allowed: ipv6HostAllowed,
		},
		{
			name: "mapped_host", key: "::ffff:192.0.2.7",
			network: "192.0.2.7/32", stored: "::/32",
			banned: ipv4HostBanned, allowed: ipv4HostAllowed,
		},
		{
			name: "mapped_port", key: "[::ffff:192.0.2.7]:8333",
			network: "192.0.2.7/32", stored: "::/32",
			banned: ipv4HostBanned, allowed: ipv4HostAllowed,
		},
		{
			name: "noncanonical_ipv4_cidr", key: "192.0.2.7/24", explicitCIDR: true,
			network: "192.0.2.0/24", stored: "192.0.2.0/24",
			banned:  []string{"192.0.2.0", "192.0.2.7", "192.0.2.255:8333", "::ffff:192.0.2.9"},
			allowed: []string{"192.0.3.7", "192.0.1.255", "2001:db8::7", "::1"},
		},
		{
			name: "noncanonical_ipv6_cidr", key: "2001:0DB8:0000:0000:0000:0000:0000:0007/64", explicitCIDR: true,
			network: "2001:db8::/64", stored: "2001:db8::/64",
			banned:  []string{"2001:db8::7", "2001:db8::ffff:ffff:ffff:ffff", "[2001:db8::8]:8333"},
			allowed: []string{"2001:db8:0:1::7", "192.0.2.7", "::ffff:192.0.2.7"},
		},
		{
			// An explicit IPv6 /32 spelled with a mapped address is an intentional
			// IPv6 prefix: it covers ::1 and ::2 but, per net.IPNet.Contains family
			// handling, not the IPv4 host whose spelling produced it.
			name: "explicit_mapped_cidr_32", key: "::ffff:192.0.2.7/32", explicitCIDR: true,
			network: "::/32", stored: "::/32",
			banned:  []string{"::", "::1", "::2", "[::1]:8333"},
			allowed: []string{"192.0.2.7", "::ffff:192.0.2.7", "192.0.2.8", "2001:db8::7"},
		},
		{
			// A prefix inside ::ffff:0:0/96 is read by net.ParseCIDR as IPv4
			// coverage; that interpretation must be preserved.
			name: "explicit_mapped_cidr_120", key: "::ffff:192.0.2.0/120", explicitCIDR: true,
			network: "192.0.2.0/24", stored: "192.0.2.0/24",
			banned:  []string{"192.0.2.7", "::ffff:192.0.2.9", "192.0.2.255:8333"},
			allowed: []string{"192.0.3.7", "::ffff:192.0.3.7", "::ffff:0:0", "::1"},
		},
	}
}

// queryIP converts a lookup string to an IP with the standard library only, so
// parser expectations never depend on the code under test.
func queryIP(t *testing.T, query string) net.IP {
	t.Helper()

	host := query
	if splitHost, _, err := net.SplitHostPort(query); err == nil {
		host = splitHost
	}

	ip := net.ParseIP(host)
	require.NotNil(t, ip, "query %q must be a valid IP", query)

	return ip
}

func requireEnforcement(t *testing.T, bl *BanList, tc enforcementCase) {
	t.Helper()

	for _, query := range tc.banned {
		require.True(t, bl.IsBanned(query), "key=%q query=%q must be banned", tc.key, query)
	}

	for _, query := range tc.allowed {
		require.False(t, bl.IsBanned(query), "key=%q query=%q must be allowed", tc.key, query)
	}
}

// requireStoredRow asserts the persisted row. Expiration is compared as an
// instant at RFC3339 (second) precision because the driver may re-render
// TIMESTAMP text; the loaders parse it with time.RFC3339 as well.
func requireStoredRow(t *testing.T, bl *BanList, key string, expiration time.Time, subnet string) {
	t.Helper()

	var storedExpiration, storedSubnet string

	require.NoError(t, bl.db.QueryRowContext(context.Background(),
		"SELECT expiration_time, subnet FROM bans WHERE key = $1", key).
		Scan(&storedExpiration, &storedSubnet), "key=%q", key)

	parsedExpiration, err := time.Parse(time.RFC3339, storedExpiration)
	require.NoError(t, err, "key=%q stored expiration %q", key, storedExpiration)
	require.True(t, parsedExpiration.Equal(expiration.Truncate(time.Second)),
		"key=%q stored expiration %s, want %s", key, parsedExpiration, expiration)
	require.Equal(t, subnet, storedSubnet, "key=%q", key)
}

// loadEnforcementFixture exercises one persistence path: "startup" builds a new
// instance over the same fixture-owned database and runs Init; "reload" calls
// the periodic reload body directly, avoiding timers.
func loadEnforcementFixture(t *testing.T, bl *BanList, mode string) *BanList {
	t.Helper()

	switch mode {
	case "startup":
		fresh := New(bl.db, bl.engine, bl.logger)
		require.NoError(t, fresh.Init(context.Background()))

		return fresh
	case "reload":
		require.NoError(t, bl.reloadFromDatabase())

		return bl
	default:
		t.Fatalf("unknown load mode %q", mode)

		return nil
	}
}

// TestParseAddress_EnforcementNetworks catches host networks built from the
// original spelling (mapped hosts becoming IPv6 /32 prefixes), wrong IP byte
// representation, and any reinterpretation of explicit CIDR input.
func TestParseAddress_EnforcementNetworks(t *testing.T) {
	for _, tc := range enforcementCases() {
		t.Run(tc.name, func(t *testing.T) {
			subnet, err := parseAddress(tc.key)
			require.NoError(t, err)
			require.NotNil(t, subnet)
			require.Equal(t, tc.network, subnet.String())

			// Host bans must equal the literal single-host network (four-byte IPv4
			// with a 32-bit mask, or a 128-bit IPv6 mask); explicit CIDRs must equal
			// the standard library's reading of the raw key.
			oracleText := tc.network
			if tc.explicitCIDR {
				oracleText = tc.key
			}

			_, expected, err := net.ParseCIDR(oracleText)
			require.NoError(t, err)
			require.Equal(t, expected, subnet, "IP bytes and mask must match %q", oracleText)

			for _, query := range tc.banned {
				require.True(t, subnet.Contains(queryIP(t, query)), "key=%q query=%q", tc.key, query)
			}

			for _, query := range tc.allowed {
				require.False(t, subnet.Contains(queryIP(t, query)), "key=%q query=%q", tc.key, query)
			}
		})
	}

	t.Run("legacy_mapped_host_construction", func(t *testing.T) {
		// Baseline host construction appended "/32" to the mapped spelling. This
		// records why the mapped negative controls exist: the result is an IPv6
		// /32 covering unrelated hosts, not the intended IPv4 host.
		_, legacy, err := net.ParseCIDR("::ffff:192.0.2.7/32")
		require.NoError(t, err)
		require.Equal(t, "::/32", legacy.String())
		require.True(t, legacy.Contains(net.ParseIP("::1")))
		require.True(t, legacy.Contains(net.ParseIP("::2")))
		require.False(t, legacy.Contains(net.ParseIP("192.0.2.7")))
	})
}

// TestBanList_EnforcementAddressForms catches slashless keys (ports, alternate
// IPv6 spellings, mapped hosts) being ignored by membership matching, host
// over-banning, lost raw keys, and loss of exact raw CIDR queries, for Add and
// for both persistence paths over rows written by the current Add.
func TestBanList_EnforcementAddressForms(t *testing.T) {
	for _, tc := range enforcementCases() {
		t.Run(tc.name, func(t *testing.T) {
			bl := newTestBanList(t)
			expiration := time.Now().Add(time.Hour)

			require.NoError(t, bl.Add(context.Background(), tc.key, expiration))
			require.ElementsMatch(t, []string{tc.key}, bl.ListBanned())
			requireEnforcement(t, bl, tc)
			require.True(t, bl.IsBanned(tc.key), "raw key query must stay banned")
			// New writes keep the raw key and serialize the corrected network.
			requireStoredRow(t, bl, tc.key, expiration, tc.network)

			for _, mode := range []string{"startup", "reload"} {
				t.Run(mode, func(t *testing.T) {
					loaded := loadEnforcementFixture(t, bl, mode)
					require.ElementsMatch(t, []string{tc.key}, loaded.ListBanned())
					requireEnforcement(t, loaded, tc)
					require.True(t, loaded.IsBanned(tc.key), "raw key query must stay banned")
				})
			}
		})
	}
}

type expiryEntry struct {
	key     string
	network string
	active  bool
}

// TestBanList_EnforcementExpirationCoverage catches an expired exact entry
// suppressing active covering bans, expired entries enforcing, and any SQL
// expiry deletion, across both families, both insertion orders, Add, startup
// and reload. Exact expiry equality is a review obligation (strict After): no
// clock hook exists to force the internal decision instant.
func TestBanList_EnforcementExpirationCoverage(t *testing.T) {
	families := []struct {
		name                       string
		hostKey, hostNetwork       string
		subnetKey, subnetNetwork   string
		overlapKey, overlapNetwork string
		unrelated                  string
		queries                    []string
	}{
		{
			name:    "ipv4",
			hostKey: "192.0.2.7", hostNetwork: "192.0.2.7/32",
			subnetKey: "192.0.2.0/24", subnetNetwork: "192.0.2.0/24",
			overlapKey: "192.0.0.0/16", overlapNetwork: "192.0.0.0/16",
			unrelated: "198.51.100.7",
			queries:   []string{"192.0.2.7", "192.0.2.7:8333", "::ffff:192.0.2.7", "[::ffff:192.0.2.7]:18444"},
		},
		{
			name:    "ipv6",
			hostKey: "2001:db8::7", hostNetwork: "2001:db8::7/128",
			subnetKey: "2001:db8::/64", subnetNetwork: "2001:db8::/64",
			overlapKey: "2001:db8::/32", overlapNetwork: "2001:db8::/32",
			unrelated: "2001:db9::7",
			queries:   []string{"2001:db8::7", "2001:0DB8:0000:0000:0000:0000:0000:0007", "[2001:db8::7]:8333"},
		},
	}

	scenarios := []struct {
		name                                                 string
		hostActive, subnetActive, withOverlap, overlapActive bool
		want                                                 bool
	}{
		{name: "expired_host_active_subnet", subnetActive: true, want: true},
		{name: "active_host_expired_subnet", hostActive: true, want: true},
		{name: "expired_only", want: false},
		{name: "expired_host_subnet_active_overlap", withOverlap: true, overlapActive: true, want: true},
		{name: "expired_host_overlap_active_subnet", subnetActive: true, withOverlap: true, want: true},
	}

	for _, family := range families {
		for _, scenario := range scenarios {
			for _, mode := range []string{"add", "startup", "reload"} {
				for _, order := range []string{"host_first", "subnet_first"} {
					t.Run(family.name+"/"+scenario.name+"/"+mode+"/"+order, func(t *testing.T) {
						now := time.Now()
						expirationFor := func(active bool) time.Time {
							if active {
								return now.Add(time.Hour)
							}

							return now.Add(-time.Hour)
						}

						entries := []expiryEntry{
							{key: family.hostKey, network: family.hostNetwork, active: scenario.hostActive},
							{key: family.subnetKey, network: family.subnetNetwork, active: scenario.subnetActive},
						}
						if scenario.withOverlap {
							entries = append(entries, expiryEntry{
								key: family.overlapKey, network: family.overlapNetwork, active: scenario.overlapActive,
							})
						}

						if order == "subnet_first" {
							slices.Reverse(entries)
						}

						bl := newTestBanList(t)
						for _, entry := range entries {
							require.NoError(t, bl.Add(context.Background(), entry.key, expirationFor(entry.active)))
						}

						if mode != "add" {
							bl = loadEnforcementFixture(t, bl, mode)
						}

						// Inventory may retain expired entries; not an enforcement assertion.
						require.Len(t, bl.ListBanned(), len(entries))

						for _, query := range family.queries {
							require.Equal(t, scenario.want, bl.IsBanned(query), "query=%q", query)
						}

						// An unrelated lookup traverses every entry, so existing memory-only
						// cleanup removes each expired key; SQL must still retain it.
						require.False(t, bl.IsBanned(family.unrelated))

						memory := bl.BannedPeers()
						for _, entry := range entries {
							_, present := memory[entry.key]
							require.Equal(t, entry.active, present, "in-memory key=%q", entry.key)
							requireStoredRow(t, bl, entry.key, expirationFor(entry.active), entry.network)
						}

						// Periodic reload brings expired rows back; they still must neither
						// enforce nor mask active coverage.
						require.NoError(t, bl.reloadFromDatabase())
						require.Len(t, bl.ListBanned(), len(entries))

						for _, query := range family.queries {
							require.Equal(t, scenario.want, bl.IsBanned(query), "after reload query=%q", query)
						}

						require.False(t, bl.IsBanned(family.unrelated))
					})
				}
			}
		}
	}
}

// TestBanList_EnforcementPersistedRows catches loaders trusting serialized
// subnets: legacy mapped "::/32" rows, malformed or stale subnets for valid
// keys, and invalid keys inheriting broad stored coverage. Rows are inserted
// directly with baseline-written subnet text and must remain unchanged in SQL.
func TestBanList_EnforcementPersistedRows(t *testing.T) {
	cases := append(enforcementCases(),
		enforcementCase{
			name: "malformed_subnet", key: "192.0.2.7",
			network: "192.0.2.7/32", stored: "broken-subnet",
			banned:  []string{"192.0.2.7", "::ffff:192.0.2.7"},
			allowed: []string{"192.0.2.8", "::1"},
		},
		enforcementCase{
			name: "stale_host_subnet", key: "192.0.2.7",
			network: "192.0.2.7/32", stored: "198.51.100.0/24",
			banned:  []string{"192.0.2.7"},
			allowed: []string{"192.0.2.8", "198.51.100.7"},
		},
		enforcementCase{
			name: "stale_broad_cidr_subnet", key: "192.0.2.0/24", explicitCIDR: true,
			network: "192.0.2.0/24", stored: "0.0.0.0/0",
			banned:  []string{"192.0.2.7"},
			allowed: []string{"198.51.100.7", "10.0.0.1"},
		},
		enforcementCase{
			// network is empty: the row must be skipped, never falling back to
			// the broad serialized subnet.
			name: "invalid_key", key: "not-an-ip/invalid", stored: "::/0",
			allowed: []string{"not-an-ip/invalid", "::1", "::2", "2001:db8::7", "192.0.2.7"},
		},
	)

	for _, mode := range []string{"startup", "reload"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				bl := newTestBanList(t)
				expiration := time.Now().Add(time.Hour)

				_, err := bl.db.ExecContext(context.Background(),
					"INSERT INTO bans (key, expiration_time, subnet) VALUES ($1, $2, $3)",
					tc.key, expiration.Format(time.RFC3339), tc.stored)
				require.NoError(t, err)

				loaded := loadEnforcementFixture(t, bl, mode)

				if tc.network == "" {
					require.Empty(t, loaded.ListBanned(), "invalid key must be skipped")
				} else {
					require.ElementsMatch(t, []string{tc.key}, loaded.ListBanned())

					info, exists := loaded.BannedPeers()[tc.key]
					require.True(t, exists)
					require.NotNil(t, info.Subnet)
					require.Equal(t, tc.network, info.Subnet.String())
					require.True(t, loaded.IsBanned(tc.key), "raw key query must stay banned")
				}

				requireEnforcement(t, loaded, tc)
				requireStoredRow(t, bl, tc.key, expiration, tc.stored)
			})
		}
	}
}

// requireHostEvent drains one event and asserts the raw key plus a host-only
// network for 192.0.2.7. The timeout is a failure guard, not synchronization.
func requireHostEvent(t *testing.T, events chan BanEvent, action, key string) {
	t.Helper()

	select {
	case event := <-events:
		require.Equal(t, action, event.Action)
		require.Equal(t, key, event.IP, "event IP keeps the raw key")
		require.NotNil(t, event.Subnet)
		require.Equal(t, "192.0.2.7/32", event.Subnet.String())

		for _, host := range []string{"192.0.2.7", "::ffff:192.0.2.7"} {
			require.True(t, event.Subnet.Contains(net.ParseIP(host)), host)
		}

		for _, control := range []string{"192.0.2.8", "::ffff:192.0.2.8", "::1", "::2"} {
			require.False(t, event.Subnet.Contains(net.ParseIP(control)), control)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %s event for %q", action, key)
	}
}

// TestBanList_EnforcementRawAliases catches loss of raw-key identity: renewal
// updates only the same key and equivalent aliases coexist as separate raw
// keys, while removing any equivalent host spelling removes every alias.
func TestBanList_EnforcementRawAliases(t *testing.T) {
	bl := newTestBanList(t)
	ctx := context.Background()
	events := bl.Subscribe()

	defer bl.Unsubscribe(events)

	key, alias := "[::ffff:192.0.2.7]:8333", "192.0.2.7"
	original := time.Now().Add(time.Hour).Truncate(time.Second)
	renewed := original.Add(time.Hour)

	require.NoError(t, bl.Add(ctx, key, original))
	requireHostEvent(t, events, "add", key)
	require.NoError(t, bl.Add(ctx, key, renewed))
	requireHostEvent(t, events, "add", key)
	require.ElementsMatch(t, []string{key}, bl.ListBanned())
	require.True(t, bl.BannedPeers()[key].ExpirationTime.Equal(renewed))
	requireStoredRow(t, bl, key, renewed, "192.0.2.7/32")

	require.NoError(t, bl.Add(ctx, alias, original))
	requireHostEvent(t, events, "add", alias)
	require.ElementsMatch(t, []string{key, alias}, bl.ListBanned())
	requireStoredRow(t, bl, key, renewed, "192.0.2.7/32")
	requireStoredRow(t, bl, alias, original, "192.0.2.7/32")

	// A never-stored equivalent spelling removes every host alias, one raw-key
	// event each; delivery order is not asserted.
	require.NoError(t, bl.Remove(ctx, "::ffff:192.0.2.7"))
	require.Equal(t, map[string]string{key: "192.0.2.7/32", alias: "192.0.2.7/32"},
		collectRemoveEvents(t, events, 2))
	requireNoEvent(t, events)
	require.Empty(t, bl.ListBanned())
	require.Empty(t, persistedKeys(t, bl))
	require.False(t, bl.IsBanned("[::ffff:192.0.2.7]:18444"))
}

// TestBanList_EnforcementInvalidInputs is a compatibility control: invalid
// queries stay unbanned beside an active ban, invalid Adds keep failing with
// existing messages, and a nil network cannot panic membership matching.
func TestBanList_EnforcementInvalidInputs(t *testing.T) {
	bl := newTestBanList(t)
	ctx := context.Background()

	require.NoError(t, bl.Add(ctx, "192.0.2.0/24", time.Now().Add(time.Hour)))

	for _, value := range []string{
		"", "not-an-ip", "192.0.2.7/33", "2001:db8::7/129", "[2001:db8::7]", "fe80::1%eth0", "::ffff:192.0.2.7:8333",
	} {
		require.False(t, bl.IsBanned(value), "query=%q", value)
		require.Error(t, bl.Add(ctx, value, time.Now().Add(time.Hour)), "add=%q", value)
	}

	require.ElementsMatch(t, []string{"192.0.2.0/24"}, bl.ListBanned())

	_, err := parseAddress("not-an-ip")
	require.ErrorContains(t, err, "can't parse IP: not-an-ip")

	_, err = parseAddress("192.0.2.7/33")
	require.ErrorContains(t, err, "can't parse subnet: 192.0.2.7/33")

	bl.mu.Lock()
	bl.bannedPeers["nil-network"] = BanInfo{ExpirationTime: time.Now().Add(time.Hour)}
	bl.mu.Unlock()

	require.False(t, bl.IsBanned("198.51.100.7"))
	require.True(t, bl.IsBanned("192.0.2.7"))
}

// TestBanList_EnforcementRenewalConcurrentCleanup races lookup cleanup of an
// observed-expired key against its renewal and asserts the completed renewal
// survives in memory and SQL. Stress only: it cannot force observation ->
// renewal -> deletion every run, so the write-lock recheck must be reviewed.
func TestBanList_EnforcementRenewalConcurrentCleanup(t *testing.T) {
	bl := newTestBanList(t)
	ctx := context.Background()

	const (
		key             = "192.0.2.7:8333"
		rounds          = 64
		lookupsPerRound = 128
	)

	for round := 0; round < rounds; round++ {
		require.NoError(t, bl.Add(ctx, key, time.Now().Add(-time.Hour)))

		renewed := time.Now().Add(time.Hour).Truncate(time.Second)
		start := make(chan struct{})
		addResult := make(chan error, 1)

		var workers sync.WaitGroup

		workers.Add(2)

		go func() {
			defer workers.Done()

			<-start

			for i := 0; i < lookupsPerRound; i++ {
				// An unrelated IP forces a full traversal that collects the expired key.
				bl.IsBanned("198.51.100.7")
			}
		}()

		go func() {
			defer workers.Done()

			<-start

			addResult <- bl.Add(ctx, key, renewed)
		}()

		close(start)
		workers.Wait()
		require.NoError(t, <-addResult)

		info, exists := bl.BannedPeers()[key]
		require.True(t, exists, "round=%d renewed key was deleted", round)
		require.True(t, info.ExpirationTime.Equal(renewed), "round=%d", round)
		requireStoredRow(t, bl, key, renewed, "192.0.2.7/32")
		require.True(t, bl.IsBanned("192.0.2.7"), "round=%d", round)
		require.False(t, bl.IsBanned("192.0.2.8"), "round=%d", round)
	}
}

type consumerOverlapCase struct {
	name          string
	expiredHost   string
	activeSubnet  string
	bannedRemotes []string
	allowedRemote string
}

func consumerOverlapCases() []consumerOverlapCase {
	return []consumerOverlapCase{
		{
			name: "ipv4", expiredHost: "192.0.2.7", activeSubnet: "192.0.2.0/24",
			bannedRemotes: []string{"192.0.2.7:8333", "[::ffff:192.0.2.7]:8333"},
			allowedRemote: "198.51.100.7:8333",
		},
		{
			name: "ipv6", expiredHost: "2001:db8::7", activeSubnet: "2001:db8::/64",
			bannedRemotes: []string{"[2001:db8::7]:8333", "[2001:0DB8::0007]:18444"},
			allowedRemote: "[2001:db9::7]:8333",
		},
	}
}

func newExpiredHostCoveredBanList(t *testing.T, tc consumerOverlapCase) *BanList {
	t.Helper()

	bl := newTestBanList(t)
	require.NoError(t, bl.Add(context.Background(), tc.expiredHost, time.Now().Add(-time.Hour)))
	require.NoError(t, bl.Add(context.Background(), tc.activeSubnet, time.Now().Add(time.Hour)))

	return bl
}

func peerContext(t *testing.T, remote string) context.Context {
	t.Helper()

	address, err := net.ResolveTCPAddr("tcp", remote)
	require.NoError(t, err)

	return grpcpeer.NewContext(context.Background(), &grpcpeer.Peer{Addr: address})
}

func invokeUnary(ctx context.Context, interceptor grpc.UnaryServerInterceptor) (handlerCalled bool, err error) {
	_, err = interceptor(ctx, nil, nil, func(context.Context, any) (any, error) {
		handlerCalled = true

		return "ok", nil
	})

	return handlerCalled, err
}

// TestBanList_EnforcementConsumers catches an expired exact host letting a
// covered peer through the real HTTP middleware and gRPC interceptor. Every
// subtest owns its fixture, so HTTP and gRPC failures reproduce independently.
func TestBanList_EnforcementConsumers(t *testing.T) {
	t.Run("http", func(t *testing.T) {
		for _, tc := range consumerOverlapCases() {
			t.Run(tc.name, func(t *testing.T) {
				e := echo.New()
				e.Use(CreateEchoMiddleware(newExpiredHostCoveredBanList(t, tc)))

				handlerCalls := 0

				e.GET("/test", func(c echo.Context) error {
					handlerCalls++

					return c.String(http.StatusOK, "ok")
				})

				serve := func(remote string) int {
					request := httptest.NewRequest(http.MethodGet, "/test", nil)
					request.RemoteAddr = remote
					response := httptest.NewRecorder()
					e.ServeHTTP(response, request)

					return response.Code
				}

				for _, remote := range tc.bannedRemotes {
					require.Equal(t, http.StatusForbidden, serve(remote), "remote=%q", remote)
					require.Zero(t, handlerCalls, "remote=%q reached handler", remote)
				}

				require.Equal(t, http.StatusOK, serve(tc.allowedRemote))
				require.Equal(t, 1, handlerCalls)
			})
		}
	})

	t.Run("grpc", func(t *testing.T) {
		for _, tc := range consumerOverlapCases() {
			t.Run(tc.name, func(t *testing.T) {
				interceptor := CreateGRPCUnaryInterceptor(newExpiredHostCoveredBanList(t, tc))

				for _, remote := range tc.bannedRemotes {
					called, err := invokeUnary(peerContext(t, remote), interceptor)
					require.Equal(t, codes.PermissionDenied, status.Code(err), "remote=%q", remote)
					require.False(t, called, "remote=%q reached handler", remote)
				}

				called, err := invokeUnary(peerContext(t, tc.allowedRemote), interceptor)
				require.NoError(t, err)
				require.True(t, called)
			})
		}

		t.Run("no_peer", func(t *testing.T) {
			interceptor := CreateGRPCUnaryInterceptor(newExpiredHostCoveredBanList(t, consumerOverlapCases()[0]))

			for _, ctx := range []context.Context{
				context.Background(),
				grpcpeer.NewContext(context.Background(), &grpcpeer.Peer{}),
			} {
				called, err := invokeUnary(ctx, interceptor)
				require.NoError(t, err)
				require.True(t, called)
			}
		})
	})
}
