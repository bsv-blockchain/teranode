package banlist

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
)

// Fixed expiry text keeps rows unambiguously active or expired.
const (
	activeExpiration  = "2100-01-01T00:00:00Z"
	expiredExpiration = "2000-01-01T00:00:00Z"
)

// seededBan is one raw row. subnet is stored text only; removal must classify
// rows by their raw key, never by this derived column.
type seededBan struct {
	key, expiration, subnet string
}

func activeBan(key, subnet string) seededBan {
	return seededBan{key: key, expiration: activeExpiration, subnet: subnet}
}

func expiredBan(key, subnet string) seededBan {
	return seededBan{key: key, expiration: expiredExpiration, subnet: subnet}
}

// seedBans writes rows directly to SQL. No Add is used, so no asynchronous add
// notification can arrive late in a subscription test.
func seedBans(t *testing.T, bl *BanList, bans ...seededBan) {
	t.Helper()

	for _, ban := range bans {
		_, err := bl.db.ExecContext(context.Background(),
			"INSERT INTO bans (key, expiration_time, subnet) VALUES ($1, $2, $3)",
			ban.key, ban.expiration, ban.subnet)
		require.NoError(t, err, "seed %q", ban.key)
	}
}

// loadSeededBans seeds rows and merges them into memory through the loader.
func loadSeededBans(t *testing.T, bl *BanList, bans ...seededBan) {
	t.Helper()

	seedBans(t, bl, bans...)
	require.NoError(t, bl.LoadFromDatabase(context.Background()))
}

func banKeys(bans ...seededBan) []string {
	keys := make([]string, 0, len(bans))
	for _, ban := range bans {
		keys = append(keys, ban.key)
	}

	return keys
}

// persistedKeys returns every raw key in SQL, checking iteration and close.
func persistedKeys(t *testing.T, bl *BanList) []string {
	t.Helper()

	rows, err := bl.db.QueryContext(context.Background(), "SELECT key FROM bans")
	require.NoError(t, err)

	defer func() { _ = rows.Close() }()

	keys := []string{}

	for rows.Next() {
		var key string

		require.NoError(t, rows.Scan(&key))
		keys = append(keys, key)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	return keys
}

func requireBanState(t *testing.T, bl *BanList, wantMemory, wantSQL []string) {
	t.Helper()

	require.ElementsMatch(t, wantMemory, bl.ListBanned(), "in-memory keys")
	require.ElementsMatch(t, wantSQL, persistedKeys(t, bl), "persisted keys")
}

// requireRemovalPersists checks a fresh instance and a periodic reload do not
// restore removed keys.
func requireRemovalPersists(t *testing.T, bl *BanList, wantMemory []string) {
	t.Helper()

	for _, mode := range []string{"startup", "reload"} {
		require.ElementsMatch(t, wantMemory, loadEnforcementFixture(t, bl, mode).ListBanned(), mode)
	}
}

// collectRemoveEvents receives exactly count remove events and returns raw IP to
// subnet text. Delivery order is not asserted; duplicates fail. The timeout is
// a failure guard, not synchronization.
func collectRemoveEvents(t *testing.T, events chan BanEvent, count int) map[string]string {
	t.Helper()

	removed := make(map[string]string, count)

	for len(removed) < count {
		select {
		case event := <-events:
			require.Equal(t, "remove", event.Action, "event for %q", event.IP)
			require.NotNil(t, event.Subnet, "event for %q", event.IP)

			_, duplicate := removed[event.IP]
			require.False(t, duplicate, "duplicate remove event for %q", event.IP)

			removed[event.IP] = event.Subnet.String()
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout after %d of %d remove events: %v", len(removed), count, removed)
		}
	}

	return removed
}

// requireNoEvent observes the channel for a bounded window. Notifications are
// asynchronous, so this detects promptly scheduled stray events only; it cannot
// prove that no event will ever arrive.
func requireNoEvent(t *testing.T, events chan BanEvent) {
	t.Helper()

	select {
	case event := <-events:
		t.Fatalf("unexpected %s event for %q", event.Action, event.IP)
	case <-time.After(100 * time.Millisecond):
	}
}

// unavailableConnector backs a separately owned, closed pool; it never
// connects, so every database operation fails.
type unavailableConnector struct{}

func (unavailableConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, driver.ErrBadConn
}

func (unavailableConnector) Driver() driver.Driver { return nil }

// TestBanList_RemoveHostAliases catches exact-key-only host removal: every
// bare, port, mapped, expanded and uppercase alias of the host must go from
// memory and SQL for any equivalent request, including spellings never stored,
// regardless of expiry. Neighbours, IPv4-compatible and other-family hosts stay.
func TestBanList_RemoveHostAliases(t *testing.T) {
	families := []struct {
		name              string
		aliases, retained []seededBan
		requests          []string
		removedHost       string
		retainedHost      string
	}{
		{
			name: "ipv4",
			aliases: []seededBan{
				activeBan("192.0.2.7", "192.0.2.7/32"),
				expiredBan("192.0.2.7:8333", "192.0.2.7/32"),
				activeBan("192.0.2.7:8334", "192.0.2.7/32"),
				activeBan("::ffff:192.0.2.7", "192.0.2.7/32"),
				expiredBan("::ffff:c000:207", "192.0.2.7/32"),
				activeBan("0:0:0:0:0:ffff:c000:0207", "192.0.2.7/32"),
				activeBan("[::ffff:192.0.2.7]:8333", "192.0.2.7/32"),
			},
			retained: []seededBan{
				activeBan("192.0.2.8", "192.0.2.8/32"),
				activeBan("::ffff:192.0.2.8", "192.0.2.8/32"),
				activeBan("::192.0.2.7", "::c000:207/128"),
				activeBan("2001:db8::7", "2001:db8::7/128"),
			},
			requests:     []string{"192.0.2.7", "192.0.2.7:18444", "::FFFF:c000:207", "[::ffff:192.0.2.7]:8334"},
			removedHost:  "192.0.2.7",
			retainedHost: "192.0.2.8",
		},
		{
			name: "ipv6",
			aliases: []seededBan{
				activeBan("2001:db8::7", "2001:db8::7/128"),
				activeBan("2001:DB8:0:0:0:0:0:7", "2001:db8::7/128"),
				expiredBan("2001:0db8:0000:0000:0000:0000:0000:0007", "2001:db8::7/128"),
				activeBan("[2001:db8::7]:8333", "2001:db8::7/128"),
				expiredBan("[2001:0DB8::0007]:8334", "2001:db8::7/128"),
			},
			retained: []seededBan{
				activeBan("2001:db8::8", "2001:db8::8/128"),
				activeBan("[2001:db8::8]:8333", "2001:db8::8/128"),
				activeBan("2001:db9::7", "2001:db9::7/128"),
				activeBan("192.0.2.7", "192.0.2.7/32"),
			},
			requests:     []string{"2001:db8::7", "2001:DB8::7", "2001:0db8:0000:0000:0000:0000:0000:0007", "[2001:db8::7]:9999"},
			removedHost:  "2001:db8::7",
			retainedHost: "2001:db8::8",
		},
	}

	for _, family := range families {
		for _, request := range family.requests {
			t.Run(family.name+"/"+request, func(t *testing.T) {
				bl := newTestBanList(t)
				loadSeededBans(t, bl, append(append([]seededBan{}, family.aliases...), family.retained...)...)

				require.NoError(t, bl.Remove(context.Background(), request))

				retained := banKeys(family.retained...)
				requireBanState(t, bl, retained, retained)
				require.False(t, bl.IsBanned(family.removedHost))
				require.True(t, bl.IsBanned(family.retainedHost))
				requireRemovalPersists(t, bl, retained)
			})
		}
	}
}

// TestBanList_RemoveExplicitCIDR catches host removal selecting slash-bearing
// keys (including /32, /128 and mapped CIDRs) and CIDR removal matching by
// effective network instead of exact raw key, or skipping SQL-only CIDR rows.
func TestBanList_RemoveExplicitCIDR(t *testing.T) {
	t.Run("host_request_preserves_cidrs", func(t *testing.T) {
		bl := newTestBanList(t)
		cidrs := []seededBan{
			activeBan("192.0.2.7/32", "192.0.2.7/32"),
			activeBan("192.0.2.0/24", "192.0.2.0/24"),
			activeBan("::ffff:192.0.2.7/32", "::/32"),
			activeBan("::ffff:192.0.2.7/128", "192.0.2.7/32"),
			activeBan("2001:db8::7/128", "2001:db8::7/128"),
			activeBan("2001:db8::/64", "2001:db8::/64"),
			activeBan("192.0.2.8", "192.0.2.8/32"),
			activeBan("2001:db8::8", "2001:db8::8/128"),
		}
		loadSeededBans(t, bl, append([]seededBan{
			activeBan("192.0.2.7", "192.0.2.7/32"),
			activeBan("192.0.2.7:8333", "192.0.2.7/32"),
			activeBan("::ffff:192.0.2.7", "192.0.2.7/32"),
			activeBan("2001:db8::7", "2001:db8::7/128"),
			activeBan("[2001:db8::7]:8333", "2001:db8::7/128"),
		}, cidrs...)...)

		require.NoError(t, bl.Remove(context.Background(), "[::ffff:192.0.2.7]:18444"))
		require.NoError(t, bl.Remove(context.Background(), "2001:DB8::7"))

		retained := banKeys(cidrs...)
		requireBanState(t, bl, retained, retained)
		// Retained explicit rules keep enforcing; removal does not mean unbanned.
		require.True(t, bl.IsBanned("192.0.2.7"))
		require.True(t, bl.IsBanned("2001:db8::7"))
		require.True(t, bl.IsBanned("::1"))
		requireRemovalPersists(t, bl, retained)
	})

	t.Run("exact_cidr_request", func(t *testing.T) {
		bl := newTestBanList(t)
		loadSeededBans(t, bl,
			activeBan("192.0.2.7/24", "192.0.2.0/24"),
			activeBan("192.0.2.0/24", "192.0.2.0/24"),
			activeBan("192.0.2.7/32", "192.0.2.7/32"),
			activeBan("2001:db8::/64", "2001:db8::/64"),
			activeBan("2001:0DB8::/64", "2001:db8::/64"),
			activeBan("192.0.2.7", "192.0.2.7/32"),
			activeBan("192.0.2.7:8333", "192.0.2.7/32"),
			activeBan("2001:db8::7", "2001:db8::7/128"),
		)
		// SQL-only CIDR on this now-stale instance.
		seedBans(t, bl, activeBan("198.51.100.0/24", "198.51.100.0/24"))

		for _, request := range []string{"192.0.2.0/24", "2001:0DB8::/64", "192.0.2.7/32", "198.51.100.0/24"} {
			require.NoError(t, bl.Remove(context.Background(), request), request)
		}

		retained := []string{"192.0.2.7/24", "2001:db8::/64", "192.0.2.7", "192.0.2.7:8333", "2001:db8::7"}
		requireBanState(t, bl, retained, retained)
		require.True(t, bl.IsBanned("192.0.2.9"))
		require.False(t, bl.IsBanned("198.51.100.7"))
		requireRemovalPersists(t, bl, retained)
	})
}

// TestBanList_RemoveHostDurability catches removal limited to memory: SQL-only
// aliases on a stale instance and expired aliases already evicted by lookup
// must be deleted durably. Malformed expiration/subnet data and legacy "::/32"
// text must not affect key-based classification; invalid keys stay untouched.
func TestBanList_RemoveHostDurability(t *testing.T) {
	t.Run("sql_only_alias_on_stale_instance", func(t *testing.T) {
		bl := newTestBanList(t)
		loadSeededBans(t, bl, activeBan("192.0.2.7", "192.0.2.7/32"), activeBan("192.0.2.8", "192.0.2.8/32"))
		seedBans(t, bl, activeBan("[::ffff:192.0.2.7]:8333", "192.0.2.7/32"))

		require.NoError(t, bl.Remove(context.Background(), "192.0.2.7"))

		requireBanState(t, bl, []string{"192.0.2.8"}, []string{"192.0.2.8"})
		requireRemovalPersists(t, bl, []string{"192.0.2.8"})
	})

	t.Run("expired_alias_evicted_by_lookup", func(t *testing.T) {
		bl := newTestBanList(t)
		loadSeededBans(t, bl,
			expiredBan("192.0.2.7:8333", "192.0.2.7/32"),
			expiredBan("[::ffff:192.0.2.7]:8334", "192.0.2.7/32"),
			activeBan("::ffff:c000:207", "192.0.2.7/32"),
			activeBan("198.51.100.7", "198.51.100.7/32"),
		)

		// An uncovered lookup traverses every entry and evicts expired ones from
		// memory only.
		require.False(t, bl.IsBanned("203.0.113.1"))
		requireBanState(t, bl,
			[]string{"::ffff:c000:207", "198.51.100.7"},
			[]string{"192.0.2.7:8333", "[::ffff:192.0.2.7]:8334", "::ffff:c000:207", "198.51.100.7"})

		require.NoError(t, bl.Remove(context.Background(), "192.0.2.7"))

		requireBanState(t, bl, []string{"198.51.100.7"}, []string{"198.51.100.7"})
		requireRemovalPersists(t, bl, []string{"198.51.100.7"})
	})

	t.Run("malformed_row_data", func(t *testing.T) {
		bl := newTestBanList(t)
		loadSeededBans(t, bl,
			activeBan("::ffff:192.0.2.7", "::/32"),
			seededBan{key: "[::ffff:192.0.2.7]:8333", expiration: "not-a-time", subnet: "broken-subnet"},
			activeBan("192.0.2.7", ""),
			activeBan("::ffff:192.0.2.7/32", "::/32"),
			// Stored subnet text naming the target must not select this key.
			activeBan("192.0.2.8", "192.0.2.7/32"),
			activeBan("not-an-ip", "192.0.2.7/32"),
			activeBan("not-a-cidr/99", "0.0.0.0/0"),
		)
		// The malformed-expiration alias and invalid keys are not loaded.
		require.ElementsMatch(t, []string{"::ffff:192.0.2.7", "192.0.2.7", "::ffff:192.0.2.7/32", "192.0.2.8"}, bl.ListBanned())

		require.NoError(t, bl.Remove(context.Background(), "[::ffff:192.0.2.7]:18444"))

		wantMemory := []string{"::ffff:192.0.2.7/32", "192.0.2.8"}
		requireBanState(t, bl, wantMemory, []string{"::ffff:192.0.2.7/32", "192.0.2.8", "not-an-ip", "not-a-cidr/99"})
		require.False(t, bl.IsBanned("192.0.2.7"))
		require.True(t, bl.IsBanned("::1"), "explicit mapped CIDR keeps its prefix")
		requireRemovalPersists(t, bl, wantMemory)
	})
}

// TestBanList_RemoveNoMatchAndInvalid catches unverified no-op success when the
// map has no match but SQL is unavailable, and guards existing behavior: a
// SQL-confirmed miss succeeds without changes or events, and invalid requests
// fail with existing messages before any database access.
func TestBanList_RemoveNoMatchAndInvalid(t *testing.T) {
	t.Run("sql_confirmed_absent", func(t *testing.T) {
		bl := newTestBanList(t)
		loadSeededBans(t, bl,
			activeBan("192.0.2.8", "192.0.2.8/32"),
			activeBan("192.0.2.0/24", "192.0.2.0/24"),
			activeBan("2001:db8::/64", "2001:db8::/64"),
		)
		events := bl.Subscribe()

		defer bl.Unsubscribe(events)

		beforeMemory, beforeSQL := bl.BannedPeers(), persistedKeys(t, bl)

		for _, request := range []string{
			"192.0.2.7", "192.0.2.7:8333", "2001:db8::7", "198.51.100.0/24", "192.0.2.0/25", "2001:db8::7/128",
		} {
			require.NoError(t, bl.Remove(context.Background(), request), request)
		}

		require.Equal(t, beforeMemory, bl.BannedPeers())
		require.ElementsMatch(t, beforeSQL, persistedKeys(t, bl))
		requireNoEvent(t, events)
	})

	t.Run("unavailable_database_empty_map", func(t *testing.T) {
		pool := sql.OpenDB(unavailableConnector{})
		require.NoError(t, pool.Close())

		bl := New(usql.WrapDB(pool), util.SqliteMemory, ulogger.TestLogger{})

		require.Error(t, bl.Remove(context.Background(), "192.0.2.7"))
		require.Error(t, bl.Remove(context.Background(), "192.0.2.0/24"))
		// Input validation precedes, and so is not masked by, database access.
		require.ErrorContains(t, bl.Remove(context.Background(), "not-an-ip"), "can't parse IP: not-an-ip")
		require.Empty(t, bl.ListBanned())
	})

	t.Run("invalid_requests", func(t *testing.T) {
		bl := newTestBanList(t)
		loadSeededBans(t, bl, activeBan("192.0.2.7", "192.0.2.7/32"), activeBan("192.0.2.0/24", "192.0.2.0/24"))
		events := bl.Subscribe()

		defer bl.Unsubscribe(events)

		beforeMemory, beforeSQL := bl.BannedPeers(), persistedKeys(t, bl)

		for _, request := range []string{
			"", "not-an-ip", "192.0.2.7/33", "2001:db8::7/129", "[2001:db8::7]", "fe80::1%eth0", "::ffff:192.0.2.7:8333",
		} {
			require.Error(t, bl.Remove(context.Background(), request), "request=%q", request)
		}

		require.ErrorContains(t, bl.Remove(context.Background(), "not-an-ip"), "can't parse IP: not-an-ip")
		require.ErrorContains(t, bl.Remove(context.Background(), "192.0.2.7/33"), "can't parse subnet: 192.0.2.7/33")
		require.Equal(t, beforeMemory, bl.BannedPeers())
		require.ElementsMatch(t, beforeSQL, persistedKeys(t, bl))
		requireNoEvent(t, events)
	})
}

// TestBanList_RemoveEvents catches missing or synthetic removal events: one
// event per distinct raw key actually removed from SQL or memory (SQL-only,
// map-only and overlapping), carrying that raw key and its reconstructed
// network, and none for a repeated no-match removal. Rows are seeded through
// SQL plus load, so no delayed add notification can reach the subscriber.
func TestBanList_RemoveEvents(t *testing.T) {
	t.Run("ipv4_host_union", func(t *testing.T) {
		bl := newTestBanList(t)
		ctx := context.Background()
		loadSeededBans(t, bl,
			activeBan("192.0.2.7", "192.0.2.7/32"),
			expiredBan("192.0.2.7:8333", "192.0.2.7/32"),
			activeBan("::ffff:192.0.2.7", "192.0.2.7/32"),
			activeBan("192.0.2.8", "192.0.2.8/32"),
			activeBan("192.0.2.0/24", "192.0.2.0/24"),
		)
		// Map-only alias: its row vanished after load.
		_, err := bl.db.ExecContext(ctx, "DELETE FROM bans WHERE key = $1", "::ffff:192.0.2.7")
		require.NoError(t, err)
		// SQL-only alias: written after load.
		seedBans(t, bl, activeBan("[::ffff:192.0.2.7]:8333", "192.0.2.7/32"))

		events := bl.Subscribe()

		defer bl.Unsubscribe(events)

		require.NoError(t, bl.Remove(ctx, "192.0.2.7:1"))
		require.Equal(t, map[string]string{
			"192.0.2.7":               "192.0.2.7/32",
			"192.0.2.7:8333":          "192.0.2.7/32",
			"::ffff:192.0.2.7":        "192.0.2.7/32",
			"[::ffff:192.0.2.7]:8333": "192.0.2.7/32",
		}, collectRemoveEvents(t, events, 4))
		requireNoEvent(t, events)
		requireBanState(t, bl, []string{"192.0.2.8", "192.0.2.0/24"}, []string{"192.0.2.8", "192.0.2.0/24"})

		require.NoError(t, bl.Remove(ctx, "::ffff:192.0.2.7"))
		requireNoEvent(t, events)
	})

	t.Run("ipv6_host_union", func(t *testing.T) {
		bl := newTestBanList(t)
		ctx := context.Background()
		loadSeededBans(t, bl, activeBan("2001:db8::7", "2001:db8::7/128"), activeBan("2001:db8::/64", "2001:db8::/64"))
		seedBans(t, bl, activeBan("[2001:0DB8::0007]:8333", "2001:db8::7/128"))

		events := bl.Subscribe()

		defer bl.Unsubscribe(events)

		require.NoError(t, bl.Remove(ctx, "2001:DB8::7"))
		require.Equal(t, map[string]string{
			"2001:db8::7":            "2001:db8::7/128",
			"[2001:0DB8::0007]:8333": "2001:db8::7/128",
		}, collectRemoveEvents(t, events, 2))
		requireNoEvent(t, events)
		requireBanState(t, bl, []string{"2001:db8::/64"}, []string{"2001:db8::/64"})
	})

	t.Run("exact_cidr", func(t *testing.T) {
		bl := newTestBanList(t)
		ctx := context.Background()
		loadSeededBans(t, bl,
			activeBan("192.0.2.0/24", "192.0.2.0/24"),
			activeBan("192.0.2.7/24", "192.0.2.0/24"),
			activeBan("192.0.2.7", "192.0.2.7/32"),
		)
		seedBans(t, bl, activeBan("2001:0DB8::/64", "2001:db8::/64"))

		events := bl.Subscribe()

		defer bl.Unsubscribe(events)

		require.NoError(t, bl.Remove(ctx, "192.0.2.0/24"))
		require.Equal(t, map[string]string{"192.0.2.0/24": "192.0.2.0/24"}, collectRemoveEvents(t, events, 1))
		requireNoEvent(t, events)

		require.NoError(t, bl.Remove(ctx, "2001:0DB8::/64"))
		require.Equal(t, map[string]string{"2001:0DB8::/64": "2001:db8::/64"}, collectRemoveEvents(t, events, 1))
		requireNoEvent(t, events)
		requireBanState(t, bl, []string{"192.0.2.7/24", "192.0.2.7"}, []string{"192.0.2.7/24", "192.0.2.7"})
	})
}
