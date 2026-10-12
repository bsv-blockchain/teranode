package banlist

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newPostgresBanList starts a PostgreSQL container, builds a real blockchain
// store on it and returns an initialized BanList. It skips when containers are
// unavailable; a skip is untested, not PostgreSQL validation.
func newPostgresBanList(t *testing.T) *BanList {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pgContainer, err := postgres.Run(ctx,
		"postgres:13",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute),
		),
	)
	test.SkipIfContainerUnavailable(t, err)
	t.Cleanup(func() { _ = pgContainer.Terminate(context.Background()) })

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	dbURL, err := url.Parse(connStr)
	require.NoError(t, err)

	store, err := blockchain.NewStore(ulogger.TestLogger{}, dbURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)
	// Registered after Terminate, so the store closes first.
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	require.Equal(t, util.Postgres, store.GetDBEngine())

	bl := New(store.GetDB(), util.Postgres, ulogger.TestLogger{})
	require.NoError(t, bl.Init(context.Background()))

	return bl
}

// TestBanList_RemovePostgres repeats the key removal semantics on a real
// PostgreSQL backend: SQL-only and expired equivalent aliases, exact explicit
// CIDRs, persistence across fresh load and reload, and transactional rollback
// after a real trigger exception on the second delete.
func TestBanList_RemovePostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	bl := newPostgresBanList(t)
	ctx := context.Background()

	t.Run("equivalent_aliases_and_exact_cidrs", func(t *testing.T) {
		loadSeededBans(t, bl,
			activeBan("192.0.2.7", "192.0.2.7/32"),
			activeBan("192.0.2.7/32", "192.0.2.7/32"),
			activeBan("192.0.2.0/24", "192.0.2.0/24"),
			activeBan("192.0.2.7/24", "192.0.2.0/24"),
			activeBan("::ffff:192.0.2.7/32", "::/32"),
			activeBan("192.0.2.8", "192.0.2.8/32"),
		)
		// SQL-only aliases on this now-stale instance, one expired.
		seedBans(t, bl,
			activeBan("[::ffff:192.0.2.7]:8333", "192.0.2.7/32"),
			expiredBan("::FFFF:c000:207", "192.0.2.7/32"),
		)

		events := bl.Subscribe()
		defer bl.Unsubscribe(events)

		require.NoError(t, bl.Remove(ctx, "192.0.2.7:18444"))
		require.Equal(t, hostEvents("192.0.2.7", "[::ffff:192.0.2.7]:8333", "::FFFF:c000:207"),
			collectRemoveEvents(t, events, 3))
		requireNoEvent(t, events)

		retained := []string{"192.0.2.7/32", "192.0.2.0/24", "192.0.2.7/24", "::ffff:192.0.2.7/32", "192.0.2.8"}
		requireBanState(t, bl, retained, retained)
		require.True(t, bl.IsBanned("192.0.2.7"), "explicit CIDRs keep enforcing")

		require.NoError(t, bl.Remove(ctx, "192.0.2.0/24"))
		require.Equal(t, map[string]string{"192.0.2.0/24": "192.0.2.0/24"}, collectRemoveEvents(t, events, 1))
		requireNoEvent(t, events)

		retained = []string{"192.0.2.7/32", "192.0.2.7/24", "::ffff:192.0.2.7/32", "192.0.2.8"}
		requireBanState(t, bl, retained, retained)
		requireRemovalPersists(t, bl, retained)
	})

	t.Run("rollback_after_second_delete_trigger", func(t *testing.T) {
		execSQL(t, bl.db, "DELETE FROM bans")
		require.NoError(t, bl.reloadFromDatabase())

		loadSeededBans(t, bl,
			activeBan("192.0.2.7", "192.0.2.7/32"),
			expiredBan("192.0.2.7:8333", "192.0.2.7/32"),
			activeBan("192.0.2.8", "192.0.2.8/32"),
		)
		seedBans(t, bl, activeBan("[::ffff:192.0.2.7]:8334", "192.0.2.7/32"))
		aliases := []string{"192.0.2.7", "192.0.2.7:8333", "[::ffff:192.0.2.7]:8334"}

		execSQL(t, bl.db,
			"CREATE TABLE ban_delete_attempts (n INTEGER NOT NULL)",
			"INSERT INTO ban_delete_attempts (n) VALUES (0)",
			`CREATE FUNCTION abort_second_ban_delete() RETURNS trigger AS $$
			BEGIN
				UPDATE ban_delete_attempts SET n = n + 1;
				IF (SELECT n FROM ban_delete_attempts) >= 2 THEN
					RAISE EXCEPTION 'injected failure on second ban delete';
				END IF;
				RETURN OLD;
			END;
			$$ LANGUAGE plpgsql`,
			`CREATE TRIGGER abort_second_ban_delete BEFORE DELETE ON bans
			FOR EACH ROW EXECUTE FUNCTION abort_second_ban_delete()`,
		)

		events := bl.Subscribe()
		defer bl.Unsubscribe(events)

		beforeMemory, beforeSQL := bl.BannedPeers(), persistedKeys(t, bl)

		require.Error(t, bl.Remove(ctx, "192.0.2.7"))
		requireUnpublishedFailure(t, bl, events, beforeMemory, beforeSQL)

		var attempts int

		require.NoError(t, bl.db.QueryRowContext(ctx, "SELECT n FROM ban_delete_attempts").Scan(&attempts))
		require.Zero(t, attempts, "transaction rollback must undo the first delete's trigger increment")

		execSQL(t, bl.db,
			"DROP TRIGGER abort_second_ban_delete ON bans",
			"DROP FUNCTION abort_second_ban_delete()",
		)

		require.NoError(t, bl.Remove(ctx, "192.0.2.7"))
		require.Equal(t, hostEvents(aliases...), collectRemoveEvents(t, events, len(aliases)))
		requireNoEvent(t, events)
		requireBanState(t, bl, []string{"192.0.2.8"}, []string{"192.0.2.8"})

		execSQL(t, bl.db, "DROP TABLE ban_delete_attempts")
	})
}
