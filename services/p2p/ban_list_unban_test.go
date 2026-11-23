package p2p

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/internal/banlist"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
)

const unbanActiveExpiration = "2100-01-01T00:00:00Z"

// newOwnedUnbanBanList builds a real in-memory blockchain store whose closure
// this test owns, unlike the shared setupBanList helper.
func newOwnedUnbanBanList(t *testing.T) (*banlist.BanList, *usql.DB) {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := blockchain.NewStore(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	bl := banlist.New(store.GetDB(), util.SqliteMemory, ulogger.TestLogger{})
	require.NoError(t, bl.Init(context.Background()))

	return bl, store.GetDB()
}

func unbanPersistedKeys(t *testing.T, db *usql.DB) []string {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), "SELECT key FROM bans")
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

// closedPoolConnector backs a separately owned pool that is closed before use.
type closedPoolConnector struct{}

func (closedPoolConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, driver.ErrBadConn
}

func (closedPoolConnector) Driver() driver.Driver { return nil }

// TestUnbanPeerEquivalentHost pins the synchronous modern UnbanPeer boundary:
// a never-stored equivalent host spelling removes every host alias from memory
// and SQL, reports Ok, and leaves the covering CIDR enforcing, including after
// a fresh load. An unavailable database yields an error, never Ok.
func TestUnbanPeerEquivalentHost(t *testing.T) {
	t.Run("equivalent_host_removes_aliases", func(t *testing.T) {
		ctx := context.Background()
		bl, db := newOwnedUnbanBanList(t)

		for key, subnet := range map[string]string{
			"::ffff:192.0.2.7":        "192.0.2.7/32",
			"[::ffff:192.0.2.7]:8333": "192.0.2.7/32",
			"192.0.2.7:8334":          "192.0.2.7/32",
			"192.0.2.0/24":            "192.0.2.0/24",
			"198.51.100.7":            "198.51.100.7/32",
		} {
			_, err := db.ExecContext(ctx, "INSERT INTO bans (key, expiration_time, subnet) VALUES ($1, $2, $3)",
				key, unbanActiveExpiration, subnet)
			require.NoError(t, err)
		}

		require.NoError(t, bl.LoadFromDatabase(ctx))

		server := &Server{banList: bl}

		response, err := server.UnbanPeer(ctx, &p2p_api.UnbanPeerRequest{Addr: "192.0.2.7"})
		require.NoError(t, err)
		require.NotNil(t, response)
		require.True(t, response.Ok)

		remaining := []string{"192.0.2.0/24", "198.51.100.7"}
		require.ElementsMatch(t, remaining, bl.ListBanned())
		require.ElementsMatch(t, remaining, unbanPersistedKeys(t, db))
		require.True(t, bl.IsBanned("192.0.2.7"), "covering CIDR still enforces")

		fresh := banlist.New(db, util.SqliteMemory, ulogger.TestLogger{})
		require.NoError(t, fresh.Init(ctx))
		require.ElementsMatch(t, remaining, fresh.ListBanned())
		require.True(t, fresh.IsBanned("192.0.2.7"))
	})

	t.Run("unavailable_database_absent_host", func(t *testing.T) {
		pool := sql.OpenDB(closedPoolConnector{})
		require.NoError(t, pool.Close())

		server := &Server{banList: banlist.New(usql.WrapDB(pool), util.SqliteMemory, ulogger.TestLogger{})}

		response, err := server.UnbanPeer(context.Background(), &p2p_api.UnbanPeerRequest{Addr: "192.0.2.7"})
		require.Error(t, err)
		require.Nil(t, response)
	})
}
