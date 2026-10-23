package sql

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestSQL_SetBlockMinedSet covers the conditional write on sqlite: mined_set is only set while
// the block's invalid flag still equals the value the caller's tagging run started from.
func TestSQL_SetBlockMinedSet(t *testing.T) {
	testSetBlockMinedSet(t, func(t *testing.T) *url.URL {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		return storeURL
	})
}

// TestSQL_SetBlockMinedSet_Postgres runs the same cases against real PostgreSQL, where the
// boolean bind in "invalid = $2" has to compare against a BOOLEAN column; sqlite stores it as
// an integer and cannot prove that.
func TestSQL_SetBlockMinedSet_Postgres(t *testing.T) {
	ctx := context.Background()

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

	defer func() {
		require.NoError(t, pgContainer.Terminate(ctx))
	}()

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	adminURL, err := url.Parse(connStr)
	require.NoError(t, err)

	admin, err := New(ulogger.TestLogger{}, adminURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	defer admin.Close(ctx)

	dbCount := 0

	// each case gets its own database, so the shared block fixtures never collide
	testSetBlockMinedSet(t, func(t *testing.T) *url.URL {
		dbCount++
		dbName := fmt.Sprintf("minedset%d", dbCount)

		_, err := admin.db.ExecContext(ctx, "CREATE DATABASE "+dbName)
		require.NoError(t, err)

		caseURL := *adminURL
		caseURL.Path = "/" + dbName

		return &caseURL
	})
}

func testSetBlockMinedSet(t *testing.T, newStoreURL func(t *testing.T) *url.URL) {
	tSettings := test.CreateBaseTestSettings(t)

	setup := func(t *testing.T) *SQL {
		s, err := New(ulogger.TestLogger{}, newStoreURL(t), tSettings)
		require.NoError(t, err)

		t.Cleanup(func() { _ = s.Close(context.Background()) })

		_, _, err = s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		return s
	}

	flags := func(t *testing.T, s *SQL, hash *chainhash.Hash) (invalid, minedSet bool) {
		err := s.db.QueryRowContext(context.Background(),
			`SELECT invalid, mined_set FROM blocks WHERE hash = $1`, hash.CloneBytes()).Scan(&invalid, &minedSet)
		require.NoError(t, err)

		return invalid, minedSet
	}

	t.Run("sets mined_set when invalid is unchanged", func(t *testing.T) {
		s := setup(t)

		require.NoError(t, s.SetBlockMinedSet(context.Background(), block2.Hash(), false))

		invalid, minedSet := flags(t, s, block2.Hash())
		assert.False(t, invalid)
		assert.True(t, minedSet)
	})

	t.Run("refuses after the block was invalidated mid-run", func(t *testing.T) {
		s := setup(t)

		// a tagging run started while the block was valid, then the block was invalidated
		_, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		err = s.SetBlockMinedSet(context.Background(), block2.Hash(), false)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrStateError), "expected ErrStateError, got %v", err)

		invalid, minedSet := flags(t, s, block2.Hash())
		assert.True(t, invalid)
		assert.False(t, minedSet, "the invalidation's mined_set=false must survive, so the block is processed again")
	})

	t.Run("refuses after the block was revalidated mid-run", func(t *testing.T) {
		s := setup(t)

		// an untagging run started while the block was invalid, then the block was revalidated
		_, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		require.NoError(t, s.RevalidateBlock(context.Background(), block2.Hash()))

		err = s.SetBlockMinedSet(context.Background(), block2.Hash(), true)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrStateError), "expected ErrStateError, got %v", err)

		invalid, minedSet := flags(t, s, block2.Hash())
		assert.False(t, invalid)
		assert.False(t, minedSet, "the revalidation's mined_set=false must survive, so the block is tagged again")
	})

	t.Run("sets mined_set on an invalid block for an untagging run", func(t *testing.T) {
		s := setup(t)

		_, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		require.NoError(t, s.SetBlockMinedSet(context.Background(), block2.Hash(), true))

		invalid, minedSet := flags(t, s, block2.Hash())
		assert.True(t, invalid)
		assert.True(t, minedSet)
	})

	t.Run("unknown block is a storage error, not a state error", func(t *testing.T) {
		s := setup(t)

		err := s.SetBlockMinedSet(context.Background(), block3.Hash(), false)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrStorageError), "expected ErrStorageError, got %v", err)
		assert.False(t, errors.Is(err, errors.ErrStateError))
	})
}
