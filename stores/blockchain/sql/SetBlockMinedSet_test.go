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
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestSQL_SetBlockMinedSet covers the conditional write on sqlite: mined_set is only set while
// the block's mined_set generation still equals the value the caller's tagging run started from.
func TestSQL_SetBlockMinedSet(t *testing.T) {
	testSetBlockMinedSet(t, func(t *testing.T) *url.URL {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		return storeURL
	})
}

// TestSQL_SetBlockMinedSet_Postgres runs the same cases against real PostgreSQL, where the
// uint64 bind in "mined_set_gen = $2" has to compare against a BIGINT column and the
// ALTER TABLE migration has to add that column with its default.
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

	// generation reads the token the way a tagging run does, through GetBlockHeader
	generation := func(t *testing.T, s *SQL, hash *chainhash.Hash) uint64 {
		_, meta, err := s.GetBlockHeader(context.Background(), hash)
		require.NoError(t, err)

		return meta.MinedSetGeneration
	}

	requireRefused := func(t *testing.T, err error) {
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStateError), "expected ErrStateError, got %v", err)
	}

	t.Run("sets mined_set when the generation is unchanged", func(t *testing.T) {
		s := setup(t)

		gen := generation(t, s, block2.Hash())
		require.NoError(t, s.SetBlockMinedSet(context.Background(), block2.Hash(), &gen))

		invalid, minedSet := flags(t, s, block2.Hash())
		require.False(t, invalid)
		require.True(t, minedSet)
	})

	t.Run("every writer that clears mined_set bumps the generation", func(t *testing.T) {
		s := setup(t)

		start := generation(t, s, block2.Hash())

		require.NoError(t, s.ClearBlockMinedSet(context.Background(), block2.Hash()))
		require.Equal(t, start+1, generation(t, s, block2.Hash()), "ClearBlockMinedSet")

		_, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)
		require.Equal(t, start+2, generation(t, s, block2.Hash()), "InvalidateBlock")

		require.NoError(t, s.RevalidateBlock(context.Background(), block2.Hash()))
		require.Equal(t, start+3, generation(t, s, block2.Hash()), "RevalidateBlock")
	})

	t.Run("refuses after the block was invalidated mid-run", func(t *testing.T) {
		s := setup(t)

		// a tagging run started while the block was valid, then the block was invalidated
		gen := generation(t, s, block2.Hash())

		_, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		requireRefused(t, s.SetBlockMinedSet(context.Background(), block2.Hash(), &gen))

		invalid, minedSet := flags(t, s, block2.Hash())
		require.True(t, invalid)
		require.False(t, minedSet, "the invalidation's mined_set=false must survive, so the block is processed again")
	})

	t.Run("refuses after the block was revalidated mid-run", func(t *testing.T) {
		s := setup(t)

		// an untagging run started while the block was invalid, then the block was revalidated
		_, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		gen := generation(t, s, block2.Hash())

		require.NoError(t, s.RevalidateBlock(context.Background(), block2.Hash()))

		requireRefused(t, s.SetBlockMinedSet(context.Background(), block2.Hash(), &gen))

		invalid, minedSet := flags(t, s, block2.Hash())
		require.False(t, invalid)
		require.False(t, minedSet, "the revalidation's mined_set=false must survive, so the block is tagged again")
	})

	t.Run("refuses after ClearBlockMinedSet mid-run, which leaves invalid untouched", func(t *testing.T) {
		s := setup(t)

		// catchup clears mined_set on a block that moved off the main chain; a run that
		// started before, with onLongestChain=true, must not write mined_set back
		gen := generation(t, s, block2.Hash())

		require.NoError(t, s.ClearBlockMinedSet(context.Background(), block2.Hash()))

		requireRefused(t, s.SetBlockMinedSet(context.Background(), block2.Hash(), &gen))

		invalid, minedSet := flags(t, s, block2.Hash())
		require.False(t, invalid, "precondition: the clear did not move invalid, so a guard on invalid would have let the write through")
		require.False(t, minedSet, "the clear's mined_set=false must survive, so the block is processed again")
	})

	t.Run("refuses after an invalidate and revalidate mid-run put invalid back", func(t *testing.T) {
		s := setup(t)

		gen := generation(t, s, block2.Hash())

		_, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)
		require.NoError(t, s.RevalidateBlock(context.Background(), block2.Hash()))

		requireRefused(t, s.SetBlockMinedSet(context.Background(), block2.Hash(), &gen))

		invalid, minedSet := flags(t, s, block2.Hash())
		require.False(t, invalid, "precondition: invalid is back at its starting value")
		require.False(t, minedSet, "the stale run must not commit over the revalidation's reset")
	})

	t.Run("sets mined_set on an invalid block for an untagging run", func(t *testing.T) {
		s := setup(t)

		_, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		gen := generation(t, s, block2.Hash())
		require.NoError(t, s.SetBlockMinedSet(context.Background(), block2.Hash(), &gen))

		invalid, minedSet := flags(t, s, block2.Hash())
		require.True(t, invalid)
		require.True(t, minedSet)
	})

	t.Run("nil generation is the unconditional write an older caller expects", func(t *testing.T) {
		s := setup(t)

		_, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)

		require.NoError(t, s.SetBlockMinedSet(context.Background(), block2.Hash(), nil))

		_, minedSet := flags(t, s, block2.Hash())
		require.True(t, minedSet)
	})

	t.Run("unknown block is a storage error, not a state error", func(t *testing.T) {
		s := setup(t)

		gen := uint64(0)
		err := s.SetBlockMinedSet(context.Background(), block3.Hash(), &gen)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "expected ErrStorageError, got %v", err)
		require.False(t, errors.Is(err, errors.ErrStateError))

		err = s.SetBlockMinedSet(context.Background(), block3.Hash(), nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "expected ErrStorageError, got %v", err)
	})
}

// TestSQL_SetBlockMinedSet_UnchangedGenerationIsStorageError covers the zero-rows path where the
// generation did NOT move. The guard was not what stopped the write, so it must be a storage
// error: the caller charges retries on that, while a state error re-queues for free and would
// loop forever on a real storage fault. A sqlite trigger that drops the UPDATE stands in for
// that fault.
func TestSQL_SetBlockMinedSet_UnchangedGenerationIsStorageError(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	defer func() { _ = s.Close(context.Background()) }()

	_, _, err = s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	_, meta, err := s.GetBlockHeader(context.Background(), block2.Hash())
	require.NoError(t, err)

	gen := meta.MinedSetGeneration

	_, err = s.db.ExecContext(context.Background(),
		`CREATE TRIGGER drop_mined_set_update BEFORE UPDATE OF mined_set ON blocks BEGIN SELECT RAISE(IGNORE); END;`)
	require.NoError(t, err)

	err = s.SetBlockMinedSet(context.Background(), block2.Hash(), &gen)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "expected ErrStorageError, got %v", err)
	require.False(t, errors.Is(err, errors.ErrStateError), "an unchanged generation must not read as a refused write")
}
