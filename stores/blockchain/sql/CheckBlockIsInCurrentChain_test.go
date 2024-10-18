package sql

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Test_PostgresCheckIfBlockIsInCurrentChain(t *testing.T) {
	t.Run("empty - no match", func(t *testing.T) {
		connStr, teardown, err := setupPostgresContainer()
		require.NoError(t, err)

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		// No blocks stored, should return false
		blockIDs := []uint32{1, 2, 3}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)
	})

	t.Run("single block in chain", func(t *testing.T) {
		connStr, teardown, err := setupPostgresContainer()
		require.NoError(t, err)

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		// Store block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, metas, err := s.GetBlockHeaders(context.Background(), block1.Hash(), 1)
		require.NoError(t, err)

		// Check if block1 is in the chain, should return true
		blockIDs := []uint32{metas[0].ID}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)

		require.NoError(t, err)
		assert.True(t, isInChain)
	})

	t.Run("multiple blocks in chain", func(t *testing.T) {
		connStr, teardown, err := setupPostgresContainer()
		require.NoError(t, err)

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		// Store block1 and block2
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// get metas for block1 and block2
		_, metas, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)

		// Check if block1 and block2 are in the chain, should return true
		blockIDs := []uint32{metas[0].ID, metas[1].ID}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)
	})

	t.Run("block not in chain", func(t *testing.T) {
		connStr, teardown, err := setupPostgresContainer()
		require.NoError(t, err)

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		// Store block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		// Check if a non-existent block is in the chain, should return false
		blockIDs := []uint32{9999} // Non-existent block
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)
	})

	t.Run("alternative block in branch", func(t *testing.T) {
		connStr, teardown, err := setupPostgresContainer()
		require.NoError(t, err)

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		// Store block1, block2, and an alternative block (block2Alt)
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		block2Alt := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1231469744,
				Nonce:          1639830026,
				HashPrevBlock:  block2PrevBlockHash,
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           *bits,
			},
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees: []*chainhash.Hash{
				subtree,
			},
		}

		_, _, err = s.StoreBlock(context.Background(), block2Alt, "")
		require.NoError(t, err)

		// Get current best block header
		// bestBlockHeader, _, err := s.GetBestBlockHeader(context.Background())
		// require.NoError(t, err)

		_, metas, err := s.GetBlockHeaders(context.Background(), block2Alt.Hash(), 10)
		require.NoError(t, err)

		// Check if block2Alt is in the chain, should return true
		blockIDs := []uint32{metas[0].ID}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)
	})

	t.Run("alternative block in correct chain", func(t *testing.T) {
		connStr, teardown, err := setupPostgresContainer()
		require.NoError(t, err)

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		// Store block1, block2, and an alternative block (block2Alt)
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// Get current best block header
		bestBlockHeader, _, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		block2Alt := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1231469744,
				Nonce:          1639830026,
				HashPrevBlock:  bestBlockHeader.Hash(),
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           *bits,
			},
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees: []*chainhash.Hash{
				subtree,
			},
		}

		_, _, err = s.StoreBlock(context.Background(), block2Alt, "")
		require.NoError(t, err)

		_, metas, err := s.GetBlockHeaders(context.Background(), block2Alt.Hash(), 10)
		require.NoError(t, err)

		// Check if block2Alt is in the chain, should return true
		blockIDs := []uint32{metas[0].ID}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)
	})
}

func setupPostgresContainer() (string, func() error, error) {
	ctx := context.Background()

	dbName := "testdb"
	dbUser := "postgres"
	dbPassword := "password"

	postgresC, err := postgres.Run(ctx,
		"docker.io/postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		return "", nil, err
	}

	host, err := postgresC.Host(ctx)
	if err != nil {
		return "", nil, err
	}

	port, err := postgresC.MappedPort(ctx, "5432")
	if err != nil {
		return "", nil, err
	}

	connStr := fmt.Sprintf("postgres://postgres:password@%s:%s/testdb?sslmode=disable", host, port.Port())

	teardown := func() error {
		return postgresC.Terminate(ctx)
	}

	return connStr, teardown, nil
}
