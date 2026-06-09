package sql

import (
	"context"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestAssignBlockID_IdempotentPerHash(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	h := chainhash.HashH([]byte("block-A"))

	id1, err := s.AssignBlockID(ctx, &h)
	require.NoError(t, err)
	require.NotZero(t, id1)

	id2, err := s.AssignBlockID(ctx, &h)
	require.NoError(t, err)
	require.Equal(t, id1, id2, "same hash must return the same reserved id")

	h2 := chainhash.HashH([]byte("block-B"))
	id3, err := s.AssignBlockID(ctx, &h2)
	require.NoError(t, err)
	require.NotEqual(t, id1, id3)
}

func TestAssignBlockID_ConcurrentCallersConverge(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	h := chainhash.HashH([]byte("block-race"))

	const n = 16
	ids := make([]uint64, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id, err := s.AssignBlockID(ctx, &h)
			require.NoError(t, err)
			ids[i] = id
		}(i)
	}
	wg.Wait()

	for i := 1; i < n; i++ {
		require.Equal(t, ids[0], ids[i], "all concurrent callers for one hash must get one id")
	}
}

func TestAssignBlockID_ClearedOnCommit(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	reserved, err := s.AssignBlockID(ctx, block1.Hash())
	require.NoError(t, err)
	require.NotZero(t, reserved)

	storedID, _, err := s.StoreBlock(ctx, block1, "test", options.WithID(reserved))
	require.NoError(t, err)
	require.Equal(t, reserved, storedID)

	require.Nil(t, s.blockIDReservations.Get(*block1.Hash()), "reservation must be cleared on commit")

	again, err := s.AssignBlockID(ctx, block1.Hash())
	require.NoError(t, err)
	require.Equal(t, reserved, again, "AssignBlockID must return the committed id after reservation is cleared")
}

// Simulates the legacy + blockvalidation race over the SAME block: both call
// AssignBlockID concurrently, then one commits. The committed block id MUST
// equal the id every caller saw — i.e. no phantom id can exist.
func TestAssignBlockID_TwoPathRace_NoPhantom(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	var legacyID, catchupID uint64
	var legacyErr, catchupErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		legacyID, legacyErr = s.AssignBlockID(ctx, block1.Hash())
	}()
	go func() {
		defer wg.Done()
		catchupID, catchupErr = s.AssignBlockID(ctx, block1.Hash())
	}()
	wg.Wait()
	require.NoError(t, legacyErr)
	require.NoError(t, catchupErr)

	require.Equal(t, legacyID, catchupID, "both ingestion paths must get the same id")

	storedID, _, err := s.StoreBlock(ctx, block1, "test", options.WithID(legacyID))
	require.NoError(t, err)
	require.Equal(t, legacyID, storedID)

	// The id every path used IS a committed, on-chain block — never a phantom.
	got, ok, err := s.blockIDByHash(ctx, block1.Hash())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, storedID, got)
}

// TestAssignBlockID_DBError covers the storage-error path: when the underlying
// DB is unavailable the committed-id lookup fails, and AssignBlockID must
// surface that error rather than silently minting a fresh id (which could
// re-introduce the divergence this method exists to prevent).
func TestAssignBlockID_DBError(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Close the store (and its DB) so the committed-id lookup SELECT errors.
	require.NoError(t, s.Close())

	h := chainhash.HashH([]byte("block-db-error"))
	_, err = s.AssignBlockID(context.Background(), &h)
	require.Error(t, err, "AssignBlockID must surface a storage error when the DB is closed, not mint an id")
}
