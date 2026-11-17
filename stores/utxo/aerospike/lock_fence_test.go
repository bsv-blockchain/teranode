package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

// TestIsFilteredOut pins the error match the lock fence rests on. releaseLock treats
// FILTERED_OUT as "the lock is no longer mine, leave it alone" and returns success;
// every other error must stay an error, or a genuine failure to release would be
// silently swallowed and the lock left to sit out its TTL.
func TestIsFilteredOut(t *testing.T) {
	require.True(t, isFilteredOut(aeroErr(types.FILTERED_OUT)))
	require.False(t, isFilteredOut(aeroErr(types.TIMEOUT)))
	require.False(t, isFilteredOut(aeroErr(types.KEY_NOT_FOUND_ERROR)))
	require.False(t, isFilteredOut(errors.NewProcessingError("not an aerospike error")))
	require.False(t, isFilteredOut(nil))
}

// setupStoreForLockTest brings up an Aerospike container and returns a store wired to it,
// plus a raw client for asserting on the lock record directly.
func setupStoreForLockTest(t *testing.T) (*Store, *uaerospike.Client) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	container, err := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport("test"))
	if err != nil {
		t.Skipf("Skipping: Aerospike container not available (%v)", err)
	}

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)

	t.Cleanup(func() {
		client.Close()
	})

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=test&externalStore=file://./data/external&block_retention=100", host, port))
	require.NoError(t, err)

	store, err := New(ctx, logger, tSettings, aeroURL)
	require.NoError(t, err)

	return store, client
}

// lockTokenOf reads the token currently stored on the lock record for txHash, and reports
// whether the record exists at all.
func lockTokenOf(t *testing.T, store *Store, client *uaerospike.Client, txHash *chainhash.Hash) (string, bool) {
	t.Helper()

	key, err := aerospike.NewKey(store.namespace, store.setName, calculateLockKey(txHash))
	require.NoError(t, err)

	record, err := client.Get(util.GetAerospikeReadPolicy(store.settings), key)
	if err != nil {
		require.True(t, isKeyNotFound(err), "unexpected error reading lock record: %v", err)
		return "", false
	}

	if record == nil {
		return "", false
	}

	token, ok := record.Bins["lock_token"].(string)
	require.True(t, ok, "lock record should carry a string lock_token bin, got %v", record.Bins["lock_token"])

	return token, true
}

// TestLockFence exercises the creation lock's acquire/release cycle against a real server.
// The case that matters is the last one: it reproduces the sequence the fence exists for -
// a writer stalls past its lock's TTL, a second writer takes a fresh lock, and the first
// writer's deferred release must not evict it. Before the fence that release deleted by key
// alone and would have let a third writer in while the second still believed it held the lock.
func TestLockFence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	store, client := setupStoreForLockTest(t)

	// Distinct hashes per sub-test so they cannot interfere with one another.
	hashFor := func(b byte) *chainhash.Hash {
		var raw [32]byte
		raw[0] = b

		hash, err := chainhash.NewHash(raw[:])
		require.NoError(t, err)

		return hash
	}

	t.Run("acquire stores a token and release with it removes the lock", func(t *testing.T) {
		txHash := hashFor(1)

		lockKey, token, err := store.acquireLock(txHash, 5)
		require.NoError(t, err)
		require.NotEmpty(t, token)

		stored, exists := lockTokenOf(t, store, client, txHash)
		require.True(t, exists, "acquireLock should have written a lock record")
		require.Equal(t, token, stored, "the token handed to the caller is the one on the record")

		require.NoError(t, store.releaseLock(lockKey, token))

		_, exists = lockTokenOf(t, store, client, txHash)
		require.False(t, exists, "releasing with the right token removes the lock")
	})

	t.Run("acquiring a lock that is already held reports the transaction exists", func(t *testing.T) {
		txHash := hashFor(2)

		lockKey, token, err := store.acquireLock(txHash, 5)
		require.NoError(t, err)

		_, _, err = store.acquireLock(txHash, 5)
		require.Error(t, err, "the second acquisition must not be handed the lock")

		var teranodeErr *errors.Error
		require.True(t, errors.As(err, &teranodeErr) && teranodeErr.Is(errors.ErrTxExists),
			"a held lock should surface as ErrTxExists, got: %v", err)

		require.NoError(t, store.releaseLock(lockKey, token))
	})

	t.Run("releasing a lock that is already gone is not an error", func(t *testing.T) {
		txHash := hashFor(3)

		lockKey, token, err := store.acquireLock(txHash, 5)
		require.NoError(t, err)

		require.NoError(t, store.releaseLock(lockKey, token))
		require.NoError(t, store.releaseLock(lockKey, token),
			"a second release must be a no-op, not an error - the deferred release runs regardless")
	})

	t.Run("two acquisitions of the same key never share a token", func(t *testing.T) {
		txHash := hashFor(4)

		lockKey, first, err := store.acquireLock(txHash, 5)
		require.NoError(t, err)
		require.NoError(t, store.releaseLock(lockKey, first))

		lockKey, second, err := store.acquireLock(txHash, 5)
		require.NoError(t, err)
		require.NotEqual(t, first, second,
			"if two acquisitions could produce the same token the fence could not tell them apart")

		require.NoError(t, store.releaseLock(lockKey, second))
	})

	// The defect this whole change exists to prevent.
	t.Run("a stale release cannot evict the lock a later writer holds", func(t *testing.T) {
		txHash := hashFor(5)

		// Writer A takes the lock.
		lockKey, tokenA, err := store.acquireLock(txHash, 5)
		require.NoError(t, err)

		// Writer A stalls and its lock expires. Deleting the record is the same end state
		// the TTL produces, without holding the test open for the TTL to elapse.
		_, err = client.Delete(util.GetAerospikeWritePolicy(store.settings, 0), lockKey)
		require.NoError(t, err)

		// Writer B, finding no lock, takes a fresh one.
		_, tokenB, err := store.acquireLock(txHash, 5)
		require.NoError(t, err)
		require.NotEqual(t, tokenA, tokenB)

		// Writer A finally returns and runs its deferred release. It must be a no-op:
		// reported as success, because there is nothing here for A to clean up, but it
		// must leave B's lock standing.
		require.NoError(t, store.releaseLock(lockKey, tokenA),
			"a stale release is not a failure - there is simply nothing of ours left to delete")

		stored, exists := lockTokenOf(t, store, client, txHash)
		require.True(t, exists, "writer B's lock must survive writer A's stale release")
		require.Equal(t, tokenB, stored, "the surviving lock must still be writer B's")

		require.NoError(t, store.releaseLock(lockKey, tokenB))
	})
}
