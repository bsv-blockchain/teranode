package txmetacache

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestTxMetaCache_DeleteIsCacheOnly_KnownContractGap pins TxMetaCache.Delete's
// current behaviour AS A KNOWN GAP, not as correct behaviour.
//
// utxo.Store.Delete is declared as "removes a UTXO and its associated metadata
// from the store". TxMetaCache satisfies utxo.Store but implements Delete as a
// pure cache eviction that never reaches the underlying record, while its
// SpendAndCreate and Unspend DO delegate. A caller that trusts the nil return
// therefore believes a record is gone when it is not — and if it then frees that
// record's inputs, a competing spend can take them while the record survives and
// can still be lifted into a mining template.
//
// The one-line remedy (make Delete delegate) is NOT safe to apply on its own:
// Delete is used as an eviction primitive by six internal callers — SetMinedMulti,
// setMinedInCacheParallel, FreezeUTXOs, UnFreezeUTXOs, ReAssignUTXO,
// SetConflicting — and by two external ones in subtreevalidation
// (DelTxMetaCache / DelTxMetaCacheMulti, via the txMetaCacheOps interface), the
// first of which is driven from the txmeta Kafka DELETE hot path. Making Delete
// delegate without first repointing all eight at an eviction-only method would
// erase UTXO records as blocks are processed and on every txmeta DELETE message.
//
// The correct sequence is: add an exported eviction-only method, repoint all eight
// callers AND the txMetaCacheOps interface, land that, and only then make Delete
// delegate. Until that happens, callers needing a durable delete must not rely on
// this decorator, and any caller whose correctness depends on Delete having
// deleted must verify with a read-back rather than trust the returned nil.
//
// FOLLOW-UP: this needs its own change with its own review, because it is a
// cross-service interface change on a Kafka hot path. No tracking issue number is
// cited here yet because the issue has not been filed at the time of writing; the
// caller inventory above is deliberately complete enough to act on without one, and
// the issue reference should be added to this comment when it is raised. A fix that
// misses any of the eight callers erases UTXO records, so treat the inventory as the
// checklist.
//
// This test exists so the trap is discoverable in code by whoever greps Delete,
// rather than only in a review thread.
func TestTxMetaCache_DeleteIsCacheOnly_KnownContractGap(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///txmetacache_delete_contract")
	require.NoError(t, err)

	underlying, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	c, err := NewTxMetaCache(ctx, settings.NewSettings(), logger, underlying, Unallocated)
	require.NoError(t, err)

	cache, ok := c.(*TxMetaCache)
	require.True(t, ok)

	txHash := tests.Tx.TxIDChainHash()

	// The record exists in the underlying store and is readable through the cache.
	_, _, err = underlying.SpendAndCreate(ctx, tests.Tx, 100, utxo.WithCreateOnly())
	require.NoError(t, err)

	require.NoError(t, cache.GetMeta(ctx, txHash, &meta.Data{}), "precondition: the record is readable through the decorator")

	// Delete reports success...
	require.NoError(t, cache.Delete(ctx, txHash))

	// ...but the underlying record is still there. This is the gap.
	require.NoError(t, underlying.GetMeta(ctx, txHash, &meta.Data{}),
		"TxMetaCache.Delete is cache-only today: the underlying record survives a successful Delete")

	// And it is still readable straight back through the decorator, because the
	// read falls through to the underlying store on a cache miss. So a caller
	// cannot even detect the gap by re-reading through the decorator it deleted
	// through — the read-back must be able to observe the truth, which is why a
	// caller relying on Delete must not assume a nil return means "gone".
	require.NoError(t, cache.GetMeta(ctx, txHash, &meta.Data{}),
		"a cache-only delete leaves the record readable through the decorator via fall-through")

	// The underlying store, by contrast, honours the contract.
	require.NoError(t, underlying.Delete(ctx, txHash))
	require.ErrorIs(t, underlying.GetMeta(ctx, txHash, &meta.Data{}), errors.ErrTxNotFound,
		"the real store's Delete does remove the record - the divergence is the decorator's")
}

// deleteCompleteFailingStore decorates a REAL utxo.Store and overrides exactly one
// method: DeleteComplete always fails. It models the half-completed cascade — the
// backing store removes the master record first, so a failure can mean the record is
// already gone — which is the state a surviving cache entry would misreport.
type deleteCompleteFailingStore struct {
	utxo.Store

	err error
}

func (s *deleteCompleteFailingStore) DeleteComplete(_ context.Context, _ *chainhash.Hash) error {
	return s.err
}

// TestTxMetaCache_DeleteComplete_EvictsEvenWhenTheDurableDeleteFails pins the
// eviction as unconditional.
//
// DeleteComplete used to return early on a durable failure, keeping the cache entry.
// Because the backing cascade removes the master record first, that failure can mean
// the record is already gone while the decorator still answers "present" — and a
// caller that reads this cache for presence to decide whether it may free a
// transaction's inputs (the validator's shed unwind does exactly that) would then be
// sent down the record-still-present arm, leaving the inputs spent for a record that
// no longer exists. A miss is never less correct than a stale hit: GetMeta falls
// through to the underlying store, so the only cost of evicting early is one
// read-through.
func TestTxMetaCache_DeleteComplete_EvictsEvenWhenTheDurableDeleteFails(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///txmetacache_delete_complete_evicts")
	require.NoError(t, err)

	underlying, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	failing := &deleteCompleteFailingStore{
		Store: underlying,
		err:   errors.NewStorageError("complete delete failed after the master record was removed"),
	}

	c, err := NewTxMetaCache(ctx, settings.NewSettings(), logger, failing, Unallocated)
	require.NoError(t, err)

	cache, ok := c.(*TxMetaCache)
	require.True(t, ok)

	hash := testHash(17)
	require.NoError(t, cache.SetCache(&hash, testMeta()))

	_, found := cache.GetMetaCached(ctx, hash)
	require.True(t, found, "precondition: the entry is in the cache")

	require.Error(t, cache.DeleteComplete(ctx, &hash), "the durable failure is still propagated to the caller")

	_, found = cache.GetMetaCached(ctx, hash)
	require.False(t, found, "the entry must be evicted even though the durable delete failed")
}
