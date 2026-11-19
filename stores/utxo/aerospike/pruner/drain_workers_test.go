package pruner

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

func TestDrainReadyCarryKeepsBlobsWithTheirMasters(t *testing.T) {
	s, client := newFlushTestService(t)
	s.chunkSize = 1
	ctx := context.Background()
	deletedChild := chainhash.HashH([]byte("ready-deleted-blob"))
	heldChild := chainhash.HashH([]byte("ready-held-blob"))
	deletedKey := putRecord(t, client, &deletedChild, aerospike.BinMap{fields.TxID.String(): deletedChild.CloneBytes()})
	heldKey := putRecord(t, client, &heldChild, aerospike.BinMap{
		fields.TxID.String(): heldChild.CloneBytes(), fields.DeleteAtHeight.String(): nil,
	})
	for _, child := range []chainhash.Hash{deletedChild, heldChild} {
		require.NoError(t, s.external.Set(ctx, child.CloneBytes(), fileformat.FileTypeTx, []byte("blob")))
	}
	carry := &deleteCarry{
		blockHeight: testPruneHeight,
		deletions: []*pendingDeletion{
			{txHash: &deletedChild, keys: []*aerospike.Key{deletedKey}},
			{txHash: &heldChild, keys: []*aerospike.Key{heldKey}},
		},
		// Reversing the order ensures ownership is matched by transaction,
		// rather than by a file's position in the pending slice.
		files: []*externalFileInfo{
			{txHash: &heldChild, fileType: fileformat.FileTypeTx},
			{txHash: &deletedChild, fileType: fileformat.FileTypeTx},
		},
	}
	deleted, held, err := s.drainReadyCarry(ctx, carry)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.Equal(t, 1, held)
	for _, tc := range []struct {
		child  chainhash.Hash
		key    *aerospike.Key
		exists bool
	}{{deletedChild, deletedKey, false}, {heldChild, heldKey, true}} {
		exists, getErr := client.Exists(nil, tc.key)
		require.NoError(t, getErr)
		require.Equal(t, tc.exists, exists)
		blobExists, blobErr := s.external.Exists(ctx, tc.child.CloneBytes(), fileformat.FileTypeTx)
		require.NoError(t, blobErr)
		require.Equal(t, tc.exists, blobExists, "a retained master must keep its own blob")
	}
}

func TestDrainReadyCarryReportsOrphanBlobAndDrainsAcknowledgedDeletes(t *testing.T) {
	s, client := newFlushTestService(t)
	ctx := context.Background()
	child := chainhash.HashH([]byte("ready-master-with-orphan"))
	orphan := chainhash.HashH([]byte("ready-orphan-blob"))
	key := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})
	for _, hash := range []chainhash.Hash{child, orphan} {
		require.NoError(t, s.external.Set(ctx, hash.CloneBytes(), fileformat.FileTypeTx, []byte("blob")))
	}
	carry := &deleteCarry{
		blockHeight: testPruneHeight,
		deletions:   []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{key}}},
		files: []*externalFileInfo{
			{txHash: &orphan, fileType: fileformat.FileTypeTx},
			{txHash: &child, fileType: fileformat.FileTypeTx},
		},
	}
	deleted, held, err := s.drainReadyCarry(ctx, carry)
	require.ErrorContains(t, err, "blob has no carried master")
	require.Equal(t, 1, deleted, "an orphan blob must not discard acknowledged record deletes")
	require.Zero(t, held)
	exists, getErr := client.Exists(nil, key)
	require.NoError(t, getErr)
	require.False(t, exists)
	for _, hash := range []chainhash.Hash{child, orphan} {
		blobExists, blobErr := s.external.Exists(ctx, hash.CloneBytes(), fileformat.FileTypeTx)
		require.NoError(t, blobErr)
		require.Equal(t, hash == orphan, blobExists, "only the confirmed master's blob may be deleted")
	}
}

func TestPruneCombinesReadyWorkerDeletes(t *testing.T) {
	s, client := newFlushTestService(t)
	const workers, children, batchSize = 16, 64, 8
	s.chunkSize = batchSize

	for round, tc := range []struct {
		name                   string
		cancel, refuse, poison bool
	}{
		{name: "normal"},
		{name: "canceled", cancel: true},
		{name: "delete refused", refuse: true},
		{name: "marker refused", poison: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			updates := make([]map[string]*parentUpdateInfo, workers)
			deletions := make([][]*pendingDeletion, workers)
			parentKeys := make([]*aerospike.Key, children)
			childKeys := make([]*aerospike.Key, children)
			hashes := make([]chainhash.Hash, children)
			for i := range children {
				parent := chainhash.HashH([]byte(fmt.Sprintf("ready-parent-%d-%d", round, i)))
				hashes[i] = chainhash.HashH([]byte(fmt.Sprintf("ready-child-%d-%d", round, i)))
				bins := spentBy(&hashes[i])
				if tc.poison && i == 0 {
					bins[fields.DeletedChildren.String()] = "invalid-map"
				}
				parentKeys[i] = putRecord(t, client, &parent, bins)
				childKeys[i] = putRecord(t, client, &hashes[i], aerospike.BinMap{
					fields.TxID.String(): hashes[i].CloneBytes(),
				})
				worker := i % workers
				if updates[worker] == nil {
					updates[worker] = make(map[string]*parentUpdateInfo)
				}
				require.NoError(t, s.addParentUpdatesForInput(updates[worker], &parent, 0, &hashes[i]))
				deletions[worker] = append(deletions[worker], &pendingDeletion{
					txHash: &hashes[i], keys: []*aerospike.Key{childKeys[i]},
				})
			}

			var calls, markerBatches, largestBatch atomic.Int64
			var entered, acknowledged, queued sync.WaitGroup
			entered.Add(workers)
			acknowledged.Add(workers)
			queued.Add(workers)
			var afterMarkers sync.Once
			var afterMarkerErr error
			s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
				calls.Add(1)
				for {
					previous := largestBatch.Load()
					if int64(len(records)) <= previous || largestBatch.CompareAndSwap(previous, int64(len(records))) {
						break
					}
				}
				initialMarker := false
				for _, record := range records {
					if _, ok := record.(*aerospike.BatchWrite); ok {
						initialMarker = markerBatches.Add(1) <= workers
						break
					}
				}
				if initialMarker {
					entered.Done()
					entered.Wait()
				}
				err := client.BatchOperate(policy, records)
				if initialMarker {
					acknowledged.Done()
					acknowledged.Wait()
					afterMarkers.Do(func() {
						if tc.refuse {
							afterMarkerErr = client.PutBins(nil, childKeys[0], aerospike.NewBin(fields.DeleteAtHeight.String(), nil))
						}
						if tc.cancel {
							cancel()
						}
					})
					if afterMarkerErr != nil {
						return afterMarkerErr
					}
				}
				return err
			}
			// Release results only once every worker has a completed tail. This
			// pins the batching bound without depending on goroutine scheduling.
			s.partitionWorkerFn = func(ctx context.Context, height uint32, start, _ int, drainTo *deleteCarry) (int64, int64, error) {
				worker := start / (4096 / workers)
				carry := &deleteCarry{blockHeight: height}
				deleted, held, err := s.flushChunk(ctx, carry, updates[worker], deletions[worker], nil)
				drainTo.addWorkerTail(carry)
				queued.Done()
				queued.Wait()
				if tc.cancel && err == nil {
					err = ctx.Err()
				}
				return int64(deleted), int64(held), err
			}

			deleted, err := s.PruneWithPartitions(ctx, testPruneHeight, "ready-workers", workers)
			if tc.cancel {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
				want := int64(children)
				if tc.refuse || tc.poison {
					want--
				}
				require.Equal(t, want, deleted)
			}
			if !tc.cancel && !tc.refuse && !tc.poison {
				require.EqualValues(t, 24, calls.Load(), "16 marker batches and eight bounded delete batches")
			}
			require.LessOrEqual(t, largestBatch.Load(), int64(batchSize))
			for i, key := range childKeys {
				exists, getErr := client.Exists(nil, key)
				require.NoError(t, getErr)
				held := i == 0 && (tc.refuse || tc.poison)
				require.Equal(t, held, exists, "child %d", i)
				if tc.poison && i == 0 {
					continue
				}
				require.Equal(t, !held, hasMarker(t, client, parentKeys[i], &hashes[i]), "marker %d", i)
			}
		})
	}
}

func TestDrainReadyCarryDoesNotFragmentLargeWorkerTails(t *testing.T) {
	s, client := newFlushTestService(t)
	s.chunkSize = 2
	ready := &deleteCarry{blockHeight: testPruneHeight}
	for worker := 0; worker < 2; worker++ {
		tail := &deleteCarry{blockHeight: testPruneHeight}
		for i := 0; i < 6; i++ {
			child := chainhash.HashH([]byte(fmt.Sprintf("large-tail-%d-%d", worker, i)))
			key := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})
			tail.deletions = append(tail.deletions, &pendingDeletion{txHash: &child, keys: []*aerospike.Key{key}})
		}
		ready.addWorkerTail(tail)
	}
	var calls atomic.Int64
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		calls.Add(1)
		return client.BatchOperate(policy, records)
	}
	deleted, held, err := s.drainReadyCarry(context.Background(), ready)
	require.NoError(t, err)
	require.Equal(t, 12, deleted)
	require.Zero(t, held)
	require.EqualValues(t, 2, calls.Load(), "two existing six-record tails must not become six smaller batches")
}
