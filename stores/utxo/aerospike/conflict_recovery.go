package aerospike

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
)

const recoveryChunkSize = 64 * 1024

// Checklists can exceed an Aerospike record. Immutable content-addressed chunks
// are published before the root reference, so a reader never sees a partial
// checklist. The chunks share the WAL's non-expiring lifetime.
func (s *Store) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	if err := r.Validate(); err != nil {
		return err
	}
	immutable := *r
	immutable.Phase = utxo.ConflictRecoveryPrepared
	data, err := utxo.EncodeConflictRecovery(&immutable)
	if err != nil {
		return err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	count := (len(data) + recoveryChunkSize - 1) / recoveryChunkSize
	key, err := as.NewKey(s.namespace, s.conflictWALSet(), id[:])
	if err != nil {
		return err
	}
	record, err := s.client.Get(nil, key, "recoveryRefs")
	if err != nil {
		return err
	}
	if record == nil {
		return errors.NewStorageError("missing conflict WAL intent")
	}
	refs, _ := record.Bins["recoveryRefs"].([]interface{})
	ref := digest + ":" + strconv.Itoa(count)
	found := false
	for _, value := range refs {
		if value == ref {
			found = true
		}
	}
	policy := util.GetAerospikeWritePolicy(s.settings, 0)
	policy.RecordExistsAction = as.UPDATE_ONLY
	if !found {
		refs = append(refs, ref)
		// Record cleanup ownership before creating any chunk. A crash at any
		// point leaves every chunk discoverable from the WAL root.
		if err = s.client.PutBins(policy, key, as.NewBin("recoveryRefs", refs)); err != nil {
			return err
		}
	}
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunkKey, err := s.recoveryChunkKey(id, digest, i)
		if err != nil {
			return err
		}
		end := min((i+1)*recoveryChunkSize, len(data))
		if err := s.client.PutBins(util.GetAerospikeWritePolicy(s.settings, 0), chunkKey, as.NewBin("data", data[i*recoveryChunkSize:end])); err != nil {
			return err
		}
	}
	return s.client.PutBins(policy, key, as.NewBin("recovery", digest), as.NewBin("recoveryN", count), as.NewBin("recoveryPhase", r.Phase))
}

func (s *Store) recoveryChunkKey(id chainhash.Hash, digest string, index int) (*as.Key, error) {
	return as.NewKey(s.namespace, s.conflictWALSet()+"_data", id.String()+":"+digest+":"+strconv.Itoa(index))
}

// deleteRecoveryChunks is restartable. The completed root is retained until
// all listed chunks have been deleted, including interrupted unpublished saves.
func (s *Store) deleteRecoveryChunks(ctx context.Context, id chainhash.Hash) error {
	key, err := as.NewKey(s.namespace, s.conflictWALSet(), id[:])
	if err != nil {
		return err
	}
	record, err := s.client.Get(nil, key, "recoveryRefs")
	if err != nil {
		if err.Matches(types.KEY_NOT_FOUND_ERROR) {
			return nil
		}
		return err
	}
	if record == nil {
		return nil
	}
	policy := util.GetAerospikeWritePolicy(s.settings, 0)
	policy.RecordExistsAction = as.UPDATE_ONLY
	if err = s.client.PutBins(policy, key, as.NewBin("recoveryDone", true)); err != nil {
		return err
	}
	refs, _ := record.Bins["recoveryRefs"].([]interface{})
	for _, value := range refs {
		ref, ok := value.(string)
		if !ok {
			return errors.NewStorageError("invalid conflict recovery cleanup reference")
		}
		parts := strings.Split(ref, ":")
		if len(parts) != 2 {
			return errors.NewStorageError("invalid conflict recovery cleanup reference")
		}
		count, err := strconv.Atoi(parts[1])
		if err != nil || count < 1 {
			return errors.NewStorageError("invalid conflict recovery cleanup count")
		}
		for i := 0; i < count; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunkKey, err := s.recoveryChunkKey(id, parts[0], i)
			if err != nil {
				return err
			}
			if _, err = s.client.Delete(util.GetAerospikeWritePolicy(s.settings, 0), chunkKey); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) LoadConflictRecovery(ctx context.Context, id chainhash.Hash) (*utxo.ConflictRecovery, error) {
	key, err := as.NewKey(s.namespace, s.conflictWALSet(), id[:])
	if err != nil {
		return nil, err
	}
	record, err := s.client.Get(nil, key, "recovery", "recoveryN", "recoveryPhase", "recoveryDone")
	if err != nil {
		if err.Matches(types.KEY_NOT_FOUND_ERROR) {
			return nil, nil
		}
		return nil, err
	}
	if record != nil && record.Bins["recoveryDone"] == true {
		if err := utxo.CompleteConflictIntent(ctx, s, id); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if record != nil && record.Bins["recoveryPhase"] == utxo.ConflictRecoveryPlanning {
		return &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning}, nil
	}
	if record == nil || record.Bins["recovery"] == nil {
		return nil, nil
	}
	digest, ok := record.Bins["recovery"].(string)
	count, countOK := record.Bins["recoveryN"].(int)
	if !ok || !countOK || count <= 0 {
		return nil, errors.NewStorageError("invalid conflict recovery reference")
	}
	var data []byte
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunkKey, err := s.recoveryChunkKey(id, digest, i)
		if err != nil {
			return nil, err
		}
		chunk, err := s.client.Get(nil, chunkKey, "data")
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			return nil, errors.NewStorageError("missing conflict recovery chunk")
		}
		b, ok := chunk.Bins["data"].([]byte)
		if !ok {
			return nil, errors.NewStorageError("invalid conflict recovery chunk")
		}
		data = append(data, b...)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != digest {
		return nil, errors.NewStorageError("conflict recovery checksum mismatch")
	}
	r, decodeErr := utxo.DecodeConflictRecovery(data)
	if decodeErr != nil {
		return nil, decodeErr
	}
	phase, ok := record.Bins["recoveryPhase"].(string)
	if !ok {
		return nil, errors.NewStorageError("missing conflict recovery phase")
	}
	r.Phase = phase
	return r, r.Validate()
}

func (s *Store) RestoreConflictSpends(ctx context.Context, spends []*utxo.Spend, height uint32) error {
	for _, sp := range spends {
		if err := ctx.Err(); err != nil {
			return err
		}
		if sp == nil || sp.SpendingData == nil {
			return errors.NewProcessingError("invalid conflict restoration spend")
		}
		group := completion.NewGroup(1)
		item := &batchSpend{spend: sp, blockHeight: height, group: group, ignoreConflicting: true, ignoreLocked: true, requireParentOutput: true}
		// Direct synchronous dispatch avoids an outstanding queued write after rollback.
		s.sendSpendBatchLua([]*batchSpend{item})
		if err := group.Wait(context.Background(), 0); err != nil {
			return err
		}
		if sp.Err != nil {
			return sp.Err
		}
	}
	return nil
}

// Aerospike Unspend does not acquire parent locks. Do not release boolean locks
// owned by validation while recovering a conflict operation (issue 1291).
func (s *Store) SetConflictLocks(_ context.Context, _ chainhash.Hash, _ []chainhash.Hash, _ bool) error {
	return nil
}
