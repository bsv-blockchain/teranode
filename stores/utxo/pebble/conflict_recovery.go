package pebble

import (
	"bytes"
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/cockroachdb/pebble/v2"
)

// recoveryKey holds a forward operation's checklist beside its intent record, so the intent and
// its checklist are written and removed in one batch.
func recoveryKey(id []byte) []byte {
	return append([]byte{prefixRecovery}, id...)
}

// ownerKey names the conflict operation that holds a parent's lock. stageMaster removes it in
// the same batch as any change that clears the lock, so an ordinary writer cannot leave a stale
// owner behind.
func ownerKey(hash []byte) []byte {
	return append([]byte{prefixLockOwner}, hash...)
}

func (s *Store) BeginConflictIntent(ctx context.Context, intent utxo.ConflictIntent) error {
	id := intent.IntentID()

	var recovery []byte

	if intent.Recovery != nil {
		var err error

		if recovery, err = utxo.EncodeConflictRecovery(intent.Recovery); err != nil {
			return err
		}
	}

	unlock := s.lockStripes(id[:])
	defer unlock()

	if _, err := s.getValue(intentKey(id[:])); err == nil {
		return nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return errors.NewStorageError("pebble: failed to read conflict intent %s", id, err)
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := batch.Set(intentKey(id[:]), encodeIntent(intent), nil); err != nil {
		return errors.NewStorageError("pebble: failed to stage conflict intent %s", id, err)
	}

	if recovery != nil {
		if err := batch.Set(recoveryKey(id[:]), recovery, nil); err != nil {
			return errors.NewStorageError("pebble: failed to stage conflict recovery %s", id, err)
		}
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to record conflict intent %s", id, err)
	}

	return nil
}

func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	unlock := s.lockStripes(intentID[:])
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := batch.Delete(intentKey(intentID[:]), nil); err != nil {
		return errors.NewStorageError("pebble: failed to stage removal of conflict intent %s", intentID, err)
	}

	if err := batch.Delete(recoveryKey(intentID[:]), nil); err != nil {
		return errors.NewStorageError("pebble: failed to stage removal of conflict recovery %s", intentID, err)
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to remove conflict intent %s", intentID, err)
	}

	return nil
}

func (s *Store) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	data, err := utxo.EncodeConflictRecovery(r)
	if err != nil {
		return err
	}

	unlock := s.lockStripes(id[:])
	defer unlock()

	if _, err = s.getValue(intentKey(id[:])); err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return errors.NewStorageError("pebble: conflict recovery intent %s is missing", id)
		}

		return errors.NewStorageError("pebble: failed to read conflict intent %s", id, err)
	}

	if err = s.setDirect(recoveryKey(id[:]), data); err != nil {
		return errors.NewStorageError("pebble: failed to save conflict recovery %s", id, err)
	}

	return nil
}

func (s *Store) LoadConflictRecovery(ctx context.Context, id chainhash.Hash) (*utxo.ConflictRecovery, error) {
	data, err := s.getValue(recoveryKey(id[:]))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, errors.NewStorageError("pebble: failed to load conflict recovery %s", id, err)
	}

	return utxo.DecodeConflictRecovery(data)
}

// RestoreConflictSpends writes every record in one batch, so a refused record leaves the store
// as it was. The conflicting and locked marks are waived, because conflict resolution sets them
// itself.
func (s *Store) RestoreConflictSpends(ctx context.Context, spends []*utxo.Spend, height uint32) error {
	if len(spends) == 0 {
		return nil
	}

	for _, sp := range spends {
		if sp == nil || sp.TxID == nil || sp.SpendingData == nil || sp.SpendingData.TxID == nil {
			return errors.NewProcessingError("pebble: invalid conflict restoration spend")
		}
	}

	if err := ctx.Err(); err != nil {
		return errors.NewContextCanceledError("pebble: conflict restore canceled", err)
	}

	groups := groupByParent(spends)
	hashes := make([][]byte, len(groups))

	for i, g := range groups {
		hashes[i] = g.hash[:]
	}

	unlock := s.lockStripes(hashes...)
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	ig := utxo.IgnoreFlags{IgnoreConflicting: true, IgnoreLocked: true}

	for _, g := range groups {
		if err := s.stageSpendGroup(batch, g, height, ig); err != nil {
			for _, sp := range g.spends {
				if sp.Err == nil {
					continue
				}

				if sp.ConflictingTxID != nil {
					return errors.NewProcessingError("pebble: %s:%d is spent by %s, not by the recorded %s",
						sp.TxID, sp.Vout, sp.ConflictingTxID, sp.SpendingData.TxID, sp.Err)
				}

				return errors.NewProcessingError("pebble: %s:%d cannot be restored", sp.TxID, sp.Vout, sp.Err)
			}

			return errors.NewProcessingError("pebble: conflict restore failed for %s", g.hash, err)
		}
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to commit conflict restore", err)
	}

	return nil
}

func (s *Store) lockOwner(hash []byte) ([]byte, error) {
	owner, err := s.getValue(ownerKey(hash))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, errors.NewStorageError("pebble: failed to read lock owner of %x", hash, err)
	}

	return owner, nil
}

func (s *Store) stageUnlockForOwner(batch *pebble.Batch, h *chainhash.Hash, m *masterRecord) error {
	old := *m
	m.flags &^= flagLocked

	if s.dahEligible(m) {
		m.deleteAtHeight = int64(s.GetBlockHeight()) + 1 + s.retention()
	}

	return s.stageMaster(batch, h[:], &old, m)
}

// SetConflictLocks takes or releases only locks this operation owns. A lock held by another
// holder, or by no recorded owner, is never released. Every parent changes in one batch.
func (s *Store) SetConflictLocks(ctx context.Context, id chainhash.Hash, parents []chainhash.Hash, locked bool) error {
	lockHashes := make([][]byte, len(parents))
	for i := range parents {
		lockHashes[i] = parents[i][:]
	}

	unlock := s.lockStripes(lockHashes...)
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for i := range parents {
		h := parents[i]

		m, err := s.getMaster(&h)
		if err != nil {
			return err
		}

		owner, err := s.lockOwner(h[:])
		if err != nil {
			return err
		}

		ownedByUs := bytes.Equal(owner, id[:])

		if locked {
			if m.flags&flagLocked != 0 && !ownedByUs {
				return errors.NewProcessingError("pebble: conflict operation %s does not own parent lock %s", id, h)
			}

			old := *m
			m.flags |= flagLocked
			m.deleteAtHeight = 0

			if err = s.stageMaster(batch, h[:], &old, m); err != nil {
				return err
			}

			if err = batch.Set(ownerKey(h[:]), id[:], nil); err != nil {
				return errors.NewStorageError("pebble: failed to stage lock owner for %s", h, err)
			}

			continue
		}

		if m.flags&flagLocked == 0 {
			continue
		}

		if !ownedByUs {
			return errors.NewProcessingError("pebble: conflict operation %s does not own parent lock %s", id, h)
		}

		if err = s.stageUnlockForOwner(batch, &h, m); err != nil {
			return err
		}
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to commit conflict locks", err)
	}

	return nil
}

// ReleaseConflictLocks releases the parent locks this operation still owns and leaves every
// other lock, and every absent parent, as it is.
func (s *Store) ReleaseConflictLocks(ctx context.Context, id chainhash.Hash, parents []chainhash.Hash) error {
	lockHashes := make([][]byte, len(parents))
	for i := range parents {
		lockHashes[i] = parents[i][:]
	}

	unlock := s.lockStripes(lockHashes...)
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for i := range parents {
		h := parents[i]

		owner, err := s.lockOwner(h[:])
		if err != nil {
			return err
		}

		if !bytes.Equal(owner, id[:]) {
			continue
		}

		m, err := s.getMaster(&h)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) {
				continue
			}

			return err
		}

		if m.flags&flagLocked == 0 {
			continue
		}

		if err = s.stageUnlockForOwner(batch, &h, m); err != nil {
			return err
		}
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to commit conflict lock release", err)
	}

	return nil
}

// SpendsMadeBy reads the outpoints from the stored inputs and answers from the parents' slots,
// so it does not depend on the transaction being stored in extended form. An input whose slot
// this transaction does not own, or whose parent has been pruned, is omitted.
func (s *Store) SpendsMadeBy(ctx context.Context, txHash chainhash.Hash) ([]*utxo.Spend, error) {
	snap, release, err := s.snapshot()
	if err != nil {
		return nil, err
	}

	defer release()

	inputsBlob, _, err := s.payloadBlobs(snap, &txHash)
	if err != nil {
		return nil, err
	}

	inputs, err := inputsFromBlob(inputsBlob)
	if err != nil {
		return nil, err
	}

	out := make([]*utxo.Spend, 0, len(inputs))

	for vin, input := range inputs {
		if err = ctx.Err(); err != nil {
			return nil, errors.NewContextCanceledError("pebble: spends-made-by canceled", err)
		}

		parent := *input.PreviousTxIDChainHash()

		m, err := getMasterFrom(snap, &parent)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) {
				continue
			}

			return nil, err
		}

		if input.PreviousTxOutIndex >= m.outputCount {
			continue
		}

		storedHash, storedSpend, err := s.readSlot(snap, &parent, m, input.PreviousTxOutIndex)
		if err != nil {
			return nil, err
		}

		current := unpackSpendingData(storedSpend)
		if current == nil || current.TxID == nil || !current.TxID.IsEqual(&txHash) || current.Vin != vin {
			continue
		}

		effective := storedHash

		if m.flags&flagHasOverrides != 0 {
			val, oErr := readValue(snap, overrideKey(parent[:], input.PreviousTxOutIndex))
			if oErr == nil {
				o, dErr := decodeOverride(val)
				if dErr != nil {
					return nil, dErr
				}

				if o.reassignedHash != nil {
					effective = o.reassignedHash
				}
			} else if !errors.Is(oErr, pebble.ErrNotFound) {
				return nil, errors.NewStorageError("pebble: failed to read override %s:%d", parent, input.PreviousTxOutIndex, oErr)
			}
		}

		utxoHash, err := chainhash.NewHash(effective)
		if err != nil {
			return nil, errors.NewStorageError("pebble: corrupt utxo hash for %s:%d", parent, input.PreviousTxOutIndex, err)
		}

		spender := txHash

		out = append(out, &utxo.Spend{
			TxID:         &parent,
			Vout:         input.PreviousTxOutIndex,
			UTXOHash:     utxoHash,
			SpendingData: spendpkg.NewSpendingData(&spender, vin),
		})
	}

	return out, nil
}

// Stamper returns NoStamp: a master record carries its block references from the moment the
// transaction is mined, so there is nothing for a deep pass to stamp.
func (s *Store) Stamper() pruner.Stamper {
	return pruner.NoStamp
}
