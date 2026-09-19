package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	pebbledb "github.com/cockroachdb/pebble/v2"
)

func txInpointsFromBlob(inputsBlob []byte) (*subtree.TxInpoints, error) {
	n := offsetBlobCount(inputsBlob)
	inputs := make([]*bt.Input, n)

	for i := 0; i < n; i++ {
		item, err := offsetBlobItem(inputsBlob, i)
		if err != nil {
			return nil, err
		}

		inputs[i] = &bt.Input{}
		if _, err = inputs[i].ReadFromExtended(bytes.NewReader(item)); err != nil {
			return nil, errors.NewTxInvalidError("pebble: could not read input %d", i, err)
		}
	}

	txInpoints, err := subtree.NewTxInpointsFromInputs(inputs)
	if err != nil {
		return nil, errors.NewProcessingError("pebble: failed to create tx inpoints from inputs", err)
	}

	return &txInpoints, nil
}

func (s *Store) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetCounterConflictingTxHashes(ctx, s, txHash, 0)
}

func (s *Store) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetConflictingChildren(ctx, s, txHash, s.settings.UtxoStore.ConflictingChildrenMaxNodes)
}

func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	affectedParentSpends := make([]*utxo.Spend, 0, len(txHashes))
	spendingTxHashes := make([]chainhash.Hash, 0, len(txHashes))

	// Discover the lock set first. A transaction's inputs never change once it is
	// created, so the parent hashes read here are stable; everything the batch
	// actually depends on is re-read under the lock below.
	lockHashes := make([][]byte, 0, len(txHashes)*2)

	for i := range txHashes {
		lockHashes = append(lockHashes, txHashes[i][:])

		md, err := s.Get(ctx, &txHashes[i], fields.Tx)
		if err != nil {
			return nil, nil, err
		}

		for _, input := range md.Tx.Inputs {
			lockHashes = append(lockHashes, input.PreviousTxIDChainHash()[:])
		}
	}

	unlock := s.lockStripes(lockHashes...)
	defer unlock()

	// One batch for every hash. Committing per hash left a partially applied
	// cascade behind on the first failure, with the conflicting flag and the DAH
	// stamped on some transactions and not others, and nothing to unwind it.
	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for i := range txHashes {
		txHash := txHashes[i]

		// Re-read under the lock. The BFS in MarkConflictingRecursively is driven
		// purely by spendingTxHashes below, so deriving it from a pre-lock snapshot
		// silently drops a spender committed in the window and terminates the
		// traversal early.
		md, err := s.Get(ctx, &txHash, fields.Tx, fields.Utxos)
		if err != nil {
			return nil, nil, err
		}

		m, err := s.getMaster(&txHash)
		if err != nil {
			return nil, nil, err
		}

		old := *m

		if value {
			m.flags |= flagConflicting

			if m.preserveUntil == 0 && m.deleteAtHeight == 0 && s.retention() > 0 {
				m.deleteAtHeight = int64(s.GetBlockHeight()) + 1 + s.retention()
			}
		} else {
			m.flags &^= flagConflicting
			m.deleteAtHeight = 0
		}

		if err = s.stageMaster(batch, txHash[:], &old, m); err != nil {
			return nil, nil, err
		}

		if value {
			createdAt := make([]byte, 8)
			putInt64(createdAt, md.CreatedAt)

			for _, input := range md.Tx.Inputs {
				parent := input.PreviousTxIDChainHash()
				_ = batch.Set(childrenKey(parent[:], txHash[:]), createdAt, nil)
				_ = batch.Set(childrenRevKey(txHash[:], parent[:]), nil, nil)
			}
		}

		for vin, input := range md.Tx.Inputs {
			utxoHash, err := util.UTXOHashFromInput(input)
			if err != nil {
				return nil, nil, err
			}

			affectedParentSpends = append(affectedParentSpends, &utxo.Spend{
				TxID:         input.PreviousTxIDChainHash(),
				Vout:         input.PreviousTxOutIndex,
				UTXOHash:     utxoHash,
				SpendingData: spendpkg.NewSpendingData(&txHash, vin),
			})
		}

		for _, sd := range md.SpendingDatas {
			if sd != nil && sd.TxID != nil && !sd.TxID.IsEqual(&subtree.FrozenBytesTxHash) {
				spendingTxHashes = append(spendingTxHashes, *sd.TxID)
			}
		}
	}

	if err := s.commit(batch); err != nil {
		return nil, nil, errors.NewStorageError("pebble: failed to commit SetConflicting", err)
	}

	return affectedParentSpends, spendingTxHashes, nil
}

func (s *Store) RemoveFromConflictingChildren(ctx context.Context, removals []utxo.ConflictingChildRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, r := range removals {
		if r.ParentHash == nil || r.ChildHash == nil {
			return errors.NewInvalidArgumentError("pebble: parent and child hash must be non-nil")
		}

		_ = batch.Delete(childrenKey(r.ParentHash[:], r.ChildHash[:]), nil)
		_ = batch.Delete(childrenRevKey(r.ChildHash[:], r.ParentHash[:]), nil)
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to commit RemoveFromConflictingChildren", err)
	}

	return nil
}

func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	lockHashes := make([][]byte, len(txHashes))
	for i := range txHashes {
		lockHashes[i] = txHashes[i][:]
	}

	unlock := s.lockStripes(lockHashes...)
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for i := range txHashes {
		h := txHashes[i]

		m, err := s.getMaster(&h)
		if err != nil {
			return err
		}

		old := *m

		if value {
			m.flags |= flagLocked
			m.deleteAtHeight = 0
		} else {
			m.flags &^= flagLocked

			if s.dahEligible(m) {
				m.deleteAtHeight = int64(s.GetBlockHeight()) + 1 + s.retention()
			}
		}

		if err = s.stageMaster(batch, h[:], &old, m); err != nil {
			return err
		}
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to commit SetLocked", err)
	}

	return nil
}

func (s *Store) RemoveBlockIDs(ctx context.Context, removals []utxo.BlockIDsRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	lockHashes := make([][]byte, 0, len(removals))

	for _, r := range removals {
		if r.TxHash == nil {
			return errors.NewInvalidArgumentError("pebble: txHash must be non-nil")
		}

		lockHashes = append(lockHashes, r.TxHash[:])
	}

	unlock := s.lockStripes(lockHashes...)
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, r := range removals {
		m, err := s.getMaster(r.TxHash)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) {
				continue
			}

			return err
		}

		old := *m

		for _, blockID := range r.BlockIDs {
			m.blockRefs = removeBlockRef(m.blockRefs, blockID)
		}

		if err = s.stageMaster(batch, r.TxHash[:], &old, m); err != nil {
			return err
		}
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to commit RemoveBlockIDs", err)
	}

	return nil
}

func encodeIntent(intent utxo.ConflictIntent) []byte {
	b := make([]byte, 0, 1+4+32+8+len(intent.TxHashes)*32)
	b = append(b, byte(len(intent.Kind)))
	b = append(b, []byte(intent.Kind)...)

	var hb [4]byte
	binary.LittleEndian.PutUint32(hb[:], intent.BlockHeight)
	b = append(b, hb[:]...)
	b = append(b, intent.BlockHash[:]...)

	var sb [8]byte
	binary.LittleEndian.PutUint64(sb[:], uint64(intent.StartedAt)) //nolint:gosec
	b = append(b, sb[:]...)

	for i := range intent.TxHashes {
		b = append(b, intent.TxHashes[i][:]...)
	}

	return b
}

func decodeIntent(b []byte) (utxo.ConflictIntent, error) {
	var intent utxo.ConflictIntent

	if len(b) < 1 {
		return intent, errors.NewStorageError("pebble: intent record too short")
	}

	kindLen := int(b[0])
	if len(b) < 1+kindLen+4+32+8 {
		return intent, errors.NewStorageError("pebble: intent record truncated")
	}

	pos := 1
	intent.Kind = utxo.ConflictIntentKind(b[pos : pos+kindLen])
	pos += kindLen
	intent.BlockHeight = binary.LittleEndian.Uint32(b[pos:])
	pos += 4
	copy(intent.BlockHash[:], b[pos:pos+32])
	pos += 32
	intent.StartedAt = int64(binary.LittleEndian.Uint64(b[pos:])) //nolint:gosec
	pos += 8

	rest := b[pos:]
	if len(rest)%32 != 0 {
		return intent, errors.NewStorageError("pebble: intent tx hashes truncated")
	}

	for off := 0; off < len(rest); off += 32 {
		intent.TxHashes = append(intent.TxHashes, chainhash.Hash(rest[off:off+32]))
	}

	return intent, nil
}

func (s *Store) BeginConflictIntent(ctx context.Context, intent utxo.ConflictIntent) error {
	id := intent.IntentID()

	if err := s.setDirect(intentKey(id[:]), encodeIntent(intent)); err != nil {
		return errors.NewStorageError("pebble: failed to record conflict intent %s", id, err)
	}

	return nil
}

func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	if err := s.deleteDirect(intentKey(intentID[:])); err != nil {
		return errors.NewStorageError("pebble: failed to remove conflict intent %s", intentID, err)
	}

	return nil
}

func (s *Store) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	lower, upper := prefixBounds([]byte{prefixIntent})

	iter, release, err := s.newIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, errors.NewStorageError("pebble: pending intents query failed", err)
	}

	defer release()

	var intents []utxo.ConflictIntent

	for iter.First(); iter.Valid(); iter.Next() {
		intent, err := decodeIntent(iter.Value())
		if err != nil {
			return nil, err
		}

		intents = append(intents, intent)
	}

	return intents, iter.Error()
}

func (s *Store) setOverridesFlag(batch *pebbledb.Batch, hash []byte, m *masterRecord, set bool) error {
	old := *m

	if set {
		m.flags |= flagHasOverrides
	} else {
		m.flags &^= flagHasOverrides
	}

	return s.stageMaster(batch, hash, &old, m)
}

// spendLockHashes returns the distinct parent hashes a spend set touches.
func spendLockHashes(spends []*utxo.Spend) [][]byte {
	hashes := make([][]byte, 0, len(spends))
	for _, sp := range spends {
		hashes = append(hashes, sp.TxID[:])
	}

	return hashes
}

func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	// Validate and write under one lock set. The precondition pass used to run
	// unlocked, so a spend committing between the passes left the slot both spent
	// and frozen — and the frozen sentinel then hid the real spender from the
	// conflict machinery for good. GetSpend acquires no locks, so it is safe here.
	unlock := s.lockStripes(spendLockHashes(spends)...)
	defer unlock()

	for _, sp := range spends {
		resp, err := s.GetSpend(ctx, sp)
		if err != nil {
			return err
		}

		if resp.Status == int(utxo.Status_NOT_FOUND) {
			return errors.NewTxNotFoundError("pebble: transaction %s not found", sp.TxID)
		}

		if resp.SpendingData != nil && !resp.SpendingData.TxID.IsEqual(&subtree.FrozenBytesTxHash) {
			var utxoHash chainhash.Hash
			if sp.UTXOHash != nil {
				utxoHash = *sp.UTXOHash
			}

			return errors.NewUtxoSpentError(*sp.TxID, sp.Vout, utxoHash, resp.SpendingData)
		}

		if resp.Status == int(utxo.Status_FROZEN) {
			return errors.NewUtxoFrozenError("pebble: transaction %s:%d already frozen", sp.TxID, sp.Vout)
		}
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	// getMaster reads the database rather than the batch, so cache each master to
	// keep repeated vouts of one transaction building on the same record.
	masters := make(map[chainhash.Hash]*masterRecord, len(spends))

	for _, sp := range spends {
		m, ok := masters[*sp.TxID]
		if !ok {
			var err error

			if m, err = s.getMaster(sp.TxID); err != nil {
				return err
			}

			masters[*sp.TxID] = m
		}

		if err := batch.Set(overrideKey(sp.TxID[:], sp.Vout), encodeOverride(&overrideRecord{frozen: true}), nil); err != nil {
			return errors.NewStorageError("pebble: failed to stage freeze for %s:%d", sp.TxID, sp.Vout, err)
		}

		if err := s.setOverridesFlag(batch, sp.TxID[:], m, true); err != nil {
			return err
		}
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to commit FreezeUTXOs", err)
	}

	return nil
}

func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	unlock := s.lockStripes(spendLockHashes(spends)...)
	defer unlock()

	for _, sp := range spends {
		resp, err := s.GetSpend(ctx, sp)
		if err != nil {
			return err
		}

		if resp.Status != int(utxo.Status_FROZEN) {
			return errors.NewUtxoFrozenError("pebble: transaction %s:%d is not frozen", sp.TxID, sp.Vout)
		}
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	// Track the vouts this call clears so the remaining-override count reflects
	// the whole batch, not just the one entry being deleted.
	cleared := make(map[chainhash.Hash]map[uint32]struct{}, len(spends))

	for _, sp := range spends {
		if cleared[*sp.TxID] == nil {
			cleared[*sp.TxID] = make(map[uint32]struct{})
		}

		cleared[*sp.TxID][sp.Vout] = struct{}{}

		_ = batch.Delete(overrideKey(sp.TxID[:], sp.Vout), nil)
	}

	for txID, vouts := range cleared {
		hash := txID

		remaining, err := s.countOverridesExcluding(hash[:], vouts)
		if err != nil {
			return err
		}

		if remaining > 0 {
			continue
		}

		m, err := s.getMaster(&hash)
		if err != nil {
			return err
		}

		if err = s.setOverridesFlag(batch, hash[:], m, false); err != nil {
			return err
		}
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to commit UnFreezeUTXOs", err)
	}

	return nil
}

func (s *Store) countOverridesExcluding(hash []byte, excludeVouts map[uint32]struct{}) (int, error) {
	lower, upper := prefixBounds(append([]byte{prefixOverride}, hash...))

	iter, release, err := s.newIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, errors.NewStorageError("pebble: override count failed", err)
	}

	defer release()

	count := 0

	for iter.First(); iter.Valid(); iter.Next() {
		if _, skip := excludeVouts[binaryBEUint32(iter.Key()[33:])]; skip {
			continue
		}

		count++
	}

	return count, iter.Error()
}

func (s *Store) ReAssignUTXO(ctx context.Context, oldUtxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	if newUtxo == nil || newUtxo.UTXOHash == nil {
		return errors.NewInvalidArgumentError("pebble: reassignment requires the new utxo hash")
	}

	reassignBlocks := uint32(utxo.ReAssignedUtxoSpendableAfterBlocks)
	if tSettings != nil && tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks > 0 {
		reassignBlocks = tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	}

	// Take the stripe before the precondition read. GetSpend acquires no locks,
	// so checking it here closes the window where a concurrent UnFreezeUTXOs
	// clears flagHasOverrides between the check and the write.
	unlock := s.lockStripes(oldUtxo.TxID[:])
	defer unlock()

	resp, err := s.GetSpend(ctx, oldUtxo)
	if err != nil {
		return err
	}

	if resp.Status != int(utxo.Status_FROZEN) {
		return errors.NewUtxoFrozenError("pebble: transaction %s:%d is not frozen", oldUtxo.TxID, oldUtxo.Vout)
	}

	m, err := s.getMaster(oldUtxo.TxID)
	if err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	rec := &overrideRecord{
		frozen:         false,
		spendableIn:    int64(s.GetBlockHeight()) + int64(reassignBlocks),
		reassignedHash: newUtxo.UTXOHash[:],
	}

	if err := batch.Set(overrideKey(oldUtxo.TxID[:], oldUtxo.Vout), encodeOverride(rec), nil); err != nil {
		return errors.NewStorageError("pebble: failed to stage reassignment for %s:%d", oldUtxo.TxID, oldUtxo.Vout, err)
	}

	// Both readers gate the override lookup on flagHasOverrides (spend.go and
	// GetSpend), so without this the reassigned hash and spendableIn are dead
	// bytes and the reassignment reports success while changing nothing.
	if err := s.setOverridesFlag(batch, oldUtxo.TxID[:], m, true); err != nil {
		return err
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to reassign %s:%d", oldUtxo.TxID, oldUtxo.Vout, err)
	}

	return nil
}

var _ pruner.PrunerServiceProvider = (*Store)(nil)

var (
	prunerServiceInstance pruner.Service
	prunerServiceMutex    sync.Mutex
)

func ResetPrunerServiceForTests() {
	prunerServiceMutex.Lock()
	defer prunerServiceMutex.Unlock()

	prunerServiceInstance = nil
}

func (s *Store) GetPrunerService() (pruner.Service, error) {
	// Matches aerospike/pruner_provider.go: with the DAH cleaner disabled there is
	// no phase-2 service at all, which is the operator's escape hatch when the
	// pruner is deleting records it should not.
	if s.settings.UtxoStore.DisableDAHCleaner {
		return nil, nil
	}

	// Defensive pruning is not implemented on this backend. The SQL store swaps in
	// a delete that refuses to remove a parent while any spending child is unmined
	// or shallower than the safety window; here there is no equivalent, so
	// accepting the setting would silently give an operator less safety than they
	// asked for. Fail loudly instead.
	if s.settings.Pruner.UTXODefensiveEnabled {
		return nil, errors.NewConfigurationError(
			"pebble: pruner_utxoDefensiveEnabled is not supported by the pebble utxo store")
	}

	prunerServiceMutex.Lock()
	defer prunerServiceMutex.Unlock()

	if prunerServiceInstance != nil {
		return prunerServiceInstance, nil
	}

	prunerServiceInstance = &prunerService{store: s}

	return prunerServiceInstance, nil
}

type prunerService struct {
	store       *Store
	observers   []pruner.Observer
	observersMu sync.Mutex
}

func (p *prunerService) Start(ctx context.Context) {}

func (p *prunerService) AddObserver(observer pruner.Observer) {
	p.observersMu.Lock()
	defer p.observersMu.Unlock()

	p.observers = append(p.observers, observer)
}

func (p *prunerService) Prune(ctx context.Context, height uint32, blockHashStr string) (int64, error) {
	startTime := time.Now()

	lower := []byte{prefixDAHIdx}
	upper := heightIndexKey(prefixDAHIdx, int64(height)+1, nil)

	iter, release, err := p.store.newIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, errors.NewStorageError("pebble: pruner query failed", err)
	}

	var victims []chainhash.Hash

	for iter.First(); iter.Valid(); iter.Next() {
		victims = append(victims, chainhash.Hash(iter.Key()[9:]))
	}

	if err = iter.Error(); err != nil {
		release()

		return 0, errors.NewStorageError("pebble: pruner iteration failed", err)
	}

	release()

	var deleted int64

	for i := range victims {
		h := victims[i]

		unlock := p.store.lockStripes(h[:])

		// The victim list came from an unlocked index scan, and the whole pass
		// takes one fsync per victim. Anything that clears delete-at-height in
		// that window — a reorg's Unspend or SetMinedMulti(UnsetMined), or
		// SetLocked(true) — must win, so re-evaluate the condition under the
		// lock. stageDelete itself stays unconditional, because the public
		// Delete must still delete on demand.
		m, err := p.store.getMaster(&h)
		if err != nil {
			unlock()

			if errors.Is(err, errors.ErrTxNotFound) {
				continue
			}

			return deleted, err
		}

		if m.deleteAtHeight == 0 || m.deleteAtHeight > int64(height) {
			unlock()

			continue
		}

		batch := p.store.db.NewBatch()

		err = p.store.stageDelete(batch, h[:])
		if err == nil {
			err = p.store.commit(batch)
		}

		_ = batch.Close()

		unlock()

		if err != nil {
			return deleted, err
		}

		deleted++
	}

	if deleted > 0 {
		p.store.logger.Infof("[pebble pruner][%s:%d] deleted %d transactions in %s", blockHashStr, height, deleted, time.Since(startTime))
	}

	p.observersMu.Lock()
	observers := make([]pruner.Observer, len(p.observers))
	copy(observers, p.observers)
	p.observersMu.Unlock()

	for _, o := range observers {
		o.OnPruneComplete(height, deleted)
	}

	return deleted, nil
}
