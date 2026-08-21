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

	for i := range txHashes {
		txHash := txHashes[i]

		md, err := s.Get(ctx, &txHash, fields.Tx, fields.Utxos)
		if err != nil {
			return nil, nil, err
		}

		unlock := s.lockStripes(txHash[:])

		batch := s.db.NewBatch()

		m, err := s.getMaster(&txHash)
		if err != nil {
			_ = batch.Close()

			unlock()

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
			_ = batch.Close()

			unlock()

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

		err = batch.Commit(s.sync)

		_ = batch.Close()

		unlock()

		if err != nil {
			return nil, nil, errors.NewStorageError("pebble: failed to commit SetConflicting", err)
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

	if err := batch.Commit(s.sync); err != nil {
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

	if err := batch.Commit(s.sync); err != nil {
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

	if err := batch.Commit(s.sync); err != nil {
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

	if err := s.db.Set(intentKey(id[:]), encodeIntent(intent), pebbledb.Sync); err != nil {
		return errors.NewStorageError("pebble: failed to record conflict intent %s", id, err)
	}

	return nil
}

func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	if err := s.db.Delete(intentKey(intentID[:]), pebbledb.Sync); err != nil {
		return errors.NewStorageError("pebble: failed to remove conflict intent %s", intentID, err)
	}

	return nil
}

func (s *Store) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	lower, upper := prefixBounds([]byte{prefixIntent})

	iter, err := s.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, errors.NewStorageError("pebble: pending intents query failed", err)
	}

	defer func() { _ = iter.Close() }()

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

func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	for _, sp := range spends {
		resp, err := s.GetSpend(ctx, sp)
		if err != nil {
			return err
		}

		if resp.Status == int(utxo.Status_NOT_FOUND) {
			return errors.NewTxNotFoundError("pebble: transaction %s not found", sp.TxID)
		}

		if resp.SpendingData != nil && !resp.SpendingData.TxID.IsEqual(&subtree.FrozenBytesTxHash) {
			return errors.NewUtxoSpentError(*sp.TxID, sp.Vout, *sp.UTXOHash, resp.SpendingData)
		}

		if resp.Status == int(utxo.Status_FROZEN) {
			return errors.NewUtxoFrozenError("pebble: transaction %s:%d already frozen", sp.TxID, sp.Vout)
		}
	}

	for _, sp := range spends {
		unlock := s.lockStripes(sp.TxID[:])

		batch := s.db.NewBatch()

		err := func() error {
			m, err := s.getMaster(sp.TxID)
			if err != nil {
				return err
			}

			if err = batch.Set(overrideKey(sp.TxID[:], sp.Vout), encodeOverride(&overrideRecord{frozen: true}), nil); err != nil {
				return errors.NewStorageError("pebble: failed to stage freeze for %s:%d", sp.TxID, sp.Vout, err)
			}

			return s.setOverridesFlag(batch, sp.TxID[:], m, true)
		}()

		if err == nil {
			err = batch.Commit(s.sync)
		}

		_ = batch.Close()

		unlock()

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	for _, sp := range spends {
		resp, err := s.GetSpend(ctx, sp)
		if err != nil {
			return err
		}

		if resp.Status != int(utxo.Status_FROZEN) {
			return errors.NewUtxoFrozenError("pebble: transaction %s:%d is not frozen", sp.TxID, sp.Vout)
		}
	}

	for _, sp := range spends {
		unlock := s.lockStripes(sp.TxID[:])

		batch := s.db.NewBatch()

		err := func() error {
			m, err := s.getMaster(sp.TxID)
			if err != nil {
				return err
			}

			_ = batch.Delete(overrideKey(sp.TxID[:], sp.Vout), nil)

			remaining, err := s.countOverrides(sp.TxID[:], sp.Vout)
			if err != nil {
				return err
			}

			if remaining == 0 {
				return s.setOverridesFlag(batch, sp.TxID[:], m, false)
			}

			return nil
		}()

		if err == nil {
			err = batch.Commit(s.sync)
		}

		_ = batch.Close()

		unlock()

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) countOverrides(hash []byte, excludeVout uint32) (int, error) {
	lower, upper := prefixBounds(append([]byte{prefixOverride}, hash...))

	iter, err := s.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, errors.NewStorageError("pebble: override count failed", err)
	}

	defer func() { _ = iter.Close() }()

	count := 0

	for iter.First(); iter.Valid(); iter.Next() {
		if binaryBEUint32(iter.Key()[33:]) == excludeVout {
			continue
		}

		count++
	}

	return count, iter.Error()
}

func (s *Store) ReAssignUTXO(ctx context.Context, oldUtxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	resp, err := s.GetSpend(ctx, oldUtxo)
	if err != nil {
		return err
	}

	if resp.Status != int(utxo.Status_FROZEN) {
		return errors.NewUtxoFrozenError("pebble: transaction %s:%d is not frozen", oldUtxo.TxID, oldUtxo.Vout)
	}

	reassignBlocks := uint32(utxo.ReAssignedUtxoSpendableAfterBlocks)
	if tSettings != nil && tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks > 0 {
		reassignBlocks = tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	}

	unlock := s.lockStripes(oldUtxo.TxID[:])
	defer unlock()

	rec := &overrideRecord{
		frozen:         false,
		spendableIn:    int64(s.GetBlockHeight()) + int64(reassignBlocks),
		reassignedHash: newUtxo.UTXOHash[:],
	}

	if err := s.db.Set(overrideKey(oldUtxo.TxID[:], oldUtxo.Vout), encodeOverride(rec), s.sync); err != nil {
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

	iter, err := p.store.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, errors.NewStorageError("pebble: pruner query failed", err)
	}

	var victims []chainhash.Hash

	for iter.First(); iter.Valid(); iter.Next() {
		victims = append(victims, chainhash.Hash(iter.Key()[9:]))
	}

	if err = iter.Error(); err != nil {
		_ = iter.Close()
		return 0, errors.NewStorageError("pebble: pruner iteration failed", err)
	}

	_ = iter.Close()

	var deleted int64

	for i := range victims {
		h := victims[i]

		unlock := p.store.lockStripes(h[:])

		batch := p.store.db.NewBatch()

		err := p.store.stageDelete(batch, h[:])
		if err == nil {
			err = batch.Commit(p.store.sync)
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
