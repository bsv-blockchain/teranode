package pebble

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	pebbledb "github.com/cockroachdb/pebble/v2"
)

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	result := make(map[chainhash.Hash][]uint32)

	lockHashes := make([][]byte, len(hashes))
	for i, h := range hashes {
		lockHashes[i] = h[:]
	}

	unlock := s.lockStripes(lockHashes...)
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, h := range hashes {
		m, err := s.getMaster(h)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) && info.UnsetMined {
				continue
			}

			return nil, err
		}

		old := *m

		m.flags &^= flagLocked

		if info.UnsetMined {
			m.blockRefs = removeBlockRef(m.blockRefs, info.BlockID)
			m.deleteAtHeight = 0

			if len(m.blockRefs) == 0 {
				m.unminedSince = int64(s.GetBlockHeight()) + 1
			}
		} else {
			m.blockRefs = appendBlockRef(m.blockRefs, info)

			if info.OnLongestChain {
				m.unminedSince = 0

				if s.retention() > 0 {
					fullySpent := m.spentCount >= m.page0Count && m.pagesSpent >= m.pagesTotal
					newDAH := int64(info.BlockHeight) + s.retention()

					switch {
					case m.preserveUntil > 0:
					case m.deleteAtHeight > 0 && m.deleteAtHeight < newDAH:
						m.deleteAtHeight = newDAH
					case m.deleteAtHeight == 0 && fullySpent:
						m.deleteAtHeight = newDAH
					}
				}
			} else {
				m.deleteAtHeight = 0
			}
		}

		if err = s.stageMaster(batch, h[:], &old, m); err != nil {
			return nil, err
		}

		ids, _, _ := unpackBlockRefs(m.blockRefs)
		result[*h] = ids
	}

	if err := batch.Commit(s.sync); err != nil {
		return nil, errors.NewStorageError("pebble: failed to commit SetMinedMulti", err)
	}

	return result, nil
}

func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
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

		if onLongestChain {
			m.unminedSince = 0
		} else {
			m.unminedSince = int64(s.GetBlockHeight())
		}

		if err = s.stageMaster(batch, h[:], &old, m); err != nil {
			return err
		}
	}

	if err := batch.Commit(s.sync); err != nil {
		return errors.NewStorageError("pebble: failed to commit MarkTransactionsOnLongestChain", err)
	}

	return nil
}

type heightIndexIterator struct {
	store    *Store
	prefix   byte
	cutoff   int64
	skipConf bool
	lastKey  []byte
	done     bool
	err      error
}

func (s *Store) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	return &heightIndexIterator{store: s, prefix: prefixUnminedIdx, cutoff: -1, skipConf: true}, nil
}

func (s *Store) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	return &heightIndexIterator{store: s, prefix: prefixUnminedIdx, cutoff: int64(cutoffBlockHeight), skipConf: true}, nil
}

func (s *Store) GetConflictingTxIterator() (utxo.UnminedTxIterator, error) {
	return &heightIndexIterator{store: s, prefix: prefixConflictIdx, cutoff: -1}, nil
}

func (it *heightIndexIterator) Next(ctx context.Context) ([]*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil {
		return nil, it.err
	}

	lower := []byte{it.prefix}
	if it.lastKey != nil {
		lower = append(it.lastKey, 0)
	}

	_, upper := prefixBounds([]byte{it.prefix})

	iter, err := it.store.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		it.err = errors.NewStorageError("pebble: iterator open failed", err)
		return nil, it.err
	}

	defer func() { _ = iter.Close() }()

	batch := make([]*utxo.UnminedTransaction, 0, 1000)

	for iter.First(); iter.Valid() && len(batch) < 1000; iter.Next() {
		key := iter.Key()
		it.lastKey = append(it.lastKey[:0], key...)

		var hashBytes []byte

		if it.prefix == prefixConflictIdx {
			hashBytes = key[1:]
		} else {
			height := int64(binaryBEUint64(key[1:9])) //nolint:gosec
			if it.cutoff >= 0 && height > it.cutoff {
				break
			}

			hashBytes = key[9:]
		}

		hash := chainhash.Hash(hashBytes)

		m, err := it.store.getMaster(&hash)
		if err != nil {
			it.err = err
			return nil, it.err
		}

		if it.skipConf && m.flags&flagConflicting != 0 {
			continue
		}

		if m.flags&flagCoinbase != 0 {
			batch = append(batch, &utxo.UnminedTransaction{Skip: true})
			continue
		}

		u, err := it.store.recordToUnmined(&hash, m)
		if err != nil {
			it.err = err
			return nil, it.err
		}

		batch = append(batch, u)
	}

	if err := iter.Error(); err != nil {
		it.err = errors.NewStorageError("pebble: iteration failed", err)
		return nil, it.err
	}

	if len(batch) == 0 {
		it.done = true
		return nil, nil
	}

	return batch, nil
}

func binaryBEUint64(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

func (s *Store) recordToUnmined(hash *chainhash.Hash, m *masterRecord) (*utxo.UnminedTransaction, error) {
	inputsBlob, _, err := s.payloadBlobs(hash)
	if err != nil {
		return nil, err
	}

	txInpoints, err := txInpointsFromBlob(inputsBlob)
	if err != nil {
		return nil, err
	}

	blockIDs, _, _ := unpackBlockRefs(m.blockRefs)

	return &utxo.UnminedTransaction{
		Node: &subtree.Node{
			Hash:        *hash,
			Fee:         m.fee,
			SizeInBytes: m.sizeInBytes,
		},
		TxInpoints:   txInpoints,
		CreatedAt:    int(m.createdAt),
		Locked:       m.flags&flagLocked != 0,
		BlockIDs:     blockIDs,
		UnminedSince: int(m.unminedSince),
	}, nil
}

func (it *heightIndexIterator) Err() error { return it.err }

func (it *heightIndexIterator) Close() error {
	it.done = true
	return nil
}

type consistencyIterator struct {
	store   *Store
	lastKey []byte
	scanned int64
	done    bool
	err     error
}

func (s *Store) ScanInconsistentUnminedTxs() (utxo.ConsistencyScanIterator, error) {
	return &consistencyIterator{store: s}, nil
}

func (it *consistencyIterator) Next(ctx context.Context) ([]*utxo.InconsistentTxRecord, error) {
	if it.done || it.err != nil {
		return nil, it.err
	}

	lower := []byte{prefixMaster}
	if it.lastKey != nil {
		lower = append(it.lastKey, 0)
	}

	_, upper := prefixBounds([]byte{prefixMaster})

	iter, err := it.store.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		it.err = errors.NewStorageError("pebble: consistency scan open failed", err)
		return nil, it.err
	}

	defer func() { _ = iter.Close() }()

	records := make([]*utxo.InconsistentTxRecord, 0, 8)
	rowCount := 0

	for iter.First(); iter.Valid() && rowCount < 5000; iter.Next() {
		key := iter.Key()
		it.lastKey = append(it.lastKey[:0], key...)
		rowCount++
		it.scanned++

		m, err := decodeMaster(iter.Value())
		if err != nil {
			it.err = err
			return nil, it.err
		}

		if m.unminedSince > 0 && len(m.blockRefs) > 0 {
			blockIDs, _, _ := unpackBlockRefs(m.blockRefs)
			records = append(records, &utxo.InconsistentTxRecord{
				Hash:         chainhash.Hash(key[1:]),
				BlockIDs:     blockIDs,
				UnminedSince: int(m.unminedSince),
			})
		}
	}

	if err := iter.Error(); err != nil {
		it.err = errors.NewStorageError("pebble: consistency scan failed", err)
		return nil, it.err
	}

	if rowCount == 0 {
		it.done = true
		return nil, nil
	}

	return records, nil
}

func (it *consistencyIterator) TotalScanned() int64 { return it.scanned }
func (it *consistencyIterator) Err() error          { return it.err }

func (it *consistencyIterator) Close() error {
	it.done = true
	return nil
}

func (s *Store) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	lower := []byte{prefixUnminedIdx}
	upper := heightIndexKey(prefixUnminedIdx, int64(cutoffBlockHeight)+1, nil)

	iter, err := s.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, errors.NewStorageError("pebble: old unmined query failed", err)
	}

	defer func() { _ = iter.Close() }()

	var out []chainhash.Hash

	for iter.First(); iter.Valid() && len(out) < 1000; iter.Next() {
		out = append(out, chainhash.Hash(iter.Key()[9:]))
	}

	return out, iter.Error()
}

func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	lockHashes := make([][]byte, len(txIDs))
	for i := range txIDs {
		lockHashes[i] = txIDs[i][:]
	}

	unlock := s.lockStripes(lockHashes...)
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for i := range txIDs {
		h := txIDs[i]

		m, err := s.getMaster(&h)
		if err != nil {
			if errors.Is(err, errors.ErrTxNotFound) {
				continue
			}

			return err
		}

		if m.deleteAtHeight == 0 && m.preserveUntil == 0 {
			continue
		}

		old := *m
		m.preserveUntil = int64(preserveUntilHeight)
		m.deleteAtHeight = 0

		if err = s.stageMaster(batch, h[:], &old, m); err != nil {
			return err
		}
	}

	if err := batch.Commit(s.sync); err != nil {
		return errors.NewStorageError("pebble: failed to commit PreserveTransactions", err)
	}

	return nil
}

func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	lower := []byte{prefixPreserveIdx}
	upper := heightIndexKey(prefixPreserveIdx, int64(currentHeight)+1, nil)

	iter, err := s.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return errors.NewStorageError("pebble: expired preservations query failed", err)
	}

	var expired []chainhash.Hash

	for iter.First(); iter.Valid(); iter.Next() {
		expired = append(expired, chainhash.Hash(iter.Key()[9:]))
	}

	if err = iter.Error(); err != nil {
		_ = iter.Close()
		return errors.NewStorageError("pebble: expired preservations iteration failed", err)
	}

	_ = iter.Close()

	if len(expired) == 0 {
		return nil
	}

	lockHashes := make([][]byte, len(expired))
	for i := range expired {
		lockHashes[i] = expired[i][:]
	}

	unlock := s.lockStripes(lockHashes...)
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for i := range expired {
		h := expired[i]

		m, err := s.getMaster(&h)
		if err != nil {
			continue
		}

		old := *m
		m.preserveUntil = 0

		if s.retention() > 0 {
			fullySpent := m.spentCount >= m.page0Count && m.pagesSpent >= m.pagesTotal

			if m.flags&flagConflicting != 0 ||
				(m.unminedSince == 0 && len(m.blockRefs) > 0 && fullySpent) {
				m.deleteAtHeight = int64(currentHeight) + s.retention()
			}
		}

		if err = s.stageMaster(batch, h[:], &old, m); err != nil {
			return err
		}
	}

	if err := batch.Commit(s.sync); err != nil {
		return errors.NewStorageError("pebble: failed to commit ProcessExpiredPreservations", err)
	}

	return nil
}
