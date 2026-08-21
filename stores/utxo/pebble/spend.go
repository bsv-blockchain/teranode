package pebble

import (
	"bytes"
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/cockroachdb/pebble/v2"
)

func (s *Store) dahEligible(m *masterRecord) bool {
	return s.retention() > 0 &&
		m.spentCount >= m.page0Count &&
		m.pagesSpent >= m.pagesTotal &&
		len(m.blockRefs) > 0 &&
		m.unminedSince == 0 &&
		m.preserveUntil == 0 &&
		m.flags&flagConflicting == 0
}

func (s *Store) stageMaster(batch *pebble.Batch, hash []byte, old, updated *masterRecord) error {
	if old.deleteAtHeight != updated.deleteAtHeight {
		if old.deleteAtHeight > 0 {
			_ = batch.Delete(heightIndexKey(prefixDAHIdx, old.deleteAtHeight, hash), nil)
		}

		if updated.deleteAtHeight > 0 {
			_ = batch.Set(heightIndexKey(prefixDAHIdx, updated.deleteAtHeight, hash), nil, nil)
		}
	}

	if old.unminedSince != updated.unminedSince {
		if old.unminedSince > 0 {
			_ = batch.Delete(heightIndexKey(prefixUnminedIdx, old.unminedSince, hash), nil)
		}

		if updated.unminedSince > 0 {
			_ = batch.Set(heightIndexKey(prefixUnminedIdx, updated.unminedSince, hash), nil, nil)
		}
	}

	if old.preserveUntil != updated.preserveUntil {
		if old.preserveUntil > 0 {
			_ = batch.Delete(heightIndexKey(prefixPreserveIdx, old.preserveUntil, hash), nil)
		}

		if updated.preserveUntil > 0 {
			_ = batch.Set(heightIndexKey(prefixPreserveIdx, updated.preserveUntil, hash), nil, nil)
		}
	}

	wasConflicting := old.flags&flagConflicting != 0
	isConflicting := updated.flags&flagConflicting != 0

	if wasConflicting != isConflicting {
		if isConflicting {
			_ = batch.Set(conflictIdxKey(hash), nil, nil)
		} else {
			_ = batch.Delete(conflictIdxKey(hash), nil)
		}
	}

	if err := batch.Set(masterKey(hash), encodeMaster(updated), nil); err != nil {
		return errors.NewStorageError("pebble: failed to stage master update", err)
	}

	return nil
}

type parentGroup struct {
	hash   chainhash.Hash
	spends []*utxo.Spend
}

func groupByParent(spends []*utxo.Spend) []*parentGroup {
	byHash := make(map[chainhash.Hash]*parentGroup)
	order := make([]*parentGroup, 0, len(spends))

	for _, sp := range spends {
		g, ok := byHash[*sp.TxID]
		if !ok {
			g = &parentGroup{hash: *sp.TxID}
			byHash[*sp.TxID] = g

			order = append(order, g)
		}

		g.spends = append(g.spends, sp)
	}

	return order
}

func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if blockHeight == 0 {
		return nil, errors.NewProcessingError("pebble: blockHeight must be greater than zero")
	}

	var ig utxo.IgnoreFlags
	if len(ignoreFlags) > 0 {
		ig = ignoreFlags[0]
	}

	var (
		spends []*utxo.Spend
		err    error
	)

	if ig.SkipUTXOHashCheck {
		spends, err = utxo.GetSpendsOutpointOnly(tx)
	} else {
		spends, err = utxo.GetSpends(tx)
	}

	if err != nil {
		return nil, err
	}

	if len(spends) == 0 {
		return nil, errors.NewProcessingError("pebble: no spends provided")
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

	var firstErr error

	for _, g := range groups {
		if err := s.stageSpendGroup(batch, g, blockHeight, ig); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return spends, errors.NewUtxoError("pebble: spend failed", firstErr)
	}

	if err := batch.Commit(s.sync); err != nil {
		return spends, errors.NewStorageError("pebble: failed to commit spend", err)
	}

	return spends, nil
}

func (s *Store) stageSpendGroup(batch *pebble.Batch, g *parentGroup, blockHeight uint32, ig utxo.IgnoreFlags) error {
	m, err := s.getMaster(&g.hash)
	if err != nil {
		for _, sp := range g.spends {
			sp.Err = err
		}

		return err
	}

	if m.flags&flagConflicting != 0 && !ig.IgnoreConflicting {
		err = errors.NewTxConflictingError("pebble: transaction %s is conflicting", g.hash)
		setAll(g.spends, err)

		return err
	}

	if m.flags&flagLocked != 0 && !ig.IgnoreLocked {
		err = errors.NewTxLockedError("pebble: transaction %s is locked", g.hash)
		setAll(g.spends, err)

		return err
	}

	if m.flags&flagFrozen != 0 {
		err = errors.NewUtxoFrozenError("pebble: transaction %s is frozen", g.hash)
		setAll(g.spends, err)

		return err
	}

	if m.flags&flagCoinbase != 0 && m.coinbaseSpendingHeight > blockHeight {
		err = errors.NewTxCoinbaseImmatureError("pebble: coinbase %s not spendable until height %d (spending at %d)",
			g.hash, m.coinbaseSpendingHeight, blockHeight)
		setAll(g.spends, err)

		return err
	}

	old := *m
	pages := map[uint32]*pageRecord{}
	pageWasComplete := map[uint32]bool{}

	var firstErr error

	for _, sp := range g.spends {
		if err := s.stageSpendSlot(m, pages, pageWasComplete, &g.hash, sp, blockHeight, ig); err != nil {
			sp.Err = err

			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		return firstErr
	}

	for page, rec := range pages {
		if !pageWasComplete[page] && pageFullySpent(rec) {
			m.pagesSpent++
		}

		if err := batch.Set(pageKey(g.hash[:], page), encodePage(rec), nil); err != nil {
			return errors.NewStorageError("pebble: failed to stage page %d for %s", page, g.hash, err)
		}
	}

	if s.dahEligible(m) {
		m.deleteAtHeight = int64(blockHeight) + s.retention()
	}

	return s.stageMaster(batch, g.hash[:], &old, m)
}

func setAll(spends []*utxo.Spend, err error) {
	for _, sp := range spends {
		sp.Err = err
	}
}

func pageFullySpent(rec *pageRecord) bool {
	spent := uint32(0)

	for off := 0; off+slotSpendSize <= len(rec.spends); off += slotSpendSize {
		if unpackSpendingData(rec.spends[off:off+slotSpendSize]) != nil {
			spent++
		}
	}

	return spent >= rec.spendableCount
}

func (s *Store) stageSpendSlot(m *masterRecord, pages map[uint32]*pageRecord, pageWasComplete map[uint32]bool,
	hash *chainhash.Hash, sp *utxo.Spend, blockHeight uint32, ig utxo.IgnoreFlags,
) error {
	page := pageOfVout(sp.Vout, s.pageSize)
	slot := slotOfVout(sp.Vout, s.pageSize)

	var spends []byte

	if page == 0 {
		spends = m.spends
	} else {
		rec, ok := pages[page]
		if !ok {
			val, err := s.getValue(pageKey(hash[:], page))
			if err != nil {
				return errors.NewStorageError("pebble: failed to read page %d for %s", page, hash, err)
			}

			if rec, err = decodePage(val); err != nil {
				return err
			}

			pages[page] = rec
			pageWasComplete[page] = pageFullySpent(rec)
		}

		spends = rec.spends
	}

	sOff := int(slot) * slotSpendSize
	if sOff+slotSpendSize > len(spends) {
		return errors.NewProcessingError("pebble: vout %d out of range for %s", sp.Vout, hash)
	}

	expectedHash := []byte(nil)
	if sp.UTXOHash != nil {
		expectedHash = sp.UTXOHash[:]
	}

	skipHashCheck := ig.SkipUTXOHashCheck

	if m.flags&flagHasOverrides != 0 {
		val, oErr := s.getValue(overrideKey(hash[:], sp.Vout))
		if oErr == nil {
			o, dErr := decodeOverride(val)
			if dErr != nil {
				return dErr
			}

			if o.frozen {
				return errors.NewUtxoFrozenError("pebble: utxo %s:%d is frozen", hash, sp.Vout)
			}

			if o.spendableIn > int64(blockHeight) {
				return errors.NewUtxoFrozenError("pebble: utxo %s:%d is not spendable until height %d", hash, sp.Vout, o.spendableIn)
			}

			if o.reassignedHash != nil && expectedHash != nil && !skipHashCheck {
				if !bytes.Equal(o.reassignedHash, expectedHash) {
					return errors.NewUtxoHashMismatchError("pebble: reassigned utxo hash mismatch for %s:%d", hash, sp.Vout)
				}

				skipHashCheck = true
			}
		} else if !errors.Is(oErr, pebble.ErrNotFound) {
			return errors.NewStorageError("pebble: failed to read override %s:%d", hash, sp.Vout, oErr)
		}
	}

	if !skipHashCheck && expectedHash != nil {
		hashes, err := s.getValue(hashesKey(hash[:], page))
		if err != nil {
			return errors.NewStorageError("pebble: failed to read hashes page %d for %s", page, hash, err)
		}

		hOff := int(slot) * slotHashSize
		if hOff+slotHashSize > len(hashes) {
			return errors.NewProcessingError("pebble: vout %d hash out of range for %s", sp.Vout, hash)
		}

		if !bytes.Equal(hashes[hOff:hOff+slotHashSize], expectedHash) {
			return errors.NewUtxoHashMismatchError("pebble: utxo hash mismatch for %s:%d", hash, sp.Vout)
		}
	}

	newSpend := packSpendingData(sp.SpendingData)
	current := spends[sOff : sOff+slotSpendSize]

	if existing := unpackSpendingData(current); existing != nil {
		if bytes.Equal(current, newSpend) {
			return nil
		}

		sp.ConflictingTxID = existing.TxID

		var uh chainhash.Hash
		if sp.UTXOHash != nil {
			uh = *sp.UTXOHash
		}

		return errors.NewUtxoSpentError(*sp.TxID, sp.Vout, uh, existing)
	}

	copy(current, newSpend)

	if page == 0 {
		m.spentCount++
	}

	return nil
}

func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	setLocked := len(flagAsLocked) > 0 && flagAsLocked[0]
	groups := groupByParent(spends)
	hashes := make([][]byte, len(groups))

	for i, g := range groups {
		hashes[i] = g.hash[:]
	}

	unlock := s.lockStripes(hashes...)
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, g := range groups {
		if err := s.stageUnspendGroup(batch, g, setLocked); err != nil {
			return err
		}
	}

	if err := batch.Commit(s.sync); err != nil {
		return errors.NewStorageError("pebble: failed to commit unspend", err)
	}

	return nil
}

func (s *Store) stageUnspendGroup(batch *pebble.Batch, g *parentGroup, setLocked bool) error {
	m, err := s.getMaster(&g.hash)
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			return nil
		}

		return err
	}

	old := *m
	pages := map[uint32]*pageRecord{}
	pageWasComplete := map[uint32]bool{}
	changed := false

	for _, sp := range g.spends {
		page := pageOfVout(sp.Vout, s.pageSize)
		slot := slotOfVout(sp.Vout, s.pageSize)

		var spendsBlob []byte

		if page == 0 {
			spendsBlob = m.spends
		} else {
			rec, ok := pages[page]
			if !ok {
				val, err := s.getValue(pageKey(g.hash[:], page))
				if err != nil {
					return errors.NewStorageError("pebble: failed to read page %d for %s", page, g.hash, err)
				}

				if rec, err = decodePage(val); err != nil {
					return err
				}

				pages[page] = rec
				pageWasComplete[page] = pageFullySpent(rec)
			}

			spendsBlob = rec.spends
		}

		sOff := int(slot) * slotSpendSize
		if sOff+slotSpendSize > len(spendsBlob) {
			continue
		}

		expected := packSpendingData(sp.SpendingData)
		current := spendsBlob[sOff : sOff+slotSpendSize]

		if !bytes.Equal(current, expected) {
			continue
		}

		copy(current, make([]byte, slotSpendSize))

		changed = true

		if page == 0 && m.spentCount > 0 {
			m.spentCount--
		}
	}

	for page, rec := range pages {
		if pageWasComplete[page] && !pageFullySpent(rec) && m.pagesSpent > 0 {
			m.pagesSpent--
		}

		if err := batch.Set(pageKey(g.hash[:], page), encodePage(rec), nil); err != nil {
			return errors.NewStorageError("pebble: failed to stage page %d for %s", page, g.hash, err)
		}
	}

	if changed {
		m.deleteAtHeight = 0
	}

	if setLocked {
		m.flags |= flagLocked
	}

	return s.stageMaster(batch, g.hash[:], &old, m)
}

func (s *Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	options, err := utxo.ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	var (
		built *builtTx
		md    *meta.Data
	)

	if !options.SpendOnly {
		if built, md, err = s.buildTx(tx, blockHeight, options); err != nil {
			return nil, nil, err
		}
	}

	var spends []*utxo.Spend

	if !options.CreateOnly {
		if options.IgnoreFlags.SkipUTXOHashCheck {
			spends, err = utxo.GetSpendsOutpointOnly(tx)
		} else {
			spends, err = utxo.GetSpends(tx)
		}

		if err != nil {
			return nil, nil, err
		}
	}

	selfHash := tx.TxIDChainHash()
	if options.TxID != nil {
		selfHash = options.TxID
	}

	groups := groupByParent(spends)
	lockHashes := make([][]byte, 0, len(groups)+1)
	lockHashes = append(lockHashes, selfHash[:])

	for _, g := range groups {
		lockHashes = append(lockHashes, g.hash[:])
	}

	unlock := s.lockStripes(lockHashes...)
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	var firstErr error

	for _, g := range groups {
		if err := s.stageSpendGroup(batch, g, blockHeight, options.IgnoreFlags); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return nil, spends, errors.NewUtxoError("pebble: spend failed", firstErr)
	}

	if options.SpendOnly {
		if err := batch.Commit(s.sync); err != nil {
			return nil, spends, errors.NewStorageError("pebble: failed to commit spend-only", err)
		}

		return nil, spends, nil
	}

	if err := s.stageCreate(batch, built); err != nil {
		if errors.Is(err, errors.ErrTxExists) {
			if len(spends) > 0 {
				if commitErr := batch.Commit(s.sync); commitErr != nil {
					return nil, spends, errors.NewStorageError("pebble: failed to commit spends on tx-exists", commitErr)
				}
			}

			return nil, spends, err
		}

		return nil, nil, err
	}

	if err := batch.Commit(s.sync); err != nil {
		return nil, spends, errors.NewStorageError("pebble: failed to commit SpendAndCreate", err)
	}

	if options.CreateOnly {
		return md, nil, nil
	}

	return md, spends, nil
}
