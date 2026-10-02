package pebble

import (
	"bytes"
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/cockroachdb/pebble/v2"
	"golang.org/x/sync/errgroup"
)

func containsField(slice []fields.FieldName, item fields.FieldName) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}

	return false
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(f) > 0 {
		bins = f
	}

	snap, release, err := s.snapshot()
	if err != nil {
		return nil, err
	}

	defer release()

	m, err := getMasterFrom(snap, hash)
	if err != nil {
		return nil, err
	}

	return s.recordToMeta(snap, hash, m, bins)
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	result, err := s.Get(ctx, hash, utxo.MetaFields...)
	if err != nil {
		return err
	}

	if result != nil {
		*data = *result
	}

	return nil
}

func (s *Store) payloadBlobs(r reader, hash *chainhash.Hash) ([]byte, []byte, error) {
	payload, err := readValue(r, payloadKey(hash[:]))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// The master and the payload are written and deleted in the same batch,
			// so an absent payload means the record is gone, not that the store is
			// broken. The validator's DAH-evicted-parent branch keys off
			// ErrTxNotFound; a storage error there rejects a valid transaction.
			return nil, nil, errors.NewTxNotFoundError("pebble: transaction %s not found", hash)
		}

		return nil, nil, errors.NewStorageError("pebble: failed to read payload for %s", hash, err)
	}

	inputsBlob, err := offsetBlobItem(payload, 0)
	if err != nil {
		return nil, nil, err
	}

	outputsBlob, err := offsetBlobItem(payload, 1)
	if err != nil {
		return nil, nil, err
	}

	return inputsBlob, outputsBlob, nil
}

func (s *Store) recordToMeta(r reader, hash *chainhash.Hash, m *masterRecord, bins []fields.FieldName) (*meta.Data, error) {
	data := &meta.Data{
		Fee:         m.fee,
		SizeInBytes: m.sizeInBytes,
		IsCoinbase:  m.flags&flagCoinbase != 0,
		Frozen:      m.flags&flagFrozen != 0,
		Conflicting: m.flags&flagConflicting != 0,
		Locked:      m.flags&flagLocked != 0,
		CreatedAt:   m.createdAt,
	}

	if m.unminedSince > 0 {
		data.UnminedSince = uint32(m.unminedSince) //nolint:gosec
	}

	tx := bt.Tx{Version: m.version, LockTime: m.lockTime}

	needInputs := containsField(bins, fields.Tx) || containsField(bins, fields.Inputs) ||
		containsField(bins, fields.TxInpoints) || containsField(bins, fields.Utxos)
	needOutputs := containsField(bins, fields.Tx) || containsField(bins, fields.Outputs) || containsField(bins, fields.Utxos)

	if needInputs || needOutputs {
		inputsBlob, outputsBlob, err := s.payloadBlobs(r, hash)
		if err != nil {
			return nil, err
		}

		if needInputs {
			n := offsetBlobCount(inputsBlob)
			tx.Inputs = make([]*bt.Input, n)

			for i := 0; i < n; i++ {
				item, err := offsetBlobItem(inputsBlob, i)
				if err != nil {
					return nil, err
				}

				tx.Inputs[i] = &bt.Input{}
				if _, err = tx.Inputs[i].ReadFromExtended(bytes.NewReader(item)); err != nil {
					return nil, errors.NewTxInvalidError("pebble: could not read input %d", i, err)
				}
			}
		}

		if needOutputs {
			n := offsetBlobCount(outputsBlob)
			tx.Outputs = make([]*bt.Output, n)

			for i := 0; i < n; i++ {
				item, err := offsetBlobItem(outputsBlob, i)
				if err != nil {
					return nil, err
				}

				tx.Outputs[i] = &bt.Output{}
				if _, err = tx.Outputs[i].ReadFrom(bytes.NewReader(item)); err != nil {
					return nil, errors.NewTxInvalidError("pebble: could not read output %d", i, err)
				}
			}
		}
	}

	if containsField(bins, fields.BlockIDs) || containsField(bins, fields.BlockHeights) || containsField(bins, fields.SubtreeIdxs) {
		data.BlockIDs, data.BlockHeights, data.SubtreeIdxs = unpackBlockRefs(m.blockRefs)
	}

	if containsField(bins, fields.ConflictingChildren) {
		children, err := s.conflictingChildrenOf(r, hash[:])
		if err != nil {
			return nil, err
		}

		data.ConflictingChildren = children
	}

	if containsField(bins, fields.Utxos) {
		if err := s.fillSpendingDatas(r, hash, m, data); err != nil {
			return nil, err
		}
	}

	if containsField(bins, fields.Tx) {
		data.Tx = &tx
	}

	if containsField(bins, fields.TxInpoints) {
		txInpoints, err := subtree.NewTxInpointsFromInputs(tx.Inputs)
		if err != nil {
			return nil, errors.NewProcessingError("pebble: failed to create tx inpoints from inputs", err)
		}

		data.TxInpoints = txInpoints
	}

	return data, nil
}

func (s *Store) conflictingChildrenOf(r reader, hash []byte) ([]chainhash.Hash, error) {
	lower, upper := prefixBounds(append([]byte{prefixChildren}, hash...))

	iter, err := r.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, errors.NewStorageError("pebble: failed to iterate conflicting children", err)
	}

	defer func() { _ = iter.Close() }()

	children := make([]chainhash.Hash, 0, 16)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		children = append(children, chainhash.Hash(key[33:]))
	}

	return children, iter.Error()
}

func (s *Store) frozenVouts(r reader, hash []byte) (map[uint32]bool, error) {
	lower, upper := prefixBounds(append([]byte{prefixOverride}, hash...))

	iter, err := r.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, errors.NewStorageError("pebble: failed to iterate overrides", err)
	}

	defer func() { _ = iter.Close() }()

	frozen := make(map[uint32]bool)

	for iter.First(); iter.Valid(); iter.Next() {
		o, err := decodeOverride(iter.Value())
		if err != nil {
			return nil, err
		}

		if o.frozen {
			vout := binaryBEUint32(iter.Key()[33:])
			frozen[vout] = true
		}
	}

	return frozen, iter.Error()
}

func binaryBEUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func (s *Store) spendSlots(r reader, hash *chainhash.Hash, m *masterRecord) ([][]byte, error) {
	blobs := [][]byte{m.spends}

	for page := uint32(1); page <= m.pagesTotal; page++ {
		val, err := readValue(r, pageKey(hash[:], page))
		if err != nil {
			return nil, errors.NewStorageError("pebble: failed to read page %d for %s", page, hash, err)
		}

		rec, err := decodePage(val)
		if err != nil {
			return nil, err
		}

		blobs = append(blobs, rec.spends)
	}

	return blobs, nil
}

func (s *Store) fillSpendingDatas(r reader, hash *chainhash.Hash, m *masterRecord, data *meta.Data) error {
	blobs, err := s.spendSlots(r, hash, m)
	if err != nil {
		return err
	}

	var frozen map[uint32]bool

	if m.flags&flagHasOverrides != 0 {
		if frozen, err = s.frozenVouts(r, hash[:]); err != nil {
			return err
		}
	}

	txFrozen := m.flags&flagFrozen != 0
	data.SpendingDatas = make([]*spendpkg.SpendingData, m.outputCount)

	for v := uint32(0); v < m.outputCount; v++ {
		page := pageOfVout(v, s.pageSize)
		slot := slotOfVout(v, s.pageSize)

		if int(page) >= len(blobs) {
			break
		}

		blob := blobs[page]
		off := int(slot) * slotSpendSize

		if off+slotSpendSize > len(blob) {
			continue
		}

		if txFrozen || frozen[v] {
			data.SpendingDatas[v] = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, int(v))
			continue
		}

		data.SpendingDatas[v] = unpackSpendingData(blob[off : off+slotSpendSize])
	}

	return nil
}

func (s *Store) GetSpend(ctx context.Context, sp *utxo.Spend) (*utxo.SpendResponse, error) {
	snap, release, err := s.snapshot()
	if err != nil {
		return nil, err
	}

	defer release()

	m, err := getMasterFrom(snap, sp.TxID)
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			return &utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil
		}

		return nil, err
	}

	// A vout past the transaction's output count is a caller error, not a store
	// failure. The asset handlers do not pre-validate vout, so returning a hard
	// error here turns an arbitrary request into a 500.
	if sp.Vout >= m.outputCount {
		return &utxo.SpendResponse{Status: int(utxo.Status_NOT_FOUND)}, nil
	}

	storedHash, storedSpend, err := s.readSlot(snap, sp.TxID, m, sp.Vout)
	if err != nil {
		return nil, err
	}

	frozen := m.flags&flagFrozen != 0
	effectiveHash := storedHash

	var spendableIn int64

	if m.flags&flagHasOverrides != 0 {
		val, oErr := readValue(snap, overrideKey(sp.TxID[:], sp.Vout))
		if oErr == nil {
			o, dErr := decodeOverride(val)
			if dErr != nil {
				return nil, dErr
			}

			frozen = frozen || o.frozen
			spendableIn = o.spendableIn

			if o.reassignedHash != nil {
				effectiveHash = o.reassignedHash
			}
		} else if !errors.Is(oErr, pebble.ErrNotFound) {
			return nil, errors.NewStorageError("pebble: failed to read override %s:%d", sp.TxID, sp.Vout, oErr)
		}
	}

	if sp.UTXOHash != nil && !bytes.Equal(effectiveHash, sp.UTXOHash[:]) {
		return nil, errors.NewUtxoHashMismatchError("pebble: utxo hash mismatch for %s:%d", sp.TxID, sp.Vout)
	}

	spendingData := unpackSpendingData(storedSpend)
	utxoStatus := utxo.CalculateUtxoStatus(spendingData, m.coinbaseSpendingHeight, s.GetBlockHeight())

	if frozen {
		utxoStatus = utxo.Status_FROZEN
		spendingData = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, int(sp.Vout))
	}

	if m.flags&flagConflicting != 0 {
		utxoStatus = utxo.Status_CONFLICTING
	}

	if m.flags&flagLocked != 0 {
		utxoStatus = utxo.Status_LOCKED
	}

	if spendableIn > 0 && int64(s.GetBlockHeight()) < spendableIn {
		utxoStatus = utxo.Status_IMMATURE
	}

	return &utxo.SpendResponse{
		Status:       int(utxoStatus),
		SpendingData: spendingData,
		LockTime:     m.coinbaseSpendingHeight,
	}, nil
}

func (s *Store) readSlot(r reader, hash *chainhash.Hash, m *masterRecord, vout uint32) ([]byte, []byte, error) {
	page := pageOfVout(vout, s.pageSize)
	slot := slotOfVout(vout, s.pageSize)

	var spends []byte

	if page == 0 {
		spends = m.spends
	} else {
		val, err := readValue(r, pageKey(hash[:], page))
		if err != nil {
			return nil, nil, errors.NewStorageError("pebble: failed to read page %d for %s", page, hash, err)
		}

		rec, err := decodePage(val)
		if err != nil {
			return nil, nil, err
		}

		spends = rec.spends
	}

	hashes, err := readValue(r, hashesKey(hash[:], page))
	if err != nil {
		return nil, nil, errors.NewStorageError("pebble: failed to read hashes page %d for %s", page, hash, err)
	}

	hOff := int(slot) * slotHashSize
	sOff := int(slot) * slotSpendSize

	if hOff+slotHashSize > len(hashes) || sOff+slotSpendSize > len(spends) {
		return nil, nil, errors.NewProcessingError("pebble: vout %d out of range for %s", vout, hash)
	}

	return hashes[hOff : hOff+slotHashSize], spends[sOff : sOff+slotSpendSize], nil
}

func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	unlock := s.lockStripes(hash[:])
	defer unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := s.stageDelete(batch, hash[:]); err != nil {
		return err
	}

	if err := s.commit(batch); err != nil {
		return errors.NewStorageError("pebble: failed to commit delete for %s", hash, err)
	}

	return nil
}

// DeleteComplete removes a transaction and all its associated data. stageDelete
// removes the master record, the payload, and every page, hash, override and
// conflicting-child record — both edge directions — in one atomic batch, so
// Delete already leaves nothing behind and never half-completes; DeleteComplete
// is therefore equivalent to Delete here.
func (s *Store) DeleteComplete(ctx context.Context, hash *chainhash.Hash) error {
	return s.Delete(ctx, hash)
}

func (s *Store) stageDelete(batch *pebble.Batch, hash []byte) error {
	snap, release, err := s.snapshot()
	if err != nil {
		return err
	}

	defer release()

	m, err := getMasterFrom(snap, (*chainhash.Hash)(hash))
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			return nil
		}

		return err
	}

	if m.unminedSince > 0 {
		_ = batch.Delete(heightIndexKey(prefixUnminedIdx, m.unminedSince, hash), nil)
	}

	if m.deleteAtHeight > 0 {
		_ = batch.Delete(heightIndexKey(prefixDAHIdx, m.deleteAtHeight, hash), nil)
	}

	if m.preserveUntil > 0 {
		_ = batch.Delete(heightIndexKey(prefixPreserveIdx, m.preserveUntil, hash), nil)
	}

	_ = batch.Delete(conflictIdxKey(hash), nil)
	_ = batch.Delete(masterKey(hash), nil)
	_ = batch.Delete(payloadKey(hash), nil)

	// Collect the forward edges where this hash is the parent before the range
	// delete below removes them, so each paired reverse edge goes too. Without
	// this, deleting a parent leaves K|child|parent behind for as long as the
	// child outlives it.
	children, err := s.conflictingChildrenOf(snap, hash)
	if err != nil {
		return err
	}

	for i := range children {
		_ = batch.Delete(childrenRevKey(children[i][:], hash), nil)
	}

	for _, prefix := range []byte{prefixPage, prefixHashes, prefixOverride, prefixChildren} {
		lower, upper := prefixBounds(append([]byte{prefix}, hash...))
		if err := batch.DeleteRange(lower, upper, nil); err != nil {
			return errors.NewStorageError("pebble: failed to stage range delete for %x", prefix, err)
		}
	}

	lower, upper := prefixBounds(append([]byte{prefixChildrenRev}, hash...))

	iter, err := snap.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return errors.NewStorageError("pebble: failed to iterate reverse children", err)
	}

	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		parent := append([]byte(nil), iter.Key()[33:]...)
		_ = batch.Delete(childrenKey(parent, hash), nil)
		_ = batch.Delete(childrenRevKey(hash, parent), nil)
	}

	if err := iter.Error(); err != nil {
		return errors.NewStorageError("pebble: reverse children iteration failed", err)
	}

	return nil
}

// decorateOne reads one record under its own snapshot. BatchDecorate resolves
// many independent transactions, so consistency is required per record rather
// than across the batch.
func (s *Store) decorateOne(hash *chainhash.Hash, bins []fields.FieldName) (*meta.Data, error) {
	snap, release, err := s.snapshot()
	if err != nil {
		return nil, err
	}

	defer release()

	m, err := getMasterFrom(snap, hash)
	if err != nil {
		return nil, err
	}

	return s.recordToMeta(snap, hash, m, bins)
}

func (s *Store) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	defaultBins := utxo.MetaFieldsWithTx
	if len(f) > 0 {
		defaultBins = f
	}

	for _, item := range items {
		if item == nil {
			continue
		}

		if err := ctx.Err(); err != nil {
			return errors.NewContextCanceledError("pebble: batch decorate canceled", err)
		}

		bins := defaultBins
		if len(item.Fields) > 0 {
			bins = item.Fields
		}

		hash := item.Hash

		item.Data, item.Err = s.decorateOne(&hash, bins)
	}

	return nil
}

func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	return s.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{tx})
}

func (s *Store) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	// Group by parent so each parent payload is fetched and heap-copied once.
	// Reading it per input made a large parent plus a many-input child quadratic
	// in memcpy, on a path that runs before script validation — so the inputs
	// need not verify for the work to be done.
	byParent := make(map[chainhash.Hash][]*bt.Input)

	for _, tx := range txs {
		if tx == nil {
			continue
		}

		for _, input := range tx.Inputs {
			if input == nil || input.PreviousTxScript != nil {
				continue
			}

			parent := *input.PreviousTxIDChainHash()
			byParent[parent] = append(byParent[parent], input)
		}
	}

	if len(byParent) == 0 {
		return nil
	}

	parents := make([]chainhash.Hash, 0, len(byParent))
	for parent := range byParent {
		parents = append(parents, parent)
	}

	snap, release, err := s.snapshot()
	if err != nil {
		return err
	}

	defer release()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(8)

	for i := range parents {
		parent := parents[i]

		g.Go(func() error {
			// Each input belongs to exactly one parent, so the goroutines write to
			// disjoint inputs.
			if err := gCtx.Err(); err != nil {
				return errors.NewContextCanceledError("pebble: previous outputs decorate canceled", err)
			}

			_, outputsBlob, err := s.payloadBlobs(snap, &parent)
			if err != nil {
				return errors.NewTxNotFoundError("pebble: previous outputs for %s not found", parent, err)
			}

			for _, input := range byParent[parent] {
				item, err := offsetBlobItem(outputsBlob, int(input.PreviousTxOutIndex))
				if err != nil {
					return errors.NewTxNotFoundError("pebble: previous output %s:%d not found", parent, input.PreviousTxOutIndex, err)
				}

				output := &bt.Output{}
				if _, err = output.ReadFrom(bytes.NewReader(item)); err != nil {
					return errors.NewTxInvalidError("pebble: could not read previous output", err)
				}

				input.PreviousTxScript = output.LockingScript
				input.PreviousTxSatoshis = output.Satoshis
			}

			return nil
		})
	}

	return g.Wait()
}
