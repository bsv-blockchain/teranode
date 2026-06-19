package teraslab

import (
	"context"
	"math"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/stores/utxo"
)

const iteratorPageSize = 1000

// unminedTxIterator implements the UnminedTxIterator interface for TeraSlab.
// Since TeraSlab doesn't support streaming iteration, we query all txids upfront
// and fetch metadata in batches.
type unminedTxIterator struct {
	store  *Store
	txids  []chainhash.Hash
	pos    int
	err    error
	closed bool
}

// GetUnminedTxIterator returns an iterator for all unmined transactions in the store.
func (s *Store) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	ctx := context.Background()

	// Query all unmined transactions using max cutoff height
	txids, err := s.client.QueryOldUnmined(ctx, math.MaxUint32)
	if err != nil {
		return nil, err
	}

	hashes := make([]chainhash.Hash, len(txids))
	for i, txid := range txids {
		hashes[i] = chainhash.Hash(txid)
	}

	return &unminedTxIterator{
		store: s,
		txids: hashes,
	}, nil
}

// GetPrunableUnminedTxIterator returns a lightweight iterator optimized for the pruner's needs.
func (s *Store) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	ctx := context.Background()

	txids, err := s.client.QueryOldUnmined(ctx, cutoffBlockHeight)
	if err != nil {
		return nil, err
	}

	hashes := make([]chainhash.Hash, len(txids))
	for i, txid := range txids {
		hashes[i] = chainhash.Hash(txid)
	}

	return &unminedTxIterator{
		store: s,
		txids: hashes,
	}, nil
}

// Next advances the iterator and returns a batch of unmined transactions.
func (it *unminedTxIterator) Next(ctx context.Context) ([]*utxo.UnminedTransaction, error) {
	if it.closed || it.pos >= len(it.txids) {
		return nil, nil
	}

	// Get the next page of txids
	end := it.pos + iteratorPageSize
	if end > len(it.txids) {
		end = len(it.txids)
	}

	pageTxids := it.txids[it.pos:end]
	it.pos = end

	// Fetch metadata for this page
	wireIDs := make([]teraslab.TxID, len(pageTxids))
	for i, h := range pageTxids {
		wireIDs[i] = hashToTxID(&h)
	}

	records, err := it.store.client.GetRecordBatch(ctx, teraslab.FieldAllMetadata|teraslab.FieldBlockEntries, wireIDs)
	if err != nil {
		it.err = err
		return nil, err
	}

	txns := make([]*utxo.UnminedTransaction, 0, len(pageTxids))
	for i := range pageTxids {
		if i >= len(records) || !records[i].Found {
			continue
		}

		ut := &utxo.UnminedTransaction{}

		if records[i].Metadata != nil {
			md := records[i].Metadata
			ut.UnminedSince = int(md.UnminedSince)
			ut.CreatedAt = int(md.CreatedAt)
			ut.Locked = md.Flags&0x04 != 0 // TeraSlab TxFlags bit 2 = LOCKED
		}

		if len(records[i].BlockEntries) > 0 {
			blockIDs := make([]uint32, len(records[i].BlockEntries))
			for j, e := range records[i].BlockEntries {
				blockIDs[j] = e.BlockID
			}
			ut.BlockIDs = blockIDs
		}

		txns = append(txns, ut)
	}

	return txns, nil
}

// Err returns the first error encountered during iteration.
func (it *unminedTxIterator) Err() error {
	return it.err
}

// Close releases any resources held by the iterator.
func (it *unminedTxIterator) Close() error {
	it.closed = true
	return nil
}

// consistencyScanIterator scans transactions whose unmined_since is still set
// and surfaces any that also have block entries — the inconsistency the
// aerospike store detects via a partition scan.
//
// TeraSlab's wire protocol does not expose a full record scan, so we lean on
// QueryOldUnmined (the unmined_since secondary index) to bound the work. This
// catches every record where unmined_since is set; records where unmined_since
// has already been cleared but block_ids remain are not detectable here, which
// is consistent with the data the server makes available.
type consistencyScanIterator struct {
	store        *Store
	txids        []chainhash.Hash
	pos          int
	totalScanned int64
	err          error
	closed       bool
}

// ScanInconsistentUnminedTxs returns an iterator that surfaces unmined-since
// inconsistencies (transactions with unmined_since set but block entries
// already present).
func (s *Store) ScanInconsistentUnminedTxs() (utxo.ConsistencyScanIterator, error) {
	ctx := context.Background()

	txids, err := s.client.QueryOldUnmined(ctx, math.MaxUint32)
	if err != nil {
		return nil, err
	}

	hashes := make([]chainhash.Hash, len(txids))
	for i, txid := range txids {
		hashes[i] = chainhash.Hash(txid)
	}

	return &consistencyScanIterator{
		store: s,
		txids: hashes,
	}, nil
}

// Next returns the next batch of inconsistent transaction records.
func (it *consistencyScanIterator) Next(ctx context.Context) ([]*utxo.InconsistentTxRecord, error) {
	if it.closed || it.err != nil {
		return nil, it.err
	}
	if it.pos >= len(it.txids) {
		return nil, nil
	}

	end := it.pos + iteratorPageSize
	if end > len(it.txids) {
		end = len(it.txids)
	}

	pageHashes := it.txids[it.pos:end]
	it.pos = end

	wireIDs := make([]teraslab.TxID, len(pageHashes))
	for i, h := range pageHashes {
		wireIDs[i] = hashToTxID(&h)
	}

	records, err := it.store.client.GetRecordBatch(ctx, teraslab.FieldUnminedSince|teraslab.FieldBlockEntries, wireIDs)
	if err != nil {
		it.err = err
		return nil, err
	}

	out := make([]*utxo.InconsistentTxRecord, 0, len(pageHashes))
	for i, h := range pageHashes {
		if i >= len(records) || !records[i].Found {
			continue
		}
		it.totalScanned++

		rec := &utxo.InconsistentTxRecord{Hash: h}
		if records[i].Metadata != nil {
			rec.UnminedSince = int(records[i].Metadata.UnminedSince)
		}
		if len(records[i].BlockEntries) > 0 {
			blockIDs := make([]uint32, len(records[i].BlockEntries))
			for j, e := range records[i].BlockEntries {
				blockIDs[j] = e.BlockID
			}
			rec.BlockIDs = blockIDs
		}

		out = append(out, rec)
	}

	return out, nil
}

// TotalScanned returns the number of records observed so far.
func (it *consistencyScanIterator) TotalScanned() int64 {
	return it.totalScanned
}

// Err returns the first error encountered during iteration.
func (it *consistencyScanIterator) Err() error {
	return it.err
}

// Close releases any resources held by the iterator.
func (it *consistencyScanIterator) Close() error {
	it.closed = true
	return nil
}
