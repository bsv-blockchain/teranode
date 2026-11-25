package aerospike

// Layout marker: the record that says what chunk size a namespace was written
// with.
//
// A coin's storage location in this engine is computed with
// `vout % utxoBatchSize` (see calculateOffsetForOutput and get.go), so the
// batch size is part of the address of every coin already on the server. The
// constructor has always carried a comment saying it is "very dangerous" to
// change the value after a node has been running - and settings ship three
// different values for three contexts (128 default, 50 docker, 512 docker.m),
// with nothing in the data recording which one wrote it. Open a 512-written
// namespace under a 50 context and most output indices resolve to the wrong
// chunk: the coin is simply not where the reader looks, silently.
//
// The blob store next door solved the same problem with an eight-byte magic
// at the front of every file (pkg/fileformat/header.go). This is the ledger's
// equivalent: one record in a dedicated set of the same namespace, written
// on first open, verified on every open after. A disagreement refuses the
// constructor instead of silently mis-reading the ledger.

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
)

const (
	// layoutSetName holds exactly one record and is deliberately not the UTXO
	// set: unmined queries, delete-at-height sweeps and the pruner all scan
	// the UTXO set, and none of them should ever meet this record.
	layoutSetName = "utxo_layout"
	// layoutRecordKey is the user key of the single layout record.
	layoutRecordKey = "v1"
	// utxoBatchSizeBin names the batch size the namespace was written with.
	utxoBatchSizeBin = "utxoBatchSize"
)

// getLayoutBatchSize reads the recorded batch size. A nil record means the
// namespace predates this check (or is brand new) and carries no opinion.
func (s *Store) getLayoutBatchSize(ctx context.Context) (int, bool, error) {
	key, err := aerospike.NewKey(s.namespace, layoutSetName, layoutRecordKey)
	if err != nil {
		return 0, false, err
	}

	rec, getErr := s.client.Get(aerospike.NewPolicy(), key)
	if getErr != nil {
		if getErr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return 0, false, nil
		}
		// v8 also signals not-found as a nil record with no error.
		return 0, false, errors.NewStorageError("failed to read the UTXO layout marker: %v", getErr)
	}

	if rec == nil {
		return 0, false, nil
	}

	value := rec.Bins[utxoBatchSizeBin]
	if value == nil {
		return 0, false, errors.NewStorageError("the UTXO layout marker exists but has no %s bin", utxoBatchSizeBin)
	}

	size, ok := value.(int)
	if !ok {
		if size64, isInt64 := value.(int64); isInt64 {
			return int(size64), true, nil
		}

		return 0, false, errors.NewStorageError("the UTXO layout marker %s bin is not an integer: %T", utxoBatchSizeBin, value)
	}

	return size, true, nil
}

// writeLayoutBatchSize records the batch size this store was opened with,
// CREATE_ONLY so a concurrent first open on another node wins cleanly: the
// loser re-reads and verifies instead of overwriting.
func (s *Store) writeLayoutBatchSize(ctx context.Context, size int) error {
	key, err := aerospike.NewKey(s.namespace, layoutSetName, layoutRecordKey)
	if err != nil {
		return err
	}

	writePolicy := aerospike.NewWritePolicy(0, 0)
	writePolicy.RecordExistsAction = aerospike.CREATE_ONLY

	if putErr := s.client.PutBins(writePolicy, key, aerospike.NewBin(utxoBatchSizeBin, size)); putErr != nil {
		if putErr.Matches(types.KEY_EXISTS_ERROR) {
			// Another opener won the race; its value is the truth. Re-read
			// and let the caller verify against it.
			recorded, found, readErr := s.getLayoutBatchSize(ctx)
			if readErr != nil {
				return readErr
			}
			if found && recorded != size {
				return errors.NewConfigurationError("utxostore_utxoBatchSize=%d but this Aerospike namespace was already initialised with utxostore_utxoBatchSize=%d: the value is part of every coin's stored address - restore the original setting (check the SETTINGS_CONTEXT) before opening this store", size, recorded)
			}
			return nil
		}
		return errors.NewStorageError("failed to write the UTXO layout marker: %v", putErr)
	}

	s.logger.Infof("[aerospike] recorded utxostore_utxoBatchSize=%d as the layout of namespace=%s (set=%s); changing it later will refuse to open this store", size, s.namespace, layoutSetName)

	return nil
}

// enforceLayoutMarker records the batch size on first open and refuses any
// later open whose setting disagrees. The namespace, not the process, is the
// unit of agreement: every node pointed at the same namespace goes through
// the same record.
func (s *Store) enforceLayoutMarker(ctx context.Context) error {
	recorded, found, err := s.getLayoutBatchSize(ctx)
	if err != nil {
		return err
	}

	if !found {
		return s.writeLayoutBatchSize(ctx, s.utxoBatchSize)
	}

	if recorded != s.utxoBatchSize {
		return errors.NewConfigurationError("utxostore_utxoBatchSize=%d but this Aerospike namespace was written with utxostore_utxoBatchSize=%d: the value is part of every coin's stored address - restore the original setting (check the SETTINGS_CONTEXT) before opening this store", s.utxoBatchSize, recorded)
	}

	return nil
}
