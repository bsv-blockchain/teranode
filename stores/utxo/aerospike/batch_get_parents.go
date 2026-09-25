package aerospike

import (
	"bytes"
	"context"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// ParentRecord is a slim view of a parent tx record containing just enough
// for the validator to extend a child tx's inputs. BlockHeight is derived
// from the first entry in the fields.BlockHeights bin (0 when unmined).
// Outputs holds the decoded transaction outputs so Phase A hydration can
// populate PreviousTxScript and PreviousTxSatoshis without any extra
// Aerospike round-trips — pure CPU work after the single BatchOperate call.
type ParentRecord struct {
	// BlockHeight is the height of the block that first mined this parent, or
	// 0 if the parent is unmined. Derived from fields.BlockHeights bin index 0.
	BlockHeight uint32
	// Outputs holds the decoded outputs of the parent tx. Decoded once at
	// BatchGetParents fetch time; callers use OutputAt to access individual
	// outputs by index.
	Outputs []*bt.Output
}

// OutputAt returns the output at the given index, or an error if the index is
// out of range or the output is nil.
func (p *ParentRecord) OutputAt(idx uint32) (*bt.Output, error) {
	if p == nil || int(idx) >= len(p.Outputs) || p.Outputs[idx] == nil { //nolint:gosec
		return nil, terrors.NewProcessingError(
			"[BatchGetParents] output index %d out of range or nil for parent record",
			idx,
		)
	}
	return p.Outputs[idx], nil
}

// outputsFromBins decodes the Outputs bin from an Aerospike BinMap into a
// slice of *bt.Output. Returns nil (not an error) when the bin is absent —
// this covers coinbase records and external-storage txs whose outputs are not
// stored inline; callers that need outputs for a non-inline tx should fall
// back to external storage.
func outputsFromBins(bins as.BinMap) ([]*bt.Output, error) {
	raw, ok := bins[fields.Outputs.String()]
	if !ok {
		return nil, nil
	}

	outputInterfaces, ok := raw.([]interface{})
	if !ok {
		return nil, terrors.NewProcessingError("[outputsFromBins] outputs bin is not []interface{}")
	}

	outputs := make([]*bt.Output, len(outputInterfaces))
	for i, oi := range outputInterfaces {
		if oi == nil {
			continue
		}
		b, ok := oi.([]byte)
		if !ok {
			return nil, terrors.NewProcessingError("[outputsFromBins] output element %d is not []byte", i)
		}
		outputs[i] = &bt.Output{}
		if _, err := outputs[i].ReadFrom(bytes.NewReader(b)); err != nil {
			return nil, terrors.NewProcessingError("[outputsFromBins] could not read output %d", i, err)
		}
	}
	return outputs, nil
}

// BatchGetParents fetches a slice of parent tx records in a single Aerospike
// BatchOperate call. It is a direct, pass-through method: no go-batcher, no
// buffering, no timeouts. The caller is responsible for chunking if needed.
//
// Each returned ParentRecord has its Outputs field decoded from the raw bins
// at fetch time — this is pure CPU work and incurs zero additional round-trips.
// Phase A of validateBatchNative uses OutputAt to hydrate child inputs without
// any further Aerospike calls.
//
// Returns:
//   - found:   map keyed by 32-byte parent hash to *ParentRecord for every
//     hash that had an Aerospike record.
//   - missing: slice of input hashes that had no record. This is NOT a fatal
//     error; the caller maps these to per-tx ErrTxMissingParent.
//   - err:     non-nil only when the entire BatchOperate call fails.
func (s *Store) BatchGetParents(ctx context.Context, parentHashes [][]byte) (map[[32]byte]*ParentRecord, [][]byte, error) {
	if len(parentHashes) == 0 {
		return map[[32]byte]*ParentRecord{}, nil, nil
	}

	batchPolicy := util.GetAerospikeBatchPolicy(s.settings)

	records := make([]as.BatchRecordIfc, len(parentHashes))
	for i, h := range parentHashes {
		key, err := as.NewKey(s.namespace, s.setName, h)
		if err != nil {
			return nil, nil, err
		}
		// nil bin list means fetch all bins.
		records[i] = as.NewBatchRead(nil, key, nil)
	}

	batchOperateFn := s.client.BatchOperate
	if s.batchOperateFn != nil {
		batchOperateFn = s.batchOperateFn
	}

	if asErr := batchOperateFn(batchPolicy, records); asErr != nil {
		return nil, nil, asErr
	}

	found := make(map[[32]byte]*ParentRecord, len(parentHashes))
	var missing [][]byte

	for i, r := range records {
		br, ok := r.(*as.BatchRead)
		if !ok {
			return nil, nil, terrors.NewProcessingError("[batchGetParents] unexpected batch record type %T at index %d", r, i)
		}
		rec := br.Record
		if rec == nil {
			missing = append(missing, parentHashes[i])
			continue
		}

		var key [32]byte
		copy(key[:], parentHashes[i])

		bh := blockHeightFromBins(rec.Bins)

		outputs, decErr := outputsFromBins(rec.Bins)
		if decErr != nil {
			return nil, nil, decErr
		}

		found[key] = &ParentRecord{
			BlockHeight: bh,
			Outputs:     outputs,
		}
	}

	return found, missing, nil
}

// blockHeightFromBins extracts the first block height stored in the
// fields.BlockHeights bin. Returns 0 when the tx is unmined or the bin is
// absent/malformed — the caller must treat 0 as "unmined".
func blockHeightFromBins(bins as.BinMap) uint32 {
	v, ok := bins[fields.BlockHeights.String()]
	if !ok {
		return 0
	}

	switch t := v.(type) {
	case []interface{}:
		if len(t) == 0 {
			return 0
		}
		switch first := t[0].(type) {
		case int:
			if first < 0 {
				return 0
			}
			return uint32(first) //nolint:gosec
		case uint32:
			return first
		}
	}

	return 0
}
