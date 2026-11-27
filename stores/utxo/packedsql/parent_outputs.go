package packedsql

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

const parentOutputsChunkSize = 2000

// ParentOutputsForValidation reads, for each distinct outpoint, one output of the parent's own
// outputs blob and the lowest block height in its block_refs. Two statements run per chunk:
// the first finds each output's byte span from the blob's offset table, the second reads
// exactly those bytes, so a parent with many outputs is never read whole. A parent with no
// row is TxNotFound, and a vout past the parent's output count is NoSuchIndex. A chunk that
// fails marks its own outpoints with the error and leaves the others answered.
func (s *Store) ParentOutputsForValidation(ctx context.Context, outpoints []utxo.Outpoint, _ ...utxo.ParentOutputOption) ([]utxo.ParentOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	answers := make([]utxo.ParentOutput, len(outpoints))
	if len(outpoints) == 0 {
		return answers, nil
	}

	slotsByOutpoint := make(map[utxo.Outpoint][]int, len(outpoints))
	distinct := make([]utxo.Outpoint, 0, len(outpoints))

	for i, op := range outpoints {
		if _, seen := slotsByOutpoint[op]; !seen {
			distinct = append(distinct, op)
		}

		slotsByOutpoint[op] = append(slotsByOutpoint[op], i)
	}

	for start := 0; start < len(distinct); start += parentOutputsChunkSize {
		chunk := distinct[start:min(start+parentOutputsChunkSize, len(distinct))]

		found, err := s.parentOutputsChunk(ctx, chunk)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
		}

		for i, op := range chunk {
			answer := utxo.ParentOutput{Status: utxo.ParentOutputTxNotFound}

			switch {
			case err != nil:
				answer = utxo.ParentOutput{Err: errors.NewStorageError("packedsql: error reading parent outputs", err)}
			case found[i] != nil:
				answer = *found[i]
			}

			for _, slot := range slotsByOutpoint[op] {
				answers[slot] = answer
			}
		}
	}

	return answers, nil
}

func (s *Store) parentOutputsChunk(ctx context.Context, chunk []utxo.Outpoint) ([]*utxo.ParentOutput, error) {
	refs := make([]outpointRef, len(chunk))
	for i, op := range chunk {
		h := op.TxID
		refs[i] = outpointRef{hash: h[:], vout: op.Vout}
	}

	offsetSQL, offsetArgs := buildOutpointQuery(refs, `CASE WHEN v.vout < `+outputCountExpr+`
	     THEN substring(t.outputs FROM v.vout * 4 + 5 FOR 8) END, octet_length(t.outputs), t.block_refs`)

	rows, err := s.pool.Query(ctx, offsetSQL, offsetArgs...)
	if err != nil {
		return nil, errors.NewStorageError("packedsql: parent output offset query failed", err)
	}

	defer rows.Close()

	results := make([]*utxo.ParentOutput, len(chunk))
	spans := make(map[int]outputSpan, len(chunk))

	for rows.Next() {
		var (
			idx         int
			offsetBytes []byte
			totalLen    int64
			blockRefs   []byte
		)

		if err = rows.Scan(&idx, &offsetBytes, &totalLen, &blockRefs); err != nil {
			return nil, errors.NewStorageError("packedsql: parent output offset scan failed", err)
		}

		if len(offsetBytes) != 8 {
			results[idx] = &utxo.ParentOutput{Status: utxo.ParentOutputNoSuchIndex}
			continue
		}

		start := int64(binary.LittleEndian.Uint32(offsetBytes[:4]))
		end := int64(binary.LittleEndian.Uint32(offsetBytes[4:]))

		if end == start {
			results[idx] = &utxo.ParentOutput{Status: utxo.ParentOutputNoSuchIndex}
			continue
		}

		if end < start || start > totalLen || end > totalLen {
			results[idx] = &utxo.ParentOutput{Err: errors.NewStorageError("packedsql: outputs blob of %s is corrupt at vout %d",
				chainhash.Hash(refs[idx].hash), refs[idx].vout)}
			continue
		}

		spans[idx] = outputSpan{start: start + 1, length: end - start}
		results[idx] = &utxo.ParentOutput{Status: utxo.ParentOutputNotMined}

		if _, heights, _ := unpackBlockRefs(blockRefs); len(heights) > 0 {
			results[idx].Status = utxo.ParentOutputMined
			results[idx].Height = slicesMin(heights)
		}
	}

	rows.Close()

	if err = rows.Err(); err != nil {
		return nil, errors.NewStorageError("packedsql: parent output offset rows failed", err)
	}

	if len(spans) == 0 {
		return results, nil
	}

	spanRefs := make([]outpointRef, 0, len(spans))
	spanList := make([]outputSpan, 0, len(spans))
	spanOwner := make([]int, 0, len(spans))

	for idx, sp := range spans {
		spanRefs = append(spanRefs, refs[idx])
		spanList = append(spanList, sp)
		spanOwner = append(spanOwner, idx)
	}

	err = s.fetchOutputBytes(ctx, spanRefs, spanList, func(i int, outputBytes []byte) error {
		output := &bt.Output{}
		if _, readErr := output.ReadFrom(bytes.NewReader(outputBytes)); readErr != nil {
			results[spanOwner[i]] = &utxo.ParentOutput{Err: errors.NewTxInvalidError("packedsql: could not read parent output", readErr)}
			return nil
		}

		results[spanOwner[i]].Satoshis = output.Satoshis
		results[spanOwner[i]].LockingScript = output.LockingScript

		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func slicesMin(v []uint32) uint32 {
	m := v[0]

	for _, x := range v[1:] {
		m = min(m, x)
	}

	return m
}
