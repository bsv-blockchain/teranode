package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	teraslab "github.com/icellan/teraslab/client/go"
)

// ParentOutputsForValidation returns, for each outpoint, the parent output's
// value and locking script and the creation height, read-only from the parent's
// own stored outputs (never a child's input copy). One record read per distinct
// parent; duplicate outpoints get identical answers. A missing parent or an
// out-of-range vout is a per-outpoint status, not an error — only a whole-call
// failure (cancelled ctx, store unreachable) returns an error. See the utxo.Store
// interface contract.
func (s *Store) ParentOutputsForValidation(ctx context.Context, outpoints []utxo.Outpoint, _ ...utxo.ParentOutputOption) ([]utxo.ParentOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	answers := make([]utxo.ParentOutput, len(outpoints))
	if len(outpoints) == 0 {
		return answers, nil
	}

	// Dedup by parent txid: one batched record read covers every outpoint on the
	// same parent. order preserves the request-aligned txid list for the batch.
	slotsByTxID := make(map[chainhash.Hash][]int, len(outpoints))
	order := make([]chainhash.Hash, 0, len(outpoints))

	for i := range outpoints {
		tid := outpoints[i].TxID
		if _, seen := slotsByTxID[tid]; !seen {
			order = append(order, tid)
		}

		slotsByTxID[tid] = append(slotsByTxID[tid], i)
	}

	txids := make([]teraslab.TxID, len(order))
	for i := range order {
		h := order[i]
		txids[i] = hashToTxID(&h)
	}

	// Outputs (value + script) and the block entries (creation heights) of each
	// parent. recordToMetaData decodes the outputs into data.Tx and the heights
	// into data.BlockHeights.
	mask := buildFieldMask([]fields.FieldName{fields.Tx, fields.BlockIDs})

	records, err := s.client.GetRecordBatch(ctx, mask, txids)
	if err != nil {
		// A clean not-found for the whole batch is a status, not a failure: every
		// requested parent is absent. Any other error is a whole-call failure.
		if _, ok := err.(*teraslab.NotFoundError); ok {
			for _, slots := range slotsByTxID {
				for _, slot := range slots {
					answers[slot] = utxo.ParentOutput{Status: utxo.ParentOutputTxNotFound}
				}
			}

			return answers, nil
		}

		return nil, err
	}

	for ri := range order {
		slots := slotsByTxID[order[ri]]

		if ri >= len(records) || !records[ri].Found {
			for _, slot := range slots {
				answers[slot] = utxo.ParentOutput{Status: utxo.ParentOutputTxNotFound}
			}

			continue
		}

		data, derr := recordToMetaData(records[ri])
		if derr != nil {
			for _, slot := range slots {
				answers[slot] = utxo.ParentOutput{Err: derr}
			}

			continue
		}

		// Height is the lowest recorded block height; absence of any block means
		// the parent is not mined (value/script still set). KNOWN LIMITATION: the
		// TeraSlab 0.9.1 server treats a create-time mined BlockID of 0 (the
		// cmd/seeder snapshot convention) as unmined and stores no block entry, so
		// a snapshot-seeded parent reads back NotMined here where SQL/Aerospike
		// report Mined — see icellan/teraslab#105.
		mined := len(data.BlockHeights) > 0
		var height uint32

		if mined {
			height = data.BlockHeights[0]
			for _, h := range data.BlockHeights[1:] {
				if h < height {
					height = h
				}
			}
		}

		for _, slot := range slots {
			vout := outpoints[slot].Vout

			// A vout past the output list, or a nil output gap (an output already
			// spent when a seeder snapshot was taken), is a NoSuchIndex verdict,
			// never a transient fault the caller would retry.
			if data.Tx == nil || int(vout) >= len(data.Tx.Outputs) || data.Tx.Outputs[vout] == nil {
				answers[slot] = utxo.ParentOutput{Status: utxo.ParentOutputNoSuchIndex}
				continue
			}

			out := data.Tx.Outputs[vout]
			answer := utxo.ParentOutput{Satoshis: out.Satoshis, LockingScript: out.LockingScript}

			if mined {
				answer.Status = utxo.ParentOutputMined
				answer.Height = height
			} else {
				answer.Status = utxo.ParentOutputNotMined
			}

			answers[slot] = answer
		}
	}

	return answers, nil
}
