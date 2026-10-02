package aerospike

import (
	"bytes"
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// ParentOutputsForValidation implements utxo.Store. Every outpoint goes through
// the shared outpoint batcher, which reads each distinct parent's master record
// once, fetching only the outputs, external and blockHeights bins. External
// parents are reconstructed through GetOutpointsFromExternalStore, which re-hashes
// the blob against its key and caches the outputs.
//
// An index past the end of the parent's output list is NoSuchIndex, and so is a
// nil entry inside it: an output the seeder left out when it rebuilt the parent
// from a UTXO snapshot, or one an external reconstruction dropped as provably
// unspendable. Both are the outcome (TxInvalid) PreviousOutputsDecorate reports
// for the same cases today.
func (s *Store) ParentOutputsForValidation(ctx context.Context, outpoints []utxo.Outpoint, _ ...utxo.ParentOutputOption) ([]utxo.ParentOutput, error) {
	_, _, deferFn := tracing.Tracer("aerospike").Start(ctx, "aerospike:ParentOutputsForValidation")
	defer deferFn()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	answers := make([]utxo.ParentOutput, len(outpoints))
	if len(outpoints) == 0 {
		return answers, nil
	}

	// One item per distinct outpoint; the batch groups items by parent itself.
	slotsByOutpoint := make(map[utxo.Outpoint][]int, len(outpoints))
	items := make([]*batchOutpoint, 0, len(outpoints))

	for i := range outpoints {
		op := outpoints[i]
		if _, seen := slotsByOutpoint[op]; !seen {
			items = append(items, &batchOutpoint{parent: &op})
		}

		slotsByOutpoint[op] = append(slotsByOutpoint[op], i)
	}

	itemCount, err := safeconversion.IntToInt32(len(items))
	if err != nil {
		return nil, errors.NewProcessingError("too many outpoints in one call: %d", len(items), err)
	}

	group := completion.NewGroup(itemCount)

	for _, item := range items {
		item.group = group
		if err := safeBatcherPut(s.outpointBatcher, item, "outpoint"); err != nil {
			item.answer = utxo.ParentOutput{Err: err}
			item.complete(nil)
		}
	}

	if err := group.Wait(ctx, s.batcherWait); err != nil {
		// Do not read item slots on the error path: the dispatcher may still be
		// writing to them after we have given up waiting.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		return nil, errors.NewServiceUnavailableError("aerospike outpoint batch did not complete within %s", s.batcherWait)
	}

	for _, item := range items {
		answer := item.answer
		if item.result != nil && answer.Err == nil {
			// The batch failed this item without writing an answer (a recovered
			// panic, for example).
			answer = utxo.ParentOutput{Err: item.result}
		}

		for _, slot := range slotsByOutpoint[*item.parent] {
			answers[slot] = answer
		}
	}

	return answers, nil
}

// parentOutputAnswer builds the answer for one outpoint of a parent read by
// sendOutpointBatch. readErr is the error recorded for the parent's record, if
// any; heightsErr is the error from parsing its blockHeights bin.
func parentOutputAnswer(vout uint32, parent *bt.Tx, readErr error, external bool, heights []uint32, heightsErr error) utxo.ParentOutput {
	if parent == nil {
		if readErr == nil {
			return utxo.ParentOutput{Status: utxo.ParentOutputTxNotFound}
		}

		if errors.Is(readErr, errors.ErrTxNotFound) && !external {
			// Only a missing master record is a definite "not found". A missing
			// external blob is a fault in this node's own storage.
			return utxo.ParentOutput{Status: utxo.ParentOutputTxNotFound}
		}

		return utxo.ParentOutput{Err: readErr}
	}

	if heightsErr != nil {
		return utxo.ParentOutput{Err: heightsErr}
	}

	if !external && parent.Outputs == nil {
		return utxo.ParentOutput{Err: errors.NewStorageError("parent record has no outputs bin")}
	}

	if int(vout) >= len(parent.Outputs) {
		return utxo.ParentOutput{Status: utxo.ParentOutputNoSuchIndex}
	}

	// A nil entry is an output the store never held: the seeder leaves spent
	// positions nil when it rebuilds a parent from a UTXO snapshot, and an
	// external reconstruction drops them the same way. Reporting it as a fault
	// made the validator retry a block that spends such an output for ever,
	// where the legacy path and the SQL store both reject it as invalid.
	out := parent.Outputs[vout]
	if out == nil {
		return utxo.ParentOutput{Status: utxo.ParentOutputNoSuchIndex}
	}

	answer := utxo.ParentOutput{
		Status:        utxo.ParentOutputNotMined,
		Satoshis:      out.Satoshis,
		LockingScript: out.LockingScript,
	}

	for i, h := range heights {
		if i == 0 || h < answer.Height {
			answer.Height = h
		}

		answer.Status = utxo.ParentOutputMined
	}

	return answer
}

// outputsFromBins decodes the outputs bin of an inline record. It reads no
// other bin, so the batch that feeds it need not fetch the parent's inputs,
// version or locktime. A record with no outputs bin gives a transaction whose
// Outputs is nil.
func outputsFromBins(bins aerospike.BinMap) (*bt.Tx, error) {
	tx := &bt.Tx{}

	outputInterfaces, ok := bins[fields.Outputs.String()].([]interface{})
	if !ok {
		return tx, nil
	}

	tx.Outputs = make([]*bt.Output, len(outputInterfaces))

	for i, outputInterface := range outputInterfaces {
		if outputInterface == nil {
			continue
		}

		outputBytes, ok := outputInterface.([]byte)
		if !ok {
			return nil, errors.NewStorageError("output %d has unexpected type %T", i, outputInterface)
		}

		tx.Outputs[i] = &bt.Output{}

		if _, err := tx.Outputs[i].ReadFrom(bytes.NewReader(outputBytes)); err != nil {
			return nil, errors.NewStorageError(errCouldNotReadOutput, err)
		}
	}

	return tx, nil
}
