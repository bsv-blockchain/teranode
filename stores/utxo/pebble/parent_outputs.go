package pebble

import (
	"bytes"
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// ParentOutputsForValidation reads parent outputs and their creation heights
// from one snapshot. Grouping by parent copies each immutable payload once.
func (s *Store) ParentOutputsForValidation(ctx context.Context, outpoints []utxo.Outpoint, _ ...utxo.ParentOutputOption) ([]utxo.ParentOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	answers := make([]utxo.ParentOutput, len(outpoints))
	if len(outpoints) == 0 {
		return answers, nil
	}

	snap, release, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	defer release()

	byParent := make(map[chainhash.Hash][]int)
	for i, op := range outpoints {
		byParent[op.TxID] = append(byParent[op.TxID], i)
	}

	for hash, slots := range byParent {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		m, err := getMasterFrom(snap, &hash)
		if err != nil {
			answer := utxo.ParentOutput{Err: err}
			if errors.Is(err, errors.ErrTxNotFound) {
				answer = utxo.ParentOutput{Status: utxo.ParentOutputTxNotFound}
			}
			for _, i := range slots {
				answers[i] = answer
			}
			continue
		}

		_, outputs, err := s.payloadBlobs(snap, &hash)
		if err != nil {
			for _, i := range slots {
				// A master without its payload in the same snapshot is a
				// storage fault, not proof that the parent is absent.
				answers[i].Err = errors.NewStorageError("pebble: parent payload unavailable", err)
			}
			continue
		}

		status := utxo.ParentOutputNotMined
		var height uint32
		_, heights, _ := unpackBlockRefs(m.blockRefs)
		for i, h := range heights {
			if i == 0 || h < height {
				height = h
			}
			status = utxo.ParentOutputMined
		}

		for _, i := range slots {
			vout := outpoints[i].Vout
			if vout >= m.outputCount {
				answers[i].Status = utxo.ParentOutputNoSuchIndex
				continue
			}

			item, err := offsetBlobItem(outputs, int(vout))
			if err != nil {
				answers[i].Err = err
				continue
			}
			if len(item) == 0 {
				answers[i].Status = utxo.ParentOutputNoSuchIndex
				continue
			}

			output := &bt.Output{}
			if _, err = output.ReadFrom(bytes.NewReader(item)); err != nil {
				answers[i].Err = errors.NewStorageError("pebble: invalid parent output", err)
				continue
			}
			answers[i] = utxo.ParentOutput{
				Status: status, Height: height,
				Satoshis: output.Satoshis, LockingScript: output.LockingScript,
			}
		}
	}

	return answers, nil
}
