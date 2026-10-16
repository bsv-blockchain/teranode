package model

import (
	"fmt"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/txstream"
)

// ReadSubtreeData reads a subtree data file against its subtree, as subtreepkg.NewSubtreeDataFromReader
// does and with the same rules and errors, but computes each transaction's id from the bytes as they
// are read instead of serializing the parsed transaction again to hash it.
//
// On mainnet on 2026-09-25 that second serialization was 38 GB of every 141 GB the node allocated.
//
// The rules it keeps: the file holds the subtree's transactions in order, in standard or extended
// form. When node zero is the coinbase placeholder the file may still start with the coinbase, which
// then fills slot zero unchecked. Every other transaction must have the id its node records. The file
// may end before the subtree is full, and a caller that needs every slot filled checks that itself
// (see peer_subtree_data.go).
func ReadSubtreeData(subtree *subtreepkg.Subtree, r io.Reader) (*subtreepkg.Data, error) {
	data, err := readSubtreeData(subtree, r)
	if err != nil {
		return nil, fmt.Errorf("unable to create subtree data from reader: %w", err)
	}

	return data, nil
}

func readSubtreeData(subtree *subtreepkg.Subtree, r io.Reader) (*subtreepkg.Data, error) {
	if subtree == nil || len(subtree.Nodes) == 0 {
		return nil, subtreepkg.ErrSubtreeNodesEmpty
	}

	data := &subtreepkg.Data{Subtree: subtree, Txs: make([]*bt.Tx, subtree.Length())}

	txIndex := 0
	if subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		txIndex = 1
	}

	txs := txstream.NewReader(r)

	for {
		tx, id, _, err := txs.Next(txstream.Options{})
		if err != nil {
			if errors.Is(err, io.EOF) {
				return data, nil
			}

			return nil, fmt.Errorf("error reading transaction: %w", err)
		}

		if txIndex == 1 && tx.IsCoinbase() {
			data.Txs[0] = tx

			continue
		}

		if txIndex >= len(subtree.Nodes) {
			return nil, subtreepkg.ErrTxIndexOutOfBounds
		}

		if !subtree.Nodes[txIndex].Hash.Equal(*id) {
			return nil, subtreepkg.ErrTxHashMismatch
		}

		data.Txs[txIndex] = tx
		txIndex++
	}
}
