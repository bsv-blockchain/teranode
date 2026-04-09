package repository

import (
	"context"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
)

// GetMiningCandidateLegacyBlockReader streams a mining candidate's block in standard Bitcoin wire format.
// This produces the exact format expected by SVNode's getblocktemplate proposal mode:
//
//	Header (80 bytes) + VarInt(txCount) + coinbaseTx + all remaining transactions
//
// The header and coinbase come from the block assembly service (via GetCandidateBlock gRPC).
// The remaining transactions are streamed from the subtree store using the same infrastructure
// as GetLegacyBlockReader.
func (repo *Repository) GetMiningCandidateLegacyBlockReader(ctx context.Context, header []byte, coinbaseTx []byte, subtreeHashes [][]byte, txCount uint64) (*io.PipeReader, error) {
	r, w := io.Pipe()

	go func() {
		// Write the 80-byte block header
		if _, err := w.Write(header); err != nil {
			_ = w.CloseWithError(errors.NewProcessingError("[GetMiningCandidateLegacyBlockReader] error writing header", err))
			return
		}

		// Write transaction count as VarInt
		txCountVarInt := bt.VarInt(txCount)
		if _, err := w.Write(txCountVarInt.Bytes()); err != nil {
			_ = w.CloseWithError(errors.NewProcessingError("[GetMiningCandidateLegacyBlockReader] error writing tx count", err))
			return
		}

		// Write coinbase transaction
		if _, err := w.Write(coinbaseTx); err != nil {
			_ = w.CloseWithError(errors.NewProcessingError("[GetMiningCandidateLegacyBlockReader] error writing coinbase tx", err))
			return
		}

		// Stream remaining transactions from subtrees
		for _, hashBytes := range subtreeHashes {
			subtreeHash, err := chainhash.NewHash(hashBytes)
			if err != nil {
				_ = w.CloseWithError(errors.NewProcessingError("[GetMiningCandidateLegacyBlockReader] invalid subtree hash", err))
				return
			}

			// Try FileTypeSubtreeData first (pre-assembled raw tx blob), same as GetLegacyBlockReader
			subtreeDataExists, err := repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			if err == nil && subtreeDataExists {
				if err := repo.streamSubtreeDataSkipCoinbase(ctx, w, subtreeHash); err != nil {
					_ = w.CloseWithError(err)
					return
				}

				continue
			}

			// Fall back to streaming individual transactions from the tx meta store.
			// Pass nil for block since coinbase was already written above.
			if err := repo.writeTransactionsViaSubtreeStoreStreaming(ctx, w, nil, subtreeHash); err != nil {
				_ = w.CloseWithError(err)
				return
			}
		}

		_ = w.Close()
	}()

	return r, nil
}

// streamSubtreeDataSkipCoinbase streams non-coinbase transactions from a pre-assembled subtree data blob.
func (repo *Repository) streamSubtreeDataSkipCoinbase(ctx context.Context, w io.Writer, subtreeHash *chainhash.Hash) error {
	subtreeDataReader, err := repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewProcessingError("[streamSubtreeDataSkipCoinbase] error getting subtree data %s", subtreeHash.String(), err)
	}

	defer func() {
		_ = subtreeDataReader.Close()
	}()

	for {
		tx := &bt.Tx{}

		if _, err = tx.ReadFrom(subtreeDataReader); err != nil {
			if err == io.EOF {
				break
			}
			return errors.NewProcessingError("[streamSubtreeDataSkipCoinbase] error reading tx: %s", err)
		}

		// Skip coinbase — it was already written by the caller
		if tx.IsCoinbase() {
			continue
		}

		if _, err = w.Write(tx.Bytes()); err != nil {
			return errors.NewProcessingError("[streamSubtreeDataSkipCoinbase] error writing tx: %s", err)
		}
	}

	return nil
}
