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
		err := repo.writeMiningCandidateBlock(ctx, w, header, coinbaseTx, subtreeHashes, txCount)
		if err != nil {
			_ = w.CloseWithError(err)
			return
		}

		_ = w.Close()
	}()

	return r, nil
}

// writeMiningCandidateBlock writes a complete wire format block to the writer.
// Extracted from the goroutine to reduce cognitive complexity and improve testability.
func (repo *Repository) writeMiningCandidateBlock(ctx context.Context, w io.Writer, header []byte, coinbaseTx []byte, subtreeHashes [][]byte, txCount uint64) error {
	if _, err := w.Write(header); err != nil {
		return errors.NewProcessingError("[writeMiningCandidateBlock] error writing header", err)
	}

	txCountVarInt := bt.VarInt(txCount)
	if _, err := w.Write(txCountVarInt.Bytes()); err != nil {
		return errors.NewProcessingError("[writeMiningCandidateBlock] error writing tx count", err)
	}

	if _, err := w.Write(coinbaseTx); err != nil {
		return errors.NewProcessingError("[writeMiningCandidateBlock] error writing coinbase tx", err)
	}

	for _, hashBytes := range subtreeHashes {
		if err := repo.streamSubtreeTransactions(ctx, w, hashBytes); err != nil {
			return err
		}
	}

	return nil
}

// streamSubtreeTransactions streams non-coinbase transactions from a single subtree.
// Tries the pre-assembled SubtreeData first, falls back to individual tx fetching.
func (repo *Repository) streamSubtreeTransactions(ctx context.Context, w io.Writer, hashBytes []byte) error {
	subtreeHash, err := chainhash.NewHash(hashBytes)
	if err != nil {
		return errors.NewProcessingError("[streamSubtreeTransactions] invalid subtree hash", err)
	}

	// Try pre-assembled subtree data first (fast path)
	subtreeDataExists, err := repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err == nil && subtreeDataExists {
		return repo.streamSubtreeDataSkipCoinbase(ctx, w, subtreeHash)
	}

	// Fall back to streaming individual transactions from the tx meta store
	return repo.writeTransactionsViaSubtreeStoreStreaming(ctx, w, nil, subtreeHash)
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

		if tx.IsCoinbase() {
			continue
		}

		if _, err = w.Write(tx.Bytes()); err != nil {
			return errors.NewProcessingError("[streamSubtreeDataSkipCoinbase] error writing tx: %s", err)
		}
	}

	return nil
}
