package redis2

import (
	"bufio"
	"context"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error) {
	data, err := s.client.HGetAll(ctx, hash.String()).Result()
	if err != nil {
		return nil, errors.NewStorageError("failed to get transaction", err)
	}

	// Add this check for empty results
	if len(data) == 0 {
		return nil, errors.NewNotFoundError("transaction not found: %s", hash.String())
	}

	tx, err := s.retrieveRawTransaction(ctx, hash)
	if err != nil {
		if !errors.Is(err, errors.ErrNotFound) {
			return nil, errors.NewStorageError("failed to get transaction from transaction store", err)
		}
	}

	fee, err := strconv.ParseUint(data["fee"], 10, 64)
	if err != nil {
		return nil, errors.NewTxInvalidError("could not parse fee: %v", err)
	}

	blockIDStrings := strings.Split(data["blockIDs"], ",")
	blockIDs := make([]uint32, 0, len(blockIDStrings))

	for _, blockID := range blockIDStrings {
		blockIDInt, err := strconv.ParseUint(blockID, 10, 32)
		if err != nil {
			return nil, errors.NewTxInvalidError("could not parse block ID: %v", err)
		}

		blockIDs = append(blockIDs, uint32(blockIDInt)) // nolint: gosec
	}

	isCoinbase, err := strconv.ParseBool(data["isCoinbase"])
	if err != nil {
		return nil, errors.NewTxInvalidError("could not parse isCoinbase: %v", err)
	}

	metaData := &meta.Data{
		Tx:         tx,
		Fee:        fee,
		BlockIDs:   blockIDs,
		IsCoinbase: isCoinbase,
	}

	if tx != nil {
		parentTxHashes := make([]chainhash.Hash, 0, len(tx.Inputs))
		for _, input := range tx.Inputs {
			parentTxHashes = append(parentTxHashes, *input.PreviousTxIDChainHash())
		}

		metaData.ParentTxHashes = parentTxHashes
		metaData.LockTime = tx.LockTime
		metaData.SizeInBytes = uint64(tx.Size())
		metaData.ParentTxHashes = parentTxHashes
	}

	return metaData, nil
}

func (s *Store) retrieveRawTransaction(ctx context.Context, txHash *chainhash.Hash) (*bt.Tx, error) {
	reader, err := s.transactionStore.GetIoReader(
		ctx,
		txHash[:],
		options.WithFileExtension("tx"),
	)
	if err != nil {
		return nil, errors.NewStorageError("[retrieveRawTransaction][%s] could not get tx from transaction store", txHash.String(), err)
	}

	tx := &bt.Tx{}

	// create a buffer for the reader
	bufferedReader := bufio.NewReaderSize(reader, 1*1024*1024) // 1MB buffer

	if _, err = tx.ReadFrom(bufferedReader); err != nil {
		return nil, errors.NewTxInvalidError("[retrieveRawTransaction][%s] could not read tx from reader: %w", txHash.String(), err)
	}

	return tx, nil
}
