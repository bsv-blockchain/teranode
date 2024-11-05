package redis

import (
	"context"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error) {
	res := s.client.HGetAll(ctx, hash.String())
	if err := res.Err(); err != nil {
		return nil, errors.NewStorageError("failed to get transaction", err)
	}

	txRecord := TXRecord{}
	if err := res.Scan(&txRecord); err != nil {
		return nil, errors.NewStorageError("failed to scan transaction", err)
	}

	data, err := res.Result()
	if err != nil {
		return nil, errors.NewStorageError("failed to get transaction fields", err)
	}

	// Add this check for empty results
	if len(data) == 0 {
		return nil, errors.NewNotFoundError("transaction not found: %s", hash.String())
	}

	tx, err := getTxFromFields(txRecord, data)
	if err != nil {
		return nil, errors.NewTxInvalidError("could not get tx from fields", err)
	}

	metaData, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.NewTxInvalidError("could not get tx meta data from tx", err)
	}

	if len(txRecord.BlockIDs) > 0 {
		blockIDStrings := strings.Split(txRecord.BlockIDs, ",")
		blockIDs := make([]uint32, 0, len(blockIDStrings))

		for _, blockID := range blockIDStrings {
			blockIDInt, err := strconv.ParseUint(blockID, 10, 32)
			if err != nil {
				return nil, errors.NewTxInvalidError("could not parse block ID: %v", err)
			}

			blockIDs = append(blockIDs, uint32(blockIDInt)) // nolint: gosec
		}

		metaData.BlockIDs = blockIDs
	}

	return metaData, nil
}
