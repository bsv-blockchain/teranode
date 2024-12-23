package redis2

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	data, err := s.client.HGet(ctx, spend.TxID.String(), fmt.Sprintf("utxo:%d", spend.Vout)).Result()
	if err != nil {
		return nil, err
	}

	switch {
	case len(data) == 0:
		return &utxo.SpendResponse{
			Status: int(utxo.Status_NOT_FOUND),
		}, nil

	case len(data) == 64 || len(data) == 128:
		utxoHash, err := chainhash.NewHashFromStr(data[:64])
		if err != nil {
			return nil, errors.NewProcessingError("chain hash error", err)
		}

		if !utxoHash.IsEqual(spend.UTXOHash) {
			return nil, errors.NewProcessingError("utxo hash mismatch", nil)
		}

		if len(data) == 64 {
			return &utxo.SpendResponse{
				Status: int(utxo.Status_OK),
			}, nil
		}

		spendingTxID, err := chainhash.NewHashFromStr(data[64:])
		if err != nil {
			return nil, errors.NewProcessingError("chain hash error", err)
		}

		if spendingTxID.IsEqual((*chainhash.Hash)(frozenUTXOBytes)) {
			return &utxo.SpendResponse{
				Status: int(utxo.Status_FROZEN),
			}, nil
		}

		return &utxo.SpendResponse{
			Status:       int(utxo.Status_SPENT),
			SpendingTxID: spendingTxID,
		}, nil

	default:
		return nil, errors.NewProcessingError("invalid utxo hash length: %d", len(data))
	}
}
