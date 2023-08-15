package coinbase

import (
	"context"

	"github.com/libsv/go-bt/v2"
)

type ClientI interface {
	GetUtxo(ctx context.Context, address string) (*bt.UTXO, error)
	MarkUtxoSpent(ctx context.Context, txId []byte, vout uint32, spentByTxId []byte) error
}
