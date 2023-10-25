package coinbase

import (
	"context"

	"github.com/libsv/go-bt/v2"
)

type ClientI interface {
	RequestFunds(ctx context.Context, address string) (*bt.Tx, error)
}
