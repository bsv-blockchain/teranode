package coinbase

import (
	"context"

	"github.com/libsv/go-bt/v2"
)

type ClientI interface {
	RequestFunds(ctx context.Context, address string, disableDistribute bool) (*bt.Tx, error)
}
