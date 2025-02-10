package coinbase

import (
	"context"

	"github.com/libsv/go-bt/v2"
)

type ClientI interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	RequestFunds(ctx context.Context, address string, disableDistribute bool) (*bt.Tx, error)
	SetMalformedUTXOConfig(ctx context.Context, percentage int32, malformationType MalformationType) error
}
