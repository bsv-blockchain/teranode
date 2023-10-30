package validator

import (
	"context"

	"github.com/libsv/go-bt/v2"
)

type Interface interface {
	Health(ctx context.Context) (int, string, error)
	Validate(ctx context.Context, tx *bt.Tx) error
}
