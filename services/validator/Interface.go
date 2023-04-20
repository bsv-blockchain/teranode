package validator

import (
	"context"

	"github.com/libsv/go-bt/v2"
)

type Interface interface {
	Validate(ctx context.Context, tx *bt.Tx) error
}
