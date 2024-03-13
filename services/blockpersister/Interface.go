package blockpersister

import (
	"context"
	"io"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
)

type reader interface {
	GetIoReader(ctx context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error)
}

type decorator interface {
	MetaBatchDecorate(ctx context.Context, hashes []*txmeta.MissingTxHash, fields ...string) error
}
