// //go:build aerospike

package aerospike

import (
	"context"

	"github.com/libsv/go-bt/v2"
)

func (s *Store) ProcessConflicting(ctx context.Context, tx *bt.Tx) (err error) {
	return nil
}
