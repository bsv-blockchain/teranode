package aerospike

import (
	"context"

	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return nil, nil, nil
}

func (s *Store) SetUnspendable(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	return nil
}
