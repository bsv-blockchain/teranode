package sql

import (
	"context"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
)

func (s *SQL) SetFSMState(ctx context.Context, state blockchain.FSMStateType) (err error) {
	return nil
}
