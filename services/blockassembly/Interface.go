package blockassembly

import (
	"context"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-bt/v2/chainhash"
)

type ClientI interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64) (bool, error)
	RemoveTx(ctx context.Context, hash *chainhash.Hash) error
	GetMiningCandidate(ctx context.Context) (*model.MiningCandidate, error)
	GetCurrentDifficulty(ctx context.Context) (float64, error)
	SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error
	GenerateBlocks(ctx context.Context, count int32) error
	DeDuplicateBlockAssembly(ctx context.Context) error
	ResetBlockAssembly(ctx context.Context) error
	GetBlockAssemblyState(ctx context.Context) (*blockassembly_api.StateMessage, error)
}

type Store interface {
	Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64) (bool, error)
	RemoveTx(ctx context.Context, hash *chainhash.Hash) error
}

type Mock struct {
	State *blockassembly_api.StateMessage
}

func NewMock() (ClientI, error) {
	c := &Mock{}

	return c, nil
}

func (m Mock) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "", nil
}

func (m Mock) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64) (bool, error) {
	// TODO implement me
	panic("implement me")
}

func (m Mock) RemoveTx(ctx context.Context, hash *chainhash.Hash) error {
	// TODO implement me
	panic("implement me")
}

func (m Mock) GetMiningCandidate(ctx context.Context) (*model.MiningCandidate, error) {
	// TODO implement me
	panic("implement me")
}

func (m Mock) GetCurrentDifficulty(ctx context.Context) (float64, error) {
	// TODO implement me
	panic("implement me")
}

func (m Mock) SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error {
	// TODO implement me
	panic("implement me")
}

func (m Mock) GenerateBlocks(ctx context.Context, count int32) error {
	// TODO implement me
	panic("implement me")
}

func (m Mock) DeDuplicateBlockAssembly(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (m Mock) ResetBlockAssembly(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (m Mock) GetBlockAssemblyState(ctx context.Context) (*blockassembly_api.StateMessage, error) {
	return m.State, nil
}
