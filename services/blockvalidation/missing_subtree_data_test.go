package blockvalidation

// Tests for the classification of missing parent/transaction data surfaced by
// subtree validation.
//
// The transient marker is what keeps a failure OUT of the per-block
// BLOCK_INCOMPLETE retry cap (and off the catchup peer-penalty path), so it has
// to mean "the network will fix this", not just "data was missing". While
// syncing that is true — the block holding the parent has not been absorbed
// yet (#1031). While caught up it is not: the data is missing locally, nothing
// is going to supply it, and grinding the block forever is the scaling-incident
// wedge the cap exists to bound.

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMissingSubtreeDataError(t *testing.T) {
	ctx := context.Background()

	prev := chainhash.HashH([]byte("missing-data-prev"))
	merkleRoot := chainhash.HashH([]byte("missing-data-merkle"))
	block := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &merkleRoot}}

	// The incident's failure: parent lookups during subtree validation come
	// back TX_NOT_FOUND because the UTXO records are gone.
	cause := errors.NewTxNotFoundError("parent tx not found")

	newBlockValidation := func(state blockchain_api.FSMStateType, stateErr error) *BlockValidation {
		bcMock := &blockchain.Mock{}

		if stateErr != nil {
			bcMock.On("GetFSMCurrentState", mock.Anything).Return(nil, stateErr)
		} else {
			st := state
			bcMock.On("GetFSMCurrentState", mock.Anything).Return(&st, nil)
		}

		return &BlockValidation{logger: ulogger.TestLogger{}, blockchainClient: bcMock}
	}

	t.Run("transient while syncing", func(t *testing.T) {
		u := newBlockValidation(blockchain_api.FSMStateType_CATCHINGBLOCKS, nil)

		err := u.missingSubtreeDataError(ctx, ulogger.TestLogger{}, block, cause)

		require.True(t, errors.Is(err, errors.ErrBlockIncomplete), "must still signal incomplete, never invalid")
		require.True(t, errors.IsTransientBlockIncomplete(err), "a catchup-ordering gap must stay transient: the peer is honest and the retry must not consume the cap")
	})

	t.Run("countable while caught up", func(t *testing.T) {
		u := newBlockValidation(blockchain_api.FSMStateType_RUNNING, nil)

		err := u.missingSubtreeDataError(ctx, ulogger.TestLogger{}, block, cause)

		require.True(t, errors.Is(err, errors.ErrBlockIncomplete), "must signal incomplete, never invalid")
		require.False(t, errors.IsTransientBlockIncomplete(err), "missing local data while caught up must reach the per-block retry cap")
	})

	t.Run("countable when the FSM state cannot be determined", func(t *testing.T) {
		u := newBlockValidation(0, errors.NewServiceError("blockchain unavailable"))

		err := u.missingSubtreeDataError(ctx, ulogger.TestLogger{}, block, cause)

		require.True(t, errors.Is(err, errors.ErrBlockIncomplete))
		require.False(t, errors.IsTransientBlockIncomplete(err), "a degraded blockchain service must not silently disable the cap")
	})

	t.Run("never persisted as invalid", func(t *testing.T) {
		for _, state := range []blockchain_api.FSMStateType{blockchain_api.FSMStateType_RUNNING, blockchain_api.FSMStateType_CATCHINGBLOCKS} {
			u := newBlockValidation(state, nil)

			err := u.missingSubtreeDataError(ctx, ulogger.TestLogger{}, block, cause)

			require.False(t, errors.Is(err, errors.ErrBlockInvalid), "missing data is not a consensus violation in state %v", state)
		}
	})
}

// TestBlockProcessingWorkerCountsCaughtUpMissingData pins the end of the chain
// the classification feeds: the worker counts a non-transient incomplete toward
// the cap and skips a transient one. Without this pairing the cap is either
// blind to the incident (everything transient) or fires during routine sync.
func TestBlockProcessingWorkerCountsCaughtUpMissingData(t *testing.T) {
	initPrometheusMetrics()

	prev := chainhash.HashH([]byte("worker-count-prev"))
	merkleRoot := chainhash.HashH([]byte("worker-count-merkle"))
	block := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &merkleRoot}}

	caughtUp := &BlockValidation{logger: ulogger.TestLogger{}, blockchainClient: fsmMock(blockchain_api.FSMStateType_RUNNING)}
	syncing := &BlockValidation{logger: ulogger.TestLogger{}, blockchainClient: fsmMock(blockchain_api.FSMStateType_CATCHINGBLOCKS)}

	cause := errors.NewTxMissingParentError("parent tx missing")

	counted := caughtUp.missingSubtreeDataError(context.Background(), ulogger.TestLogger{}, block, cause)
	skipped := syncing.missingSubtreeDataError(context.Background(), ulogger.TestLogger{}, block, cause)

	// This is the exact predicate blockProcessingWorker applies.
	require.True(t, errors.Is(counted, errors.ErrBlockIncomplete) && !errors.IsTransientBlockIncomplete(counted), "the caught-up failure must be counted by the worker")
	require.False(t, errors.Is(skipped, errors.ErrBlockIncomplete) && !errors.IsTransientBlockIncomplete(skipped), "the syncing failure must be skipped by the worker")
}

func fsmMock(state blockchain_api.FSMStateType) *blockchain.Mock {
	bcMock := &blockchain.Mock{}
	st := state
	bcMock.On("GetFSMCurrentState", mock.Anything).Return(&st, nil)

	return bcMock
}
