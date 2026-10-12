package netsync

import (
	"bytes"
	"context"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// failingBlockValidation embeds the no-op mock and makes ProcessBlock return a
// chosen error, counting the calls.
type failingBlockValidation struct {
	*blockvalidation.MockBlockValidation
	err   error
	calls int
}

func (f *failingBlockValidation) ProcessBlock(context.Context, *model.Block, uint32, string, string, uint32) error {
	f.calls++
	return f.err
}

// During catch-up, prepareSubtrees no longer checks each subtree itself; block
// validation checks the whole block through the batch path and its verdict comes
// back through ProcessBlock. The peer's treatment is decided on that returned
// error: handleBlockMsg records a transient-failure backoff only for a local
// fault, and the peer server's shouldDisconnectOnBlockErr disconnects on
// anything that is not one. So an invalid transaction from a legacy peer gives
// the same strike as before as long as the two error shapes the batch path
// produces, a block verdict for a consensus failure and a storage fault for a
// store failure, reach that return value unchanged. This drives handleBlockMsg
// with each shape and checks where it lands.
func TestHandleBlockMsg_CatchUpBatchPathVerdictsKeepPeerTreatment(t *testing.T) {
	initPrometheusMetrics()

	cases := []struct {
		name        string
		processErr  error
		wantBackoff bool
	}{
		{
			name: "an invalid transaction is a block verdict: no backoff, and the peer server disconnects",
			// The shape processTransactionsBatched returns for a consensus
			// failure, wrapped as block validation and netsync wrap it.
			processErr: errors.NewBlockInvalidError("block invalid",
				errors.NewTxInvalidError("transaction spends output 7 of a parent that has 1")),
			wantBackoff: false,
		},
		{
			name: "a store fault is transient and local: backoff, and the peer server keeps the peer",
			// The shape processTransactionsBatched returns when a parent read
			// fails, which it reports as a processing error over the storage error.
			processErr: errors.NewProcessingError("failed to read parent output",
				errors.NewStorageError("aerospike timeout")),
			wantBackoff: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			block := twoTxLegacyBlock(t)
			msgBlock := block.MsgBlock()
			msgBlock.Header.Bits = 0x207fffff // regtest max target

			for {
				var hdr bytes.Buffer
				require.NoError(t, msgBlock.Header.Serialize(&hdr))
				mh, err := model.NewBlockHeaderFromBytes(hdr.Bytes())
				require.NoError(t, err)
				if ok, _, _ := mh.HasMetTargetDifficulty(); ok {
					break
				}
				msgBlock.Header.Nonce++
			}

			blockHash := msgBlock.Header.BlockHash()
			catching := blockchain.FSMStateCATCHINGBLOCKS

			blockchainClient := &blockchain.Mock{}
			blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catching, nil)
			blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
			blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: uint32(block.Height()) - 1}, nil)
			blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
			blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 99}, nil)
			blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
			subtreeValidationClient.On("CheckSubtreeFromBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

			blockValidationClient := &failingBlockValidation{MockBlockValidation: &blockvalidation.MockBlockValidation{}, err: tc.processErr}

			sm, p := newBackoffTestManager(t, blockchainClient, blockHash)
			sm.chainParams = &chaincfg.RegressionNetParams
			sm.validationClient = &validator.MockValidator{}
			sm.utxoStore = &nullstore.NullStore{}
			sm.subtreeStore = memory.New()
			sm.subtreeValidation = subtreeValidationClient
			sm.blockValidation = blockValidationClient
			sm.logger = ulogger.TestLogger{}
			sm.ctx = context.Background()

			err := sm.handleBlockMsg(&blockQueueMsg{block: msgBlock, blockHash: blockHash, blockHeight: block.Height(), peer: p})
			require.Error(t, err)

			// The block was handed to block validation, not checked subtree by subtree here.
			require.Equal(t, 1, blockValidationClient.calls, "the block must reach block validation exactly once")
			subtreeValidationClient.AssertNotCalled(t, "CheckSubtreeFromBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)

			// The verdict's class survives the wrapping on the way back.
			require.True(t, errors.Is(err, tc.processErr), "the block validation error must come back as itself, got %v", err)

			_, failed := sm.recentlyFailedBlocks.Get(blockHash)
			require.True(t, failed, "a failed block is remembered either way")

			_, backoff := sm.blockFailureBackoff.Get(blockHash)
			require.Equal(t, tc.wantBackoff, backoff, "backoff is for local faults only")

			// What the peer server does with the same error (shouldDisconnectOnBlockErr
			// in services/legacy/peer_server.go): everything but a transient local
			// fault disconnects.
			require.Equal(t, !tc.wantBackoff, !errors.IsTransientLocalError(err), "disconnect follows the same classifier as the backoff")
		})
	}
}
