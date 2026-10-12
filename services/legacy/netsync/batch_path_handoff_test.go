package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// twoTxLegacyBlock is a regtest block of a coinbase and one ordinary
// transaction. Regtest has no checkpoints, so prepareSubtrees takes the
// full-validation path for it.
func twoTxLegacyBlock(t *testing.T) *bsvutil.Block {
	t.Helper()

	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{PreviousOutPoint: wire.OutPoint{Index: 0xffffffff}, SignatureScript: []byte{0x00}, Sequence: 0xffffffff})
	coinbase.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})

	regular := wire.NewMsgTx(1)
	regular.AddTxIn(&wire.TxIn{PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{0x01}, Index: 0}, SignatureScript: []byte{0x00}, Sequence: 0xffffffff})
	regular.AddTxOut(&wire.TxOut{Value: 1000, PkScript: []byte{0x76, 0xa9, 0x14}})

	msgBlock := &wire.MsgBlock{
		Header:       wire.BlockHeader{Version: 1, Timestamp: time.Now(), Bits: 0x1d00ffff},
		Transactions: []*wire.MsgTx{coinbase, regular},
	}

	setBodyMerkleRoot(msgBlock)

	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(100)

	return block
}

// While the node is catching up, legacy's full validation hands the block's
// subtrees to block validation, whose CheckBlockSubtrees validates the whole
// block through the batch path (SpendAndCreateMulti). In every other case,
// including a state it cannot read, legacy keeps checking each subtree itself.
func TestPrepareSubtrees_HandsCatchUpBlocksToTheBatchPath(t *testing.T) {
	initPrometheusMetrics()

	catching := blockchain.FSMStateCATCHINGBLOCKS
	running := blockchain.FSMStateRUNNING
	idle := blockchain.FSMStateIDLE

	cases := []struct {
		name         string
		state        *blockchain.FSMStateType
		stateErr     error
		noClient     bool
		checksItself bool
	}{
		{name: "catching up hands the block over", state: &catching, checksItself: false},
		{name: "running checks each subtree", state: &running, checksItself: true},
		{name: "idle (state unknown) checks each subtree", state: &idle, checksItself: true},
		{name: "unreadable state checks each subtree", stateErr: errors.NewServiceError("down"), checksItself: true},
		{name: "no blockchain client checks each subtree", noClient: true, checksItself: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			block := twoTxLegacyBlock(t)

			subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
			subtreeValidationClient.On("CheckSubtreeFromBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

			sm := &SyncManager{
				settings:          test.CreateBaseTestSettings(t),
				logger:            ulogger.TestLogger{},
				chainParams:       &chaincfg.RegressionNetParams,
				validationClient:  &validator.MockValidator{},
				utxoStore:         &nullstore.NullStore{},
				subtreeStore:      memory.New(),
				subtreeValidation: subtreeValidationClient,
				ctx:               context.Background(),
			}

			if !tc.noClient {
				blockchainClient := &blockchain.Mock{}
				blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(tc.state, tc.stateErr)
				sm.blockchainClient = blockchainClient
			}

			subtrees, _, _, err := sm.prepareSubtrees(context.Background(), block, headerProven, bodyCommitment(t, block))
			require.NoError(t, err)
			require.Len(t, subtrees, 1)

			if tc.checksItself {
				subtreeValidationClient.AssertCalled(t, "CheckSubtreeFromBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			} else {
				subtreeValidationClient.AssertNotCalled(t, "CheckSubtreeFromBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}
