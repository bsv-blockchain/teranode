package rpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// blockNotFoundChain reproduces the error shape reported in bitcoin-sv/teranode
// issue 4778: a BLOCK_NOT_FOUND raised by the store, wrapped by the blockchain
// client, wrapped again by the block validation service. The storage layer's
// own text sits at the bottom of it.
func blockNotFoundChain() error {
	return errors.NewServiceError("[RevalidateBlock][1000] failed to get block",
		errors.NewBlockNotFoundError("error in GetBlock",
			fmt.Errorf("sql: no rows in result set")))
}

func TestRPCError_NotFoundIsClassifiedThroughTheWrapChain(t *testing.T) {
	// The caller's fallback code says "rejected on its merits", which is what
	// the handler passed before this mapping existed. A not-found must not be
	// reported that way however deeply it is wrapped.
	rpcErr := rpcError(blockNotFoundChain(), bsvjson.ErrRPCVerify, "Block failed revalidation: ")

	require.Equal(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code,
		"an unknown block is -5 (RPC_INVALID_ADDRESS_OR_KEY), matching bitcoind, not -25 (RPC_VERIFY_ERROR)")
	require.Equal(t, "Block not found", rpcErr.Message,
		"the mapped message is returned verbatim so callers porting from bitcoind match on it")
}

func TestRPCError_DoesNotDiscloseTheStorageLayer(t *testing.T) {
	err := blockNotFoundChain()

	// Precondition: the raw error really does carry the internals. If this ever
	// stops being true the rest of this test proves nothing.
	require.Contains(t, err.Error(), "sql: no rows in result set")
	require.Contains(t, err.Error(), "RevalidateBlock")

	rpcErr := rpcError(err, bsvjson.ErrRPCVerify, "Block failed revalidation: ")

	require.NotContains(t, rpcErr.Message, "sql: no rows in result set", "storage layer must not cross the API boundary")
	require.NotContains(t, rpcErr.Message, "SERVICE_ERROR", "internal error codes must not cross the API boundary")
	require.NotContains(t, rpcErr.Message, "RevalidateBlock", "internal method names must not cross the API boundary")
}

func TestRPCError_TxNotFoundUsesBitcoindMessage(t *testing.T) {
	rpcErr := rpcError(errors.NewTxNotFoundError("no tx"), bsvjson.ErrRPCVerify, "TX rejected: ")

	require.Equal(t, bsvjson.ErrRPCInvalidAddressOrKey, rpcErr.Code)
	require.Equal(t, "No such mempool or blockchain transaction", rpcErr.Message)
}

func TestRPCError_FallbackKeepsTheReasonAndDropsTheChain(t *testing.T) {
	// A rejection the caller genuinely needs the reason for. The reason lives
	// in the topmost message; the wrapped remainder is internal.
	err := errors.NewTxInvalidError("script execution failed for input 0",
		fmt.Errorf("OP_RETURN encountered"))

	rpcErr := rpcError(err, bsvjson.ErrRPCVerify, "TX rejected: ")

	require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code, "a genuine rejection keeps the caller's code")
	require.Equal(t, "TX rejected: script execution failed for input 0", rpcErr.Message)
	require.NotContains(t, rpcErr.Message, "OP_RETURN encountered", "the wrapped cause stays in the log")
}

func TestRPCError_NonTeranodeErrorKeepsItsText(t *testing.T) {
	rpcErr := rpcError(fmt.Errorf("plain failure"), bsvjson.ErrRPCInternal.Code, "prefix: ")

	require.Equal(t, bsvjson.ErrRPCInternal.Code, rpcErr.Code)
	require.Equal(t, "prefix: plain failure", rpcErr.Message,
		"an error with no wrap chain has nothing to strip")
}

func TestPublicErrorMessage_FallsBackWhenTopLevelMessageIsEmpty(t *testing.T) {
	// A Teranode error carrying no message of its own must not produce an empty
	// RPC message; the rendered form is better than nothing.
	err := errors.New(errors.ERR_ERROR, "")

	require.NotEmpty(t, publicErrorMessage(err))
}

// TestHandleReconsiderBlock_UnknownBlockMatchesBitcoind is the end-to-end form
// of the contract bitcoin-sv's bsv-command-line-invalid-block.py asserts:
//
//	assert_raises_rpc_error(-5, 'Block not found', reconsiderblock, <unknown hash>)
//
// Before this mapping the handler answered -25 with four levels of internal
// error chain, so a caller could not distinguish "no such block" from "that
// block is bad" — and was told which storage engine had missed.
func TestHandleReconsiderBlock_UnknownBlockMatchesBitcoind(t *testing.T) {
	mockClient := &blockchain.Mock{}
	mockClient.On("GetLastNInvalidBlocks", mock.Anything, mock.Anything).Return([]*model.BlockInfo{}, nil)

	s := &RPCServer{
		logger:           ulogger.TestLogger{},
		blockchainClient: mockClient,
		blockValidationClient: &mockBlockValidationClient{
			revalidateBlockFunc: func(_ context.Context, _ chainhash.Hash) error {
				return blockNotFoundChain()
			},
		},
		settings: &settings.Settings{ChainCfgParams: &chaincfg.MainNetParams},
	}

	cmd := &bsvjson.ReconsiderBlockCmd{
		BlockHash: "0000000000000000000000000000000000000000000000000000000000000000",
	}

	result, err := handleReconsiderBlock(context.Background(), s, cmd, nil)
	require.Error(t, err)
	require.Nil(t, result)

	rpcErr, ok := err.(*bsvjson.RPCError)
	require.True(t, ok)
	require.Equal(t, bsvjson.RPCErrorCode(-5), rpcErr.Code)
	require.Equal(t, "Block not found", rpcErr.Message)
}
