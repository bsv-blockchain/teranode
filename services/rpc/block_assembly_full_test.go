package rpc

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/require"
)

// newRPCServerForBlockAssemblyFullTest builds an RPC server wired to a blockchain client whose
// block-assembly-full flag the test controls directly.
//
// The validator is the panicking stub, so a transaction reaching validation fails the test loudly
// rather than silently proving nothing. MaxRawTxFee is 0 so the absurd-fee ceiling stays out of the
// way and any rejection can only have come from the ingress gate.
func newRPCServerForBlockAssemblyFullTest(t *testing.T, txStore blob.Store) (*RPCServer, *blockchain.Mock) {
	t.Helper()

	blockchainClient := &blockchain.Mock{}

	return &RPCServer{
		logger: mocklogger.NewTestLogger(),
		settings: &settings.Settings{
			ChainCfgParams: &chaincfg.MainNetParams,
			Policy:         &settings.PolicySettings{MaxRawTxFee: 0},
		},
		utxoStore:        &decoratingUtxoStore{inputSats: 100_000_000},
		validatorClient:  rejectingValidator{},
		blockchainClient: blockchainClient,
		txStore:          txStore,
	}, blockchainClient
}

// TestHandleSendRawTransactionRejectedWhenBlockAssemblyFull checks the RPC ingress gate.
//
// handleSendRawTransaction stores the transaction and calls the validator directly, so it reaches
// block assembly without passing the propagation or legacy netsync gates. Left ungated, a full node
// would keep growing past the configured limit through the RPC surface and would pay the storage
// cost the propagation gate exists to avoid.
func TestHandleSendRawTransactionRejectedWhenBlockAssemblyFull(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	txStore := memory.New()

	s, blockchainClient := newRPCServerForBlockAssemblyFullTest(t, txStore)
	blockchainClient.BlockAssemblyFull.Store(true)

	cmd := buildSendRawTxCmd(t, 99_999_000, nil)

	// rejectingValidator panics if reached, so simply not panicking proves the gate short-circuits
	// before validation.
	_, err := handleSendRawTransaction(ctx, s, cmd, nil)

	require.Error(t, err, "the RPC must refuse transactions while block assembly is full")

	rpcErr, ok := err.(*bsvjson.RPCError)
	require.True(t, ok, "the handler must return a JSON-RPC error, got %T", err)
	require.Contains(t, rpcErr.Message, "block assembly is full")

	// The refusal must cost no storage, which is the whole reason the check sits before txStore.Set.
	stored, err := txStoreHasCmdTx(ctx, txStore, cmd)
	require.NoError(t, err)
	require.False(t, stored, "a refused transaction must not be written to the transaction store")
}

// TestHandleSendRawTransactionAcceptedWhenBlockAssemblyNotFull checks the accept case, including the
// default a node holds before it has heard anything from block assembly.
func TestHandleSendRawTransactionAcceptedWhenBlockAssemblyNotFull(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	txStore := memory.New()

	s, blockchainClient := newRPCServerForBlockAssemblyFullTest(t, txStore)
	s.validatorClient = acceptingValidator{}

	require.False(t, blockchainClient.IsBlockAssemblyFull(),
		"a client that has heard nothing must default to accepting transactions")

	cmd := buildSendRawTxCmd(t, 99_999_000, nil)

	_, err := handleSendRawTransaction(ctx, s, cmd, nil)
	require.NoError(t, err, "the RPC must accept transactions while block assembly has room")

	stored, err := txStoreHasCmdTx(ctx, txStore, cmd)
	require.NoError(t, err)
	require.True(t, stored, "an accepted transaction must be written to the transaction store")

	// Block assembly then reports full, and the same submission is refused.
	blockchainClient.BlockAssemblyFull.Store(true)

	_, err = handleSendRawTransaction(ctx, s, cmd, nil)
	require.Error(t, err, "the RPC must refuse once block assembly reports full")
}

// txStoreHasCmdTx reports whether the transaction carried in cmd is present in the store.
func txStoreHasCmdTx(ctx context.Context, txStore blob.Store, cmd *bsvjson.SendRawTransactionCmd) (bool, error) {
	raw, err := hex.DecodeString(cmd.HexTx)
	if err != nil {
		return false, err
	}

	tx, err := bt.NewTxFromBytes(raw)
	if err != nil {
		return false, err
	}

	return txStore.Exists(ctx, tx.TxIDChainHash()[:], fileformat.FileTypeTx)
}
