package validator

import (
	"context"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestIgnoreConsensusFreeze_WireRoundTrip pins the client-build → wire →
// server-reconstruction path for ignore_consensus_freeze (field 16) on both transports.
//
// The flag lifts the alert system's consensus tier for the spends of a block a hardcoded
// checkpoint already proves canonical. A silent drop would make a remote validator
// enforce a freeze window against a checkpointed block and stall the sync (issue #1422),
// so true AND false must survive.
func TestIgnoreConsensusFreeze_WireRoundTrip(t *testing.T) {
	t.Run("true survives gRPC build, marshal, and reconstruction", func(t *testing.T) {
		opts := ProcessOptions(WithIgnoreConsensusFreeze(true))

		req := buildValidateTxRequest([]byte{1, 2, 3}, 500000, opts)
		require.NotNil(t, req.IgnoreConsensusFreeze)
		require.True(t, *req.IgnoreConsensusFreeze)

		bytesOut, err := proto.Marshal(req)
		require.NoError(t, err)

		got := &validator_api.ValidateTransactionRequest{}
		require.NoError(t, proto.Unmarshal(bytesOut, got))

		reconstructed, err := optionsFromValidateRequest(got)
		require.NoError(t, err)
		require.True(t, reconstructed.IgnoreConsensusFreeze, "IgnoreConsensusFreeze must survive gRPC round-trip")
	})

	t.Run("false survives gRPC round-trip and is the default", func(t *testing.T) {
		opts := ProcessOptions(WithIgnorePolicyFreeze(true)) // the sibling flag must not imply it

		req := buildValidateTxRequest([]byte{1, 2, 3}, 500000, opts)

		bytesOut, err := proto.Marshal(req)
		require.NoError(t, err)

		got := &validator_api.ValidateTransactionRequest{}
		require.NoError(t, proto.Unmarshal(bytesOut, got))

		reconstructed, err := optionsFromValidateRequest(got)
		require.NoError(t, err)
		require.True(t, reconstructed.IgnorePolicyFreeze)
		require.False(t, reconstructed.IgnoreConsensusFreeze, "IgnoreConsensusFreeze must default to false")
	})

	t.Run("absent field reconstructs as false (old sender, new validator)", func(t *testing.T) {
		reconstructed, err := optionsFromValidateRequest(&validator_api.ValidateTransactionRequest{
			TransactionData: []byte{1, 2, 3},
			BlockHeight:     500000,
		})
		require.NoError(t, err)
		require.False(t, reconstructed.IgnoreConsensusFreeze)
	})

	t.Run("true survives the HTTP fallback query string", func(t *testing.T) {
		q := buildValidateTxHTTPQuery(&Options{IgnoreConsensusFreeze: true}, 500000)

		e := echo.New()
		ctx, err := echoRequestWithQuery(e, q.Encode())
		require.NoError(t, err)

		_, opts := extractValidationParams(ctx)
		require.True(t, opts.IgnoreConsensusFreeze, "IgnoreConsensusFreeze must survive HTTP fallback round-trip")
	})

	t.Run("false is not emitted on the HTTP fallback query string", func(t *testing.T) {
		q := buildValidateTxHTTPQuery(&Options{}, 500000)
		require.Empty(t, q.Get("ignoreConsensusFreeze"))

		e := echo.New()
		ctx, err := echoRequestWithQuery(e, q.Encode())
		require.NoError(t, err)

		_, opts := extractValidationParams(ctx)
		require.False(t, opts.IgnoreConsensusFreeze)
	})
}

// newGuardTestValidator builds a validator over a fresh sqlitememory store, plus a
// minimal child transaction, for the option guards that fire before any store access.
func newGuardTestValidator(t *testing.T, storeName string, checkpointHeight int32) (*Validator, *bt.Tx) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}

	utxoStoreURL, err := url.Parse("sqlitememory:///" + storeName)
	require.NoError(t, err)
	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(500))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	childTx := bt.NewTx()
	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	childInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	require.NoError(t, childInput.PreviousTxIDAdd(new(chainhash.Hash)))
	childTx.Inputs = append(childTx.Inputs, childInput)
	childTx.Outputs = append(childTx.Outputs, &bt.Output{Satoshis: 400, LockingScript: coinbaseScript})

	return &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}, childTx
}

// TestValidate_IgnoreConsensusFreeze_RequiresSkipScriptValidation: the consensus bypass
// is only legitimate on the checkpoint path, which SkipScriptValidation marks; a request
// carrying it alone is a misconfigured caller and must be rejected.
func TestValidate_IgnoreConsensusFreeze_RequiresSkipScriptValidation(t *testing.T) {
	tracing.SetupMockTracer()

	v, childTx := newGuardTestValidator(t, "ignoreconsensus_noskip", 1000)

	_, err := v.ValidateWithOptions(context.Background(), childTx, 500, &Options{
		SkipUtxoCreation:      true,
		IgnoreConsensusFreeze: true,
	})
	require.Error(t, err, "IgnoreConsensusFreeze without SkipScriptValidation must return an error")
	require.Contains(t, err.Error(), "IgnoreConsensusFreeze requires SkipScriptValidation")
}

// TestValidate_IgnoreConsensusFreeze_RejectedAboveCheckpoint: even correctly paired, the
// bypass must not reach a block above the highest hardcoded checkpoint — that is the
// bound that makes the block canonical, and nothing else does.
func TestValidate_IgnoreConsensusFreeze_RejectedAboveCheckpoint(t *testing.T) {
	tracing.SetupMockTracer()

	v, childTx := newGuardTestValidator(t, "ignoreconsensus_abovecp", 100)

	_, err := v.ValidateWithOptions(context.Background(), childTx, 500, &Options{
		SkipUtxoCreation:      true,
		SkipScriptValidation:  true,
		IgnoreConsensusFreeze: true,
		IgnoreLocked:          true,
	})
	require.Error(t, err, "IgnoreConsensusFreeze above the highest checkpoint must be rejected")
	require.Contains(t, err.Error(), "IgnoreConsensusFreeze must not be used above the highest checkpoint")
}
