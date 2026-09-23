package validator

import (
	"context"
	"fmt"
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
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestIgnoreConsensusFreeze_WireRoundTrip pins the client-build → wire →
// server-reconstruction path for ignore_consensus_freeze (field 16) over gRPC.
//
// The flag lifts the alert system's consensus tier for the spends of a block a hardcoded
// checkpoint already proves canonical. A silent drop would make a remote validator
// enforce a freeze window against a checkpointed block and stall the sync (issue #1422),
// so true AND false must survive.
//
// gRPC is the only transport: the /tx HTTP endpoint carries transaction bytes only
// and refuses a body that sets this field (http_trust_flags_test.go), so there is no
// HTTP round trip to pin.
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
}

// newGuardTestValidator builds a validator over a fresh sqlitememory store whose tip is
// tipHeight, plus a minimal child transaction, for the option guards that fire before
// any store access.
func newGuardTestValidator(t *testing.T, storeName string, checkpointHeight int32, tipHeight uint32) (*Validator, *bt.Tx) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}

	utxoStoreURL, err := url.Parse("sqlitememory:///" + storeName)
	require.NoError(t, err)
	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(tipHeight))
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

// TestValidate_FreezeBypass_RequiresInBlock: both freeze bypasses describe a spend made
// while validating a block, and every producer sets InBlock alongside them. A request
// carrying either without it would admit a coin this node has frozen into block
// assembly, so it is rejected before any store access — whatever else it carries.
func TestValidate_FreezeBypass_RequiresInBlock(t *testing.T) {
	tracing.SetupMockTracer()

	cases := []struct {
		name string
		opts *Options
	}{
		{"IgnorePolicyFreeze alone", &Options{
			SkipUtxoCreation:   true,
			IgnorePolicyFreeze: true,
		}},
		{"IgnoreConsensusFreeze correctly paired but not InBlock", &Options{
			SkipUtxoCreation:      true,
			SkipScriptValidation:  true,
			IgnoreConsensusFreeze: true,
			IgnoreLocked:          true,
		}},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, childTx := newGuardTestValidator(t, fmt.Sprintf("freezebypass_noinblock_%d", i), 1000, 500)

			_, err := v.ValidateWithOptions(context.Background(), childTx, 500, tc.opts)
			require.Error(t, err, "a freeze bypass without InBlock must return an error")
			require.Contains(t, err.Error(), "IgnorePolicyFreeze and IgnoreConsensusFreeze require InBlock")
		})
	}

	t.Run("IgnorePolicyFreeze with InBlock passes the guard", func(t *testing.T) {
		v, childTx := newGuardTestValidator(t, "freezebypass_inblock", 1000, 500)

		_, err := v.ValidateWithOptions(context.Background(), childTx, 500, &Options{
			SkipUtxoCreation:   true,
			InBlock:            true,
			IgnorePolicyFreeze: true,
		})
		if err != nil {
			// The child spends a parent the store does not hold, so validation may fail
			// later — but not on this guard.
			require.NotContains(t, err.Error(), "require InBlock")
		}
	})
}

// TestValidate_IgnoreConsensusFreeze_RequiresSkipScriptValidation: the consensus bypass
// is only legitimate on the checkpoint path, which SkipScriptValidation marks; a request
// carrying it alone is a misconfigured caller and must be rejected.
func TestValidate_IgnoreConsensusFreeze_RequiresSkipScriptValidation(t *testing.T) {
	tracing.SetupMockTracer()

	v, childTx := newGuardTestValidator(t, "ignoreconsensus_noskip", 1000, 500)

	_, err := v.ValidateWithOptions(context.Background(), childTx, 500, &Options{
		SkipUtxoCreation:      true,
		InBlock:               true,
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

	v, childTx := newGuardTestValidator(t, "ignoreconsensus_abovecp", 100, 500)

	_, err := v.ValidateWithOptions(context.Background(), childTx, 500, &Options{
		SkipUtxoCreation:      true,
		SkipScriptValidation:  true,
		InBlock:               true,
		IgnoreConsensusFreeze: true,
		IgnoreLocked:          true,
	})
	require.Error(t, err, "IgnoreConsensusFreeze above the highest checkpoint must be rejected")
	require.Contains(t, err.Error(), "IgnoreConsensusFreeze must not be used above the highest checkpoint")
}

// TestValidate_IgnoreConsensusFreeze_TipBound mirrors OutpointOnlySpend's tip-derived
// bound (issue 4840, finding B-022): the caller-asserted height is the attacker's lever,
// so the node's own tip must also be at or below the checkpoint. Legacy block sync, the
// only producer, validates block H while the tip is still below H, so a tip at the
// checkpoint (validating the checkpoint block itself) still qualifies.
func TestValidate_IgnoreConsensusFreeze_TipBound(t *testing.T) {
	tracing.SetupMockTracer()

	const checkpointHeight = 1000

	cases := []struct {
		name      string
		tipHeight uint32
		rejected  bool
	}{
		{name: "tip below checkpoint", tipHeight: checkpointHeight - 1},
		{name: "tip at checkpoint", tipHeight: checkpointHeight},
		{name: "tip past checkpoint", tipHeight: checkpointHeight + 1, rejected: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, childTx := newGuardTestValidator(t, fmt.Sprintf("ignoreconsensus_tipbound_%d", tc.tipHeight), checkpointHeight, tc.tipHeight)

			_, err := v.ValidateWithOptions(context.Background(), childTx, 500, &Options{
				SkipUtxoCreation:      true,
				SkipScriptValidation:  true,
				InBlock:               true,
				IgnoreConsensusFreeze: true,
				IgnoreLocked:          true,
			})

			if !tc.rejected {
				if err != nil {
					// Past the guards the child's parent is missing from the store; only the
					// guard's verdict is under test here.
					require.NotContains(t, err.Error(), "chain tip is past the highest checkpoint")
				}

				return
			}

			require.Error(t, err, "a node whose own tip is past the checkpoint must reject the bypass")
			require.Contains(t, err.Error(), "IgnoreConsensusFreeze must not be used once the node's chain tip is past the highest checkpoint")
			require.Contains(t, err.Error(), fmt.Sprintf("%d", tc.tipHeight), "the error must name the tip height the operator can act on")
		})
	}
}
