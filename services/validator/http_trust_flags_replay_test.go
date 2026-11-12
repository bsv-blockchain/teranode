package validator

import (
	"context"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestHandleSingleTx_TwoHashBodyCannotSpendViaTrustFlags drives the reported
// primitive end to end: a body that is exactly two 32-byte hashes, which is also a
// valid 64-byte transaction spending a funded outpoint with an empty unlocking
// script, POSTed at the real /tx handler with the attacker's query string.
//
// Scope: this pins the query-string deletion, not the chain-tip bound. The request
// is reduced to default options, so OutpointOnlySpend is false here and the tip
// guard is never reached — that is TestValidate_OutpointOnlySpend_TipBound's job.
// The settings below are deliberately the most permissive configuration possible
// (checkpoint far above the height, so every pre-existing guard AND the new tip
// bound are satisfied), which is what makes the rejection attributable to the
// query-string deletion alone rather than to one of the sibling guards.
func TestHandleSingleTx_TwoHashBodyCannotSpendViaTrustFlags(t *testing.T) {
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1_000_000}}

	utxoStoreURL, err := url.Parse("sqlitememory:///trustflag_replay")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(500))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	coinbaseScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	// Fund a parent with a single 500-satoshi P2PKH output.
	parentTx := bt.NewTx()
	coinbaseInput := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
	}
	require.NoError(t, coinbaseInput.PreviousTxIDAdd(new(chainhash.Hash)))
	parentTx.Inputs = append(parentTx.Inputs, coinbaseInput)
	parentTx.Outputs = append(parentTx.Outputs, &bt.Output{Satoshis: 500, LockingScript: coinbaseScript})

	_, err = store.Create(ctx, parentTx, 499, utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	// The attack transaction, sized to exactly 64 bytes:
	//   4 version + 1 input count + 41 input (32 txid + 4 vout + 1 zero script-len
	//   + 4 sequence) + 1 output count + 8 value + 1 script-len + 4 script
	//   + 4 locktime = 64.
	// The unlocking script is empty — that is the signatureless part — and the
	// four-byte locking script keeps the total on the boundary.
	attackTx := bt.NewTx()
	attackInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xffffffff,
	}
	require.NoError(t, attackInput.PreviousTxIDAdd(parentTx.TxIDChainHash()))
	attackTx.Inputs = append(attackTx.Inputs, attackInput)
	attackTx.Outputs = append(attackTx.Outputs, &bt.Output{
		Satoshis:      400,
		LockingScript: bscript.NewFromBytes([]byte{0x51, 0x61, 0x61, 0x61}),
	})
	attackTx.LockTime = 0

	raw := attackTx.SerializeBytes()
	require.Len(t, raw, 64, "the two-hash primitive requires exactly 64 bytes")

	// The two attacker-chosen 32-byte halves the body doubles as.
	_ = raw[:32]
	_ = raw[32:]

	// Drive the real validator, not the recorder: the claim under test is about
	// the UTXO store, not about option plumbing.
	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	// Direct assignment, not SetValidatorForTesting — see newTrustFlagServer for
	// why that helper leaves the field nil.
	srv := NewServer(ulogger.TestLogger{}, tSettings, store, nil, nil, nil, nil, nil, nil)
	srv.validator = v

	res := postToHandler(t, srv.HTTP().HandleSingleTx(ctx), "/tx?"+attackQueryString,
		"application/octet-stream", raw)

	require.GreaterOrEqual(t, res.Code, 400,
		"the signatureless spend must be rejected; got %d with body %s", res.Code, res.Body.String())

	// Positive proof the outpoint was never consumed: a competing spender of the
	// same outpoint still succeeds. Reading a flag could pass against a store that
	// never recorded anything; a successful competing spend cannot.
	competingTx := bt.NewTx()
	competingInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xfffffffe,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x01}),
	}
	require.NoError(t, competingInput.PreviousTxIDAdd(parentTx.TxIDChainHash()))
	competingTx.Inputs = append(competingTx.Inputs, competingInput)
	competingTx.Outputs = append(competingTx.Outputs, &bt.Output{Satoshis: 300, LockingScript: coinbaseScript})

	_, err = store.Spend(ctx, competingTx, 500, utxostore.IgnoreFlags{
		IgnoreLocked:      true,
		SkipUTXOHashCheck: true,
	})
	require.NoError(t, err,
		"the parent output must still be spendable — the 64-byte body must not have consumed it")
}
