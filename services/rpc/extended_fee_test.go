package rpc

import (
	"context"
	"encoding/hex"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// A caller's extended input value must not disable or spuriously trigger the
// user-protection fee ceiling. Use the real fill-only decorator so merely
// dropping the IsExtended gate, without clearing supplied fields, also fails.
func TestHandleSendRawTransaction_AbsurdFee_ExtendedInputs(t *testing.T) {
	for _, tc := range []struct {
		name            string
		claimed, output uint64
		allow, reject   bool
	}{
		{"zero_claim_cannot_bypass", 0, 50_000_000, false, true},
		{"plausible_claim_cannot_bypass", 50_001_000, 50_000_000, false, true},
		{"inflated_claim_cannot_reject", 200_000_000, 99_999_000, false, false},
		{"explicit_opt_out", 0, 50_000_000, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			settings := test.CreateBaseTestSettings(t)
			logger := ulogger.NewErrorTestLogger(t)
			storeURL, err := url.Parse("sqlitememory:///rpc_extended_" + tc.name)
			require.NoError(t, err)
			store, err := sql.New(ctx, logger, settings, storeURL)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
			parent := bt.NewTx()
			require.NoError(t, parent.From("a000000000000000000000000000000000000000000000000000000000000001", 0, "51", 100_000_000))
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 100_000_000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
			_, err = store.Create(ctx, parent, 0)
			require.NoError(t, err)
			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, "51", tc.claimed))
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", tc.output))
			child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
			child.SetExtended(true)
			encoded := child.ExtendedBytes()
			parsed, err := bt.NewTxFromBytes(encoded)
			require.NoError(t, err)
			require.True(t, parsed.IsExtended())
			require.Equal(t, tc.claimed, parsed.Inputs[0].PreviousTxSatoshis)
			var client validator.Interface = acceptingValidator{}
			if tc.reject {
				client = rejectingValidator{}
			}
			s := newRPCServerForAbsurdFeeTest(t, 10_000_000, 100_000_000, client)
			s.utxoStore = store
			_, err = handleSendRawTransaction(ctx, s, &bsvjson.SendRawTransactionCmd{
				HexTx: hex.EncodeToString(encoded), AllowHighFees: &tc.allow,
			}, nil)
			if !tc.reject {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			rpcErr, ok := err.(*bsvjson.RPCError)
			require.True(t, ok)
			require.Equal(t, bsvjson.ErrRPCVerify, rpcErr.Code)
			require.Contains(t, rpcErr.Message, "absurdly-high-fee 50000000 > 10000000")
		})
	}
}
