package validator

import (
	"io"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// The rejected-tx reason is re-broadcast to every mesh peer, so it must be a
// bounded code list carrying none of the (attacker-shaped) error text, plus at
// most one fixed reject literal from the allowlist. The chain walk itself is
// covered in errors.CodeChain's tests; this pins the joining, the detail
// selection and the fallback.
func TestRejectedTxReason(t *testing.T) {
	inputShaped := "script failed: OP_RETURN <attacker controlled blob> " + strings.Repeat("x", 4096)

	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "single code drops the message text",
			err:  errors.NewTxInvalidError(inputShaped),
			want: "TX_INVALID",
		},
		{
			name: "chain joined outermost first",
			err:  errors.NewTxInvalidError("outer", errors.NewProcessingError("inner")),
			want: "TX_INVALID/PROCESSING",
		},
		{
			name: "no teranode code falls back to the rejection condition",
			err:  io.EOF,
			want: "TX_INVALID",
		},
		{
			name: "standard reject literal travels with the code",
			err:  errors.NewTxInvalidError("bad-txns-in-belowout"),
			want: "TX_INVALID: bad-txns-in-belowout",
		},
		{
			name: "formatted message is not a fixed literal",
			err:  errors.NewTxInvalidError("missing MTP value for input %d", 3),
			want: "TX_INVALID",
		},
		{
			// The shape mapBDKValidationError produces for a script failure:
			// a fixed outer literal over the raw BDK cause.
			name: "script failure keeps the outer literal and drops the BDK cause",
			err:  errors.NewTxInvalidError(errMsgInvalidTx, errors.New(errors.ERR_TX_INVALID, "%s", inputShaped)),
			want: "TX_INVALID: " + errMsgInvalidTx,
		},
		{
			// The BDK policy shape: the deepest allowlisted literal is the
			// most specific verdict.
			name: "deepest fixed literal wins",
			err: errors.NewTxInvalidError(errMsgInvalidTx,
				errors.NewTxPolicyError("transaction fee is too low", errors.New(errors.ERR_TX_POLICY, "%s", inputShaped))),
			want: "TX_INVALID/TX_POLICY: transaction fee is too low",
		},
		{
			name: "a literal that merely contains an allowlisted string does not match",
			err:  errors.NewTxInvalidError("bad-txns-in-belowout " + inputShaped),
			want: "TX_INVALID",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rejectedTxReason(tc.err)
			require.Equal(t, tc.want, got)
			require.NotContains(t, got, "attacker", "reason must not carry error text")
		})
	}
}

// Every allowlisted detail must be a fixed literal: no format verbs and no
// characters the p2p ingress sanitizer would strip, so what the validator
// sends is exactly what peers log.
func TestRejectedTxReasonDetails_AreFixedLiterals(t *testing.T) {
	for detail := range rejectedTxReasonDetails {
		require.NotContains(t, detail, "%", "detail %q must not be a format string", detail)

		for _, r := range detail {
			require.True(t, r >= 0x20 && r <= 0x7E, "detail %q must be printable ASCII", detail)
			require.NotContains(t, `<>&"'`+"`\\", string(r), "detail %q must survive the p2p sanitizer", detail)
		}
	}
}
