package rejectedtx

import (
	"io"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// The reason is re-broadcast to every mesh peer, so it must be a bounded code
// list carrying none of the (attacker-shaped) error text, plus at most one
// fixed reject literal from the allowlist. The chain walk itself is covered in
// errors.CodeChain's tests; this pins the joining, the detail selection and
// the fallback.
func TestReason(t *testing.T) {
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
			want: Fallback,
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
			err:  errors.NewTxInvalidError(DetailGoBDKInvalidTx, errors.New(errors.ERR_TX_INVALID, "%s", inputShaped)),
			want: "TX_INVALID: " + DetailGoBDKInvalidTx,
		},
		{
			// The BDK policy shape: the deepest allowlisted literal is the
			// most specific verdict.
			name: "deepest fixed literal wins",
			err: errors.NewTxInvalidError(DetailGoBDKInvalidTx,
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
			got := Reason(tc.err)
			require.Equal(t, tc.want, got)
			require.NotContains(t, got, "attacker", "reason must not carry error text")
			require.True(t, Valid(got), "every produced reason must pass the chokepoint check")
		})
	}
}

// Every allowlisted detail must be a fixed literal: no format verbs, so what
// the validator sends is exactly one of these strings. (That each survives the
// p2p ingress sanitizer is pinned in services/p2p against the real sanitizer.)
func TestDetails_AreFixedLiterals(t *testing.T) {
	require.NotEmpty(t, Details())

	for _, detail := range Details() {
		require.NotContains(t, detail, "%", "detail %q must not be a format string", detail)
		require.NotContains(t, detail, detailSeparator, "detail %q would be ambiguous with the separator", detail)
		require.True(t, Valid("TX_INVALID"+detailSeparator+detail), "detail %q must be accepted by Valid", detail)
	}
}

// The chokepoint check accepts exactly what Reason produces and nothing that
// looks like error text.
func TestValid(t *testing.T) {
	accepted := []string{
		"TX_INVALID",
		"TX_INVALID/TX_POLICY",
		"TX_INVALID/PROCESSING/UTXO_FROZEN/UTXO_NON_FINAL",
		"TX_INVALID: bad-txns-in-belowout",
		"TX_INVALID/TX_POLICY: transaction fee is too low",
	}
	for _, r := range accepted {
		require.True(t, Valid(r), r)
		require.Equal(t, r, Normalize(r))
	}

	rejected := []string{
		"",
		"TX_INVALID (30): GoBDK fail to ValidateTransaction -> TX_INVALID (30): script failed", // pre-upgrade err.Error()
		"tx_invalid",
		"UNKNOWN",
		"TX_INVALID/UNKNOWN",
		"TX_INVALID/TX_INVALID",
		"TX_INVALID/PROCESSING/UTXO_FROZEN/UTXO_NON_FINAL/TX_INVALID_DOUBLE_SPEND",
		"TX_INVALID:bad-txns-in-belowout",
		"TX_INVALID: not on the list",
		"TX_INVALID: bad-txns-in-belowout: bad-txns-in-belowout",
		"TX_INVALID/",
		"/TX_INVALID",
		" TX_INVALID",
		"NOT_A_CODE",
	}
	for _, r := range rejected {
		require.False(t, Valid(r), r)
		require.Equal(t, Fallback, Normalize(r))
	}
}
