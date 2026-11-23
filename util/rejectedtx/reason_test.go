package rejectedtx

import (
	"io"
	"sort"
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

// Valid bounds its work by length before splitting: an oversized reason is
// refused outright, while the longest reason the grammar admits (MaxCodes of
// the longest code names plus the longest detail) still fits under the bound.
func TestValid_LengthBound(t *testing.T) {
	names := make([]string, 0, len(errors.ERR_value))
	for name, value := range errors.ERR_value {
		if errors.ERR(value) != errors.ERR_UNKNOWN {
			names = append(names, name)
		}
	}

	sort.Slice(names, func(i, j int) bool { return len(names[i]) > len(names[j]) })

	details := Details()
	sort.Slice(details, func(i, j int) bool { return len(details[i]) > len(details[j]) })

	longest := strings.Join(names[:MaxCodes], "/") + detailSeparator + details[0]
	require.True(t, Valid(longest), "the longest reason the grammar admits must pass: %q", longest)
	require.LessOrEqual(t, len(longest), maxReasonLen)

	// Just over the bound, but otherwise shaped like a valid reason: a code
	// list padded with slashes would cost one string header per slash.
	slashDense := "TX_INVALID" + strings.Repeat("/", maxReasonLen)
	require.False(t, Valid(slashDense))
	require.Equal(t, Fallback, Normalize(slashDense))

	allocs := testing.AllocsPerRun(10, func() { Valid(slashDense) })
	require.Zero(t, allocs, "an oversized reason must be refused before any allocation")
}

// A valid code list survives an unrecognized detail: a newer validator's
// allowlist may hold a literal this node does not, and its codes are still
// safe to send. Free text in the code list still collapses to Fallback.
func TestNormalize_KeepsValidCodeListWhenOnlyDetailIsUnknown(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"TX_INVALID/TX_POLICY: a literal from a newer release", "TX_INVALID/TX_POLICY"},
		{"TX_INVALID/PROCESSING: " + strings.Repeat("attacker ", 64), "TX_INVALID/PROCESSING"},
		{"TX_INVALID (30): GoBDK fail to ValidateTransaction", Fallback},
		{"TX_INVALID/TX_INVALID: bad-txns-in-belowout", Fallback},
		{"NOT_A_CODE: a literal from a newer release", Fallback},
	}

	for _, tc := range cases {
		got := Normalize(tc.in)
		require.Equal(t, tc.want, got, tc.in)
		require.True(t, Valid(got), "Normalize must only ever return a valid reason")
	}
}
