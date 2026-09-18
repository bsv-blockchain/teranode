// Package rejectedtx defines the Reason carried by KafkaRejectedTxTopicMessage,
// the validator's notice of an internally rejected transaction that the p2p
// service re-broadcasts to every mesh peer. The format is shared by its
// producer (services/validator) and the egress chokepoint (services/p2p) so
// that what reaches the network is bounded and input-free regardless of which
// side was upgraded first.
//
// Grammar:
//
//	reason := CODE ("/" CODE)* (": " DETAIL)?
//
// CODE is a teranode error code name (errors.ERR, printed without the ERR_
// prefix), outermost first along the rejection's error chain, deduplicated and
// at most MaxCodes long. DETAIL is one of the fixed literals in the closed
// allowlist below. Examples: "TX_INVALID", "TX_INVALID: bad-txns-in-belowout",
// "TX_INVALID/TX_POLICY: transaction fee is too low".
//
// This allowlist is stricter than errors.publicCauseCodes, which lets the same
// codes surface their message verbatim at the public error boundary. There the
// text goes back to the submitter, who supplied the input it describes; here it
// is fanned out to every peer at this node's expense, so only literals that are
// provably fixed may travel. Widening the list means asserting the new literal
// is a string constant carrying neither input nor node state.
package rejectedtx

import (
	"sort"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

// MaxCodes caps how many codes from an error chain make it into the reason.
// Chains deeper than this carry no extra information worth sending to every
// peer.
const MaxCodes = 4

// Fallback is the reason used when an error carries no teranode code, and what
// the chokepoint substitutes for a reason that does not fit the grammar: the
// condition that led here.
const Fallback = "TX_INVALID"

// detailSeparator separates the code list from the detail.
const detailSeparator = ": "

// The GoBDK script verifier's fixed outer messages. Defined here so the
// verifier and the allowlist cannot drift apart: the verifier builds its
// errors from these constants.
const (
	// DetailGoBDKInvalidTx is the outer literal over a script verification
	// failure; the BDK cause text beneath it is never carried.
	DetailGoBDKInvalidTx = "GoBDK fail to ValidateTransaction"
	// DetailGoBDKPolicy is the outer literal over a policy-level script
	// verification failure.
	DetailGoBDKPolicy = "GoBDK fail to ValidateTransaction by policy settings"
)

// details is the closed set of rejection messages that may travel with the
// code list. Every entry is a fixed literal in the validator's code (Bitcoin's
// standard bad-txns-* reject vocabulary plus the few fixed verdicts of our
// own), matched by exact equality against each link's message, so the network
// copy can only ever carry one of these strings byte for byte. Messages with
// formatted values (input indexes, heights, times) and the BDK cause text are
// deliberately absent: they are what the code-only form is for.
var details = map[string]struct{}{
	// Consensus verdicts (ERR_TX_INVALID) from the validator.
	"bad-txns-vout-negative":                  {},
	"bad-txns-vout-toolarge":                  {},
	"bad-txns-txouttotal-toolarge":            {},
	"bad-txns-inputvalues-outofrange":         {},
	"bad-txns-in-belowout":                    {},
	"bad-txns-unconfirmed-input-in-block":     {},
	"coinbase transactions are not supported": {},
	// Policy verdicts (ERR_TX_POLICY, wrapped in ERR_TX_INVALID by the BDK
	// mapper) from the validator and the script verifier.
	"bad-txns-inputs-too-large":  {},
	"transaction fee is too low": {},
	DetailGoBDKPolicy:            {},
	DetailGoBDKInvalidTx:         {},
}

// Details returns the allowlisted detail literals, sorted, for tests that pin
// properties of the whole set.
func Details() []string {
	out := make([]string, 0, len(details))
	for d := range details {
		out = append(out, d)
	}

	sort.Strings(out)

	return out
}

// Reason reduces a validation error to its network-safe reason: the codes
// along the chain (errors.CodeChain, at most MaxCodes) joined by "/", followed
// by the detail separator and the deepest link message that is on the
// allowlist, if any. The deepest match is the most specific verdict, in the
// spirit of errors.DeepestPublicCause, though the walk is bounded far tighter
// (errors.Walk): a literal past that bound is deliberately lost and the reason
// falls back to codes alone. An error carrying no teranode code at all reports
// Fallback.
func Reason(err error) string {
	codes := errors.CodeChain(err, MaxCodes)

	var detail string

	errors.Walk(err, func(te *errors.Error) bool {
		if _, ok := details[te.Message()]; ok {
			detail = te.Message()
		}

		return true
	})

	var b strings.Builder

	if len(codes) == 0 {
		b.WriteString(Fallback)
	}

	for i, code := range codes {
		if i > 0 {
			b.WriteByte('/')
		}

		b.WriteString(code.String())
	}

	if detail != "" {
		b.WriteString(detailSeparator)
		b.WriteString(detail)
	}

	return b.String()
}

// Valid reports whether reason fits the grammar exactly: one to MaxCodes
// distinct, known, non-UNKNOWN code names, and at most one allowlisted detail.
// Anything else, including a pre-upgrade validator's raw error text, is not a
// reason this package produced.
func Valid(reason string) bool {
	codeList, detail, hasDetail := strings.Cut(reason, detailSeparator)

	if hasDetail {
		if _, ok := details[detail]; !ok {
			return false
		}
	}

	codes := strings.Split(codeList, "/")
	if len(codes) == 0 || len(codes) > MaxCodes {
		return false
	}

	seen := make(map[string]struct{}, len(codes))

	for _, code := range codes {
		value, known := errors.ERR_value[code]
		if !known || errors.ERR(value) == errors.ERR_UNKNOWN {
			return false
		}

		if _, dup := seen[code]; dup {
			return false
		}

		seen[code] = struct{}{}
	}

	return true
}

// Normalize returns reason unchanged when it fits the grammar and Fallback
// otherwise. It is the chokepoint's guard: a reason built by an un-upgraded
// validator, or by anything else on the in-cluster topic, never leaves the
// node as free text.
func Normalize(reason string) string {
	if Valid(reason) {
		return reason
	}

	return Fallback
}
