/*
Package validator implements BSV Blockchain transaction validation functionality.

This file holds the guard that keeps validation options off the validator's HTTP
transport. The HTTP /tx endpoint is unauthenticated (no auth middleware is installed
in startHTTPServer), so a flag set there is a caller assertion from an arbitrary
peer, not a trust basis. Consensus-affecting options — SkipScriptValidation,
OutpointOnlySpend, SkipPolicyChecks, InBlock, the candidate times, an asserted block
height — are only expressible over the validator's gRPC API. That listener is not
itself authenticated on the shipped profile (security_level_grpc defaults to 0 and
no auth interceptor is installed), so this guard removes the HTTP route rather than
removing the capability from unauthenticated callers.
*/
package validator

import (
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
)

// nonDefaultValidationOptions reports whether a ValidateTransactionRequest asks for
// anything other than plain mempool-submission semantics. It returns the name of the
// first offending field, or "" when the request carries transaction bytes only.
//
// It compares field by field against the defaults rather than against a zero or a
// prototype request, for two reasons: buildValidateTxRequest always emits non-nil
// pointers for every bool field (so presence proves nothing about intent), and
// Options contains a map, so the struct is not comparable with ==.
//
// MAINTENANCE CONTRACT: every field of ValidateTransactionRequest that can change
// validation behaviour must appear below. A new field on the proto message must be
// added here in the same change, or it becomes a silent hole in this guard — the
// same hole issue 4840 (finding B-022) exists to close. The only fields deliberately
// absent are TransactionData, which is the payload this transport exists to carry,
// and the protobuf-internal bookkeeping fields.
func nonDefaultValidationOptions(req *validator_api.ValidateTransactionRequest) string {
	if req == nil {
		return ""
	}

	// An asserted height is the attacker's lever in B-022: it satisfies the
	// below-checkpoint guard by construction. Zero makes the validator derive the
	// height from its own chain state.
	if req.BlockHeight != 0 {
		return "blockHeight"
	}

	if req.SkipUtxoCreation != nil && *req.SkipUtxoCreation {
		return "skipUtxoCreation"
	}

	// Default is true (NewDefaultOptions), so only a request turning it OFF is
	// non-default.
	if req.AddTxToBlockAssembly != nil && !*req.AddTxToBlockAssembly {
		return "addTxToBlockAssembly=false"
	}

	if req.SkipPolicyChecks != nil && *req.SkipPolicyChecks {
		return "skipPolicyChecks"
	}

	if req.CreateConflicting != nil && *req.CreateConflicting {
		return "createConflicting"
	}

	if req.SkipTxmetaPublishing != nil && *req.SkipTxmetaPublishing {
		return "skipTxmetaPublishing"
	}

	if req.InBlock != nil && *req.InBlock {
		return "inBlock"
	}

	if req.CandidateBlockTime != nil && *req.CandidateBlockTime != 0 {
		return "candidateBlockTime"
	}

	if req.CandidateParentMedianTime != nil && *req.CandidateParentMedianTime != 0 {
		return "candidateParentMedianTime"
	}

	if req.UnconfirmedParentsAtCandidateHeight != nil && *req.UnconfirmedParentsAtCandidateHeight {
		return "unconfirmedParentsAtCandidateHeight"
	}

	if req.SkipScriptValidation != nil && *req.SkipScriptValidation {
		return "skipScriptValidation"
	}

	if req.OutpointOnlySpend != nil && *req.OutpointOnlySpend {
		return "outpointOnlySpend"
	}

	return ""
}
