package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// allLuaErrorCodes mirrors the LuaErrorCode constants in teranode.go, so the
// tests below sweep every code createGeneralError can be handed rather than
// just the interesting ones. Go constants cannot be enumerated at runtime, so
// this list is not self-checking: adding a LuaErrorCode constant means adding
// it here too, or the new code goes unswept.
var allLuaErrorCodes = []LuaErrorCode{
	LuaErrorCodeTxNotFound, LuaErrorCodeConflicting, LuaErrorCodeLocked,
	LuaErrorCodeCreating, LuaErrorCodeFrozen, LuaErrorCodeAlreadyFrozen,
	LuaErrorCodeFrozenUntil, LuaErrorCodeCoinbaseImmature, LuaErrorCodeSpent,
	LuaErrorCodeInvalidSpend, LuaErrorCodeUtxosNotFound, LuaErrorCodeUtxoNotFound,
	LuaErrorCodeUtxoInvalidSize, LuaErrorCodeUtxoHashMismatch,
	LuaErrorCodeUtxoNotFrozen, LuaErrorCodeInvalidParameter,
}

// TestPublicVerdictMessagesOmitInternalBatchID pins the rule that a spend verdict
// whose code is on errors.publicCauseCodes must not let the client see batchID.
//
// createGeneralError is handed batchID, the Store's process-lifetime spend-batch
// counter. For codes on the allowlist, DeepestPublicCause surfaces the message
// verbatim to external HTTP and gRPC clients, so embedding the counter there hands
// out the node's spend-batch throughput and uptime to anyone who submits two
// transactions and diffs the value. That is exactly what shipped for CREATING,
// which teranode.lua returns as a top-level response and so is formatted here
// rather than by the per-record createSpendError.
//
// The check is that the client-visible message does not vary with batchID, rather
// than a substring search for the value, so it cannot pass by coincidence of hex
// digits in the transaction id. Codes that are not allowlisted are exempt: the
// boundary collapses their message anyway, so they keep the counter for
// diagnostics.
//
// createSpendError, the per-record formatter, is not covered here because it is
// never given batchID; it can only echo the transaction id, the output index and
// the fixed Lua message.
func TestPublicVerdictMessagesOmitInternalBatchID(t *testing.T) {
	txID, err := chainhash.NewHashFromStr("3f4e1c2b9a8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f")
	require.NoError(t, err)

	const (
		blockHeight = uint32(812345)
		luaMessage  = "TX is being created and cannot be spent yet"
	)

	s := &Store{}

	for _, code := range allLuaErrorCodes {
		t.Run(string(code), func(t *testing.T) {
			first := s.createGeneralError(code, txID, blockHeight, 1, luaMessage)
			second := s.createGeneralError(code, txID, blockHeight, 987654321, luaMessage)

			require.Error(t, first)
			require.Error(t, second)

			if errors.DeepestPublicCause(first) == nil {
				// Not allowlisted: the public boundary collapses this message to
				// the outermost generic code, so batchID never reaches a client.
				return
			}

			require.Equal(t, errors.UserMessage(first), errors.UserMessage(second),
				"%s is on the public-cause allowlist, so its client-visible message must not vary with the internal batch id", code)

			// Guard against passing vacuously on an empty or stripped message.
			require.Contains(t, errors.UserMessage(first), txID.String())
			require.Contains(t, errors.UserMessage(first), luaMessage)
		})
	}
}
