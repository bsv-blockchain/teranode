package aerospike

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNativeOpResultBinMatchesLuaSuccess(t *testing.T) {
	if nativeOpResultBin != LuaSuccess.String() {
		t.Fatalf("native result bin %q must match Lua success bin %q",
			nativeOpResultBin, LuaSuccess.String())
	}
}

func TestEncodeNativeOpPayloadUsesSubOpAndArgsArray(t *testing.T) {
	payload, err := encodeNativeOpPayload(subOpSetLocked, []any{true})
	if err != nil {
		t.Fatalf("encode native op payload: %v", err)
	}

	var decoded []any
	if err := msgpack.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode native op payload: %v", err)
	}

	if len(decoded) != 2 {
		t.Fatalf("payload length = %d, want 2", len(decoded))
	}
	if decoded[0] != subOpSetLocked {
		t.Fatalf("sub op = %v (%T), want %d", decoded[0], decoded[0], subOpSetLocked)
	}

	args, ok := decoded[1].([]any)
	if !ok {
		t.Fatalf("args = %T, want []any", decoded[1])
	}
	if len(args) != 1 || args[0] != true {
		t.Fatalf("args = %#v, want []any{true}", args)
	}
}

// TestHasResultCode covers the bare-error fast path and the chain walk it falls
// back to.
func TestHasResultCode(t *testing.T) {
	require.True(t, hasResultCode(aeroErr(types.KEY_NOT_FOUND_ERROR), types.KEY_NOT_FOUND_ERROR))
	require.False(t, hasResultCode(aeroErr(types.KEY_NOT_FOUND_ERROR), types.PARAMETER_ERROR))

	wrapped := fmt.Errorf("op failed: %w", aeroErr(types.PARAMETER_ERROR)) //nolint:forbidigo // std %w chain needed; errors.New* erases the aerospike type
	require.True(t, hasResultCode(wrapped, types.PARAMETER_ERROR))
	require.False(t, hasResultCode(wrapped, types.KEY_NOT_FOUND_ERROR))

	require.False(t, hasResultCode(nil, types.KEY_NOT_FOUND_ERROR))

	var typedNil *aerospike.AerospikeError
	require.False(t, hasResultCode(typedNil, types.KEY_NOT_FOUND_ERROR))
}
